//! zen-free-api — an OpenAI-compatible localhost proxy for opencode.ai/zen/v1
//!
//! Receives OpenAI-format requests on localhost, injects opencode wire headers,
//! forwards to the real upstream, and streams the response back.
//!
//! Endpoints:
//!   GET  /                        health info
//!   GET  /v1/models               list upstream models
//!   POST /v1/chat/completions     chat (streaming + non-streaming)
//!   GET  /v1/usage                per-model usage stats
//!   GET  /v1/conversation/{model} view saved conversation
//!   POST /v1/reset/{model}        reset conversation state
//!
//! Environment:
//!   OPENCODE_ZEN_BASE   upstream base (default https://opencode.ai/zen/v1)
//!   OPENCODE_API_KEY    api key (default "public")
//!
//!   NOTE: When OPENCODE_API_KEY is unset it defaults to "public"
//!   (anonymous access). The "public" key ONLY permits free-tier models,
//!   i.e. model ids ending in "-free" (e.g. deepseek-v4-flash-free,
//!   mimo-v2.5-free, nemotron-3.5-lightning-free). Paid models
//!   (e.g. claude-opus-5, gpt-5.5) are rejected with an auth error unless
//!   a real API key is set — get one at https://opencode.ai/zen/
//!   OPENCODE_CLIENT     x-opencode-client value (default "cli")
//!   OPENCODE_USER_AGENT User-Agent (default "opencode/0.0.0")
//!   OPENCODE_SESSION_ID stable session id
//!   OPENCODE_PROJECT_ID project id
//!   ZEN_STATE_DIR       state dir (default ~/.cache/opencode-zen)
//!   ZEN_LISTEN          bind address (default 127.0.0.1:8080)

use async_stream::stream;
use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use bytes::Bytes;
use futures::StreamExt;
use serde_json::{json, Value};
use std::{collections::HashMap, env, fs, path::PathBuf, sync::Arc};
use tokio::sync::RwLock;
use tower_http::cors::CorsLayer;
use uuid::Uuid;

// ═══════════════════════════════════════════════════════════════════════════
// Configuration
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Clone, Debug)]
struct Config {
    base_url: String,
    api_key: String,
    client_name: String,
    user_agent: String,
    session_id: String,
    project_id: String,
    state_dir: PathBuf,
    listen_addr: String,
}

impl Config {
    fn from_env() -> Self {
        let pwd = env::current_dir()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|_| "/".to_string());
        let hash = md5::compute(pwd.as_bytes());
        let project_id = format!("{:x}", hash)[..16].to_string();

        let state_dir = env::var("ZEN_STATE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                let home = env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
                PathBuf::from(home).join(".cache").join("opencode-zen")
            });
        fs::create_dir_all(&state_dir).ok();

        Self {
            base_url: env::var("OPENCODE_ZEN_BASE")
                .unwrap_or_else(|_| "https://opencode.ai/zen/v1".to_string()),
            // When OPENCODE_API_KEY is undefined it defaults to "public"
            // (anonymous). IMPORTANT: the "public" key only works with
            // free-tier models — ids ending in "-free" (e.g.
            // deepseek-v4-flash-free, mimo-v2.5-free). Paid models
            // (e.g. claude-opus-5, gpt-5.5) fail with an auth error unless
            // a real key from https://opencode.ai/zen/ is provided.
            api_key: env::var("OPENCODE_API_KEY").unwrap_or_else(|_| "public".to_string()),
            client_name: env::var("OPENCODE_CLIENT").unwrap_or_else(|_| "cli".to_string()),
            user_agent: env::var("OPENCODE_USER_AGENT")
                .unwrap_or_else(|_| "opencode/0.0.0".to_string()),
            session_id: env::var("OPENCODE_SESSION_ID")
                .unwrap_or_else(|_| format!("sess_{}", hex_id(24))),
            project_id: env::var("OPENCODE_PROJECT_ID")
                .unwrap_or_else(|_| format!("prj_{}", project_id)),
            state_dir,
            listen_addr: env::var("ZEN_LISTEN")
                .unwrap_or_else(|_| "127.0.0.1:8080".to_string()),
        }
    }
}

/// Generate a random hex string of the given length.
fn hex_id(num_chars: usize) -> String {
    let mut result = String::new();
    while result.len() < num_chars {
        result.push_str(&Uuid::new_v4().simple().to_string());
    }
    result[..num_chars].to_string()
}

/// Sanitize a model name for use as a filename.
fn sanitize_filename(s: &str) -> String {
    s.replace('/', "_")
        .replace('\\', "_")
        .replace(':', "_")
        .replace(' ', "_")
}

// ═══════════════════════════════════════════════════════════════════════════
// Application State
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Clone)]
struct AppState {
    client: reqwest::Client,
    config: Config,
    /// In-memory conversation store: model -> messages
    conversations: Arc<RwLock<HashMap<String, Vec<Value>>>>,
    /// In-memory usage store: model -> usage string
    usage: Arc<RwLock<HashMap<String, String>>>,
}

impl AppState {
    fn new(config: Config) -> Self {
        let client = reqwest::Client::builder()
            .build()
            .expect("failed to build HTTP client");
        Self {
            client,
            config,
            conversations: Arc::new(RwLock::new(HashMap::new())),
            usage: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Build the upstream headers, mirroring opencode's wire format.
    /// Client can override API key via Authorization header and
    /// session/project via x-opencode-* headers.
    fn build_upstream_headers(&self, client_headers: &HeaderMap, is_stream: bool) -> HeaderMap {
        let mut headers = HeaderMap::new();

        // ── API Key ────────────────────────────────────────────────────────
        // Allow client to override with Authorization: Bearer <key> or x-api-key
        let api_key = client_headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.strip_prefix("Bearer "))
            .map(|s| s.to_string())
            .or_else(|| {
                client_headers
                    .get("x-api-key")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string())
            })
            .filter(|s| !s.is_empty() && s != "public")
            .unwrap_or_else(|| self.config.api_key.clone());

        headers.insert(
            "authorization",
            HeaderValue::from_str(&format!("Bearer {}", api_key)).unwrap(),
        );
        headers.insert("content-type", HeaderValue::from_static("application/json"));
        headers.insert(
            "accept",
            HeaderValue::from_str(if is_stream {
                "text/event-stream"
            } else {
                "application/json"
            })
            .unwrap(),
        );

        // ── Session ID ────────────────────────────────────────────────────
        let session = client_headers
            .get("x-opencode-session")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
            .unwrap_or_else(|| self.config.session_id.clone());
        headers.insert(
            "x-opencode-session",
            HeaderValue::from_str(&session).unwrap(),
        );

        // ── Request ID (unique per request) ────────────────────────────────
        headers.insert(
            "x-opencode-request",
            HeaderValue::from_str(&format!("req_{}", hex_id(24))).unwrap(),
        );

        // ── Project ID ────────────────────────────────────────────────────
        let project = client_headers
            .get("x-opencode-project")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
            .unwrap_or_else(|| self.config.project_id.clone());
        headers.insert(
            "x-opencode-project",
            HeaderValue::from_str(&project).unwrap(),
        );

        // ── Client & User-Agent ───────────────────────────────────────────
        headers.insert(
            "x-opencode-client",
            HeaderValue::from_str(&self.config.client_name).unwrap(),
        );
        headers.insert(
            "user-agent",
            HeaderValue::from_str(&self.config.user_agent).unwrap(),
        );

        headers
    }

    fn state_file(&self, model: &str) -> PathBuf {
        self.config
            .state_dir
            .join(format!("{}.json", sanitize_filename(model)))
    }

    async fn load_messages(&self, model: &str) -> Vec<Value> {
        // Try in-memory first
        {
            let convs = self.conversations.read().await;
            if let Some(msgs) = convs.get(model) {
                return msgs.clone();
            }
        }
        // Try disk
        let path = self.state_file(model);
        if path.exists() {
            if let Ok(data) = fs::read_to_string(&path) {
                if let Ok(arr) = serde_json::from_str::<Vec<Value>>(&data) {
                    return arr;
                }
            }
        }
        Vec::new()
    }

    async fn save_messages(&self, model: &str, messages: Vec<Value>) {
        // Save to memory
        self.conversations
            .write()
            .await
            .insert(model.to_string(), messages.clone());
        // Save to disk
        let path = self.state_file(model);
        if let Ok(json) = serde_json::to_string_pretty(&messages) {
            let _ = fs::write(&path, json);
        }
    }

    async fn append_assistant(&self, model: &str, content: String) {
        let mut messages = self.load_messages(model).await;
        messages.push(json!({"role": "assistant", "content": content}));
        self.save_messages(model, messages).await;
    }

    async fn set_usage(&self, model: &str, usage: String) {
        self.usage.write().await.insert(model.to_string(), usage);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Route Handlers
// ═══════════════════════════════════════════════════════════════════════════

/// GET / — health check
async fn health() -> impl IntoResponse {
    Json(json!({
        "status": "ok",
        "service": "zen-free-api",
        "description": "OpenAI-compatible proxy for opencode.ai/zen/v1",
        "endpoints": {
            "models": "GET /v1/models",
            "chat": "POST /v1/chat/completions",
            "usage": "GET /v1/usage",
            "conversation": "GET /v1/conversation/{model}",
            "reset": "POST /v1/reset/{model}",
        }
    }))
}

/// GET /v1/models — forward to upstream /models
async fn list_models(
    State(app): State<Arc<AppState>>,
    client_headers: HeaderMap,
) -> Response {
    let url = format!("{}/models", app.config.base_url);
    let headers = app.build_upstream_headers(&client_headers, false);

    match app.client.get(&url).headers(headers).send().await {
        Ok(resp) => {
            let status = resp.status();
            // Forward content-type
            let ct = resp
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("application/json")
                .to_string();
            let bytes = resp.bytes().await.unwrap_or_default();
            Response::builder()
                .status(status)
                .header("content-type", ct)
                .body(Body::from(bytes))
                .unwrap()
        }
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            format!("upstream connection failed: {}", e),
        )
            .into_response(),
    }
}

/// POST /v1/chat/completions — the main proxy handler
///
/// Forwards the ENTIRE client request body (all fields: model, messages,
/// temperature, top_p, stream, stream_options, tools, tool_choice, etc.)
/// to the upstream with the proper opencode wire headers.
async fn chat_completions(
    State(app): State<Arc<AppState>>,
    client_headers: HeaderMap,
    body: Bytes,
) -> Response {
    // ── Parse body to extract metadata ────────────────────────────────────
    if body.is_empty() {
        return (StatusCode::BAD_REQUEST, "empty request body")
            .into_response();
    }

    let body_json: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, format!("invalid JSON: {}", e))
                .into_response()
        }
    };

    let model = body_json
        .get("model")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();

    let is_stream = body_json
        .get("stream")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    tracing::info!(
        "chat_completions: model={}, stream={}, msg_count={}",
        model,
        is_stream,
        body_json
            .get("messages")
            .and_then(|v| v.as_array())
            .map(|a| a.len())
            .unwrap_or(0)
    );

    // ── Save incoming messages to state ───────────────────────────────────
    if let Some(messages) = body_json.get("messages").and_then(|v| v.as_array()) {
        app.save_messages(&model, messages.clone()).await;
    }

    // ── Forward to upstream ───────────────────────────────────────────────
    let url = format!("{}/chat/completions", app.config.base_url);
    let upstream_headers = app.build_upstream_headers(&client_headers, is_stream);

    let resp = match app
        .client
        .post(&url)
        .headers(upstream_headers)
        .body(body) // Forward the EXACT raw bytes from the client
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("upstream request failed: {}", e);
            return (
                StatusCode::BAD_GATEWAY,
                format!("upstream request failed: {}", e),
            )
                .into_response();
        }
    };

    let status = resp.status();

    // ── Forward error responses ───────────────────────────────────────────
    if !status.is_success() {
        let ct = resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/json")
            .to_string();
        let bytes = resp.bytes().await.unwrap_or_default();
        tracing::warn!("upstream error: {} ({} bytes)", status, bytes.len());
        return Response::builder()
            .status(status)
            .header("content-type", ct)
            .body(Body::from(bytes))
            .unwrap();
    }

    // ── Check if upstream is actually streaming ───────────────────────────
    let content_type = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let upstream_is_sse = content_type.contains("text/event-stream");

    if is_stream && upstream_is_sse {
        stream_response(app, model, resp).await
    } else {
        non_stream_response(app, model, resp).await
    }
}

/// Handle non-streaming response: parse JSON, track state, forward to client.
async fn non_stream_response(
    app: Arc<AppState>,
    model: String,
    resp: reqwest::Response,
) -> Response {
    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json")
        .to_string();

    let bytes = match resp.bytes().await {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                format!("failed to read upstream body: {}", e),
            )
                .into_response()
        }
    };

    // Try to parse as JSON for state tracking
    if let Ok(json_resp) = serde_json::from_slice::<Value>(&bytes) {
        // Extract and save assistant message
        if let Some(content) = json_resp
            .get("choices")
            .and_then(|v| v.as_array())
            .and_then(|v| v.first())
            .and_then(|v| v.get("message"))
            .and_then(|v| v.get("content"))
            .and_then(|v| v.as_str())
        {
            app.append_assistant(&model, content.to_string()).await;
        }

        // Track usage
        if let Some(usage) = json_resp.get("usage") {
            let pt = usage
                .get("prompt_tokens")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let ct = usage
                .get("completion_tokens")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let cached = usage
                .get("prompt_tokens_details")
                .and_then(|v| v.get("cached_tokens"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            app.set_usage(
                &model,
                format!("usage: in={} out={} cached={}", pt, ct, cached),
            )
            .await;
        }

        // Track cost
        if let Some(cost) = json_resp.get("cost") {
            app.set_usage(&model, format!("cost: ${}", cost)).await;
        }
    }

    // Forward the raw response bytes with original content-type
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", ct)
        .body(Body::from(bytes))
        .unwrap()
}

/// Handle streaming response: pipe SSE events, track content for state.
async fn stream_response(
    app: Arc<AppState>,
    model: String,
    resp: reqwest::Response,
) -> Response {
    let upstream = resp.bytes_stream();
    let model_for_stream = model.clone();
    let app_for_stream = Arc::clone(&app);

    let s = stream! {
        let mut full_reply = String::new();
        let mut last_usage = String::new();
        let mut buffer = String::new();
        let mut upstream = Box::pin(upstream);

        while let Some(chunk_result) = upstream.next().await {
            match chunk_result {
                Ok(bytes) => {
                    buffer.push_str(&String::from_utf8_lossy(&bytes));

                    // Process complete SSE events (separated by \n\n)
                    while let Some(pos) = buffer.find("\n\n") {
                        let event = buffer[..pos].to_string();
                        buffer = buffer[pos + 2..].to_string();

                        // Parse data lines for state tracking
                        for line in event.lines() {
                            if let Some(data) = line.strip_prefix("data: ") {
                                if data == "[DONE]" {
                                    continue;
                                }

                                if let Ok(chunk) = serde_json::from_str::<Value>(data) {
                                    // Check for API error in stream
                                    if let Some(err) = chunk.get("error") {
                                        tracing::warn!(
                                            "upstream stream error: {}",
                                            err
                                        );
                                        continue;
                                    }

                                    // Track content delta
                                    if let Some(choices) =
                                        chunk.get("choices").and_then(|v| v.as_array())
                                    {
                                        if let Some(first) = choices.first() {
                                            if let Some(delta) = first.get("delta") {
                                                if let Some(content) =
                                                    delta.get("content").and_then(|v| v.as_str())
                                                {
                                                    full_reply.push_str(content);
                                                }
                                            }
                                        }
                                    }

                                    // Track usage
                                    if let Some(usage) = chunk.get("usage") {
                                        let pt = usage
                                            .get("prompt_tokens")
                                            .and_then(|v| v.as_u64())
                                            .unwrap_or(0);
                                        let ct = usage
                                            .get("completion_tokens")
                                            .and_then(|v| v.as_u64())
                                            .unwrap_or(0);
                                        let cached = usage
                                            .get("prompt_tokens_details")
                                            .and_then(|v| v.get("cached_tokens"))
                                            .and_then(|v| v.as_u64())
                                            .unwrap_or(0);
                                        last_usage = format!(
                                            "usage: in={} out={} cached={}",
                                            pt, ct, cached
                                        );
                                    }

                                    // Track cost
                                    if let Some(cost) = chunk.get("cost") {
                                        last_usage = format!("cost: ${}", cost);
                                    }
                                }
                            }
                        }

                        // Forward the raw SSE event to the client
                        yield Ok::<Bytes, std::io::Error>(Bytes::from(
                            format!("{}\n\n", event),
                        ));
                    }
                }
                Err(e) => {
                    tracing::error!("stream read error: {}", e);
                    yield Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    ));
                    break;
                }
            }
        }

        // ── Save state after stream completes ──────────────────────────────
        if !full_reply.is_empty() {
            app_for_stream
                .append_assistant(&model_for_stream, full_reply)
                .await;
        }
        if !last_usage.is_empty() {
            app_for_stream
                .set_usage(&model_for_stream, last_usage)
                .await;
        }
    };

    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/event-stream")
        .header("cache-control", "no-cache")
        .header("connection", "keep-alive")
        .body(Body::from_stream(s))
        .unwrap()
}

/// GET /v1/usage — per-model usage stats
async fn get_usage(State(app): State<Arc<AppState>>) -> Response {
    let usage = app.usage.read().await;
    let convs = app.conversations.read().await;

    let models: Vec<Value> = convs
        .keys()
        .map(|model| {
            json!({
                "model": model,
                "message_count": convs.get(model).map(|v| v.len()).unwrap_or(0),
                "last_usage": usage.get(model).cloned().unwrap_or_default(),
            })
        })
        .collect();

    Json(json!({"models": models})).into_response()
}

/// GET /v1/conversation/{model} — view saved conversation
async fn get_conversation(
    State(app): State<Arc<AppState>>,
    Path(model): Path<String>,
) -> Response {
    let messages = app.load_messages(&model).await;
    Json(json!({"model": model, "message_count": messages.len(), "messages": messages}))
        .into_response()
}

/// POST /v1/reset/{model} — reset conversation state
async fn reset_conversation(
    State(app): State<Arc<AppState>>,
    Path(model): Path<String>,
) -> Response {
    app.save_messages(&model, Vec::new()).await;
    Json(json!({"status": "reset", "model": model})).into_response()
}

// ═══════════════════════════════════════════════════════════════════════════
// Main
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().init();

    let config = Config::from_env();
    let listen_addr = config.listen_addr.clone();
    let session_id = config.session_id.clone();
    let project_id = config.project_id.clone();
    let base_url = config.base_url.clone();
    let api_key_display = if config.api_key == "public" {
        // "public" = anonymous; only "-free" models are accessible
        "public (anonymous, free models only)".to_string()
    } else {
        format!("{}...{}", &config.api_key[..4], &config.api_key[config.api_key.len().saturating_sub(4)..])
    };

    let app = Arc::new(AppState::new(config));

    let router = Router::new()
        // Health
        .route("/", get(health))
        .route("/health", get(health))
        // OpenAI-compatible endpoints (with /v1 prefix)
        .route("/v1/models", get(list_models))
        .route("/v1/chat/completions", post(chat_completions))
        // Also without /v1 prefix for compatibility
        .route("/models", get(list_models))
        .route("/chat/completions", post(chat_completions))
        // Zen-free-api management endpoints
        .route("/v1/usage", get(get_usage))
        .route("/v1/conversation/{model}", get(get_conversation))
        .route("/v1/reset/{model}", post(reset_conversation))
        .layer(CorsLayer::permissive())
        .with_state(app);

    println!();
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║                     zen-free-api  v0.1.0                         ║");
    println!("║          OpenAI-compatible proxy for opencode.ai             ║");
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║  Listen:    http://{:41}║", format!("{}/", listen_addr));
    println!("║  Upstream:  {:47}║", base_url);
    println!("║  API Key:   {:47}║", api_key_display);
    println!("║  Session:   {:47}║", session_id);
    println!("║  Project:   {:47}║", project_id);
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║  Endpoints:                                                   ║");
    println!("║  GET  /v1/models            List available models              ║");
    println!("║  POST /v1/chat/completions  Chat (stream + non-stream)         ║");
    println!("║  GET  /v1/usage             Per-model usage stats             ║");
    println!("║  GET  /v1/conversation/:m   View conversation state           ║");
    println!("║  POST /v1/reset/:m          Reset conversation               ║");
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║  Client overrides (via request headers):                      ║");
    println!("║  Authorization: Bearer <key>   Override API key               ║");
    println!("║  x-opencode-session: <id>     Override session ID             ║");
    println!("║  x-opencode-project: <id>     Override project ID             ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();

    let listener = tokio::net::TcpListener::bind(&listen_addr)
        .await
        .expect("failed to bind listener");

    tracing::info!("zen-api listening on http://{}", listen_addr);

    let shutdown = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for ctrl+c");
        tracing::info!("received SIGINT, shutting down...");
    };

    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown)
        .await
        .expect("server error");
}
