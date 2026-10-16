# Zen Free API

> A pure Rust proxy for `opencode.ai/zen/v1` with an OpenAI-compatible interface — no paid API key required.

![Rust](https://img.shields.io/badge/Rust-2021+-orange?style=flat-square)
![OpenAI Compatible](https://img.shields.io/badge/OpenAI-Compatible-green?style=flat-square)
![Streaming](https://img.shields.io/badge/SSE-Streaming-blue?style=flat-square)
![Free API](https://img.shields.io/badge/API-Free-orange?style=flat-square)

---

## Overview

Zen Free API is a lightweight, OpenAI-compatible proxy for the opencode.ai Zen service, written entirely in Rust. It forwards chat completion requests upstream, injects the required opencode wire headers, and streams responses back to the client byte-for-byte — all without requiring a paid API key for free-tier models.

---

## Features

- **OpenAI-compatible chat completions** — pure Rust implementation using `axum` and `tokio`
- **Streaming and non-streaming responses** — SSE passthrough or standard JSON, forwarded byte-for-byte from upstream
- **Automatic header injection** — injects `x-opencode-session`, `x-opencode-request`, `x-opencode-project`, and `x-opencode-client` headers
- **Per-model conversation state** — persisted in memory and on disk at `~/.cache/opencode-zen`
- **Usage and cost tracking** — per-model statistics available via `/v1/usage`
- **Per-request header overrides** — customize API key, session, and project on a per-request basis
- **CORS enabled** — ready for browser-based clients
- **Graceful shutdown** — clean termination on Ctrl+C

---

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/your-org/ZenFreeAPI.git
cd ZenFreeAPI
```

### 2. Build

Requires Rust 1.70 or later.

```bash
cargo build --release
```

### 3. Configure environment

All configuration is handled via environment variables with sensible defaults. No `.env` file is required.

| Variable | Default | Description |
|---|---|---|
| `OPENCODE_ZEN_BASE` | `https://opencode.ai/zen/v1` | Upstream base URL |
| `OPENCODE_API_KEY` | `public` | API key (see below) |
| `OPENCODE_CLIENT` | `cli` | Value for the `x-opencode-client` header |
| `OPENCODE_USER_AGENT` | `opencode/0.0.0` | User-Agent sent upstream |
| `OPENCODE_SESSION_ID` | auto-generated | Stable session ID |
| `OPENCODE_PROJECT_ID` | derived from cwd | Project ID |
| `ZEN_STATE_DIR` | `~/.cache/opencode-zen` | Conversation state directory |
| `ZEN_LISTEN` | `127.0.0.1:8080` | Bind address |

#### A note on API keys

When `OPENCODE_API_KEY` is unset, it defaults to `public` (anonymous access). The `public` key only permits free-tier models — those whose IDs end in `-free`. Paid models such as `claude-opus-5` or `gpt-5.5` are rejected with an authentication error unless a real key is provided. You can obtain a key at [https://opencode.ai/zen/](https://opencode.ai/zen/).

```bash
# Linux / macOS
export OPENCODE_API_KEY=your_key_here

# Windows (PowerShell)
$env:OPENCODE_API_KEY="your_key_here"
```

---

## Running

```bash
cargo run --release
```

On startup, the proxy prints a banner with the listen address, upstream URL, and API key status:

```
╔══════════════════════════════════════════════════════════════╗
║                     zen-free-api  v0.1.0                     ║
║          OpenAI-compatible proxy for opencode.ai             ║
╠══════════════════════════════════════════════════════════════╣
║  Listen:    http://127.0.0.1:8080/                           ║
║  Upstream:  https://opencode.ai/zen/v1                       ║
║  API Key:   public (anonymous, free models only)             ║
╚══════════════════════════════════════════════════════════════╝
```

Point any OpenAI-compatible client at `http://localhost:8080`. The proxy speaks the full OpenAI wire format, so tools such as opencode, `curl`, or custom scripts work without modification.

---

## API Reference

The proxy listens at `http://localhost:8080`.

### `GET /` — Health check

```bash
curl http://localhost:8080/
```

### `GET /v1/models` — List available models

```bash
curl http://localhost:8080/v1/models \
  -H "Authorization: Bearer public"
```

### `POST /v1/chat/completions` — Chat completions (OpenAI format)

Works with any model exposed by the upstream, including free-tier models such as `model-id-free`. Both `/v1/chat/completions` and `/chat/completions` are accepted.

**Non-streaming:**

```bash
curl -X POST http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer public" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "model-id-free",
    "messages": [{"role": "user", "content": "What is a proxy?"}],
    "stream": false
  }'
```

**Streaming:**

```bash
curl -N -X POST http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer public" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "model-id-free",
    "messages": [{"role": "user", "content": "Explain quantum computing in simple terms"}],
    "stream": true
  }'
```

The entire request body is forwarded upstream untouched. Parameters such as `temperature`, `top_p`, `stream_options`, `tools`, and `tool_choice` all pass through as-is.

### `GET /v1/usage` — Per-model usage statistics

```bash
curl http://localhost:8080/v1/usage
```

```json
{
  "models": [
    {
      "model": "model-id-free",
      "message_count": 4,
      "last_usage": "usage: in=120 out=85 cached=0"
    }
  ]
}
```

### `GET /v1/conversation/{model}` — View saved conversation

```bash
curl http://localhost:8080/v1/conversation/model-id-free
```

### `POST /v1/reset/{model}` — Reset conversation state

```bash
curl -X POST http://localhost:8080/v1/reset/model-id-free
```

---

## Client overrides

The proxy injects opencode wire headers automatically, but you can override them on a per-request basis.

| Header | Overrides |
|---|---|
| `Authorization: Bearer <key>` | Upstream API key (also accepted via `x-api-key`) |
| `x-opencode-session: <id>` | Session ID |
| `x-opencode-project: <id>` | Project ID |

```bash
curl -X POST http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer your_key" \
  -H "x-opencode-session: sess_abc123" \
  -H "Content-Type: application/json" \
  -d '{"model": "model-id-free", "messages": [{"role": "user", "content": "Hi"}]}'
```

> **Note:** Sending `Authorization: Bearer public` explicitly is treated as anonymous access and falls back to the configured key.

---

## How it works

1. You send an OpenAI-format request to `localhost:8080`.
2. The proxy parses the body for model and stream metadata, then saves your messages to state.
3. It injects the opencode wire headers (`x-opencode-session`, `x-opencode-request`, `x-opencode-project`, `x-opencode-client`, and `user-agent`).
4. The exact raw body is forwarded to `https://opencode.ai/zen/v1/chat/completions`.
5. Responses are streamed back (SSE passthrough) or returned as JSON, while the assistant reply and usage/cost data are tracked per model.
6. Conversation state is persisted to `~/.cache/opencode-zen/{model}.json`, so context survives restarts.

---

## Acknowledgements

- [opencode.ai](https://opencode.ai) — the upstream service and wire protocol this proxy mirrors.
