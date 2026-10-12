# ferry

[![uses nix](https://img.shields.io/badge/uses-nix-%237EBAE4)](https://nixos.org/)
![rust](https://img.shields.io/badge/Rust-1.95%2B-orange.svg)

Transparent MCP stdio-to-HTTP proxy and HTTP tunnel.

`ferry` can run in two modes:

- client mode, where it runs as a local stdio MCP server and forwards JSON-RPC traffic to a remote MCP server over HTTP
- serve mode, where it runs a local stdio MCP server or relays to another HTTP MCP server and exposes Streamable HTTP plus legacy SSE downstream

It prefers Streamable HTTP, can fall back to legacy SSE, and supports optional SOCKS5 proxying.

## Features

- `auto` transport mode by default, probing Streamable HTTP first and falling back to legacy SSE
- Streamable HTTP and legacy SSE support
- `serve` mode with `stdio` and `relay` backends
- default downstream surfaces for `/mcp`, `/sse`, `/messages`, and `/health`
- `MCP-Session-Id` and `MCP-Protocol-Version` handling for modern HTTP servers
- Best-effort SSE resume support via `Last-Event-ID`
- Config from CLI, environment, and TOML with precedence: CLI > env > config > defaults
- `rustls` TLS and optional `socks5h://` proxy support
- Nix-first build flow with static builds via `cargo-zigbuild`

## Quick Start

Minimal usage:

```bash
ferry --url https://mcp.example.com/mcp
```

Typical MCP client config:

```json
{
  "mcpServers": {
    "remote": {
      "command": "ferry",
      "args": ["--url", "https://mcp.example.com/mcp"]
    }
  }
}
```

Legacy SSE only:

```bash
ferry --url https://old.example.com/sse --transport sse
```

Through a SOCKS5 proxy:

```bash
ferry --url http://mcp.internal/mcp --socks5 127.0.0.1:1080
```

Use a config file explicitly:

```bash
ferry --config /path/to/ferry.toml
```

Or via env:

```bash
FERRY_CONFIG=/path/to/ferry.toml ferry
```

## Serve Mode

Expose a local stdio MCP server over HTTP:

```bash
ferry serve --listen 127.0.0.1:9000 stdio -- my-mcp-server --stdio
```

That serves:

- Streamable HTTP on `/mcp`
- legacy SSE on `/sse` plus `/messages`
- health on `/health`

Relay to another HTTP MCP server instead of spawning stdio:

```bash
ferry serve --listen 127.0.0.1:9001 relay --url http://127.0.0.1:9000/mcp --transport streamable-http
```

Use a config file instead of passing the backend on the command line:

```bash
ferry serve --config /path/to/ferry.toml
```

Example serve config:

```toml
[serve]
listen = "127.0.0.1:9000"
path = "/mcp"
allow_origins = ["https://app.example.com"]

[serve.auth]
bearer_token = "${FERRY_SHARED_TOKEN}"

[serve.stdio]
command = "my-mcp-server"
args = ["--stdio"]
```

Or drive serve mode from env:

```bash
FERRY_SERVE_LISTEN=127.0.0.1:9001 \
FERRY_SERVE_BACKEND=relay \
FERRY_SERVE_RELAY_URL=http://127.0.0.1:9000/mcp \
FERRY_SERVE_RELAY_TRANSPORT=streamable-http \
ferry serve
```

Use another `ferry` as a local stdio bridge into that relay:

```bash
ferry --url http://127.0.0.1:9001/mcp
```

Loopback is the default bind target. Exposing `serve` mode remotely requires an explicit non-loopback `--listen`, for example:

```bash
ferry serve --listen 0.0.0.0:9000 stdio -- my-mcp-server --stdio
```

## Client Examples

### Cursor

Cursor reads MCP config from `.cursor/mcp.json` in your repo or `~/.cursor/mcp.json` globally.

Flags:

```json
{
  "mcpServers": {
    "ferry": {
      "command": "ferry",
      "args": ["--url", "https://mcp.example.com/mcp", "-H", "Authorization=Bearer <token>"]
    }
  }
}
```

Env vars:

```json
{
  "mcpServers": {
    "ferry": {
      "command": "ferry",
      "env": {
        "FERRY_URL": "https://mcp.example.com/mcp",
        "FERRY_HEADER_AUTHORIZATION": "Bearer ${env:MCP_TOKEN}"
      }
    }
  }
}
```

### Codex CLI

Codex CLI reads MCP config from `~/.codex/config.toml`.

Flags:

```toml
[mcp_servers.ferry]
command = "ferry"
args = ["--url", "https://mcp.example.com/mcp", "-H", "Authorization=Bearer <token>"]
```

Env vars:

```toml
[mcp_servers.ferry]
command = "ferry"

[mcp_servers.ferry.env]
FERRY_URL = "https://mcp.example.com/mcp"
FERRY_HEADER_AUTHORIZATION = "Bearer <token>"
```

Config file via env:

```toml
[mcp_servers.ferry]
command = "ferry"

[mcp_servers.ferry.env]
FERRY_CONFIG = "/absolute/path/to/ferry.toml"
```

### Claude Code

Claude Code supports `.mcp.json` in the project root or `--mcp-config <file>`.

Flags:

```json
{
  "mcpServers": {
    "ferry": {
      "command": "ferry",
      "args": ["--url", "https://mcp.example.com/mcp"]
    }
  }
}
```

Env vars:

```json
{
  "mcpServers": {
    "ferry": {
      "command": "ferry",
      "env": {
        "FERRY_URL": "https://mcp.example.com/mcp",
        "FERRY_HEADER_AUTHORIZATION": "Bearer ${MCP_TOKEN}"
      }
    }
  }
}
```

## Configuration

Supported inputs:

- CLI flags such as `--url`, `--transport`, `--header`, `--timeout`
- Environment variables such as `FERRY_URL`, `FERRY_TRANSPORT`, `FERRY_TIMEOUT`, `FERRY_CONFIG`
- Header env vars of the form `FERRY_HEADER_<NAME>=<VALUE>`
- Serve mode env vars such as `FERRY_SERVE_LISTEN`, `FERRY_SERVE_BACKEND`, `FERRY_SERVE_RELAY_URL`
- Serve auth and relay header env vars of the form `FERRY_SERVE_AUTH_HEADER_<NAME>=<VALUE>` and `FERRY_SERVE_RELAY_HEADER_<NAME>=<VALUE>`
- TOML config via `--config`, `FERRY_CONFIG`, `./ferry.toml`, or `$XDG_CONFIG_HOME/ferry/config.toml`

Example config:

```toml
url = "https://mcp.internal.example.com/mcp"
transport = "auto"
timeout = 30

[headers]
authorization = "Bearer ${MCP_TOKEN}"

[retry]
count = 3
base_delay_ms = 1000
```

Serve mode config lives under `[serve]`:

```toml
[serve]
listen = "127.0.0.1:9000"
path = "/mcp"
legacy_sse_path = "/sse"
legacy_messages_path = "/messages"
health_path = "/health"
idle_timeout = 1800

[serve.auth]
bearer_token = "${FERRY_SHARED_TOKEN}"

[serve.auth.headers]
x-ferry-auth = "expected"

[serve.relay]
url = "https://upstream.example.com/mcp"
transport = "auto"
timeout = 30

[serve.relay.headers]
authorization = "Bearer ${UPSTREAM_TOKEN}"

[serve.relay.retry]
count = 3
base_delay_ms = 1000
```

Notes:

- `ferry serve` follows the same precedence as client mode, `CLI > env > config > defaults`
- `timeout` bounds connecting, `initialize`, notifications, and SSE setup. Other JSON-RPC requests (for example slow tool calls) wait for the server, relying on the MCP client's own request timeout and cancellation
- Requests are only retried when they provably never reached the server (connection failures and HTTP 503), so tool calls are never sent twice
- serve sessions with no requests and no open stream for `idle_timeout` seconds (default 1800, also `--idle-timeout` or `FERRY_SERVE_IDLE_TIMEOUT`) are shut down, which stops their stdio backend
- `FERRY_SERVE_STDIO_ARGS` expects a JSON string array, for example `FERRY_SERVE_STDIO_ARGS='[\"--stdio\"]'`

## Development

```bash
nix-shell  # or direnv!
cargo test
cargo clippy --all --benches --tests --examples --all-features
build_static
```
