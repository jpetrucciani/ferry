# ferry

[![uses nix](https://img.shields.io/badge/uses-nix-%237EBAE4)](https://nixos.org/)
![rust](https://img.shields.io/badge/Rust-1.95%2B-orange.svg)

Transparent MCP stdio-to-remote transport proxy.

`ferry` runs as a local stdio MCP server and forwards JSON-RPC traffic to a remote MCP server over HTTP. It prefers Streamable HTTP, can fall back to legacy SSE, and supports optional SOCKS5 proxying.

## Features

- `auto` transport mode by default, probing Streamable HTTP first and falling back to legacy SSE
- Streamable HTTP and legacy SSE support
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

## Client Examples

### Cursor

Cursor reads MCP config from `.cursor/mcp.json` in your repo or `~/.cursor/mcp.json` globally.

Flags:

```json
{
  "mcpServers": {
    "ferry": {
      "command": "ferry",
      "args": [
        "--url",
        "https://mcp.example.com/mcp",
        "-H",
        "Authorization=Bearer <token>"
      ]
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

## Development

```bash
nix-shell  # or direnv!
cargo test
cargo clippy --all --benches --tests --examples --all-features
build_static
```
