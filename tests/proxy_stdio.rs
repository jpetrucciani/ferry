use std::net::TcpListener;
use std::process::Stdio;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::time::timeout;

#[tokio::test]
async fn proxy_answers_failed_requests_with_jsonrpc_errors(
) -> Result<(), Box<dyn std::error::Error>> {
    let unreachable_port = TcpListener::bind("127.0.0.1:0")?.local_addr()?.port();
    let mut child = Command::new(env!("CARGO_BIN_EXE_ferry"))
        .args([
            "--url",
            &format!("http://127.0.0.1:{unreachable_port}/mcp"),
            "--transport",
            "streamable-http",
            "--retry",
            "0",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()?;
    let mut stdin = child.stdin.take().ok_or("missing stdin")?;
    let mut stdout = BufReader::new(child.stdout.take().ok_or("missing stdout")?).lines();

    let initialize = json!({
        "jsonrpc": "2.0",
        "id": 7,
        "method": "initialize",
        "params": { "protocolVersion": "2025-11-25" }
    });
    stdin
        .write_all(format!("{initialize}\n").as_bytes())
        .await?;

    let line = timeout(Duration::from_secs(10), stdout.next_line())
        .await??
        .ok_or("ferry closed stdout without answering")?;
    let response: Value = serde_json::from_str(&line)?;
    assert_eq!(response["id"], json!(7));
    assert_eq!(response["error"]["code"], json!(-32603));

    child.kill().await?;
    Ok(())
}
