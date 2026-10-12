use reqwest::StatusCode;
use serde_json::{json, Value};

mod common;

use common::{header_str, read_sse_event, FerryProcess};

const PROTOCOL_VERSION: &str = "2025-11-25";

#[tokio::test]
async fn serve_relay_tunnels_to_upstream_ferry() -> Result<(), Box<dyn std::error::Error>> {
    let mut upstream = FerryProcess::spawn_stdio().await?;
    let mut relay =
        FerryProcess::spawn_relay(&format!("{}/mcp", upstream.base_url), "streamable-http").await?;
    let client = reqwest::Client::new();

    let initialize = client
        .post(format!("{}/mcp", relay.base_url))
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": PROTOCOL_VERSION
            }
        }))
        .send()
        .await?;
    assert_eq!(initialize.status(), StatusCode::OK);
    let session_id = header_str(initialize.headers(), "mcp-session-id")?.to_string();
    let initialize_body: Value = initialize.json().await?;
    assert_eq!(
        initialize_body["result"]["protocolVersion"],
        Value::String(PROTOCOL_VERSION.to_string())
    );

    let mut sse = client
        .get(format!("{}/mcp", relay.base_url))
        .header("mcp-session-id", &session_id)
        .header("mcp-protocol-version", PROTOCOL_VERSION)
        .send()
        .await?;
    assert_eq!(sse.status(), StatusCode::OK);
    let first_event = read_sse_event(&mut sse).await?;
    assert!(first_event.contains("\"method\":\"test/ready\""));

    let echo = client
        .post(format!("{}/mcp", relay.base_url))
        .header("mcp-session-id", &session_id)
        .header("mcp-protocol-version", PROTOCOL_VERSION)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "delayed_echo",
            "params": {
                "value": "through-relay",
                "delayMs": 25
            }
        }))
        .send()
        .await?;
    assert_eq!(echo.status(), StatusCode::OK);
    let echo_body: Value = echo.json().await?;
    assert_eq!(
        echo_body["result"]["echo"],
        Value::String("through-relay".to_string())
    );

    let delete = client
        .delete(format!("{}/mcp", relay.base_url))
        .header("mcp-session-id", &session_id)
        .header("mcp-protocol-version", PROTOCOL_VERSION)
        .send()
        .await?;
    assert_eq!(delete.status(), StatusCode::NO_CONTENT);

    relay.shutdown().await?;
    upstream.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn serve_relay_runs_slow_requests_concurrently_past_timeout(
) -> Result<(), Box<dyn std::error::Error>> {
    let mut upstream = FerryProcess::spawn_stdio().await?;
    let mut relay = FerryProcess::spawn_relay_with(
        &format!("{}/mcp", upstream.base_url),
        "streamable-http",
        &["--timeout", "1"],
    )
    .await?;
    let client = reqwest::Client::new();

    let initialize = client
        .post(format!("{}/mcp", relay.base_url))
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": { "protocolVersion": PROTOCOL_VERSION }
        }))
        .send()
        .await?;
    let session_id = header_str(initialize.headers(), "mcp-session-id")?.to_string();

    let echo = |id: u64, value: &'static str, delay_ms: u64| {
        let request = client
            .post(format!("{}/mcp", relay.base_url))
            .header("mcp-session-id", &session_id)
            .header("mcp-protocol-version", PROTOCOL_VERSION)
            .json(&json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": "delayed_echo",
                "params": { "value": value, "delayMs": delay_ms }
            }));
        async move {
            let body: Value = request.send().await?.json().await?;
            Ok::<_, reqwest::Error>((body, tokio::time::Instant::now()))
        }
    };

    // The slow request outlives the 1s relay timeout and must neither fail nor be resent,
    // and it must not block the fast request behind it.
    let ((slow, slow_done), (fast, fast_done)) =
        tokio::try_join!(echo(2, "slow", 1_500), echo(3, "fast", 0))?;
    assert_eq!(slow["result"]["echo"], json!("slow"));
    assert_eq!(fast["result"]["echo"], json!("fast"));
    assert!(fast_done < slow_done);

    relay.shutdown().await?;
    upstream.shutdown().await?;
    Ok(())
}
