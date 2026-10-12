use std::time::Duration;

use reqwest::StatusCode;
use serde_json::{json, Value};
use tokio::time::sleep;

mod common;

use common::{header_str, read_sse_event, FerryProcess};

const PROTOCOL_VERSION: &str = "2025-11-25";

#[tokio::test]
async fn serve_stdio_handles_session_flow_end_to_end() -> Result<(), Box<dyn std::error::Error>> {
    let mut ferry = FerryProcess::spawn_stdio().await?;
    let client = reqwest::Client::new();

    let initialize = client
        .post(format!("{}/mcp", ferry.base_url))
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
        .get(format!("{}/mcp", ferry.base_url))
        .header("mcp-session-id", &session_id)
        .header("mcp-protocol-version", PROTOCOL_VERSION)
        .send()
        .await?;
    assert_eq!(sse.status(), StatusCode::OK);
    let first_event = read_sse_event(&mut sse).await?;
    assert!(first_event.contains("\"method\":\"test/ready\""));

    let notify = client
        .post(format!("{}/mcp", ferry.base_url))
        .header("mcp-session-id", &session_id)
        .header("mcp-protocol-version", PROTOCOL_VERSION)
        .json(&json!({
            "jsonrpc": "2.0",
            "method": "notify_client",
            "params": {
                "value": "later"
            }
        }))
        .send()
        .await?;
    assert_eq!(notify.status(), StatusCode::ACCEPTED);

    let second_event = read_sse_event(&mut sse).await?;
    assert!(second_event.contains("\"method\":\"test/notification\""));
    assert!(second_event.contains("\"value\":\"later\""));

    let first_request = {
        let client = client.clone();
        let base_url = ferry.base_url.clone();
        let session_id = session_id.clone();
        tokio::spawn(async move {
            client
                .post(format!("{base_url}/mcp"))
                .header("mcp-session-id", session_id)
                .header("mcp-protocol-version", PROTOCOL_VERSION)
                .json(&json!({
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "delayed_echo",
                    "params": {
                        "value": "first",
                        "delayMs": 200
                    }
                }))
                .send()
                .await
        })
    };

    sleep(Duration::from_millis(25)).await;

    let duplicate = client
        .post(format!("{}/mcp", ferry.base_url))
        .header("mcp-session-id", &session_id)
        .header("mcp-protocol-version", PROTOCOL_VERSION)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "delayed_echo",
            "params": {
                "value": "duplicate",
                "delayMs": 10
            }
        }))
        .send()
        .await?;
    assert_eq!(duplicate.status(), StatusCode::CONFLICT);

    let first_response = first_request.await??;
    assert_eq!(first_response.status(), StatusCode::OK);
    let first_body: Value = first_response.json().await?;
    assert_eq!(
        first_body["result"]["echo"],
        Value::String("first".to_string())
    );

    let delete = client
        .delete(format!("{}/mcp", ferry.base_url))
        .header("mcp-session-id", &session_id)
        .header("mcp-protocol-version", PROTOCOL_VERSION)
        .send()
        .await?;
    assert_eq!(delete.status(), StatusCode::NO_CONTENT);

    let missing = client
        .get(format!("{}/mcp", ferry.base_url))
        .header("mcp-session-id", &session_id)
        .header("mcp-protocol-version", PROTOCOL_VERSION)
        .send()
        .await?;
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);

    ferry.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn serve_stdio_returns_rejected_initialize_without_a_session(
) -> Result<(), Box<dyn std::error::Error>> {
    let mut ferry = FerryProcess::spawn_stdio().await?;

    let initialize = reqwest::Client::new()
        .post(format!("{}/mcp", ferry.base_url))
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "unsupported"
            }
        }))
        .send()
        .await?;
    assert_eq!(initialize.status(), StatusCode::OK);
    assert!(initialize.headers().get("mcp-session-id").is_none());
    let body: Value = initialize.json().await?;
    assert_eq!(body["error"]["code"], json!(-32602));

    ferry.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn serve_stdio_reaps_idle_sessions() -> Result<(), Box<dyn std::error::Error>> {
    let mut ferry = FerryProcess::spawn_stdio_with(&["--idle-timeout", "1"]).await?;
    let client = reqwest::Client::new();

    let initialize = client
        .post(format!("{}/mcp", ferry.base_url))
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
    let session_id = header_str(initialize.headers(), "mcp-session-id")?.to_string();

    // Clients on 2025-03-26 do not send MCP-Protocol-Version.
    let echo = client
        .post(format!("{}/mcp", ferry.base_url))
        .header("mcp-session-id", &session_id)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "delayed_echo",
            "params": { "value": "no-version-header" }
        }))
        .send()
        .await?;
    assert_eq!(echo.status(), StatusCode::OK);

    sleep(Duration::from_millis(2_500)).await;

    let expired = client
        .post(format!("{}/mcp", ferry.base_url))
        .header("mcp-session-id", &session_id)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "delayed_echo",
            "params": { "value": "too-late" }
        }))
        .send()
        .await?;
    assert_eq!(expired.status(), StatusCode::NOT_FOUND);

    ferry.shutdown().await?;
    Ok(())
}
