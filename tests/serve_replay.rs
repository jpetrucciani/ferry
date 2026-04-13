use std::time::Duration;

use reqwest::StatusCode;
use serde_json::{json, Value};
use tokio::time::sleep;

mod common;

use common::{read_sse_event, sse_data, sse_field, FerryProcess};

const PROTOCOL_VERSION: &str = "2025-11-25";

#[tokio::test]
async fn streamable_http_replays_events_after_last_event_id(
) -> Result<(), Box<dyn std::error::Error>> {
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
    let session_id = common::header_str(initialize.headers(), "mcp-session-id")?.to_string();

    let mut sse = client
        .get(format!("{}/mcp", ferry.base_url))
        .header("mcp-session-id", &session_id)
        .header("mcp-protocol-version", PROTOCOL_VERSION)
        .send()
        .await?;
    assert_eq!(sse.status(), StatusCode::OK);

    let _ready_event = read_sse_event(&mut sse).await?;

    let first = client
        .post(format!("{}/mcp", ferry.base_url))
        .header("mcp-session-id", &session_id)
        .header("mcp-protocol-version", PROTOCOL_VERSION)
        .json(&json!({
            "jsonrpc": "2.0",
            "method": "notify_client",
            "params": {
                "value": "first"
            }
        }))
        .send()
        .await?;
    assert_eq!(first.status(), StatusCode::ACCEPTED);
    let first_event = read_sse_event(&mut sse).await?;
    let last_event_id =
        sse_field(&first_event, "id").ok_or("first notification was missing an SSE id")?;
    let first_body: Value = serde_json::from_str(&sse_data(&first_event))?;
    assert_eq!(
        first_body["params"]["value"],
        Value::String("first".to_string())
    );

    let second = client
        .post(format!("{}/mcp", ferry.base_url))
        .header("mcp-session-id", &session_id)
        .header("mcp-protocol-version", PROTOCOL_VERSION)
        .json(&json!({
            "jsonrpc": "2.0",
            "method": "notify_client",
            "params": {
                "value": "replay-me"
            }
        }))
        .send()
        .await?;
    assert_eq!(second.status(), StatusCode::ACCEPTED);
    sleep(Duration::from_millis(50)).await;
    drop(sse);

    let mut resumed = client
        .get(format!("{}/mcp", ferry.base_url))
        .header("mcp-session-id", &session_id)
        .header("mcp-protocol-version", PROTOCOL_VERSION)
        .header("last-event-id", last_event_id)
        .send()
        .await?;
    assert_eq!(resumed.status(), StatusCode::OK);

    let replayed_event = read_sse_event(&mut resumed).await?;
    let replayed_body: Value = serde_json::from_str(&sse_data(&replayed_event))?;
    assert_eq!(
        replayed_body["params"]["value"],
        Value::String("replay-me".to_string())
    );

    ferry.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn legacy_sse_reuses_session_from_last_event_id() -> Result<(), Box<dyn std::error::Error>> {
    let mut ferry = FerryProcess::spawn_stdio().await?;
    let client = reqwest::Client::new();

    let mut sse = client.get(format!("{}/sse", ferry.base_url)).send().await?;
    assert_eq!(sse.status(), StatusCode::OK);

    let endpoint_event = read_sse_event(&mut sse).await?;
    assert_eq!(
        sse_field(&endpoint_event, "event").as_deref(),
        Some("endpoint")
    );
    let endpoint = format!("{}{}", ferry.base_url, sse_data(&endpoint_event));

    let initialize = client
        .post(&endpoint)
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
    assert_eq!(initialize.status(), StatusCode::ACCEPTED);

    let initialize_event = read_sse_event(&mut sse).await?;
    let initialize_body: Value = serde_json::from_str(&sse_data(&initialize_event))?;
    assert_eq!(
        initialize_body["result"]["protocolVersion"],
        Value::String(PROTOCOL_VERSION.to_string())
    );

    let _ready_event = read_sse_event(&mut sse).await?;

    let first = client
        .post(&endpoint)
        .json(&json!({
            "jsonrpc": "2.0",
            "method": "notify_client",
            "params": {
                "value": "legacy-first"
            }
        }))
        .send()
        .await?;
    assert_eq!(first.status(), StatusCode::ACCEPTED);
    let first_event = read_sse_event(&mut sse).await?;
    let last_event_id =
        sse_field(&first_event, "id").ok_or("legacy notification was missing an SSE id")?;
    let first_body: Value = serde_json::from_str(&sse_data(&first_event))?;
    assert_eq!(
        first_body["params"]["value"],
        Value::String("legacy-first".to_string())
    );

    let second = client
        .post(&endpoint)
        .json(&json!({
            "jsonrpc": "2.0",
            "method": "notify_client",
            "params": {
                "value": "legacy-replay"
            }
        }))
        .send()
        .await?;
    assert_eq!(second.status(), StatusCode::ACCEPTED);
    sleep(Duration::from_millis(50)).await;
    drop(sse);

    let mut resumed = client
        .get(format!("{}/sse", ferry.base_url))
        .header("last-event-id", last_event_id)
        .send()
        .await?;
    assert_eq!(resumed.status(), StatusCode::OK);

    let replayed_event = read_sse_event(&mut resumed).await?;
    let replayed_body: Value = serde_json::from_str(&sse_data(&replayed_event))?;
    assert_eq!(
        replayed_body["params"]["value"],
        Value::String("legacy-replay".to_string())
    );

    ferry.shutdown().await?;
    Ok(())
}
