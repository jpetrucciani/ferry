use std::time::Duration;

use ferry::error::{Error, Result};
use ferry::stdio::{read_message_from, write_message_to};
use serde_json::{json, Value};
use tokio::io::{BufReader, BufWriter};
use tokio::sync::mpsc;
use tokio::time::sleep;

const PROTOCOL_VERSION: &str = "2025-11-25";
const UNSUPPORTED_PROTOCOL_VERSION: &str = "unsupported";

#[tokio::main]
async fn main() -> Result<()> {
    let (sender, mut receiver) = mpsc::channel::<Value>(32);
    let writer_task = tokio::spawn(async move {
        let stdout = tokio::io::stdout();
        let mut writer = BufWriter::new(stdout);
        while let Some(message) = receiver.recv().await {
            write_message_to(&mut writer, &message).await?;
        }
        Ok::<(), Error>(())
    });

    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin);
    while let Some(message) = read_message_from(&mut reader).await? {
        handle_message(message, &sender).await?;
    }

    drop(sender);
    writer_task
        .await
        .map_err(|error| Error::Message(format!("writer task failed: {error}")))?
}

async fn handle_message(message: Value, sender: &mpsc::Sender<Value>) -> Result<()> {
    let Some(method) = message_method(&message) else {
        return Ok(());
    };

    match method {
        "initialize" => {
            let id = request_id(&message).ok_or_else(|| {
                Error::InvalidResponse("initialize request was missing an id".to_string())
            })?;
            let requested_version = message
                .pointer("/params/protocolVersion")
                .and_then(Value::as_str);
            if requested_version == Some(UNSUPPORTED_PROTOCOL_VERSION) {
                return send_message(
                    sender,
                    json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "error": {
                            "code": -32602,
                            "message": "unsupported protocol version"
                        }
                    }),
                )
                .await;
            }
            send_message(
                sender,
                json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {
                        "protocolVersion": PROTOCOL_VERSION,
                        "capabilities": {},
                        "serverInfo": {
                            "name": "ferry-test-server",
                            "version": "0.0.0"
                        }
                    }
                }),
            )
            .await?;
            send_message(
                sender,
                json!({
                    "jsonrpc": "2.0",
                    "method": "test/ready",
                    "params": {
                        "status": "ok"
                    }
                }),
            )
            .await?;
        }
        "delayed_echo" => {
            let id = request_id(&message).ok_or_else(|| {
                Error::InvalidResponse("delayed_echo request was missing an id".to_string())
            })?;
            let delay_ms = message
                .as_object()
                .and_then(|object| object.get("params"))
                .and_then(Value::as_object)
                .and_then(|params| params.get("delayMs"))
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let value = message
                .as_object()
                .and_then(|object| object.get("params"))
                .and_then(Value::as_object)
                .and_then(|params| params.get("value"))
                .cloned()
                .unwrap_or(Value::Null);
            let sender = sender.clone();
            tokio::spawn(async move {
                if delay_ms > 0 {
                    sleep(Duration::from_millis(delay_ms)).await;
                }
                let _ = send_message(
                    &sender,
                    json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "result": {
                            "echo": value
                        }
                    }),
                )
                .await;
            });
        }
        "notify_client" => {
            let value = message
                .as_object()
                .and_then(|object| object.get("params"))
                .and_then(Value::as_object)
                .and_then(|params| params.get("value"))
                .cloned()
                .unwrap_or(Value::Null);
            send_message(
                sender,
                json!({
                    "jsonrpc": "2.0",
                    "method": "test/notification",
                    "params": {
                        "value": value
                    }
                }),
            )
            .await?;
        }
        _ => {
            if let Some(id) = request_id(&message) {
                send_message(
                    sender,
                    json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "error": {
                            "code": -32601,
                            "message": format!("unknown method: {method}")
                        }
                    }),
                )
                .await?;
            }
        }
    }

    Ok(())
}

async fn send_message(sender: &mpsc::Sender<Value>, message: Value) -> Result<()> {
    sender.send(message).await.map_err(|_| Error::Shutdown)
}

fn message_method(message: &Value) -> Option<&str> {
    message
        .as_object()
        .and_then(|object| object.get("method"))
        .and_then(Value::as_str)
}

fn request_id(message: &Value) -> Option<Value> {
    message.as_object().and_then(|object| {
        if object.get("method").is_some() {
            object.get("id").cloned()
        } else {
            None
        }
    })
}
