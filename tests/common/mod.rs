use std::net::TcpListener;
use std::process::Stdio;
use std::time::Duration;

use reqwest::StatusCode;
use tokio::process::{Child, Command};
use tokio::time::{sleep, timeout};

pub struct FerryProcess {
    pub base_url: String,
    child: Child,
}

impl FerryProcess {
    pub async fn spawn_stdio() -> Result<Self, Box<dyn std::error::Error>> {
        Self::spawn_stdio_with(&[]).await
    }

    /// Spawns `ferry serve <serve_args> stdio -- ferry_test_server`.
    pub async fn spawn_stdio_with(serve_args: &[&str]) -> Result<Self, Box<dyn std::error::Error>> {
        let mut args = serve_args
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        args.extend([
            "stdio".to_string(),
            "--".to_string(),
            binary_path("ferry_test_server")?,
        ]);
        Self::spawn(args).await
    }

    #[allow(dead_code)]
    pub async fn spawn_relay(
        upstream_url: &str,
        transport: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::spawn_relay_with(upstream_url, transport, &[]).await
    }

    /// Spawns `ferry serve relay --url <upstream_url> --transport <transport> <relay_args>`.
    #[allow(dead_code)]
    pub async fn spawn_relay_with(
        upstream_url: &str,
        transport: &str,
        relay_args: &[&str],
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let args = ["relay", "--url", upstream_url, "--transport", transport]
            .iter()
            .chain(relay_args)
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        Self::spawn(args).await
    }

    async fn spawn(args: Vec<String>) -> Result<Self, Box<dyn std::error::Error>> {
        let port = open_port()?;
        let base_url = format!("http://127.0.0.1:{port}");
        let child = Command::new(binary_path("ferry")?)
            .arg("serve")
            .arg("--listen")
            .arg(format!("127.0.0.1:{port}"))
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()?;

        let process = Self { base_url, child };
        process.wait_for_health().await?;
        Ok(process)
    }

    pub async fn shutdown(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        match self.child.kill().await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::InvalidInput => {}
            Err(error) => return Err(Box::new(error)),
        }
        let _ = self.child.wait().await;
        Ok(())
    }

    async fn wait_for_health(&self) -> Result<(), Box<dyn std::error::Error>> {
        let client = reqwest::Client::new();
        for _ in 0..100 {
            match client.get(format!("{}/health", self.base_url)).send().await {
                Ok(response) if response.status() == StatusCode::OK => return Ok(()),
                Ok(_) | Err(_) => sleep(Duration::from_millis(50)).await,
            }
        }

        Err(format!("timed out waiting for {} to become healthy", self.base_url).into())
    }
}

#[allow(dead_code)]
pub fn header_str<'a>(
    headers: &'a reqwest::header::HeaderMap,
    name: &str,
) -> Result<&'a str, Box<dyn std::error::Error>> {
    headers
        .get(name)
        .ok_or_else(|| format!("missing response header {name}").into())
        .and_then(|value| {
            value
                .to_str()
                .map_err(|error| format!("invalid response header {name}: {error}").into())
        })
}

#[allow(dead_code)]
pub async fn read_sse_event(
    response: &mut reqwest::Response,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut buffer = String::new();
    loop {
        let chunk = timeout(Duration::from_secs(2), response.chunk())
            .await??
            .ok_or_else(|| "SSE stream ended before an event was received".to_string())?;
        buffer.push_str(std::str::from_utf8(&chunk)?);
        if let Some(index) = buffer.find("\n\n") {
            return Ok(buffer[..index].to_string());
        }
        if let Some(index) = buffer.find("\r\n\r\n") {
            return Ok(buffer[..index].to_string());
        }
    }
}

#[allow(dead_code)]
pub fn sse_field(event: &str, field: &str) -> Option<String> {
    let prefix = format!("{field}:");
    event.lines().find_map(|line| {
        line.strip_prefix(&prefix)
            .map(|value| value.trim_start().to_string())
    })
}

#[allow(dead_code)]
pub fn sse_data(event: &str) -> String {
    event
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(|value| value.trim_start())
        .collect::<Vec<_>>()
        .join("\n")
}

fn open_port() -> Result<u16, Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

fn binary_path(name: &str) -> Result<String, Box<dyn std::error::Error>> {
    let variable = format!("CARGO_BIN_EXE_{name}");
    std::env::var(&variable)
        .map_err(|error| format!("missing cargo binary env var {variable}: {error}").into())
}
