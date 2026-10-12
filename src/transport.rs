use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use reqwest::header::{HeaderMap, ACCEPT, CONTENT_TYPE};
use reqwest::{Response, StatusCode, Url};
use serde_json::Value;
use tokio::sync::{mpsc, watch, Mutex, RwLock};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{sleep, timeout};
use tokio_stream::StreamExt;
use tracing::{debug, info, warn};

use crate::config::{Config, RetryConfig, Socks5Config, TransportMode};
use crate::error::{Error, Result};
use crate::retry::{
    backoff_delay, compatibility_fallback_status, is_retryable_error, is_retryable_post_status,
    is_undelivered_error, unknown_session_status,
};
use crate::sse::{SseEvent, SseParser};

const USER_AGENT: &str = concat!("ferry/", env!("CARGO_PKG_VERSION"));
const CONTENT_TYPE_JSON: &str = "application/json";
const CONTENT_TYPE_SSE: &str = "text/event-stream";
const HEADER_MCP_SESSION_ID: &str = "MCP-Session-Id";
const HEADER_MCP_PROTOCOL_VERSION: &str = "MCP-Protocol-Version";
const HEADER_LAST_EVENT_ID: &str = "Last-Event-ID";

#[derive(Debug)]
pub enum TransportEvent {
    Message(Value),
    Fatal(Error),
}

#[derive(Debug)]
pub struct TransportHandle {
    inner: TransportKind,
}

#[derive(Debug)]
enum TransportKind {
    Auto(Box<AutoTransport>),
    StreamableHttp(Box<StreamableHttpTransport>),
    Sse(Box<SseTransport>),
}

#[derive(Debug)]
struct AutoTransport {
    config: Config,
    sender: mpsc::Sender<TransportEvent>,
    resolved: OnceLock<ResolvedTransport>,
}

#[derive(Debug)]
enum ResolvedTransport {
    StreamableHttp(Box<StreamableHttpTransport>),
    Sse(Box<SseTransport>),
}

#[derive(Debug)]
struct StreamableHttpTransport {
    client: reqwest::Client,
    url: Url,
    timeout: Duration,
    retry: RetryConfig,
    sender: mpsc::Sender<TransportEvent>,
    session_id: Arc<RwLock<Option<String>>>,
    protocol_version: Arc<RwLock<Option<String>>>,
    last_event_id: Arc<RwLock<Option<String>>>,
    listener_disabled: Arc<AtomicBool>,
    shutdown_tx: watch::Sender<bool>,
    get_listener: Mutex<Option<JoinHandle<()>>>,
    post_streams: Mutex<JoinSet<()>>,
}

#[derive(Debug)]
struct SseTransport {
    client: reqwest::Client,
    url: Url,
    timeout: Duration,
    retry: RetryConfig,
    sender: mpsc::Sender<TransportEvent>,
    endpoint: Arc<watch::Sender<Option<Url>>>,
    last_event_id: Arc<RwLock<Option<String>>>,
    shutdown_tx: watch::Sender<bool>,
    listener: Mutex<Option<JoinHandle<()>>>,
}

/// Builds a transport that reports server-initiated messages and fatal errors on `sender`.
pub fn build_transport(
    config: Config,
    sender: mpsc::Sender<TransportEvent>,
) -> Result<TransportHandle> {
    let handle = match config.transport {
        TransportMode::Auto => TransportHandle {
            inner: TransportKind::Auto(Box::new(AutoTransport {
                config,
                sender,
                resolved: OnceLock::new(),
            })),
        },
        TransportMode::StreamableHttp => {
            let client = build_client(&config)?;
            TransportHandle {
                inner: TransportKind::StreamableHttp(Box::new(StreamableHttpTransport::new(
                    config, client, sender,
                ))),
            }
        }
        TransportMode::Sse => {
            let client = build_client(&config)?;
            TransportHandle {
                inner: TransportKind::Sse(Box::new(SseTransport::new(config, client, sender))),
            }
        }
    };

    Ok(handle)
}

impl TransportHandle {
    pub async fn connect(&self) -> Result<()> {
        match &self.inner {
            TransportKind::Auto(_) => Ok(()),
            TransportKind::StreamableHttp(transport) => transport.connect().await,
            TransportKind::Sse(transport) => transport.connect().await.map(|_| ()),
        }
    }

    pub async fn send(&self, message: Value) -> Result<Option<Value>> {
        match &self.inner {
            TransportKind::Auto(transport) => transport.send(message).await,
            TransportKind::StreamableHttp(transport) => transport.send(message).await,
            TransportKind::Sse(transport) => transport.send(message).await,
        }
    }

    pub async fn shutdown(&self) -> Result<()> {
        match &self.inner {
            TransportKind::Auto(transport) => transport.shutdown().await,
            TransportKind::StreamableHttp(transport) => transport.shutdown().await,
            TransportKind::Sse(transport) => transport.shutdown().await,
        }
    }
}

impl AutoTransport {
    async fn send(&self, message: Value) -> Result<Option<Value>> {
        if let Some(transport) = self.resolved.get() {
            return transport.send(message).await;
        }

        if !message_is_initialize_request(&message) {
            return Err(Error::Message(
                "auto transport expected the first outbound message to be initialize".to_string(),
            ));
        }

        let client = build_client(&self.config)?;
        let streamable =
            StreamableHttpTransport::new(self.config.clone(), client, self.sender.clone());

        let (resolved, response) = match streamable.send_initial_probe(message.clone()).await {
            Ok(response) => {
                info!("transport probe selected streamable-http");
                (
                    ResolvedTransport::StreamableHttp(Box::new(streamable)),
                    response,
                )
            }
            Err(Error::CompatibilityFallback(status)) => {
                info!(status = %status, "transport probe falling back to legacy sse");
                let client = build_client(&self.config)?;
                let legacy = SseTransport::new(self.config.clone(), client, self.sender.clone());
                legacy.connect().await?;
                let response = legacy.send(message).await?;
                (ResolvedTransport::Sse(Box::new(legacy)), response)
            }
            Err(error) => return Err(error),
        };

        if self.resolved.set(resolved).is_err() {
            return Err(Error::Message(
                "auto transport received a second initialize request".to_string(),
            ));
        }
        Ok(response)
    }

    async fn shutdown(&self) -> Result<()> {
        if let Some(transport) = self.resolved.get() {
            return transport.shutdown().await;
        }
        Ok(())
    }
}

impl ResolvedTransport {
    async fn send(&self, message: Value) -> Result<Option<Value>> {
        match self {
            Self::StreamableHttp(transport) => transport.send(message).await,
            Self::Sse(transport) => transport.send(message).await,
        }
    }

    async fn shutdown(&self) -> Result<()> {
        match self {
            Self::StreamableHttp(transport) => transport.shutdown().await,
            Self::Sse(transport) => transport.shutdown().await,
        }
    }
}

impl StreamableHttpTransport {
    fn new(config: Config, client: reqwest::Client, sender: mpsc::Sender<TransportEvent>) -> Self {
        let (shutdown_tx, _) = watch::channel(false);
        Self {
            client,
            url: config.url,
            timeout: config.timeout,
            retry: config.retry,
            sender,
            session_id: Arc::new(RwLock::new(None)),
            protocol_version: Arc::new(RwLock::new(None)),
            last_event_id: Arc::new(RwLock::new(None)),
            listener_disabled: Arc::new(AtomicBool::new(false)),
            shutdown_tx,
            get_listener: Mutex::new(None),
            post_streams: Mutex::new(JoinSet::new()),
        }
    }

    async fn connect(&self) -> Result<()> {
        self.maybe_start_get_listener().await
    }

    async fn send_initial_probe(&self, message: Value) -> Result<Option<Value>> {
        self.send_internal(message, true).await
    }

    async fn send(&self, message: Value) -> Result<Option<Value>> {
        self.maybe_start_get_listener().await?;
        self.send_internal(message, false).await
    }

    async fn shutdown(&self) -> Result<()> {
        let _ = self.shutdown_tx.send(true);
        if let Some(handle) = self.get_listener.lock().await.take() {
            handle.abort();
        }
        self.post_streams.lock().await.abort_all();

        let session_id = { self.session_id.read().await.clone() };
        if let Some(session_id) = session_id {
            let mut request = self.client.delete(self.url.clone());
            request = request.header(HEADER_MCP_SESSION_ID, session_id);
            if let Some(protocol_version) = self.protocol_version.read().await.clone() {
                request = request.header(HEADER_MCP_PROTOCOL_VERSION, protocol_version);
            }

            match timeout(self.timeout, request.send()).await {
                Ok(Ok(response)) => {
                    if response.status() != StatusCode::METHOD_NOT_ALLOWED
                        && !response.status().is_success()
                    {
                        warn!(status = %response.status(), "session delete returned non-success status");
                    }
                }
                Ok(Err(error)) => warn!(error = %error, "session delete failed"),
                Err(_) => warn!("session delete timed out"),
            }
        }

        Ok(())
    }

    async fn send_internal(&self, message: Value, allow_fallback: bool) -> Result<Option<Value>> {
        let initialize_id = initialize_request_id(&message);
        let had_session = self.session_id.read().await.is_some();
        let limit = response_time_limit(&message, self.timeout);

        for attempt in 0..=self.retry.count {
            let response = match self.send_once(&message, limit).await {
                Ok(response) => response,
                Err(error) if is_undelivered(&error) && attempt < self.retry.count => {
                    let delay = backoff_delay(&self.retry, attempt, None);
                    warn!(error = %error, attempt, delay_ms = delay.as_millis(), "streamable-http send failed, retrying");
                    sleep(delay).await;
                    continue;
                }
                Err(error) => return Err(error),
            };

            if initialize_id.is_some()
                && allow_fallback
                && compatibility_fallback_status(response.status())
            {
                return Err(Error::CompatibilityFallback(response.status()));
            }

            if response.status() == StatusCode::METHOD_NOT_ALLOWED {
                return Err(Error::Message(
                    "remote MCP endpoint rejected POST".to_string(),
                ));
            }

            if is_retryable_post_status(response.status()) && attempt < self.retry.count {
                let delay = backoff_delay(&self.retry, attempt, None);
                warn!(
                    status = %response.status(),
                    attempt,
                    delay_ms = delay.as_millis(),
                    "streamable-http request returned retryable status"
                );
                sleep(delay).await;
                continue;
            }

            if had_session && unknown_session_status(response.status()) {
                let status = response.status();
                let body = read_response_text(response, self.timeout).await?;
                if status == StatusCode::NOT_FOUND || session_rejection_body(&body) {
                    self.clear_session().await;
                    return Err(Error::SessionExpired);
                }
                return Err(Error::Message(format!(
                    "remote MCP endpoint returned HTTP {status}: {body}"
                )));
            }

            if response.status() == StatusCode::ACCEPTED {
                return Ok(None);
            }

            if !response.status().is_success() {
                let status = response.status();
                let body = read_response_text(response, self.timeout).await?;
                return Err(Error::Message(format!(
                    "remote MCP endpoint returned HTTP {status}: {body}"
                )));
            }

            if let Some(session_id) = extract_header(response.headers(), HEADER_MCP_SESSION_ID) {
                *self.session_id.write().await = Some(session_id);
            }

            let content_type = response
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string)
                .unwrap_or_default();

            if content_type.starts_with(CONTENT_TYPE_JSON) {
                let value = read_response_json(response, limit).await?;
                if let Some(version) = extract_protocol_version(&value, initialize_id.as_ref()) {
                    *self.protocol_version.write().await = Some(version);
                }
                self.maybe_start_get_listener().await?;
                return Ok(Some(value));
            }

            if content_type.starts_with(CONTENT_TYPE_SSE) {
                {
                    let mut post_streams = self.post_streams.lock().await;
                    while post_streams.try_join_next().is_some() {}
                    post_streams.spawn(read_post_stream(
                        response,
                        self.sender.clone(),
                        self.last_event_id.clone(),
                        self.protocol_version.clone(),
                        initialize_id,
                    ));
                }
                self.maybe_start_get_listener().await?;
                return Ok(None);
            }

            return Err(Error::InvalidContentType(content_type));
        }

        Err(Error::Message("request retry loop exhausted".to_string()))
    }

    async fn send_once(&self, message: &Value, limit: Option<Duration>) -> Result<Response> {
        let mut request = self
            .client
            .post(self.url.clone())
            .header(CONTENT_TYPE, CONTENT_TYPE_JSON)
            .header(ACCEPT, format!("{CONTENT_TYPE_JSON}, {CONTENT_TYPE_SSE}"))
            .json(message);

        if let Some(session_id) = self.session_id.read().await.clone() {
            request = request.header(HEADER_MCP_SESSION_ID, session_id);
        }
        if let Some(protocol_version) = self.protocol_version.read().await.clone() {
            request = request.header(HEADER_MCP_PROTOCOL_VERSION, protocol_version);
        }

        within(limit, request.send()).await
    }

    async fn maybe_start_get_listener(&self) -> Result<()> {
        if self.listener_disabled.load(Ordering::SeqCst) {
            return Ok(());
        }

        let session_id = self.session_id.read().await.clone();
        let protocol_version = self.protocol_version.read().await.clone();
        if session_id.is_none() || protocol_version.is_none() {
            return Ok(());
        }

        let mut get_listener = self.get_listener.lock().await;
        if get_listener.is_some() {
            return Ok(());
        }

        let client = self.client.clone();
        let url = self.url.clone();
        let timeout_duration = self.timeout;
        let retry = self.retry.clone();
        let sender = self.sender.clone();
        let session_id = self.session_id.clone();
        let protocol_version = self.protocol_version.clone();
        let last_event_id = self.last_event_id.clone();
        let listener_disabled = self.listener_disabled.clone();
        let shutdown_rx = self.shutdown_tx.subscribe();

        *get_listener = Some(tokio::spawn(async move {
            let mut attempt = 0usize;
            let mut retry_hint = None;

            loop {
                if *shutdown_rx.borrow() {
                    break;
                }

                let current_session_id = session_id.read().await.clone();
                let current_protocol_version = protocol_version.read().await.clone();
                if current_session_id.is_none() || current_protocol_version.is_none() {
                    break;
                }

                let mut request = client.get(url.clone()).header(ACCEPT, CONTENT_TYPE_SSE);
                if let Some(value) = current_session_id {
                    request = request.header(HEADER_MCP_SESSION_ID, value);
                }
                if let Some(value) = current_protocol_version {
                    request = request.header(HEADER_MCP_PROTOCOL_VERSION, value);
                }
                if let Some(last_id) = last_event_id.read().await.clone() {
                    request = request.header(HEADER_LAST_EVENT_ID, last_id);
                }

                let response = match timeout(timeout_duration, request.send()).await {
                    Ok(Ok(response)) => response,
                    Ok(Err(error)) => {
                        if is_retryable_error(&error) && attempt < retry.count {
                            let delay = backoff_delay(&retry, attempt, retry_hint);
                            warn!(error = %error, attempt, delay_ms = delay.as_millis(), "streamable-http GET listener failed, retrying");
                            attempt += 1;
                            sleep(delay).await;
                            continue;
                        }
                        let _ = sender.send(TransportEvent::Fatal(Error::from(error))).await;
                        break;
                    }
                    Err(_) => {
                        if attempt < retry.count {
                            let delay = backoff_delay(&retry, attempt, retry_hint);
                            warn!(
                                attempt,
                                delay_ms = delay.as_millis(),
                                "streamable-http GET listener timed out, retrying"
                            );
                            attempt += 1;
                            sleep(delay).await;
                            continue;
                        }
                        let _ = sender
                            .send(TransportEvent::Fatal(Error::RequestTimeout(
                                timeout_duration,
                            )))
                            .await;
                        break;
                    }
                };

                if response.status() == StatusCode::METHOD_NOT_ALLOWED {
                    listener_disabled.store(true, Ordering::SeqCst);
                    info!("streamable-http GET listener disabled by remote 405");
                    break;
                }

                if response.status() == StatusCode::NOT_FOUND {
                    let _ = sender
                        .send(TransportEvent::Fatal(Error::SessionExpired))
                        .await;
                    break;
                }

                if !response.status().is_success() {
                    if attempt < retry.count {
                        let delay = backoff_delay(&retry, attempt, retry_hint);
                        warn!(status = %response.status(), attempt, delay_ms = delay.as_millis(), "streamable-http GET listener returned error status, retrying");
                        attempt += 1;
                        sleep(delay).await;
                        continue;
                    }
                    let _ = sender
                        .send(TransportEvent::Fatal(Error::Message(format!(
                            "streamable-http GET listener returned HTTP {}",
                            response.status()
                        ))))
                        .await;
                    break;
                }

                match read_streamable_events(
                    response,
                    &sender,
                    &last_event_id,
                    &protocol_version,
                    None,
                )
                .await
                {
                    Ok(Some(delay)) => {
                        retry_hint = Some(delay);
                    }
                    Ok(None) => {
                        retry_hint = None;
                    }
                    Err(error) => {
                        warn!(error = %error, "streamable-http GET listener ended with parse error")
                    }
                }

                attempt = 0;
                let delay = backoff_delay(&retry, attempt, retry_hint);
                debug!(
                    delay_ms = delay.as_millis(),
                    "streamable-http GET listener reconnecting"
                );
                sleep(delay).await;
            }
        }));

        Ok(())
    }

    async fn clear_session(&self) {
        *self.session_id.write().await = None;
        *self.protocol_version.write().await = None;
        *self.last_event_id.write().await = None;
    }
}

impl SseTransport {
    fn new(config: Config, client: reqwest::Client, sender: mpsc::Sender<TransportEvent>) -> Self {
        let (shutdown_tx, _) = watch::channel(false);
        Self {
            client,
            url: config.url,
            timeout: config.timeout,
            retry: config.retry,
            sender,
            endpoint: Arc::new(watch::Sender::new(None)),
            last_event_id: Arc::new(RwLock::new(None)),
            shutdown_tx,
            listener: Mutex::new(None),
        }
    }

    /// Starts the SSE listener if needed and waits for the server's `endpoint` event.
    async fn connect(&self) -> Result<Url> {
        let mut listener = self.listener.lock().await;
        if listener.is_none() {
            let client = self.client.clone();
            let url = self.url.clone();
            let timeout_duration = self.timeout;
            let retry = self.retry.clone();
            let sender = self.sender.clone();
            let endpoint = self.endpoint.clone();
            let last_event_id = self.last_event_id.clone();
            let shutdown_rx = self.shutdown_tx.subscribe();

            *listener = Some(tokio::spawn(async move {
                let mut attempt = 0usize;
                let mut retry_hint = None;

                loop {
                    if *shutdown_rx.borrow() {
                        break;
                    }

                    let mut request = client.get(url.clone()).header(ACCEPT, CONTENT_TYPE_SSE);
                    if let Some(last_id) = last_event_id.read().await.clone() {
                        request = request.header(HEADER_LAST_EVENT_ID, last_id);
                    }

                    let response = match timeout(timeout_duration, request.send()).await {
                        Ok(Ok(response)) => response,
                        Ok(Err(error)) => {
                            if is_retryable_error(&error) && attempt < retry.count {
                                let delay = backoff_delay(&retry, attempt, retry_hint);
                                warn!(error = %error, attempt, delay_ms = delay.as_millis(), "legacy sse listener failed, retrying");
                                attempt += 1;
                                sleep(delay).await;
                                continue;
                            }
                            let _ = sender.send(TransportEvent::Fatal(Error::from(error))).await;
                            break;
                        }
                        Err(_) => {
                            if attempt < retry.count {
                                let delay = backoff_delay(&retry, attempt, retry_hint);
                                warn!(
                                    attempt,
                                    delay_ms = delay.as_millis(),
                                    "legacy sse listener timed out, retrying"
                                );
                                attempt += 1;
                                sleep(delay).await;
                                continue;
                            }
                            let _ = sender
                                .send(TransportEvent::Fatal(Error::RequestTimeout(
                                    timeout_duration,
                                )))
                                .await;
                            break;
                        }
                    };

                    if !response.status().is_success() {
                        if attempt < retry.count {
                            let delay = backoff_delay(&retry, attempt, retry_hint);
                            warn!(status = %response.status(), attempt, delay_ms = delay.as_millis(), "legacy sse listener returned error status, retrying");
                            attempt += 1;
                            sleep(delay).await;
                            continue;
                        }
                        let _ = sender
                            .send(TransportEvent::Fatal(Error::Message(format!(
                                "legacy sse listener returned HTTP {}",
                                response.status()
                            ))))
                            .await;
                        break;
                    }

                    match read_legacy_events(response, &sender, &url, &endpoint, &last_event_id)
                        .await
                    {
                        Ok(Some(delay)) => retry_hint = Some(delay),
                        Ok(None) => retry_hint = None,
                        Err(error) => {
                            warn!(error = %error, "legacy sse listener ended with parse error")
                        }
                    }

                    attempt = 0;
                    let delay = backoff_delay(&retry, attempt, retry_hint);
                    sleep(delay).await;
                }
            }));
        }
        drop(listener);

        let mut endpoint = self.endpoint.subscribe();
        let resolved = match timeout(self.timeout, endpoint.wait_for(Option::is_some)).await {
            Ok(Ok(value)) => value.clone(),
            Ok(Err(_)) => return Err(Error::Shutdown),
            Err(_) => return Err(Error::RequestTimeout(self.timeout)),
        };
        resolved.ok_or_else(|| {
            Error::InvalidResponse("legacy SSE server never provided endpoint".to_string())
        })
    }

    async fn send(&self, message: Value) -> Result<Option<Value>> {
        let endpoint = self.connect().await?;

        for attempt in 0..=self.retry.count {
            let request = self
                .client
                .post(endpoint.clone())
                .header(CONTENT_TYPE, CONTENT_TYPE_JSON)
                .json(&message);

            match timeout(self.timeout, request.send()).await {
                Ok(Ok(response)) => {
                    if response.status().is_success() || response.status() == StatusCode::ACCEPTED {
                        return Ok(None);
                    }
                    if is_retryable_post_status(response.status()) && attempt < self.retry.count {
                        let delay = backoff_delay(&self.retry, attempt, None);
                        warn!(status = %response.status(), attempt, delay_ms = delay.as_millis(), "legacy sse POST returned retryable status");
                        sleep(delay).await;
                        continue;
                    }
                    let status = response.status();
                    let body = read_response_text(response, self.timeout).await?;
                    return Err(Error::Message(format!(
                        "legacy sse POST returned HTTP {status}: {body}"
                    )));
                }
                Ok(Err(error)) if is_undelivered_error(&error) && attempt < self.retry.count => {
                    let delay = backoff_delay(&self.retry, attempt, None);
                    warn!(error = %error, attempt, delay_ms = delay.as_millis(), "legacy sse POST failed, retrying");
                    sleep(delay).await;
                }
                Ok(Err(error)) => return Err(Error::from(error)),
                Err(_) => return Err(Error::RequestTimeout(self.timeout)),
            }
        }

        Err(Error::Message(
            "legacy sse POST retry loop exhausted".to_string(),
        ))
    }

    async fn shutdown(&self) -> Result<()> {
        let _ = self.shutdown_tx.send(true);
        if let Some(handle) = self.listener.lock().await.take() {
            handle.abort();
        }
        Ok(())
    }
}

fn build_client(config: &Config) -> Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .use_rustls_tls()
        .connect_timeout(config.timeout)
        .user_agent(USER_AGENT)
        .default_headers(config.headers.clone());

    if let Some(proxy) = build_proxy(config.socks5.as_ref())? {
        builder = builder.proxy(proxy);
    }

    builder.build().map_err(Into::into)
}

fn build_proxy(config: Option<&Socks5Config>) -> Result<Option<reqwest::Proxy>> {
    match config {
        Some(proxy) => {
            let proxy_url = match &proxy.auth {
                Some(auth) => format!(
                    "socks5h://{}:{}@{}",
                    auth.username, auth.password, proxy.address
                ),
                None => format!("socks5h://{}", proxy.address),
            };
            Ok(Some(reqwest::Proxy::all(&proxy_url)?))
        }
        None => Ok(None),
    }
}

fn initialize_request_id(message: &Value) -> Option<Value> {
    message
        .as_object()
        .and_then(|object| {
            object
                .get("method")
                .and_then(Value::as_str)
                .filter(|value| *value == "initialize")
        })
        .and_then(|_| {
            message
                .as_object()
                .and_then(|object| object.get("id").cloned())
        })
}

fn message_is_initialize_request(message: &Value) -> bool {
    initialize_request_id(message).is_some()
}

/// JSON-RPC requests other than `initialize` can legitimately run for a long time (slow
/// tool calls), so only connection setup bounds them. MCP clients enforce their own
/// request timeouts and cancel through `notifications/cancelled`.
fn response_time_limit(message: &Value, timeout: Duration) -> Option<Duration> {
    let is_request = message.get("method").is_some() && message.get("id").is_some();
    if is_request && !message_is_initialize_request(message) {
        None
    } else {
        Some(timeout)
    }
}

async fn within<T>(
    limit: Option<Duration>,
    future: impl Future<Output = reqwest::Result<T>>,
) -> Result<T> {
    match limit {
        Some(limit) => match timeout(limit, future).await {
            Ok(result) => result.map_err(Into::into),
            Err(_) => Err(Error::RequestTimeout(limit)),
        },
        None => future.await.map_err(Into::into),
    }
}

fn extract_protocol_version(value: &Value, expected_id: Option<&Value>) -> Option<String> {
    if let Some(values) = value.as_array() {
        for entry in values {
            if let Some(version) = extract_protocol_version(entry, expected_id) {
                return Some(version);
            }
        }
        return None;
    }

    let object = value.as_object()?;
    if let Some(expected_id) = expected_id {
        let actual_id = object.get("id")?;
        if actual_id != expected_id {
            return None;
        }
    }

    object
        .get("result")
        .and_then(Value::as_object)
        .and_then(|result| result.get("protocolVersion"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn extract_header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(ToString::to_string)
}

fn is_undelivered(error: &Error) -> bool {
    matches!(error, Error::Http(error) if is_undelivered_error(error))
}

async fn read_response_json(response: Response, limit: Option<Duration>) -> Result<Value> {
    let bytes = within(limit, response.bytes()).await?;
    serde_json::from_slice(&bytes).map_err(Into::into)
}

async fn read_response_text(response: Response, limit: Duration) -> Result<String> {
    within(Some(limit), response.text()).await
}

fn session_rejection_body(body: &str) -> bool {
    let lower = body.to_ascii_lowercase();
    lower.contains("session")
        && (lower.contains("unknown") || lower.contains("invalid") || lower.contains("expired"))
}

async fn read_streamable_events(
    response: Response,
    sender: &mpsc::Sender<TransportEvent>,
    last_event_id: &Arc<RwLock<Option<String>>>,
    protocol_version: &Arc<RwLock<Option<String>>>,
    initialize_id: Option<Value>,
) -> Result<Option<Duration>> {
    let mut parser = SseParser::new();
    let mut stream = response.bytes_stream();
    let mut retry_hint = None;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(Error::from)?;
        for event in parser.feed(&chunk)? {
            if let Some(id) = event.id.clone() {
                *last_event_id.write().await = Some(id);
            }
            if event.retry.is_some() {
                retry_hint = event.retry;
            }
            if let Some(message) = parse_json_message_from_event(&event)? {
                if let Some(version) = extract_protocol_version(&message, initialize_id.as_ref()) {
                    *protocol_version.write().await = Some(version);
                }
                let _ = sender.send(TransportEvent::Message(message)).await;
            }
        }
    }

    if let Some(event) = parser.finish() {
        if let Some(id) = event.id.clone() {
            *last_event_id.write().await = Some(id);
        }
        if event.retry.is_some() {
            retry_hint = event.retry;
        }
        if let Some(message) = parse_json_message_from_event(&event)? {
            if let Some(version) = extract_protocol_version(&message, initialize_id.as_ref()) {
                *protocol_version.write().await = Some(version);
            }
            let _ = sender.send(TransportEvent::Message(message)).await;
        }
    }

    Ok(retry_hint)
}

async fn read_legacy_events(
    response: Response,
    sender: &mpsc::Sender<TransportEvent>,
    base_url: &Url,
    endpoint: &watch::Sender<Option<Url>>,
    last_event_id: &Arc<RwLock<Option<String>>>,
) -> Result<Option<Duration>> {
    let mut parser = SseParser::new();
    let mut stream = response.bytes_stream();
    let mut retry_hint = None;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(Error::from)?;
        for event in parser.feed(&chunk)? {
            if let Some(id) = event.id.clone() {
                *last_event_id.write().await = Some(id);
            }
            if event.retry.is_some() {
                retry_hint = event.retry;
            }
            handle_legacy_event(event, sender, base_url, endpoint).await?;
        }
    }

    if let Some(event) = parser.finish() {
        if let Some(id) = event.id.clone() {
            *last_event_id.write().await = Some(id);
        }
        if event.retry.is_some() {
            retry_hint = event.retry;
        }
        handle_legacy_event(event, sender, base_url, endpoint).await?;
    }

    Ok(retry_hint)
}

async fn handle_legacy_event(
    event: SseEvent,
    sender: &mpsc::Sender<TransportEvent>,
    base_url: &Url,
    endpoint: &watch::Sender<Option<Url>>,
) -> Result<()> {
    if event.event == "endpoint" {
        let resolved = base_url
            .join(&event.data)
            .or_else(|_| Url::parse(&event.data))
            .map_err(|error| Error::Message(format!("invalid endpoint event URL: {error}")))?;
        endpoint.send_replace(Some(resolved));
        return Ok(());
    }

    if event.event == "message" {
        if let Some(message) = parse_json_message_from_event(&event)? {
            let _ = sender.send(TransportEvent::Message(message)).await;
        }
    }

    Ok(())
}

async fn read_post_stream(
    response: Response,
    sender: mpsc::Sender<TransportEvent>,
    last_event_id: Arc<RwLock<Option<String>>>,
    protocol_version: Arc<RwLock<Option<String>>>,
    initialize_id: Option<Value>,
) {
    if let Err(error) = read_streamable_events(
        response,
        &sender,
        &last_event_id,
        &protocol_version,
        initialize_id,
    )
    .await
    {
        warn!(error = %error, "streamable SSE response reader failed");
    }
}

fn parse_json_message_from_event(event: &SseEvent) -> Result<Option<Value>> {
    if event.data.is_empty() {
        return Ok(None);
    }
    serde_json::from_str(&event.data)
        .map(Some)
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::transport::{
        extract_protocol_version, initialize_request_id, message_is_initialize_request,
    };

    #[test]
    fn detects_initialize_requests() {
        let message = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-11-25"
            }
        });

        assert!(message_is_initialize_request(&message));
        assert_eq!(initialize_request_id(&message), Some(json!(1)));
    }

    #[test]
    fn extracts_protocol_version_from_response() {
        let response = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "protocolVersion": "2025-11-25"
            }
        });

        assert_eq!(
            extract_protocol_version(&response, Some(&json!(1))),
            Some("2025-11-25".to_string())
        );
    }
}
