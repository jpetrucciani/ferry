use std::collections::{BTreeSet, HashMap, VecDeque};
use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, MutexGuard, PoisonError, Weak};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use axum::extract::{Query, State};
use axum::http::header::{AUTHORIZATION, ORIGIN};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::{get, post};
use axum::{body::Body, Router};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, info, warn};

use crate::cli::{ServeBackendCli, ServeCli, ServeRelayCli, ServeStdioCli};
use crate::config::{Config, LogLevel, LoggingConfig, RetryConfig, TransportMode};
use crate::error::{Error, Result};
use crate::stdio::{read_message_from, write_message_to};
use crate::transport::{build_transport, TransportEvent, TransportHandle};

const DEFAULT_TIMEOUT_SECONDS: u64 = 30;
const DEFAULT_RETRY_COUNT: usize = 3;
const DEFAULT_RETRY_DELAY_MS: u64 = 1_000;
const DEFAULT_IDLE_TIMEOUT_SECONDS: u64 = 1_800;
const HEADER_MCP_SESSION_ID: &str = "MCP-Session-Id";
const HEADER_MCP_PROTOCOL_VERSION: &str = "MCP-Protocol-Version";
const HEADER_LAST_EVENT_ID: &str = "Last-Event-ID";
const GET_STREAM_CAPACITY: usize = 256;
const SSE_REPLAY_HISTORY_LIMIT: usize = 256;
const QUEUED_MESSAGE_LIMIT: usize = 1_024;
const RELAY_EVENT_CHANNEL_CAPACITY: usize = 256;
const IDLE_REAPER_MAX_INTERVAL: Duration = Duration::from_secs(60);
const LEGACY_SESSION_RECONNECT_GRACE: Duration = Duration::from_secs(30);
const SESSION_SHUTDOWN_GRACE: Duration = Duration::from_secs(3);

#[derive(Clone, Debug)]
pub struct ServeConfig {
    pub listen: SocketAddr,
    pub mcp_path: String,
    pub legacy_sse_path: String,
    pub legacy_messages_path: String,
    pub health_path: String,
    pub auth: ServeAuthConfig,
    pub allow_origins: Vec<HeaderValue>,
    pub idle_timeout: Duration,
    pub logging: LoggingConfig,
    pub backend: ServeBackendConfig,
}

#[derive(Clone, Debug, Default)]
pub struct ServeAuthConfig {
    pub bearer_token: Option<String>,
    pub required_headers: Vec<(HeaderName, HeaderValue)>,
}

#[derive(Clone, Debug)]
pub enum ServeBackendConfig {
    Stdio(ServeStdioConfig),
    Relay(ServeRelayConfig),
}

#[derive(Clone, Debug)]
pub struct ServeStdioConfig {
    pub command: String,
    pub args: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct ServeRelayConfig {
    pub url: Url,
    pub transport: TransportMode,
    pub headers: Vec<(HeaderName, HeaderValue)>,
    pub timeout_seconds: u64,
    pub retry_count: usize,
    pub retry_delay_ms: u64,
}

#[derive(Clone)]
struct AppState {
    config: Arc<ServeConfig>,
    sessions: SessionRegistry,
}

#[derive(Clone)]
struct SessionRegistry {
    inner: Arc<SessionRegistryInner>,
}

struct SessionRegistryInner {
    backend: ServeBackendConfig,
    sessions: RwLock<HashMap<String, Arc<SessionState>>>,
}

struct SessionState {
    id: String,
    registry: SessionRegistry,
    backend: SessionBackend,
    protocol_version: RwLock<Option<String>>,
    active_get_stream: Mutex<Option<ActiveGetStream>>,
    replay_history: Mutex<VecDeque<SseMessage>>,
    queued_messages: Mutex<VecDeque<SseMessage>>,
    pending_requests: std::sync::Mutex<HashMap<String, PendingRequestTarget>>,
    last_activity: std::sync::Mutex<Instant>,
    legacy_cleanup_task: Mutex<Option<JoinHandle<()>>>,
    legacy_session: bool,
    next_get_stream_id: AtomicU64,
    next_event_sequence: AtomicU64,
    closed: AtomicBool,
}

struct StdioSessionBackend {
    writer: Mutex<BufWriter<ChildStdin>>,
    child: Mutex<Child>,
}

struct RelaySessionBackend {
    transport: TransportHandle,
}

enum SessionBackend {
    Stdio(Box<StdioSessionBackend>),
    Relay(RelaySessionBackend),
}

#[derive(Clone)]
struct ActiveGetStream {
    stream_id: u64,
    sender: mpsc::Sender<SseMessage>,
}

#[derive(Clone)]
struct SseMessage {
    event_id: String,
    event_sequence: u64,
    event: Option<String>,
    data: String,
}

struct SseEventStream {
    inner: ReceiverStream<SseMessage>,
    drop_action: Option<SseDropAction>,
}

struct SseDropAction {
    sessions: SessionRegistry,
    session_id: String,
}

struct ParsedSseEventId {
    session_id: String,
    sequence: u64,
}

enum PendingRequestTarget {
    Http(oneshot::Sender<Result<Value>>),
    Sse,
}

/// Owns the response channel for an HTTP request routed to the backend. Dropping it (the
/// response arrived, the send failed, or the HTTP client disconnected) releases the request id
/// so a later request can reuse it.
struct PendingHttpRequest<'a> {
    session: &'a SessionState,
    id_key: String,
    receiver: Option<oneshot::Receiver<Result<Value>>>,
}

impl Drop for PendingHttpRequest<'_> {
    fn drop(&mut self) {
        drop(self.receiver.take());
        let mut pending = self.session.pending_requests();
        // Only remove our own entry; a closed sender means its receiver (ours) is gone.
        if matches!(
            pending.get(&self.id_key),
            Some(PendingRequestTarget::Http(sender)) if sender.is_closed()
        ) {
            pending.remove(&self.id_key);
        }
    }
}

enum InitializeOutcome {
    Ready { session_id: String, response: Value },
    Rejected(Value),
}

#[derive(Debug)]
struct HttpError {
    status: StatusCode,
    message: String,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    backend: &'static str,
    mcp_path: String,
    legacy_sse_path: String,
    legacy_messages_path: String,
}

#[derive(Debug)]
enum JsonRpcMessageKind {
    Request { method: String, id_key: String },
    Notification,
    Response,
}

#[derive(Debug, Deserialize)]
struct LegacySessionQuery {
    session_id: String,
}

impl ServeConfig {
    pub fn from_cli(cli: ServeCli) -> Result<Self> {
        let mcp_path = validate_path(cli.path.as_deref().unwrap_or("/mcp"), "mcp path")?;
        let legacy_sse_path = validate_path(
            cli.legacy_sse_path.as_deref().unwrap_or("/sse"),
            "legacy SSE path",
        )?;
        let legacy_messages_path = validate_path(
            cli.legacy_messages_path.as_deref().unwrap_or("/messages"),
            "legacy messages path",
        )?;
        let health_path = validate_path(
            cli.health_path.as_deref().unwrap_or("/health"),
            "health path",
        )?;
        validate_unique_paths([
            mcp_path.as_str(),
            legacy_sse_path.as_str(),
            legacy_messages_path.as_str(),
            health_path.as_str(),
        ])?;

        let allow_origins = parse_header_values(&cli.allow_origins, "allow-origin")?;
        let auth = ServeAuthConfig {
            bearer_token: cli.auth_bearer_token,
            required_headers: parse_header_pairs(&cli.auth_headers)?,
        };

        let logging = LoggingConfig {
            level: cli.log_level.map(LogLevel::from).unwrap_or(LogLevel::Warn),
            file: cli.log_file,
        };

        let idle_timeout_seconds = cli.idle_timeout.unwrap_or(DEFAULT_IDLE_TIMEOUT_SECONDS);
        if idle_timeout_seconds == 0 {
            return Err(Error::Message(
                "serve idle timeout must be at least 1 second".to_string(),
            ));
        }

        let backend = match cli.backend {
            Some(ServeBackendCli::Stdio(stdio)) => {
                ServeBackendConfig::Stdio(parse_stdio_backend(stdio)?)
            }
            Some(ServeBackendCli::Relay(relay)) => {
                ServeBackendConfig::Relay(parse_relay_backend(relay)?)
            }
            None => {
                return Err(Error::Message(
                    "missing serve backend, expected `stdio` or `relay`".to_string(),
                ))
            }
        };

        Ok(Self {
            listen: cli
                .listen
                .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 0))),
            mcp_path,
            legacy_sse_path,
            legacy_messages_path,
            health_path,
            auth,
            allow_origins,
            idle_timeout: Duration::from_secs(idle_timeout_seconds),
            logging,
            backend,
        })
    }

    pub fn warns_on_unprotected_non_loopback(&self) -> bool {
        !self.listen.ip().is_loopback() && !self.auth.is_configured()
    }
}

impl ServeAuthConfig {
    fn is_configured(&self) -> bool {
        self.bearer_token.is_some() || !self.required_headers.is_empty()
    }
}

impl ServeBackendConfig {
    fn name(&self) -> &'static str {
        match self {
            Self::Stdio(_) => "stdio",
            Self::Relay(_) => "relay",
        }
    }
}

impl SseEventStream {
    fn new(receiver: mpsc::Receiver<SseMessage>, drop_action: Option<SseDropAction>) -> Self {
        Self {
            inner: ReceiverStream::new(receiver),
            drop_action,
        }
    }
}

impl tokio_stream::Stream for SseEventStream {
    type Item = std::result::Result<Event, Infallible>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_next(context) {
            Poll::Ready(Some(message)) => {
                let mut event = Event::default().id(message.event_id);
                if let Some(name) = message.event {
                    event = event.event(name);
                }
                Poll::Ready(Some(Ok(event.data(message.data))))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for SseEventStream {
    fn drop(&mut self) {
        let Some(action) = self.drop_action.take() else {
            return;
        };

        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };

        handle.spawn(async move {
            if let Some(session) = action.sessions.get(&action.session_id).await {
                session.schedule_legacy_cleanup().await;
            }
        });
    }
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        (self.status, self.message).into_response()
    }
}

impl HttpError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: message.into(),
        }
    }

    fn forbidden(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            message: message.into(),
        }
    }

    fn conflict(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }

    fn bad_gateway(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_GATEWAY,
            message: message.into(),
        }
    }
}

impl SessionRegistry {
    fn new(backend: ServeBackendConfig) -> Self {
        Self {
            inner: Arc::new(SessionRegistryInner {
                backend,
                sessions: RwLock::new(HashMap::new()),
            }),
        }
    }

    /// Session ids double as the legacy `/messages` capability, so they must be unguessable.
    fn next_session_id(&self) -> Result<String> {
        let mut bytes = [0_u8; 16];
        getrandom::getrandom(&mut bytes)
            .map_err(|error| Error::Message(format!("failed to generate session id: {error}")))?;
        let hex = bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        Ok(format!("ferry-{hex}"))
    }

    async fn spawn_session(
        &self,
        session_id: String,
        legacy_session: bool,
    ) -> Result<Arc<SessionState>> {
        match &self.inner.backend {
            ServeBackendConfig::Stdio(config) => {
                SessionState::spawn_stdio(session_id, self.clone(), config.clone(), legacy_session)
                    .await
            }
            ServeBackendConfig::Relay(config) => {
                SessionState::spawn_relay(session_id, self.clone(), config.clone(), legacy_session)
                    .await
            }
        }
    }

    async fn create_legacy_session(&self) -> Result<(String, Arc<SessionState>)> {
        let session_id = self.next_session_id()?;
        let session = self.spawn_session(session_id.clone(), true).await?;
        self.inner
            .sessions
            .write()
            .await
            .insert(session_id.clone(), Arc::clone(&session));
        Ok((session_id, session))
    }

    async fn initialize_session(&self, message: Value) -> Result<InitializeOutcome> {
        let initialize_id = request_id(&message).ok_or_else(|| {
            Error::InvalidResponse("initialize request was missing an id".to_string())
        })?;
        let session_id = self.next_session_id()?;
        let session = self.spawn_session(session_id.clone(), false).await?;

        let response = match session.send_request(message).await {
            Ok(response) => response,
            Err(error) => {
                let _ = session.shutdown().await;
                return Err(error);
            }
        };

        let Some(protocol_version) = extract_protocol_version(&response, Some(&initialize_id))
        else {
            let _ = session.shutdown().await;
            // A JSON-RPC error (for example an unsupported protocol version) belongs to the
            // client; anything else is a broken backend.
            if response.get("error").is_some() {
                return Ok(InitializeOutcome::Rejected(response));
            }
            return Err(Error::InvalidResponse(
                "initialize response missing result.protocolVersion".to_string(),
            ));
        };
        *session.protocol_version.write().await = Some(protocol_version);

        self.inner
            .sessions
            .write()
            .await
            .insert(session_id.clone(), session);

        Ok(InitializeOutcome::Ready {
            session_id,
            response,
        })
    }

    async fn get(&self, session_id: &str) -> Option<Arc<SessionState>> {
        self.inner.sessions.read().await.get(session_id).cloned()
    }

    async fn remove(&self, session_id: &str) -> Option<Arc<SessionState>> {
        self.inner.sessions.write().await.remove(session_id)
    }

    /// Shuts down sessions that clients abandoned without a DELETE.
    fn spawn_idle_reaper(&self, idle_timeout: Duration) {
        let registry = Arc::downgrade(&self.inner);
        let interval = (idle_timeout / 4).clamp(Duration::from_secs(1), IDLE_REAPER_MAX_INTERVAL);
        tokio::spawn(async move {
            loop {
                sleep(interval).await;
                let Some(inner) = registry.upgrade() else {
                    break;
                };
                let registry = SessionRegistry { inner };

                let sessions = registry
                    .inner
                    .sessions
                    .read()
                    .await
                    .values()
                    .cloned()
                    .collect::<Vec<_>>();
                for session in sessions {
                    if !session.is_idle(idle_timeout).await {
                        continue;
                    }
                    if registry.remove(&session.id).await.is_some() {
                        info!(session_id = %session.id, "shutting down idle session");
                        if let Err(error) = session.shutdown().await {
                            warn!(session_id = %session.id, error = %error, "failed to shut down idle session");
                        }
                    }
                }
            }
        });
    }
}

impl SessionState {
    async fn spawn_stdio(
        session_id: String,
        registry: SessionRegistry,
        config: ServeStdioConfig,
        legacy_session: bool,
    ) -> Result<Arc<Self>> {
        let mut command = Command::new(&config.command);
        command
            .args(&config.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        let mut child = command.spawn().map_err(|error| {
            Error::Message(format!(
                "failed to spawn stdio backend '{}': {error}",
                config.command
            ))
        })?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| Error::Message("stdio backend did not expose stdin".to_string()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| Error::Message("stdio backend did not expose stdout".to_string()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| Error::Message("stdio backend did not expose stderr".to_string()))?;

        let session = Arc::new(Self {
            id: session_id,
            registry,
            backend: SessionBackend::Stdio(Box::new(StdioSessionBackend {
                writer: Mutex::new(BufWriter::new(stdin)),
                child: Mutex::new(child),
            })),
            protocol_version: RwLock::new(None),
            active_get_stream: Mutex::new(None),
            replay_history: Mutex::new(VecDeque::new()),
            queued_messages: Mutex::new(VecDeque::new()),
            pending_requests: std::sync::Mutex::new(HashMap::new()),
            last_activity: std::sync::Mutex::new(Instant::now()),
            legacy_cleanup_task: Mutex::new(None),
            legacy_session,
            next_get_stream_id: AtomicU64::new(1),
            next_event_sequence: AtomicU64::new(1),
            closed: AtomicBool::new(false),
        });

        session.start_stdio_tasks(stdout, stderr);
        Ok(session)
    }

    async fn spawn_relay(
        session_id: String,
        registry: SessionRegistry,
        config: ServeRelayConfig,
        legacy_session: bool,
    ) -> Result<Arc<Self>> {
        let (sender, receiver) = mpsc::channel(RELAY_EVENT_CHANNEL_CAPACITY);
        let transport = build_transport(build_relay_transport_config(&config)?, sender)?;
        transport.connect().await?;

        let session = Arc::new(Self {
            id: session_id,
            registry,
            backend: SessionBackend::Relay(RelaySessionBackend { transport }),
            protocol_version: RwLock::new(None),
            active_get_stream: Mutex::new(None),
            replay_history: Mutex::new(VecDeque::new()),
            queued_messages: Mutex::new(VecDeque::new()),
            pending_requests: std::sync::Mutex::new(HashMap::new()),
            last_activity: std::sync::Mutex::new(Instant::now()),
            legacy_cleanup_task: Mutex::new(None),
            legacy_session,
            next_get_stream_id: AtomicU64::new(1),
            next_event_sequence: AtomicU64::new(1),
            closed: AtomicBool::new(false),
        });

        session.start_relay_tasks(receiver);
        Ok(session)
    }

    fn start_stdio_tasks(
        self: &Arc<Self>,
        stdout: tokio::process::ChildStdout,
        stderr: tokio::process::ChildStderr,
    ) {
        let session = Arc::clone(self);
        tokio::spawn(async move {
            let mut reader = BufReader::new(stdout);
            loop {
                match read_message_from(&mut reader).await {
                    Ok(Some(message)) => {
                        if let Err(error) = session.handle_backend_message(message).await {
                            error!(session_id = %session.id, error = %error, "failed to route backend message");
                            session.fail(error).await;
                            break;
                        }
                    }
                    Ok(None) => {
                        session
                            .fail(Error::Message(
                                "stdio backend closed stdout unexpectedly".to_string(),
                            ))
                            .await;
                        break;
                    }
                    Err(error) => {
                        session.fail(error).await;
                        break;
                    }
                }
            }
        });

        let session_id = self.id.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            loop {
                match lines.next_line().await {
                    Ok(Some(line)) => {
                        warn!(session_id = %session_id, stderr = %line, "stdio backend stderr");
                    }
                    Ok(None) => break,
                    Err(error) => {
                        warn!(session_id = %session_id, error = %error, "failed reading stdio backend stderr");
                        break;
                    }
                }
            }
        });
    }

    fn start_relay_tasks(self: &Arc<Self>, mut receiver: mpsc::Receiver<TransportEvent>) {
        let session = Arc::downgrade(self);
        tokio::spawn(async move {
            while let Some(event) = receiver.recv().await {
                let Some(session) = Weak::upgrade(&session) else {
                    break;
                };

                match event {
                    TransportEvent::Message(message) => {
                        if let Err(error) = session.handle_backend_message(message).await {
                            error!(session_id = %session.id, error = %error, "failed to route relay backend message");
                            session.fail(error).await;
                            break;
                        }
                    }
                    TransportEvent::Fatal(error) => {
                        session.fail(error).await;
                        break;
                    }
                }
            }
        });
    }

    async fn send_request(&self, message: Value) -> Result<Value> {
        let id = request_id(&message)
            .ok_or_else(|| Error::Message("request is missing a JSON-RPC id".to_string()))?;
        let id_key = jsonrpc_id_key(&id)?;
        let (sender, receiver) = oneshot::channel();

        self.insert_pending_request(id_key.clone(), PendingRequestTarget::Http(sender))?;
        let mut pending = PendingHttpRequest {
            session: self,
            id_key,
            receiver: Some(receiver),
        };

        if let Some(response) = self.backend.send(&message).await? {
            self.record_protocol_version_from_message(&response).await;
            return Ok(response);
        }

        let Some(receiver) = pending.receiver.as_mut() else {
            return Err(Error::Message(
                "pending request lost its receiver".to_string(),
            ));
        };
        match receiver.await {
            Ok(result) => result,
            Err(_) => Err(Error::Message(
                "request was cancelled before a backend response arrived".to_string(),
            )),
        }
    }

    async fn send_request_to_stream(&self, message: Value) -> Result<()> {
        let id = request_id(&message)
            .ok_or_else(|| Error::Message("request is missing a JSON-RPC id".to_string()))?;
        let id_key = jsonrpc_id_key(&id)?;
        self.insert_pending_request(id_key.clone(), PendingRequestTarget::Sse)?;

        let direct_response = match self.backend.send(&message).await {
            Ok(response) => response,
            Err(error) => {
                self.pending_requests().remove(&id_key);
                return Err(error);
            }
        };

        if let Some(response) = direct_response {
            self.pending_requests().remove(&id_key);
            self.record_protocol_version_from_message(&response).await;
            self.queue_backend_message(response).await?;
        }

        Ok(())
    }

    fn pending_requests(&self) -> MutexGuard<'_, HashMap<String, PendingRequestTarget>> {
        self.pending_requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn touch(&self) {
        *self
            .last_activity
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Instant::now();
    }

    async fn is_idle(&self, idle_timeout: Duration) -> bool {
        let last_activity = *self
            .last_activity
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if last_activity.elapsed() < idle_timeout || !self.pending_requests().is_empty() {
            return false;
        }

        let stream_open = self
            .active_get_stream
            .lock()
            .await
            .as_ref()
            .is_some_and(|stream| !stream.sender.is_closed());
        !stream_open
    }

    fn insert_pending_request(&self, id_key: String, target: PendingRequestTarget) -> Result<()> {
        let mut pending = self.pending_requests();
        if pending.contains_key(&id_key) {
            return Err(Error::Message(format!(
                "request id {id_key} is already outstanding for this session"
            )));
        }
        pending.insert(id_key, target);
        Ok(())
    }

    fn has_outstanding_request(&self, id_key: &str) -> bool {
        self.pending_requests().contains_key(id_key)
    }

    async fn send_oneway(&self, message: Value) -> Result<()> {
        self.backend.send(&message).await.map(|_| ())
    }

    async fn open_get_stream(
        &self,
        replay_after_sequence: Option<u64>,
    ) -> (mpsc::Receiver<SseMessage>, bool) {
        self.cancel_legacy_cleanup().await;

        let stream_id = self.next_get_stream_id.fetch_add(1, Ordering::SeqCst);
        let (sender, receiver) = mpsc::channel(GET_STREAM_CAPACITY);
        let replaced = {
            let mut active = self.active_get_stream.lock().await;
            active
                .replace(ActiveGetStream {
                    stream_id,
                    sender: sender.clone(),
                })
                .is_some()
        };

        let queued_messages = if let Some(after_sequence) = replay_after_sequence {
            let replay_messages = {
                let history = self.replay_history.lock().await;
                history
                    .iter()
                    .filter(|message| message.event_sequence > after_sequence)
                    .cloned()
                    .collect::<Vec<_>>()
            };
            if !replay_messages.is_empty() {
                let replayed_sequences = replay_messages
                    .iter()
                    .map(|message| message.event_sequence)
                    .collect::<BTreeSet<_>>();
                self.queued_messages
                    .lock()
                    .await
                    .retain(|message| !replayed_sequences.contains(&message.event_sequence));
            }
            replay_messages
        } else {
            let mut queued = self.queued_messages.lock().await;
            queued.drain(..).collect::<Vec<_>>()
        };

        for message in queued_messages {
            if sender.send(message.clone()).await.is_err() {
                self.queued_messages.lock().await.push_front(message);
                break;
            }
        }

        (receiver, replaced)
    }

    async fn queue_backend_message(&self, message: Value) -> Result<()> {
        self.queue_sse_message(None, serde_json::to_string(&message)?)
            .await
    }

    async fn queue_endpoint_message(&self, endpoint: String) -> Result<()> {
        self.queue_sse_message(Some("endpoint"), endpoint).await
    }

    async fn queue_sse_message(&self, event: Option<&str>, data: String) -> Result<()> {
        let event_sequence = self.next_event_sequence.fetch_add(1, Ordering::SeqCst);
        let queued = SseMessage {
            event_id: format_sse_event_id(&self.id, event_sequence),
            event_sequence,
            event: event.map(ToString::to_string),
            data,
        };

        {
            let mut history = self.replay_history.lock().await;
            history.push_back(queued.clone());
            while history.len() > SSE_REPLAY_HISTORY_LIMIT {
                history.pop_front();
            }
        }

        let active = { self.active_get_stream.lock().await.clone() };
        if let Some(active) = active {
            if active.sender.send(queued.clone()).await.is_ok() {
                return Ok(());
            }

            let mut current = self.active_get_stream.lock().await;
            if current
                .as_ref()
                .map(|stream| stream.stream_id == active.stream_id)
                .unwrap_or(false)
            {
                current.take();
            }
        }

        let mut queued_messages = self.queued_messages.lock().await;
        if queued_messages.len() >= QUEUED_MESSAGE_LIMIT {
            queued_messages.pop_front();
            warn!(
                session_id = %self.id,
                limit = QUEUED_MESSAGE_LIMIT,
                "no client stream is open, dropping the oldest queued server message"
            );
        }
        queued_messages.push_back(queued);
        Ok(())
    }

    async fn handle_backend_message(&self, message: Value) -> Result<()> {
        self.record_protocol_version_from_message(&message).await;

        if let Some(id) = response_id(&message) {
            let key = jsonrpc_id_key(&id)?;
            let pending = self.pending_requests().remove(&key);
            if let Some(target) = pending {
                match target {
                    PendingRequestTarget::Http(sender) => {
                        let _ = sender.send(Ok(message));
                    }
                    PendingRequestTarget::Sse => {
                        self.queue_backend_message(message).await?;
                    }
                }
                return Ok(());
            }
        }

        self.queue_backend_message(message).await
    }

    async fn record_protocol_version_from_message(&self, message: &Value) {
        if let Some(protocol_version) = extract_protocol_version(message, None) {
            *self.protocol_version.write().await = Some(protocol_version);
        }
    }

    async fn cancel_legacy_cleanup(&self) {
        if let Some(task) = self.legacy_cleanup_task.lock().await.take() {
            task.abort();
        }
    }

    async fn schedule_legacy_cleanup(&self) {
        if !self.legacy_session || self.closed.load(Ordering::SeqCst) {
            return;
        }

        let mut cleanup_task = self.legacy_cleanup_task.lock().await;
        if cleanup_task.is_some() {
            return;
        }

        let registry = self.registry.clone();
        let session_id = self.id.clone();
        *cleanup_task = Some(tokio::spawn(async move {
            sleep(LEGACY_SESSION_RECONNECT_GRACE).await;
            if let Some(session) = registry.remove(&session_id).await {
                if let Err(error) = session.shutdown_for_cleanup().await {
                    warn!(session_id = %session_id, error = %error, "failed to shut down expired legacy SSE session");
                }
            }
        }));
    }

    async fn fail(&self, error: Error) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }

        error!(session_id = %self.id, error = %error, "session backend failed");

        self.registry.remove(&self.id).await;
        self.cancel_legacy_cleanup().await;
        self.active_get_stream.lock().await.take();

        let pending = self.pending_requests().drain().collect::<Vec<_>>();
        for (_, target) in pending {
            if let PendingRequestTarget::Http(sender) = target {
                let _ = sender.send(Err(Error::Message(format!(
                    "session {} failed: {error}",
                    self.id
                ))));
            }
        }

        if let Err(shutdown_error) = self.backend.shutdown().await {
            warn!(session_id = %self.id, error = %shutdown_error, "failed to shut down backend after session failure");
        }
    }

    async fn shutdown(&self) -> Result<()> {
        self.shutdown_inner(true).await
    }

    async fn shutdown_for_cleanup(&self) -> Result<()> {
        self.shutdown_inner(false).await
    }

    async fn shutdown_inner(&self, cancel_cleanup_task: bool) -> Result<()> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        if cancel_cleanup_task {
            self.cancel_legacy_cleanup().await;
        } else {
            self.legacy_cleanup_task.lock().await.take();
        }
        self.active_get_stream.lock().await.take();

        let pending = self.pending_requests().drain().collect::<Vec<_>>();
        for (_, target) in pending {
            if let PendingRequestTarget::Http(sender) = target {
                let _ = sender.send(Err(Error::Shutdown));
            }
        }

        self.backend.shutdown().await
    }
}

impl StdioSessionBackend {
    async fn send(&self, message: &Value) -> Result<()> {
        let mut writer = self.writer.lock().await;
        write_message_to(&mut *writer, message).await
    }

    async fn shutdown(&self) -> Result<()> {
        {
            let mut writer = self.writer.lock().await;
            writer.shutdown().await?;
        }

        let mut child = self.child.lock().await;
        match timeout(SESSION_SHUTDOWN_GRACE, child.wait()).await {
            Ok(Ok(_status)) => Ok(()),
            Ok(Err(error)) => Err(Error::Io(error)),
            Err(_) => {
                child.start_kill().map_err(|error| {
                    Error::Message(format!("failed to kill stdio backend: {error}"))
                })?;
                match timeout(SESSION_SHUTDOWN_GRACE, child.wait()).await {
                    Ok(Ok(_status)) => Ok(()),
                    Ok(Err(error)) => Err(Error::Io(error)),
                    Err(_) => Err(Error::Message(
                        "timed out waiting for stdio backend to exit".to_string(),
                    )),
                }
            }
        }
    }
}

impl RelaySessionBackend {
    async fn send(&self, message: &Value) -> Result<Option<Value>> {
        self.transport.send(message.clone()).await
    }

    async fn shutdown(&self) -> Result<()> {
        self.transport.shutdown().await
    }
}

impl SessionBackend {
    async fn send(&self, message: &Value) -> Result<Option<Value>> {
        match self {
            Self::Stdio(backend) => {
                backend.send(message).await?;
                Ok(None)
            }
            Self::Relay(backend) => backend.send(message).await,
        }
    }

    async fn shutdown(&self) -> Result<()> {
        match self {
            Self::Stdio(backend) => backend.shutdown().await,
            Self::Relay(backend) => backend.shutdown().await,
        }
    }
}

pub async fn run<F>(config: ServeConfig, shutdown: F) -> Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    let local_addr = listener.local_addr()?;

    if !local_addr.ip().is_loopback() && config.warns_on_unprotected_non_loopback() {
        warn!(
            listen = %local_addr,
            "serve mode is bound to a non-loopback address without auth"
        );
    }

    info!(
        listen = %local_addr,
        backend = config.backend.name(),
        mcp_path = %config.mcp_path,
        health_path = %config.health_path,
        legacy_sse_path = %config.legacy_sse_path,
        legacy_messages_path = %config.legacy_messages_path,
        "serve mode listening"
    );

    axum::serve(listener, build_app(config).into_make_service())
        .with_graceful_shutdown(shutdown)
        .await
        .map_err(|error| Error::Message(format!("serve mode failed: {error}")))
}

fn build_app(config: ServeConfig) -> Router {
    let sessions = SessionRegistry::new(config.backend.clone());
    sessions.spawn_idle_reaper(config.idle_timeout);
    let state = AppState {
        sessions,
        config: Arc::new(config),
    };

    let protected_state = state.clone();
    let protected_routes = Router::new()
        .route(
            &state.config.mcp_path,
            post(post_mcp_handler)
                .get(get_mcp_handler)
                .delete(delete_mcp_handler),
        )
        .route(&state.config.legacy_sse_path, get(get_legacy_sse_handler))
        .route(
            &state.config.legacy_messages_path,
            post(post_legacy_messages_handler),
        )
        .with_state(protected_state.clone())
        .route_layer(middleware::from_fn_with_state(
            protected_state,
            enforce_access,
        ));

    Router::new()
        .merge(protected_routes)
        .route(&state.config.health_path, get(health_handler))
        .with_state(state)
}

async fn health_handler(State(state): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        backend: state.config.backend.name(),
        mcp_path: state.config.mcp_path.clone(),
        legacy_sse_path: state.config.legacy_sse_path.clone(),
        legacy_messages_path: state.config.legacy_messages_path.clone(),
    })
}

async fn post_mcp_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(message): Json<Value>,
) -> std::result::Result<Response, HttpError> {
    if message.is_array() {
        return Err(HttpError::bad_request(
            "batch JSON-RPC messages are not supported in ferry serve mode",
        ));
    }

    let message_kind = classify_message(&message)?;
    let session_id = header_value(&headers, HEADER_MCP_SESSION_ID);

    match session_id {
        Some(session_id) => {
            let session = state
                .sessions
                .get(&session_id)
                .await
                .ok_or_else(|| HttpError::not_found("unknown MCP session"))?;
            validate_protocol_version_header(&session, &headers).await?;
            session.touch();

            if matches!(
                message_kind,
                JsonRpcMessageKind::Request { ref method, .. } if method == "initialize"
            ) {
                return Err(HttpError::bad_request(
                    "initialize cannot be sent on an existing session",
                ));
            }

            match message_kind {
                JsonRpcMessageKind::Request { id_key, .. } => {
                    if session.has_outstanding_request(&id_key) {
                        return Err(HttpError::conflict(
                            "request id is already outstanding for this session",
                        ));
                    }
                    let response = session
                        .send_request(message)
                        .await
                        .map_err(as_backend_error)?;
                    let mut response = Json(response).into_response();
                    response.headers_mut().insert(
                        HeaderName::from_static("content-type"),
                        HeaderValue::from_static("application/json"),
                    );
                    Ok(response)
                }
                JsonRpcMessageKind::Notification | JsonRpcMessageKind::Response => {
                    session
                        .send_oneway(message)
                        .await
                        .map_err(as_backend_error)?;
                    Ok(StatusCode::ACCEPTED.into_response())
                }
            }
        }
        None => match message_kind {
            JsonRpcMessageKind::Request { method, .. } if method == "initialize" => {
                let outcome = state
                    .sessions
                    .initialize_session(message)
                    .await
                    .map_err(as_backend_error)?;
                let (session_id, response) = match outcome {
                    InitializeOutcome::Ready {
                        session_id,
                        response,
                    } => (session_id, response),
                    InitializeOutcome::Rejected(response) => {
                        return Ok(Json(response).into_response());
                    }
                };
                let mut response = Json(response).into_response();
                response.headers_mut().insert(
                    HeaderName::from_static("content-type"),
                    HeaderValue::from_static("application/json"),
                );
                response.headers_mut().insert(
                    HeaderName::from_static("mcp-session-id"),
                    HeaderValue::from_str(&session_id).map_err(|error| {
                        HttpError::bad_gateway(format!("invalid generated session id: {error}"))
                    })?,
                );
                Ok(response)
            }
            _ => Err(HttpError::bad_request(
                "new sessions must start with an initialize request",
            )),
        },
    }
}

async fn get_mcp_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> std::result::Result<
    Sse<impl tokio_stream::Stream<Item = std::result::Result<Event, Infallible>>>,
    HttpError,
> {
    let session_id = header_value(&headers, HEADER_MCP_SESSION_ID)
        .ok_or_else(|| HttpError::bad_request("GET /mcp requires MCP-Session-Id"))?;
    let session = state
        .sessions
        .get(&session_id)
        .await
        .ok_or_else(|| HttpError::not_found("unknown MCP session"))?;
    validate_protocol_version_header(&session, &headers).await?;
    session.touch();
    let replay_after_sequence = match header_value(&headers, HEADER_LAST_EVENT_ID) {
        Some(last_event_id) => {
            let parsed = parse_sse_event_id(&last_event_id)
                .ok_or_else(|| HttpError::bad_request("invalid Last-Event-ID header"))?;
            if parsed.session_id != session_id {
                return Err(HttpError::bad_request(
                    "Last-Event-ID does not belong to the requested MCP session",
                ));
            }
            Some(parsed.sequence)
        }
        None => None,
    };

    let (receiver, replaced) = session.open_get_stream(replay_after_sequence).await;
    if replaced {
        info!(
            session_id = %session.id,
            "replaced existing GET stream for session"
        );
    }

    Ok(sse_response(receiver, None))
}

async fn delete_mcp_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> std::result::Result<StatusCode, HttpError> {
    let session_id = header_value(&headers, HEADER_MCP_SESSION_ID)
        .ok_or_else(|| HttpError::bad_request("DELETE /mcp requires MCP-Session-Id"))?;
    let session = state
        .sessions
        .remove(&session_id)
        .await
        .ok_or_else(|| HttpError::not_found("unknown MCP session"))?;

    session.shutdown().await.map_err(as_backend_error)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn get_legacy_sse_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> std::result::Result<
    Sse<impl tokio_stream::Stream<Item = std::result::Result<Event, Infallible>>>,
    HttpError,
> {
    let last_event_id = header_value(&headers, HEADER_LAST_EVENT_ID);
    let (session_id, session, replay_after_sequence, created_session) = match last_event_id
        .as_deref()
    {
        Some(last_event_id) => {
            let parsed = parse_sse_event_id(last_event_id)
                .ok_or_else(|| HttpError::bad_request("invalid Last-Event-ID header"))?;
            let session = state
                .sessions
                .get(&parsed.session_id)
                .await
                .ok_or_else(|| HttpError::not_found("unknown legacy session for Last-Event-ID"))?;
            (parsed.session_id, session, Some(parsed.sequence), false)
        }
        None => {
            let (session_id, session) = state
                .sessions
                .create_legacy_session()
                .await
                .map_err(as_backend_error)?;
            (session_id, session, None, true)
        }
    };

    session.touch();
    let (receiver, replaced) = session.open_get_stream(replay_after_sequence).await;
    if replaced {
        info!(
            session_id = %session.id,
            "replaced existing legacy SSE stream for session"
        );
    }

    if created_session {
        let endpoint = format!(
            "{}?session_id={session_id}",
            state.config.legacy_messages_path
        );
        session
            .queue_endpoint_message(endpoint)
            .await
            .map_err(as_backend_error)?;
    }

    Ok(sse_response(
        receiver,
        Some(SseDropAction {
            sessions: state.sessions.clone(),
            session_id,
        }),
    ))
}

async fn post_legacy_messages_handler(
    State(state): State<AppState>,
    Query(query): Query<LegacySessionQuery>,
    Json(message): Json<Value>,
) -> std::result::Result<StatusCode, HttpError> {
    if message.is_array() {
        return Err(HttpError::bad_request(
            "batch JSON-RPC messages are not supported in ferry serve mode",
        ));
    }

    let session = state
        .sessions
        .get(&query.session_id)
        .await
        .ok_or_else(|| HttpError::not_found("unknown MCP session"))?;
    session.touch();
    let initialized = session.protocol_version.read().await.is_some();
    let message_kind = classify_message(&message)?;

    match message_kind {
        JsonRpcMessageKind::Request { method, id_key } => {
            if initialized && method == "initialize" {
                return Err(HttpError::bad_request(
                    "initialize cannot be sent on an existing session",
                ));
            }
            if !initialized && method != "initialize" {
                return Err(HttpError::bad_request(
                    "legacy sessions must start with an initialize request",
                ));
            }
            if session.has_outstanding_request(&id_key) {
                return Err(HttpError::conflict(
                    "request id is already outstanding for this session",
                ));
            }

            session
                .send_request_to_stream(message)
                .await
                .map_err(as_backend_error)?;
            Ok(StatusCode::ACCEPTED)
        }
        JsonRpcMessageKind::Notification | JsonRpcMessageKind::Response => {
            if !initialized {
                return Err(HttpError::bad_request(
                    "legacy sessions must start with an initialize request",
                ));
            }
            session
                .send_oneway(message)
                .await
                .map_err(as_backend_error)?;
            Ok(StatusCode::ACCEPTED)
        }
    }
}

fn sse_response(
    receiver: mpsc::Receiver<SseMessage>,
    drop_action: Option<SseDropAction>,
) -> Sse<impl tokio_stream::Stream<Item = std::result::Result<Event, Infallible>>> {
    Sse::new(SseEventStream::new(receiver, drop_action)).keep_alive(KeepAlive::default())
}

async fn enforce_access(
    State(state): State<AppState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    if origin_rejected(request.headers(), &state.config.allow_origins) {
        return HttpError::forbidden("request origin is not allowed for serve mode")
            .into_response();
    }

    if !authorized(request.headers(), &state.config.auth) {
        return HttpError::unauthorized("request is missing required serve mode authentication")
            .into_response();
    }

    next.run(request).await
}

fn authorized(headers: &HeaderMap, auth: &ServeAuthConfig) -> bool {
    if let Some(token) = &auth.bearer_token {
        let expected = format!("Bearer {token}");
        let actual = headers
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok());
        if actual != Some(expected.as_str()) {
            return false;
        }
    }

    for (name, value) in &auth.required_headers {
        match headers.get(name) {
            Some(actual) if actual == value => {}
            _ => return false,
        }
    }

    true
}

fn origin_rejected(headers: &HeaderMap, allow_origins: &[HeaderValue]) -> bool {
    let Some(origin) = headers.get(ORIGIN) else {
        return false;
    };

    !allow_origins.iter().any(|allowed| allowed == origin)
}

fn parse_stdio_backend(cli: ServeStdioCli) -> Result<ServeStdioConfig> {
    let mut command = cli.command.into_iter();
    let executable = command
        .next()
        .ok_or_else(|| Error::Message("stdio backend requires a command".to_string()))?;

    Ok(ServeStdioConfig {
        command: executable,
        args: command.collect(),
    })
}

fn parse_relay_backend(cli: ServeRelayCli) -> Result<ServeRelayConfig> {
    let url =
        Url::parse(&cli.url).map_err(|error| Error::Message(format!("invalid URL: {error}")))?;
    let timeout_seconds = cli.timeout.unwrap_or(DEFAULT_TIMEOUT_SECONDS);
    if timeout_seconds == 0 {
        return Err(Error::InvalidTimeout(timeout_seconds));
    }
    let retry_count = cli.retry.unwrap_or(DEFAULT_RETRY_COUNT);
    if retry_count > 1_000 {
        return Err(Error::InvalidRetryCount(retry_count));
    }
    let retry_delay_ms = cli.retry_delay.unwrap_or(DEFAULT_RETRY_DELAY_MS);
    if retry_delay_ms == 0 {
        return Err(Error::InvalidRetryDelay(retry_delay_ms));
    }

    Ok(ServeRelayConfig {
        url,
        transport: cli
            .transport
            .map(TransportMode::from)
            .unwrap_or(TransportMode::Auto),
        headers: parse_header_pairs(&cli.headers)?,
        timeout_seconds,
        retry_count,
        retry_delay_ms,
    })
}

fn build_relay_transport_config(config: &ServeRelayConfig) -> Result<Config> {
    let mut headers = reqwest::header::HeaderMap::new();
    for (name, value) in &config.headers {
        headers.insert(name.clone(), value.clone());
    }

    Ok(Config {
        url: config.url.clone(),
        transport: config.transport,
        headers,
        timeout: Duration::from_secs(config.timeout_seconds),
        socks5: None,
        retry: RetryConfig {
            count: config.retry_count,
            base_delay: Duration::from_millis(config.retry_delay_ms),
        },
        logging: LoggingConfig {
            level: LogLevel::Warn,
            file: None,
        },
    })
}

fn validate_path(path: &str, label: &str) -> Result<String> {
    if !path.starts_with('/') || path.contains('?') || path.contains('#') {
        return Err(Error::Message(format!(
            "{label} must start with '/' and must not contain '?' or '#'"
        )));
    }

    Ok(path.to_string())
}

fn validate_unique_paths<'a>(paths: impl IntoIterator<Item = &'a str>) -> Result<()> {
    let mut seen = BTreeSet::new();
    for path in paths {
        if !seen.insert(path) {
            return Err(Error::Message(format!(
                "serve mode paths must be unique, duplicate path: {path}"
            )));
        }
    }
    Ok(())
}

fn parse_header_pairs(values: &[String]) -> Result<Vec<(HeaderName, HeaderValue)>> {
    let mut headers = Vec::new();
    for value in values {
        let (name, value) = value
            .split_once('=')
            .ok_or_else(|| Error::InvalidHeader(value.clone()))?;
        if name.trim().is_empty() {
            return Err(Error::InvalidHeader(value.to_string()));
        }

        let canonical_name = name.trim().replace('_', "-").to_ascii_lowercase();
        headers.push((
            HeaderName::from_bytes(canonical_name.as_bytes())?,
            HeaderValue::from_str(value)?,
        ));
    }
    Ok(headers)
}

fn parse_header_values(values: &[String], label: &str) -> Result<Vec<HeaderValue>> {
    values
        .iter()
        .map(|value| {
            HeaderValue::from_str(value)
                .map_err(|error| Error::Message(format!("invalid {label} value: {error}")))
        })
        .collect()
}

fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(ToString::to_string)
}

fn format_sse_event_id(session_id: &str, sequence: u64) -> String {
    format!("{session_id}:{sequence}")
}

fn parse_sse_event_id(value: &str) -> Option<ParsedSseEventId> {
    let (session_id, sequence) = value.rsplit_once(':')?;
    let sequence = sequence.parse::<u64>().ok()?;
    if session_id.is_empty() {
        return None;
    }

    Some(ParsedSseEventId {
        session_id: session_id.to_string(),
        sequence,
    })
}

async fn validate_protocol_version_header(
    session: &SessionState,
    headers: &HeaderMap,
) -> std::result::Result<(), HttpError> {
    let expected = session
        .protocol_version
        .read()
        .await
        .clone()
        .ok_or_else(|| {
            HttpError::bad_gateway("session is missing a negotiated protocol version")
        })?;
    // Clients on 2025-03-26 predate the header; the version negotiated at initialize applies.
    let Some(actual) = header_value(headers, HEADER_MCP_PROTOCOL_VERSION) else {
        return Ok(());
    };
    if actual != expected {
        return Err(HttpError::bad_request(format!(
            "MCP-Protocol-Version must be {expected}"
        )));
    }
    Ok(())
}

fn classify_message(message: &Value) -> std::result::Result<JsonRpcMessageKind, HttpError> {
    let object = message
        .as_object()
        .ok_or_else(|| HttpError::bad_request("JSON-RPC message must be an object"))?;

    match object.get("method").and_then(Value::as_str) {
        Some(method) => match object.get("id") {
            Some(id) => Ok(JsonRpcMessageKind::Request {
                method: method.to_string(),
                id_key: jsonrpc_id_key(id)
                    .map_err(|error| HttpError::bad_request(error.to_string()))?,
            }),
            None => Ok(JsonRpcMessageKind::Notification),
        },
        None => match object.get("id") {
            Some(id) => {
                let _ = jsonrpc_id_key(id)
                    .map_err(|error| HttpError::bad_request(error.to_string()))?;
                Ok(JsonRpcMessageKind::Response)
            }
            None => Err(HttpError::bad_request(
                "JSON-RPC object must contain either a method or an id",
            )),
        },
    }
}

fn request_id(message: &Value) -> Option<Value> {
    message
        .as_object()
        .and_then(|object| object.get("method"))
        .and_then(Value::as_str)
        .and_then(|_| {
            message
                .as_object()
                .and_then(|object| object.get("id").cloned())
        })
}

fn response_id(message: &Value) -> Option<Value> {
    message.as_object().and_then(|object| {
        if object.get("method").is_some() {
            None
        } else {
            object.get("id").cloned()
        }
    })
}

fn jsonrpc_id_key(id: &Value) -> Result<String> {
    serde_json::to_string(id).map_err(Into::into)
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

fn as_backend_error(error: Error) -> HttpError {
    HttpError::bad_gateway(format!("backend error: {error}"))
}

#[cfg(test)]
mod tests {
    use std::future;

    use reqwest::StatusCode;
    use serde_json::json;

    use crate::cli::{ServeBackendCli, ServeCli, ServeRelayCli, ServeStdioCli};
    use crate::server::{build_app, run, ServeConfig};

    fn stdio_cli() -> ServeCli {
        ServeCli {
            listen: Some("127.0.0.1:0".parse().expect("listen")),
            path: Some("/mcp".to_string()),
            legacy_sse_path: Some("/sse".to_string()),
            legacy_messages_path: Some("/messages".to_string()),
            health_path: Some("/health".to_string()),
            auth_bearer_token: None,
            auth_headers: Vec::new(),
            allow_origins: Vec::new(),
            idle_timeout: None,
            log_level: Some(crate::cli::LogLevelArg::Warn),
            log_file: None,
            config: None,
            backend: Some(ServeBackendCli::Stdio(ServeStdioCli {
                command: vec!["example-server".to_string(), "--stdio".to_string()],
            })),
        }
    }

    async fn spawn_server(
        config: ServeConfig,
    ) -> (String, tokio::task::JoinHandle<crate::error::Result<()>>) {
        let listen = config.listen;
        let listener = tokio::net::TcpListener::bind(listen)
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let handle = tokio::spawn(async move {
            axum::serve(listener, build_app(config).into_make_service())
                .with_graceful_shutdown(future::pending::<()>())
                .await
                .map_err(|error| crate::error::Error::Message(format!("serve failed: {error}")))
        });
        (format!("http://{addr}"), handle)
    }

    #[test]
    fn rejects_duplicate_paths() {
        let mut cli = stdio_cli();
        cli.health_path = Some("/mcp".to_string());

        let error = ServeConfig::from_cli(cli).expect_err("duplicate path should fail");
        assert!(
            error
                .to_string()
                .contains("serve mode paths must be unique"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn detects_non_loopback_without_auth() {
        let mut cli = stdio_cli();
        cli.listen = Some("0.0.0.0:9000".parse().expect("listen"));

        let config = ServeConfig::from_cli(cli).expect("config");
        assert!(config.warns_on_unprotected_non_loopback());
    }

    #[tokio::test]
    async fn serves_health_endpoint() {
        let config = ServeConfig::from_cli(stdio_cli()).expect("config");
        let (base_url, handle) = spawn_server(config).await;

        let response = reqwest::get(format!("{base_url}/health"))
            .await
            .expect("health request");
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.text().await.expect("body");
        assert!(body.contains("\"status\":\"ok\""));
        assert!(body.contains("\"backend\":\"stdio\""));

        handle.abort();
    }

    #[tokio::test]
    async fn protects_mcp_route_with_bearer_token() {
        let mut cli = stdio_cli();
        cli.auth_bearer_token = Some("secret".to_string());
        let config = ServeConfig::from_cli(cli).expect("config");
        let (base_url, handle) = spawn_server(config).await;

        let unauthenticated = reqwest::Client::new()
            .post(format!("{base_url}/mcp"))
            .json(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": { "protocolVersion": "2025-11-25" }
            }))
            .send()
            .await
            .expect("unauthenticated request");
        assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);

        let authenticated = reqwest::Client::new()
            .post(format!("{base_url}/mcp"))
            .header("authorization", "Bearer secret")
            .json(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": { "protocolVersion": "2025-11-25" }
            }))
            .send()
            .await
            .expect("authenticated request");
        assert_eq!(authenticated.status(), StatusCode::BAD_GATEWAY);

        handle.abort();
    }

    #[tokio::test]
    async fn rejects_disallowed_origin_on_legacy_route() {
        let mut cli = stdio_cli();
        cli.allow_origins = vec!["https://allowed.example.com".to_string()];
        let config = ServeConfig::from_cli(cli).expect("config");
        let (base_url, handle) = spawn_server(config).await;

        let response = reqwest::Client::new()
            .get(format!("{base_url}/sse"))
            .header("origin", "https://blocked.example.com")
            .send()
            .await
            .expect("legacy request");
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        handle.abort();
    }

    #[tokio::test]
    async fn parses_relay_backend_defaults() {
        let cli = ServeCli {
            listen: Some("127.0.0.1:0".parse().expect("listen")),
            path: Some("/mcp".to_string()),
            legacy_sse_path: Some("/sse".to_string()),
            legacy_messages_path: Some("/messages".to_string()),
            health_path: Some("/health".to_string()),
            auth_bearer_token: None,
            auth_headers: Vec::new(),
            allow_origins: Vec::new(),
            idle_timeout: None,
            log_level: Some(crate::cli::LogLevelArg::Warn),
            log_file: None,
            config: None,
            backend: Some(ServeBackendCli::Relay(ServeRelayCli {
                url: "https://example.com/mcp".to_string(),
                transport: None,
                headers: Vec::new(),
                timeout: None,
                retry: None,
                retry_delay: None,
            })),
        };

        let config = ServeConfig::from_cli(cli).expect("config");
        match config.backend {
            crate::server::ServeBackendConfig::Relay(relay) => {
                assert_eq!(relay.url.as_str(), "https://example.com/mcp");
                assert_eq!(relay.timeout_seconds, 30);
                assert_eq!(relay.retry_count, 3);
                assert_eq!(relay.retry_delay_ms, 1_000);
            }
            crate::server::ServeBackendConfig::Stdio(_) => panic!("expected relay backend"),
        }
    }

    #[tokio::test]
    async fn run_returns_on_ready_shutdown() {
        let config = ServeConfig::from_cli(stdio_cli()).expect("config");
        run(config, async {})
            .await
            .expect("run should stop cleanly");
    }
}
