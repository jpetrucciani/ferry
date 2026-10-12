use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::Url;
use serde::Deserialize;

use crate::cli::{
    LogLevelArg, ProxyCli, ServeBackendCli, ServeCli, ServeRelayCli, ServeStdioCli, TransportArg,
};
use crate::error::{Error, Result};
use crate::server::ServeConfig;

const DEFAULT_TIMEOUT_SECONDS: u64 = 30;
const DEFAULT_RETRY_COUNT: usize = 3;
const DEFAULT_RETRY_DELAY_MS: u64 = 1_000;
const DEFAULT_SERVE_PATH: &str = "/mcp";
const DEFAULT_SERVE_LEGACY_SSE_PATH: &str = "/sse";
const DEFAULT_SERVE_LEGACY_MESSAGES_PATH: &str = "/messages";
const DEFAULT_SERVE_HEALTH_PATH: &str = "/health";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportMode {
    Auto,
    StreamableHttp,
    Sse,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Socks5Auth {
    pub username: String,
    pub password: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Socks5Config {
    pub address: String,
    pub auth: Option<Socks5Auth>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetryConfig {
    pub count: usize,
    pub base_delay: Duration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoggingConfig {
    pub level: LogLevel,
    pub file: Option<PathBuf>,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub url: Url,
    pub transport: TransportMode,
    pub headers: HeaderMap,
    pub timeout: Duration,
    pub socks5: Option<Socks5Config>,
    pub retry: RetryConfig,
    pub logging: LoggingConfig,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileConfig {
    url: Option<String>,
    transport: Option<String>,
    timeout: Option<u64>,
    #[serde(default)]
    headers: BTreeMap<String, String>,
    socks5: Option<FileSocks5Config>,
    retry: Option<FileRetryConfig>,
    logging: Option<FileLoggingConfig>,
    serve: Option<FileServeConfig>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileSocks5Config {
    address: Option<String>,
    username: Option<String>,
    password: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileRetryConfig {
    count: Option<usize>,
    base_delay_ms: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileLoggingConfig {
    level: Option<String>,
    file: Option<PathBuf>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileServeConfig {
    listen: Option<String>,
    path: Option<String>,
    legacy_sse_path: Option<String>,
    legacy_messages_path: Option<String>,
    health_path: Option<String>,
    allow_origins: Option<Vec<String>>,
    idle_timeout: Option<u64>,
    auth: Option<FileServeAuthConfig>,
    logging: Option<FileLoggingConfig>,
    stdio: Option<FileServeStdioConfig>,
    relay: Option<FileServeRelayConfig>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileServeAuthConfig {
    bearer_token: Option<String>,
    #[serde(default)]
    headers: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileServeStdioConfig {
    command: Option<String>,
    args: Option<Vec<String>>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileServeRelayConfig {
    url: Option<String>,
    transport: Option<String>,
    timeout: Option<u64>,
    #[serde(default)]
    headers: BTreeMap<String, String>,
    retry: Option<FileRetryConfig>,
}

#[derive(Clone, Debug, Default)]
struct PartialConfig {
    url: Option<String>,
    transport: Option<TransportMode>,
    headers: BTreeMap<String, String>,
    timeout_seconds: Option<u64>,
    socks5: Option<String>,
    socks5_auth: Option<String>,
    retry_count: Option<usize>,
    retry_delay_ms: Option<u64>,
    log_level: Option<LogLevel>,
    log_file: Option<PathBuf>,
}

#[derive(Clone, Debug, Default)]
struct PartialServeConfig {
    listen: Option<SocketAddr>,
    path: Option<String>,
    legacy_sse_path: Option<String>,
    legacy_messages_path: Option<String>,
    health_path: Option<String>,
    auth_bearer_token: Option<String>,
    auth_headers: BTreeMap<String, String>,
    allow_origins: Option<Vec<String>>,
    idle_timeout_seconds: Option<u64>,
    log_level: Option<LogLevel>,
    log_file: Option<PathBuf>,
    backend: Option<PartialServeBackendConfig>,
}

#[derive(Clone, Debug)]
enum PartialServeBackendConfig {
    Stdio(PartialServeStdioConfig),
    Relay(PartialServeRelayConfig),
}

#[derive(Clone, Debug, Default)]
struct PartialServeStdioConfig {
    command: Option<String>,
    args: Option<Vec<String>>,
}

#[derive(Clone, Debug, Default)]
struct PartialServeRelayConfig {
    url: Option<String>,
    transport: Option<TransportMode>,
    headers: BTreeMap<String, String>,
    timeout_seconds: Option<u64>,
    retry_count: Option<usize>,
    retry_delay_ms: Option<u64>,
}

impl Config {
    pub fn load(cli: ProxyCli) -> Result<Self> {
        let file_path = discover_config(
            cli.config.as_deref(),
            env::var_os("FERRY_CONFIG").as_deref(),
        )?;
        let file_config = match file_path {
            Some(path) => load_file_config(&path)?,
            None => PartialConfig::default(),
        };
        let env_config = load_env_config()?;
        let cli_config = load_cli_config(&cli)?;

        let merged = merge_configs(file_config, env_config, cli_config);

        let url = match merged.url {
            Some(value) => Url::parse(&value)
                .map_err(|error| Error::Message(format!("invalid URL: {error}")))?,
            None => return Err(Error::MissingUrl),
        };

        let timeout_seconds = merged.timeout_seconds.unwrap_or(DEFAULT_TIMEOUT_SECONDS);
        if timeout_seconds == 0 {
            return Err(Error::InvalidTimeout(timeout_seconds));
        }

        let retry_count = merged.retry_count.unwrap_or(DEFAULT_RETRY_COUNT);
        if retry_count > 1_000 {
            return Err(Error::InvalidRetryCount(retry_count));
        }

        let retry_delay_ms = merged.retry_delay_ms.unwrap_or(DEFAULT_RETRY_DELAY_MS);
        if retry_delay_ms == 0 {
            return Err(Error::InvalidRetryDelay(retry_delay_ms));
        }

        let headers = build_headers(&merged.headers)?;
        let socks5 = build_socks5_config(merged.socks5, merged.socks5_auth)?;

        Ok(Self {
            url,
            transport: merged.transport.unwrap_or(TransportMode::Auto),
            headers,
            timeout: Duration::from_secs(timeout_seconds),
            socks5,
            retry: RetryConfig {
                count: retry_count,
                base_delay: Duration::from_millis(retry_delay_ms),
            },
            logging: LoggingConfig {
                level: merged.log_level.unwrap_or(LogLevel::Warn),
                file: merged.log_file,
            },
        })
    }
}

impl ServeConfig {
    pub fn load(cli: ServeCli) -> Result<Self> {
        let file_path = discover_config(
            cli.config.as_deref(),
            env::var_os("FERRY_CONFIG").as_deref(),
        )?;
        let file_config = match file_path {
            Some(path) => load_file_serve_config(&path)?,
            None => PartialServeConfig::default(),
        };
        let env_config = load_env_serve_config()?;
        let cli_config = load_cli_serve_config(&cli)?;

        let merged = merge_serve_configs(file_config, env_config, cli_config);
        Self::from_cli(resolve_serve_cli(merged)?)
    }
}

impl LogLevel {
    pub fn as_tracing_level(self) -> tracing::Level {
        match self {
            Self::Error => tracing::Level::ERROR,
            Self::Warn => tracing::Level::WARN,
            Self::Info => tracing::Level::INFO,
            Self::Debug => tracing::Level::DEBUG,
            Self::Trace => tracing::Level::TRACE,
        }
    }
}

fn discover_config(
    explicit_path: Option<&Path>,
    env_path: Option<&std::ffi::OsStr>,
) -> Result<Option<PathBuf>> {
    if let Some(path) = explicit_path {
        if path.exists() {
            return Ok(Some(path.to_path_buf()));
        }
        return Err(Error::InvalidConfigPath(path.to_path_buf()));
    }

    if let Some(path) = env_path {
        let path = PathBuf::from(path);
        if path.exists() {
            return Ok(Some(path));
        }
        return Err(Error::InvalidConfigPath(path));
    }

    let local = PathBuf::from("ferry.toml");
    if local.exists() {
        return Ok(Some(local));
    }

    let xdg_path = match env::var_os("XDG_CONFIG_HOME") {
        Some(value) => PathBuf::from(value),
        None => match env::var_os("HOME") {
            Some(home) => PathBuf::from(home).join(".config"),
            None => return Ok(None),
        },
    };
    let config = xdg_path.join("ferry").join("config.toml");
    if config.exists() {
        return Ok(Some(config));
    }

    Ok(None)
}

fn default_serve_listen() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 0))
}

fn load_file_config(path: &Path) -> Result<PartialConfig> {
    let config: FileConfig = load_file_value(path)?.try_into()?;

    let mut partial = PartialConfig {
        url: config.url,
        transport: match config.transport {
            Some(value) => Some(parse_transport(&value)?),
            None => None,
        },
        headers: normalize_header_map(config.headers)?,
        timeout_seconds: config.timeout,
        retry_count: config.retry.as_ref().and_then(|retry| retry.count),
        retry_delay_ms: config.retry.as_ref().and_then(|retry| retry.base_delay_ms),
        log_level: match config
            .logging
            .as_ref()
            .and_then(|logging| logging.level.clone())
        {
            Some(value) => Some(parse_log_level(&value)?),
            None => None,
        },
        log_file: config
            .logging
            .as_ref()
            .and_then(|logging| logging.file.clone()),
        ..PartialConfig::default()
    };

    if let Some(socks5) = config.socks5 {
        partial.socks5 = socks5.address;
        partial.socks5_auth = match (socks5.username, socks5.password) {
            (Some(username), Some(password)) => Some(format!("{username}:{password}")),
            (None, None) => None,
            (Some(_), None) | (None, Some(_)) => {
                return Err(Error::InvalidSocks5Auth(
                    "config socks5 auth requires both username and password".to_string(),
                ))
            }
        };
    }

    Ok(partial)
}

fn load_file_serve_config(path: &Path) -> Result<PartialServeConfig> {
    let config: FileConfig = load_file_value(path)?.try_into()?;
    let serve = config.serve.unwrap_or_default();

    let backend = match (serve.stdio, serve.relay) {
        (Some(stdio), None) => Some(PartialServeBackendConfig::Stdio(PartialServeStdioConfig {
            command: stdio.command,
            args: stdio.args,
        })),
        (None, Some(relay)) => Some(PartialServeBackendConfig::Relay(PartialServeRelayConfig {
            url: relay.url,
            transport: match relay.transport {
                Some(value) => Some(parse_transport(&value)?),
                None => None,
            },
            headers: normalize_header_map(relay.headers)?,
            timeout_seconds: relay.timeout,
            retry_count: relay.retry.as_ref().and_then(|retry| retry.count),
            retry_delay_ms: relay.retry.as_ref().and_then(|retry| retry.base_delay_ms),
        })),
        (Some(_), Some(_)) => {
            return Err(Error::Message(
                "serve config must define only one backend, `[serve.stdio]` or `[serve.relay]`"
                    .to_string(),
            ))
        }
        (None, None) => None,
    };

    Ok(PartialServeConfig {
        listen: match serve.listen {
            Some(value) => Some(parse_socket_addr(&value, "serve.listen")?),
            None => None,
        },
        path: serve.path,
        legacy_sse_path: serve.legacy_sse_path,
        legacy_messages_path: serve.legacy_messages_path,
        health_path: serve.health_path,
        auth_bearer_token: serve
            .auth
            .as_ref()
            .and_then(|auth| auth.bearer_token.clone()),
        auth_headers: normalize_header_map(
            serve.auth.map(|auth| auth.headers).unwrap_or_default(),
        )?,
        allow_origins: serve.allow_origins,
        idle_timeout_seconds: serve.idle_timeout,
        log_level: match serve
            .logging
            .as_ref()
            .and_then(|logging| logging.level.clone())
        {
            Some(value) => Some(parse_log_level(&value)?),
            None => None,
        },
        log_file: serve
            .logging
            .as_ref()
            .and_then(|logging| logging.file.clone()),
        backend,
    })
}

fn load_file_value(path: &Path) -> Result<toml::Value> {
    let contents = fs::read_to_string(path)?;
    let mut value: toml::Value = toml::from_str(&contents)?;
    interpolate_env(&mut value)?;
    Ok(value)
}

fn load_env_config() -> Result<PartialConfig> {
    let mut partial = PartialConfig {
        url: env::var("FERRY_URL").ok(),
        transport: match env::var("FERRY_TRANSPORT").ok() {
            Some(value) => Some(parse_transport(&value)?),
            None => None,
        },
        socks5: env::var("FERRY_SOCKS5").ok(),
        socks5_auth: env::var("FERRY_SOCKS5_AUTH").ok(),
        timeout_seconds: read_env_u64("FERRY_TIMEOUT")?,
        retry_count: read_env_usize("FERRY_RETRY")?,
        retry_delay_ms: read_env_u64("FERRY_RETRY_DELAY")?,
        log_level: match env::var("FERRY_LOG_LEVEL").ok() {
            Some(value) => Some(parse_log_level(&value)?),
            None => None,
        },
        log_file: env::var_os("FERRY_LOG_FILE").map(PathBuf::from),
        ..PartialConfig::default()
    };

    partial.headers = load_env_header_map("FERRY_HEADER_")?;
    Ok(partial)
}

fn load_env_serve_config() -> Result<PartialServeConfig> {
    let stdio = PartialServeStdioConfig {
        command: env::var("FERRY_SERVE_STDIO_COMMAND").ok(),
        args: read_env_string_list("FERRY_SERVE_STDIO_ARGS")?,
    };
    let relay = PartialServeRelayConfig {
        url: env::var("FERRY_SERVE_RELAY_URL").ok(),
        transport: match env::var("FERRY_SERVE_RELAY_TRANSPORT").ok() {
            Some(value) => Some(parse_transport(&value)?),
            None => None,
        },
        headers: load_env_header_map("FERRY_SERVE_RELAY_HEADER_")?,
        timeout_seconds: read_env_u64("FERRY_SERVE_RELAY_TIMEOUT")?,
        retry_count: read_env_usize("FERRY_SERVE_RELAY_RETRY")?,
        retry_delay_ms: read_env_u64("FERRY_SERVE_RELAY_RETRY_DELAY")?,
    };

    Ok(PartialServeConfig {
        listen: match env::var("FERRY_SERVE_LISTEN").ok() {
            Some(value) => Some(parse_socket_addr(&value, "FERRY_SERVE_LISTEN")?),
            None => None,
        },
        path: env::var("FERRY_SERVE_PATH").ok(),
        legacy_sse_path: env::var("FERRY_SERVE_LEGACY_SSE_PATH").ok(),
        legacy_messages_path: env::var("FERRY_SERVE_LEGACY_MESSAGES_PATH").ok(),
        health_path: env::var("FERRY_SERVE_HEALTH_PATH").ok(),
        auth_bearer_token: env::var("FERRY_SERVE_AUTH_BEARER_TOKEN").ok(),
        auth_headers: load_env_header_map("FERRY_SERVE_AUTH_HEADER_")?,
        allow_origins: read_env_csv("FERRY_SERVE_ALLOW_ORIGINS"),
        idle_timeout_seconds: read_env_u64("FERRY_SERVE_IDLE_TIMEOUT")?,
        log_level: match env::var("FERRY_SERVE_LOG_LEVEL").ok() {
            Some(value) => Some(parse_log_level(&value)?),
            None => None,
        },
        log_file: env::var_os("FERRY_SERVE_LOG_FILE").map(PathBuf::from),
        backend: read_env_serve_backend(env::var("FERRY_SERVE_BACKEND").ok(), stdio, relay)?,
    })
}

fn load_cli_config(cli: &ProxyCli) -> Result<PartialConfig> {
    let mut partial = PartialConfig {
        url: cli.url.clone(),
        transport: cli.transport.map(TransportMode::from),
        socks5: cli.socks5.clone(),
        socks5_auth: cli.socks5_auth.clone(),
        timeout_seconds: cli.timeout,
        retry_count: cli.retry,
        retry_delay_ms: cli.retry_delay,
        log_level: cli.log_level.map(LogLevel::from),
        log_file: cli.log_file.clone(),
        ..PartialConfig::default()
    };

    partial.headers = parse_header_inputs(&cli.headers)?;
    Ok(partial)
}

fn load_cli_serve_config(cli: &ServeCli) -> Result<PartialServeConfig> {
    let backend = match cli.backend.as_ref() {
        Some(ServeBackendCli::Stdio(stdio)) => {
            let (command, args) = split_stdio_command(&stdio.command)?;
            Some(PartialServeBackendConfig::Stdio(PartialServeStdioConfig {
                command: Some(command),
                args: Some(args),
            }))
        }
        Some(ServeBackendCli::Relay(relay)) => {
            Some(PartialServeBackendConfig::Relay(PartialServeRelayConfig {
                url: Some(relay.url.clone()),
                transport: relay.transport.map(TransportMode::from),
                headers: parse_header_inputs(&relay.headers)?,
                timeout_seconds: relay.timeout,
                retry_count: relay.retry,
                retry_delay_ms: relay.retry_delay,
            }))
        }
        None => None,
    };

    Ok(PartialServeConfig {
        listen: cli.listen,
        path: cli.path.clone(),
        legacy_sse_path: cli.legacy_sse_path.clone(),
        legacy_messages_path: cli.legacy_messages_path.clone(),
        health_path: cli.health_path.clone(),
        auth_bearer_token: cli.auth_bearer_token.clone(),
        auth_headers: parse_header_inputs(&cli.auth_headers)?,
        allow_origins: (!cli.allow_origins.is_empty()).then_some(cli.allow_origins.clone()),
        idle_timeout_seconds: cli.idle_timeout,
        log_level: cli.log_level.map(LogLevel::from),
        log_file: cli.log_file.clone(),
        backend,
    })
}

fn merge_configs(
    base: PartialConfig,
    overlay_one: PartialConfig,
    overlay_two: PartialConfig,
) -> PartialConfig {
    let mut merged = base;
    apply_partial(&mut merged, overlay_one);
    apply_partial(&mut merged, overlay_two);
    merged
}

fn merge_serve_configs(
    base: PartialServeConfig,
    overlay_one: PartialServeConfig,
    overlay_two: PartialServeConfig,
) -> PartialServeConfig {
    let mut merged = base;
    apply_serve_partial(&mut merged, overlay_one);
    apply_serve_partial(&mut merged, overlay_two);
    merged
}

fn apply_partial(target: &mut PartialConfig, overlay: PartialConfig) {
    if overlay.url.is_some() {
        target.url = overlay.url;
    }
    if overlay.transport.is_some() {
        target.transport = overlay.transport;
    }
    if overlay.timeout_seconds.is_some() {
        target.timeout_seconds = overlay.timeout_seconds;
    }
    if overlay.socks5.is_some() {
        target.socks5 = overlay.socks5;
    }
    if overlay.socks5_auth.is_some() {
        target.socks5_auth = overlay.socks5_auth;
    }
    if overlay.retry_count.is_some() {
        target.retry_count = overlay.retry_count;
    }
    if overlay.retry_delay_ms.is_some() {
        target.retry_delay_ms = overlay.retry_delay_ms;
    }
    if overlay.log_level.is_some() {
        target.log_level = overlay.log_level;
    }
    if overlay.log_file.is_some() {
        target.log_file = overlay.log_file;
    }

    for (key, value) in overlay.headers {
        target.headers.insert(key, value);
    }
}

fn apply_serve_partial(target: &mut PartialServeConfig, overlay: PartialServeConfig) {
    if overlay.listen.is_some() {
        target.listen = overlay.listen;
    }
    if overlay.path.is_some() {
        target.path = overlay.path;
    }
    if overlay.legacy_sse_path.is_some() {
        target.legacy_sse_path = overlay.legacy_sse_path;
    }
    if overlay.legacy_messages_path.is_some() {
        target.legacy_messages_path = overlay.legacy_messages_path;
    }
    if overlay.health_path.is_some() {
        target.health_path = overlay.health_path;
    }
    if overlay.auth_bearer_token.is_some() {
        target.auth_bearer_token = overlay.auth_bearer_token;
    }
    if overlay.allow_origins.is_some() {
        target.allow_origins = overlay.allow_origins;
    }
    if overlay.idle_timeout_seconds.is_some() {
        target.idle_timeout_seconds = overlay.idle_timeout_seconds;
    }
    if overlay.log_level.is_some() {
        target.log_level = overlay.log_level;
    }
    if overlay.log_file.is_some() {
        target.log_file = overlay.log_file;
    }

    for (key, value) in overlay.auth_headers {
        target.auth_headers.insert(key, value);
    }

    if let Some(backend) = overlay.backend {
        apply_serve_backend(&mut target.backend, backend);
    }
}

fn apply_serve_backend(
    target: &mut Option<PartialServeBackendConfig>,
    overlay: PartialServeBackendConfig,
) {
    match (target.as_mut(), overlay) {
        (
            Some(PartialServeBackendConfig::Stdio(target_stdio)),
            PartialServeBackendConfig::Stdio(overlay_stdio),
        ) => {
            if overlay_stdio.command.is_some() {
                target_stdio.command = overlay_stdio.command;
            }
            if overlay_stdio.args.is_some() {
                target_stdio.args = overlay_stdio.args;
            }
        }
        (
            Some(PartialServeBackendConfig::Relay(target_relay)),
            PartialServeBackendConfig::Relay(overlay_relay),
        ) => {
            if overlay_relay.url.is_some() {
                target_relay.url = overlay_relay.url;
            }
            if overlay_relay.transport.is_some() {
                target_relay.transport = overlay_relay.transport;
            }
            if overlay_relay.timeout_seconds.is_some() {
                target_relay.timeout_seconds = overlay_relay.timeout_seconds;
            }
            if overlay_relay.retry_count.is_some() {
                target_relay.retry_count = overlay_relay.retry_count;
            }
            if overlay_relay.retry_delay_ms.is_some() {
                target_relay.retry_delay_ms = overlay_relay.retry_delay_ms;
            }
            for (key, value) in overlay_relay.headers {
                target_relay.headers.insert(key, value);
            }
        }
        (_, overlay_backend) => {
            *target = Some(overlay_backend);
        }
    }
}

fn resolve_serve_cli(partial: PartialServeConfig) -> Result<ServeCli> {
    Ok(ServeCli {
        listen: Some(partial.listen.unwrap_or_else(default_serve_listen)),
        path: Some(
            partial
                .path
                .unwrap_or_else(|| DEFAULT_SERVE_PATH.to_string()),
        ),
        legacy_sse_path: Some(
            partial
                .legacy_sse_path
                .unwrap_or_else(|| DEFAULT_SERVE_LEGACY_SSE_PATH.to_string()),
        ),
        legacy_messages_path: Some(
            partial
                .legacy_messages_path
                .unwrap_or_else(|| DEFAULT_SERVE_LEGACY_MESSAGES_PATH.to_string()),
        ),
        health_path: Some(
            partial
                .health_path
                .unwrap_or_else(|| DEFAULT_SERVE_HEALTH_PATH.to_string()),
        ),
        auth_bearer_token: partial.auth_bearer_token,
        auth_headers: header_map_to_inputs(partial.auth_headers),
        allow_origins: partial.allow_origins.unwrap_or_default(),
        idle_timeout: partial.idle_timeout_seconds,
        log_level: Some(partial.log_level.unwrap_or(LogLevel::Warn).into()),
        log_file: partial.log_file,
        config: None,
        backend: partial.backend.map(resolve_serve_backend).transpose()?,
    })
}

fn resolve_serve_backend(partial: PartialServeBackendConfig) -> Result<ServeBackendCli> {
    match partial {
        PartialServeBackendConfig::Stdio(stdio) => {
            let command = stdio
                .command
                .ok_or_else(|| Error::Message("missing serve stdio command".to_string()))?;
            let mut argv = vec![command];
            argv.extend(stdio.args.unwrap_or_default());
            Ok(ServeBackendCli::Stdio(ServeStdioCli { command: argv }))
        }
        PartialServeBackendConfig::Relay(relay) => Ok(ServeBackendCli::Relay(ServeRelayCli {
            url: relay
                .url
                .ok_or_else(|| Error::Message("missing serve relay url".to_string()))?,
            transport: relay.transport.map(TransportArg::from),
            headers: header_map_to_inputs(relay.headers),
            timeout: relay.timeout_seconds,
            retry: relay.retry_count,
            retry_delay: relay.retry_delay_ms,
        })),
    }
}

fn build_headers(values: &BTreeMap<String, String>) -> Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    for (name, value) in values {
        headers.insert(
            HeaderName::from_bytes(name.as_bytes())?,
            HeaderValue::from_str(value)?,
        );
    }
    Ok(headers)
}

fn build_socks5_config(
    address: Option<String>,
    auth: Option<String>,
) -> Result<Option<Socks5Config>> {
    match (address, auth) {
        (None, None) => Ok(None),
        (Some(address), auth) => Ok(Some(Socks5Config {
            address,
            auth: match auth {
                Some(value) => Some(parse_socks5_auth(&value)?),
                None => None,
            },
        })),
        (None, Some(value)) => Err(Error::InvalidSocks5Auth(value)),
    }
}

fn parse_socks5_auth(value: &str) -> Result<Socks5Auth> {
    match value.split_once(':') {
        Some((username, password)) if !username.is_empty() => Ok(Socks5Auth {
            username: username.to_string(),
            password: password.to_string(),
        }),
        _ => Err(Error::InvalidSocks5Auth(value.to_string())),
    }
}

fn parse_transport(value: &str) -> Result<TransportMode> {
    match value.to_ascii_lowercase().as_str() {
        "auto" => Ok(TransportMode::Auto),
        "streamable-http" => Ok(TransportMode::StreamableHttp),
        "sse" => Ok(TransportMode::Sse),
        other => Err(Error::InvalidTransport(other.to_string())),
    }
}

fn parse_log_level(value: &str) -> Result<LogLevel> {
    match value.to_ascii_lowercase().as_str() {
        "error" => Ok(LogLevel::Error),
        "warn" | "warning" => Ok(LogLevel::Warn),
        "info" => Ok(LogLevel::Info),
        "debug" => Ok(LogLevel::Debug),
        "trace" => Ok(LogLevel::Trace),
        other => Err(Error::InvalidLogLevel(other.to_string())),
    }
}

fn split_header(input: &str) -> Result<(String, String)> {
    match input.split_once('=') {
        Some((name, value)) if !name.trim().is_empty() => {
            Ok((canonicalize_header_name(name), value.to_string()))
        }
        _ => Err(Error::InvalidHeader(input.to_string())),
    }
}

fn normalize_header_map(headers: BTreeMap<String, String>) -> Result<BTreeMap<String, String>> {
    let mut normalized = BTreeMap::new();
    for (name, value) in headers {
        if name.trim().is_empty() {
            return Err(Error::InvalidHeader(name));
        }
        normalized.insert(canonicalize_header_name(&name), value);
    }
    Ok(normalized)
}

fn parse_header_inputs(values: &[String]) -> Result<BTreeMap<String, String>> {
    let mut headers = BTreeMap::new();
    for value in values {
        let (name, value) = split_header(value)?;
        headers.insert(name, value);
    }
    normalize_header_map(headers)
}

fn header_map_to_inputs(headers: BTreeMap<String, String>) -> Vec<String> {
    headers
        .into_iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect()
}

fn load_env_header_map(prefix: &str) -> Result<BTreeMap<String, String>> {
    let mut headers = BTreeMap::new();
    for (key, value) in env::vars() {
        if let Some(name) = key.strip_prefix(prefix) {
            headers.insert(canonicalize_header_name(name), value);
        }
    }
    normalize_header_map(headers)
}

fn canonicalize_header_name(name: &str) -> String {
    name.trim().replace('_', "-").to_ascii_lowercase()
}

fn parse_socket_addr(value: &str, label: &str) -> Result<SocketAddr> {
    value
        .parse::<SocketAddr>()
        .map_err(|error| Error::Message(format!("invalid {label}: {error}")))
}

fn split_stdio_command(command: &[String]) -> Result<(String, Vec<String>)> {
    let executable = command
        .first()
        .cloned()
        .ok_or_else(|| Error::Message("stdio backend command cannot be empty".to_string()))?;
    Ok((executable, command.iter().skip(1).cloned().collect()))
}

fn read_env_u64(name: &str) -> Result<Option<u64>> {
    match env::var(name) {
        Ok(value) => value
            .parse::<u64>()
            .map(Some)
            .map_err(|error| Error::Message(format!("invalid {name}: {error}"))),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(Error::Message(format!("failed to read {name}: {error}"))),
    }
}

fn read_env_usize(name: &str) -> Result<Option<usize>> {
    match env::var(name) {
        Ok(value) => value
            .parse::<usize>()
            .map(Some)
            .map_err(|error| Error::Message(format!("invalid {name}: {error}"))),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(Error::Message(format!("failed to read {name}: {error}"))),
    }
}

fn read_env_string_list(name: &str) -> Result<Option<Vec<String>>> {
    match env::var(name) {
        Ok(value) => serde_json::from_str::<Vec<String>>(&value)
            .map(Some)
            .map_err(|error| {
                Error::Message(format!(
                    "invalid {name}: expected JSON string array: {error}"
                ))
            }),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(Error::Message(format!("failed to read {name}: {error}"))),
    }
}

fn read_env_csv(name: &str) -> Option<Vec<String>> {
    env::var(name).ok().map(|value| {
        value
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(ToString::to_string)
            .collect()
    })
}

fn read_env_serve_backend(
    configured_backend: Option<String>,
    stdio: PartialServeStdioConfig,
    relay: PartialServeRelayConfig,
) -> Result<Option<PartialServeBackendConfig>> {
    let stdio_present = stdio.command.is_some() || stdio.args.is_some();
    let relay_present = relay.url.is_some()
        || relay.transport.is_some()
        || !relay.headers.is_empty()
        || relay.timeout_seconds.is_some()
        || relay.retry_count.is_some()
        || relay.retry_delay_ms.is_some();

    match configured_backend
        .as_deref()
        .map(|value| value.to_ascii_lowercase())
        .as_deref()
    {
        Some("stdio") => Ok(Some(PartialServeBackendConfig::Stdio(stdio))),
        Some("relay") => Ok(Some(PartialServeBackendConfig::Relay(relay))),
        Some(other) => Err(Error::Message(format!(
            "invalid FERRY_SERVE_BACKEND: {other}, expected `stdio` or `relay`"
        ))),
        None if stdio_present && relay_present => Err(Error::Message(
            "serve backend is ambiguous, set FERRY_SERVE_BACKEND to `stdio` or `relay`".to_string(),
        )),
        None if stdio_present => Ok(Some(PartialServeBackendConfig::Stdio(stdio))),
        None if relay_present => Ok(Some(PartialServeBackendConfig::Relay(relay))),
        None => Ok(None),
    }
}

fn interpolate_env(value: &mut toml::Value) -> Result<()> {
    match value {
        toml::Value::String(current) => {
            let mut output = String::new();
            let mut rest = current.as_str();

            while let Some(start) = rest.find("${") {
                let prefix = &rest[..start];
                output.push_str(prefix);
                let after = &rest[start + 2..];
                let end = after
                    .find('}')
                    .ok_or_else(|| Error::InvalidEnvInterpolation(current.clone()))?;
                let name = &after[..end];
                let replacement =
                    env::var(name).map_err(|_| Error::InvalidEnvInterpolation(name.to_string()))?;
                output.push_str(&replacement);
                rest = &after[end + 1..];
            }

            output.push_str(rest);
            *current = output;
        }
        toml::Value::Array(values) => {
            for entry in values {
                interpolate_env(entry)?;
            }
        }
        toml::Value::Table(entries) => {
            for (_, entry) in entries.iter_mut() {
                interpolate_env(entry)?;
            }
        }
        _ => {}
    }

    Ok(())
}

impl From<TransportArg> for TransportMode {
    fn from(value: TransportArg) -> Self {
        match value {
            TransportArg::Auto => Self::Auto,
            TransportArg::StreamableHttp => Self::StreamableHttp,
            TransportArg::Sse => Self::Sse,
        }
    }
}

impl From<TransportMode> for TransportArg {
    fn from(value: TransportMode) -> Self {
        match value {
            TransportMode::Auto => Self::Auto,
            TransportMode::StreamableHttp => Self::StreamableHttp,
            TransportMode::Sse => Self::Sse,
        }
    }
}

impl From<LogLevelArg> for LogLevel {
    fn from(value: LogLevelArg) -> Self {
        match value {
            LogLevelArg::Error => Self::Error,
            LogLevelArg::Warn => Self::Warn,
            LogLevelArg::Info => Self::Info,
            LogLevelArg::Debug => Self::Debug,
            LogLevelArg::Trace => Self::Trace,
        }
    }
}

impl From<LogLevel> for LogLevelArg {
    fn from(value: LogLevel) -> Self {
        match value {
            LogLevel::Error => Self::Error,
            LogLevel::Warn => Self::Warn,
            LogLevel::Info => Self::Info,
            LogLevel::Debug => Self::Debug,
            LogLevel::Trace => Self::Trace,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::{Mutex, OnceLock};

    use tempfile::tempdir;

    use crate::cli::{ProxyCli, ServeBackendCli, ServeCli, ServeStdioCli, TransportArg};
    use crate::config::{Config, TransportMode};
    use crate::server::{ServeBackendConfig, ServeConfig};

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn empty_proxy_cli(config: Option<PathBuf>) -> ProxyCli {
        ProxyCli {
            url: None,
            transport: None,
            socks5: None,
            socks5_auth: None,
            headers: Vec::new(),
            timeout: None,
            retry: None,
            retry_delay: None,
            log_level: None,
            log_file: None,
            config,
        }
    }

    fn empty_serve_cli(config: Option<PathBuf>) -> ServeCli {
        ServeCli {
            listen: None,
            path: None,
            legacy_sse_path: None,
            legacy_messages_path: None,
            health_path: None,
            auth_bearer_token: None,
            auth_headers: Vec::new(),
            allow_origins: Vec::new(),
            idle_timeout: None,
            log_level: None,
            log_file: None,
            config,
            backend: None,
        }
    }

    #[test]
    fn interpolates_config_values() {
        let _guard = env_lock().lock().expect("env lock");
        let temp_dir = tempdir().expect("temp dir");
        let path = temp_dir.path().join("ferry.toml");
        env::set_var("FERRY_TEST_TOKEN", "abc123");
        fs::write(
            &path,
            r#"
url = "https://example.com/mcp"
[headers]
authorization = "Bearer ${FERRY_TEST_TOKEN}"
"#,
        )
        .expect("write config");

        let config = Config::load(empty_proxy_cli(Some(path))).expect("load config");

        assert_eq!(config.url.as_str(), "https://example.com/mcp");
        assert_eq!(
            config
                .headers
                .get("authorization")
                .and_then(|value| value.to_str().ok()),
            Some("Bearer abc123")
        );

        env::remove_var("FERRY_TEST_TOKEN");
    }

    #[test]
    fn cli_overrides_file_values() {
        let _guard = env_lock().lock().expect("env lock");
        let temp_dir = tempdir().expect("temp dir");
        let path = temp_dir.path().join("ferry.toml");
        fs::write(
            &path,
            r#"
url = "https://example.com/mcp"
transport = "sse"
"#,
        )
        .expect("write config");

        let config = Config::load(ProxyCli {
            url: Some("https://override.example.com/mcp".to_string()),
            transport: Some(TransportArg::Auto),
            config: Some(path),
            ..empty_proxy_cli(None)
        })
        .expect("load config");

        assert_eq!(config.url.as_str(), "https://override.example.com/mcp");
        assert_eq!(config.transport, TransportMode::Auto);
    }

    #[test]
    fn env_config_path_is_used_when_cli_config_is_absent() {
        let _guard = env_lock().lock().expect("env lock");
        let temp_dir = tempdir().expect("temp dir");
        let path = temp_dir.path().join("ferry-env.toml");
        fs::write(
            &path,
            r#"
url = "https://env-config.example.com/mcp"
"#,
        )
        .expect("write config");
        env::set_var("FERRY_CONFIG", &path);

        let config = Config::load(empty_proxy_cli(None)).expect("load config");

        assert_eq!(config.url.as_str(), "https://env-config.example.com/mcp");
        env::remove_var("FERRY_CONFIG");
    }

    #[test]
    fn serve_loads_backend_from_config_file() {
        let _guard = env_lock().lock().expect("env lock");
        let temp_dir = tempdir().expect("temp dir");
        let path = temp_dir.path().join("ferry-serve.toml");
        fs::write(
            &path,
            r#"
[serve]
listen = "127.0.0.1:9100"
path = "/bridge"
allow_origins = ["https://app.example.com"]

[serve.auth]
bearer_token = "shared-token"

[serve.auth.headers]
x-ferry-auth = "expected"

[serve.stdio]
command = "example-mcp"
args = ["--stdio"]

[serve.logging]
level = "info"
"#,
        )
        .expect("write config");

        let config = ServeConfig::load(empty_serve_cli(Some(path))).expect("load serve config");

        assert_eq!(config.listen.to_string(), "127.0.0.1:9100");
        assert_eq!(config.mcp_path, "/bridge");
        assert_eq!(config.allow_origins.len(), 1);
        assert_eq!(config.logging.level, crate::config::LogLevel::Info);
        match config.backend {
            ServeBackendConfig::Stdio(stdio) => {
                assert_eq!(stdio.command, "example-mcp");
                assert_eq!(stdio.args, vec!["--stdio"]);
            }
            ServeBackendConfig::Relay(_) => panic!("expected stdio backend"),
        }
    }

    #[test]
    fn serve_cli_overrides_file_backend() {
        let _guard = env_lock().lock().expect("env lock");
        let temp_dir = tempdir().expect("temp dir");
        let path = temp_dir.path().join("ferry-serve.toml");
        fs::write(
            &path,
            r#"
[serve]
listen = "127.0.0.1:9100"

[serve.relay]
url = "https://example.com/mcp"
transport = "sse"
"#,
        )
        .expect("write config");

        let config = ServeConfig::load(ServeCli {
            listen: Some("127.0.0.1:9200".parse().expect("listen")),
            config: Some(path),
            backend: Some(ServeBackendCli::Stdio(ServeStdioCli {
                command: vec!["override-server".to_string(), "--stdio".to_string()],
            })),
            ..empty_serve_cli(None)
        })
        .expect("load serve config");

        assert_eq!(config.listen.to_string(), "127.0.0.1:9200");
        match config.backend {
            ServeBackendConfig::Stdio(stdio) => {
                assert_eq!(stdio.command, "override-server");
                assert_eq!(stdio.args, vec!["--stdio"]);
            }
            ServeBackendConfig::Relay(_) => panic!("expected stdio backend"),
        }
    }

    #[test]
    fn serve_env_overrides_file_values() {
        let _guard = env_lock().lock().expect("env lock");
        let temp_dir = tempdir().expect("temp dir");
        let path = temp_dir.path().join("ferry-serve-env.toml");
        fs::write(
            &path,
            r#"
[serve]
listen = "127.0.0.1:9100"
path = "/configured"

[serve.stdio]
command = "file-server"
args = ["--stdio"]
"#,
        )
        .expect("write config");

        env::set_var("FERRY_CONFIG", &path);
        env::set_var("FERRY_SERVE_LISTEN", "127.0.0.1:9300");
        env::set_var("FERRY_SERVE_PATH", "/env");
        env::set_var("FERRY_SERVE_BACKEND", "relay");
        env::set_var("FERRY_SERVE_RELAY_URL", "https://env.example.com/mcp");
        env::set_var("FERRY_SERVE_RELAY_TRANSPORT", "streamable-http");
        env::set_var("FERRY_SERVE_RELAY_HEADER_AUTHORIZATION", "Bearer env-token");
        env::set_var("FERRY_SERVE_RELAY_RETRY", "8");

        let config = ServeConfig::load(empty_serve_cli(None)).expect("load serve config");

        assert_eq!(config.listen.to_string(), "127.0.0.1:9300");
        assert_eq!(config.mcp_path, "/env");
        match config.backend {
            ServeBackendConfig::Relay(relay) => {
                assert_eq!(relay.url.as_str(), "https://env.example.com/mcp");
                assert_eq!(relay.transport, TransportMode::StreamableHttp);
                assert_eq!(relay.retry_count, 8);
                assert_eq!(relay.headers.len(), 1);
            }
            ServeBackendConfig::Stdio(_) => panic!("expected relay backend"),
        }

        env::remove_var("FERRY_CONFIG");
        env::remove_var("FERRY_SERVE_LISTEN");
        env::remove_var("FERRY_SERVE_PATH");
        env::remove_var("FERRY_SERVE_BACKEND");
        env::remove_var("FERRY_SERVE_RELAY_URL");
        env::remove_var("FERRY_SERVE_RELAY_TRANSPORT");
        env::remove_var("FERRY_SERVE_RELAY_HEADER_AUTHORIZATION");
        env::remove_var("FERRY_SERVE_RELAY_RETRY");
    }
}
