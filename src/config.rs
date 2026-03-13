use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::Url;
use serde::Deserialize;

use crate::cli::{Cli, LogLevelArg, TransportArg};
use crate::error::{Error, Result};

const DEFAULT_TIMEOUT_SECONDS: u64 = 30;
const DEFAULT_RETRY_COUNT: usize = 3;
const DEFAULT_RETRY_DELAY_MS: u64 = 1_000;

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

impl Config {
    pub fn load(cli: Cli) -> Result<Self> {
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

fn load_file_config(path: &Path) -> Result<PartialConfig> {
    let contents = fs::read_to_string(path)?;
    let mut value: toml::Value = toml::from_str(&contents)?;
    interpolate_env(&mut value)?;
    let config: FileConfig = value.try_into()?;

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

    let mut headers = BTreeMap::new();
    for (key, value) in env::vars() {
        if let Some(name) = key.strip_prefix("FERRY_HEADER_") {
            headers.insert(canonicalize_header_name(name), value);
        }
    }
    partial.headers = normalize_header_map(headers)?;

    Ok(partial)
}

fn load_cli_config(cli: &Cli) -> Result<PartialConfig> {
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

    let mut headers = BTreeMap::new();
    for header in &cli.headers {
        let (name, value) = split_header(header)?;
        headers.insert(name, value);
    }
    partial.headers = normalize_header_map(headers)?;

    Ok(partial)
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

fn canonicalize_header_name(name: &str) -> String {
    name.trim().replace('_', "-").to_ascii_lowercase()
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

#[cfg(test)]
mod tests {
    use std::env;
    use std::fs;

    use tempfile::tempdir;

    use crate::cli::Cli;
    use crate::config::{Config, TransportMode};

    #[test]
    fn interpolates_config_values() {
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

        let config = Config::load(Cli {
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
            config: Some(path),
        })
        .expect("load config");

        assert_eq!(config.url.as_str(), "https://example.com/mcp");
        assert_eq!(
            config
                .headers
                .get("authorization")
                .and_then(|value| value.to_str().ok()),
            Some("Bearer abc123")
        );
    }

    #[test]
    fn cli_overrides_file_values() {
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

        let config = Config::load(Cli {
            url: Some("https://override.example.com/mcp".to_string()),
            transport: Some(crate::cli::TransportArg::Auto),
            socks5: None,
            socks5_auth: None,
            headers: Vec::new(),
            timeout: None,
            retry: None,
            retry_delay: None,
            log_level: None,
            log_file: None,
            config: Some(path),
        })
        .expect("load config");

        assert_eq!(config.url.as_str(), "https://override.example.com/mcp");
        assert_eq!(config.transport, TransportMode::Auto);
    }

    #[test]
    fn env_config_path_is_used_when_cli_config_is_absent() {
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

        let config = Config::load(Cli {
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
            config: None,
        })
        .expect("load config");

        assert_eq!(config.url.as_str(), "https://env-config.example.com/mcp");
        env::remove_var("FERRY_CONFIG");
    }
}
