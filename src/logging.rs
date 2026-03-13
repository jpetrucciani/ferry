use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

use tracing_subscriber::fmt::MakeWriter;

use crate::config::LoggingConfig;
use crate::error::{Error, Result};

#[derive(Clone)]
struct SharedWriter {
    inner: Arc<Mutex<Box<dyn Write + Send>>>,
}

impl SharedWriter {
    fn stderr() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Box::new(io::stderr()))),
        }
    }

    fn file(path: &Path) -> Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(Box::new(file))),
        })
    }
}

struct LockedWriter<'a> {
    guard: std::sync::MutexGuard<'a, Box<dyn Write + Send>>,
}

impl Write for LockedWriter<'_> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.guard.write(buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.guard.flush()
    }
}

impl<'a> MakeWriter<'a> for SharedWriter {
    type Writer = LockedWriter<'a>;

    fn make_writer(&'a self) -> Self::Writer {
        let guard = match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        LockedWriter { guard }
    }
}

pub fn init(config: &LoggingConfig) -> Result<()> {
    let level = config.level.as_tracing_level();

    match &config.file {
        Some(path) => {
            let writer = SharedWriter::file(path)?;
            tracing_subscriber::fmt()
                .with_max_level(level)
                .json()
                .with_writer(writer)
                .with_ansi(false)
                .try_init()
                .map_err(|error| {
                    Error::Message(format!("failed to initialize logging: {error}"))
                })?;
        }
        None => {
            let writer = SharedWriter::stderr();
            tracing_subscriber::fmt()
                .with_max_level(level)
                .with_writer(writer)
                .with_ansi(true)
                .try_init()
                .map_err(|error| {
                    Error::Message(format!("failed to initialize logging: {error}"))
                })?;
        }
    }

    Ok(())
}
