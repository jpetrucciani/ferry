use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter, Stdin, Stdout};

use crate::error::{Error, Result};

#[derive(Debug)]
pub struct StdioCodec {
    reader: BufReader<Stdin>,
    writer: BufWriter<Stdout>,
}

impl StdioCodec {
    pub fn new(stdin: Stdin, stdout: Stdout) -> Self {
        Self {
            reader: BufReader::new(stdin),
            writer: BufWriter::new(stdout),
        }
    }

    pub async fn read_message(&mut self) -> Result<Option<serde_json::Value>> {
        let mut line = String::new();
        let read = self.reader.read_line(&mut line).await?;
        if read == 0 {
            return Ok(None);
        }

        while line.ends_with('\n') || line.ends_with('\r') {
            line.pop();
        }

        if line.trim().is_empty() {
            return Err(Error::InvalidResponse(
                "blank JSON-RPC frame on stdin".to_string(),
            ));
        }

        let value = serde_json::from_str(&line)?;
        Ok(Some(value))
    }

    pub async fn write_message(&mut self, value: &serde_json::Value) -> Result<()> {
        let serialized = serde_json::to_vec(value)?;
        self.writer.write_all(&serialized).await?;
        self.writer.write_all(b"\n").await?;
        self.writer.flush().await?;
        Ok(())
    }
}
