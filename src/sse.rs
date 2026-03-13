use std::time::Duration;

use crate::error::{Error, Result};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SseEvent {
    pub event: String,
    pub data: String,
    pub id: Option<String>,
    pub retry: Option<Duration>,
}

#[derive(Debug, Default)]
pub struct SseParser {
    buffer: Vec<u8>,
    current_event: Option<String>,
    current_data: String,
    current_id: Option<String>,
    current_retry: Option<Duration>,
}

impl SseParser {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn feed(&mut self, chunk: &[u8]) -> Result<Vec<SseEvent>> {
        self.buffer.extend_from_slice(chunk);
        let mut events = Vec::new();

        while let Some(position) = self.find_line_end() {
            let mut line = self.buffer.drain(..position).collect::<Vec<u8>>();
            let has_cr = line.last().copied() == Some(b'\r');
            if has_cr {
                line.pop();
            }
            if self.buffer.first().copied() == Some(b'\n') {
                self.buffer.drain(..1);
            }

            let line = String::from_utf8(line)
                .map_err(|error| Error::Message(format!("invalid SSE UTF-8: {error}")))?;

            if line.is_empty() {
                if let Some(event) = self.finish_event() {
                    events.push(event);
                }
                continue;
            }

            if line.starts_with(':') {
                continue;
            }

            let (field, value) = match line.split_once(':') {
                Some((field, value)) => (field, value.strip_prefix(' ').unwrap_or(value)),
                None => (line.as_str(), ""),
            };

            match field {
                "event" => self.current_event = Some(value.to_string()),
                "data" => {
                    self.current_data.push_str(value);
                    self.current_data.push('\n');
                }
                "id" if !value.contains('\0') => {
                    self.current_id = Some(value.to_string());
                }
                "retry" => {
                    if let Ok(parsed) = value.parse::<u64>() {
                        self.current_retry = Some(Duration::from_millis(parsed));
                    }
                }
                "id" => {}
                _ => {}
            }
        }

        Ok(events)
    }

    pub fn finish(&mut self) -> Option<SseEvent> {
        self.finish_event()
    }

    fn find_line_end(&self) -> Option<usize> {
        self.buffer.iter().position(|byte| *byte == b'\n')
    }

    fn finish_event(&mut self) -> Option<SseEvent> {
        if self.current_data.is_empty() && self.current_event.is_none() && self.current_id.is_none()
        {
            self.current_retry = None;
            return None;
        }

        let mut data = self.current_data.clone();
        if data.ends_with('\n') {
            data.pop();
        }

        let event = SseEvent {
            event: self
                .current_event
                .clone()
                .unwrap_or_else(|| "message".to_string()),
            data,
            id: self.current_id.clone(),
            retry: self.current_retry,
        };

        self.current_event = None;
        self.current_data.clear();
        self.current_id = None;
        self.current_retry = None;

        Some(event)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::sse::{SseEvent, SseParser};

    #[test]
    fn parses_multiline_events_and_metadata() {
        let mut parser = SseParser::new();
        let events = parser
            .feed(b"event: endpoint\nid: 42\nretry: 1500\ndata: /messages\ndata: /next\n\n")
            .expect("parse");

        assert_eq!(
            events,
            vec![SseEvent {
                event: "endpoint".to_string(),
                data: "/messages\n/next".to_string(),
                id: Some("42".to_string()),
                retry: Some(Duration::from_millis(1_500)),
            }]
        );
    }
}
