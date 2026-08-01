use std::error::Error;
use std::fmt::{Display, Formatter};

pub struct SseEvent {
    data: String,
    event: Option<String>,
    id: Option<String>,
    retry: Option<u64>,
    comment: Option<String>,
}

impl SseEvent {
    pub fn new(data: impl Into<String>) -> Self {
        Self {
            data: data.into(),
            event: None,
            id: None,
            retry: None,
            comment: None,
        }
    }

    /// A comment-only event (`: text`). Browsers ignore it; it keeps the
    /// connection alive through proxies (see `Response::sse_with_heartbeat`).
    pub fn comment(text: impl Into<String>) -> Self {
        Self {
            data: String::new(),
            event: None,
            id: None,
            retry: None,
            comment: Some(text.into()),
        }
    }

    pub fn event(mut self, event: impl Into<String>) -> Self {
        self.event = Some(event.into());
        self
    }

    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    pub fn retry(mut self, retry: u64) -> Self {
        self.retry = Some(retry);
        self
    }

    #[cfg(test)]
    pub(super) fn format(&self) -> String {
        self.try_format()
            .expect("SseEvent::format is only used with already valid test events")
    }

    pub(super) fn try_format(&self) -> Result<String, SseError> {
        let mut out = String::new();
        if let Some(comment) = &self.comment {
            validate_single_line("comment", comment)?;
            out.push_str(": ");
            out.push_str(comment);
            out.push('\n');
            out.push('\n');
            return Ok(out);
        }
        if let Some(id) = &self.id {
            validate_single_line("id", id)?;
            out.push_str("id: ");
            out.push_str(id);
            out.push('\n');
        }
        if let Some(event) = &self.event {
            validate_single_line("event", event)?;
            out.push_str("event: ");
            out.push_str(event);
            out.push('\n');
        }
        if let Some(retry) = self.retry {
            out.push_str("retry: ");
            out.push_str(&retry.to_string());
            out.push('\n');
        }
        append_data_lines(&mut out, &self.data);
        out.push('\n');
        Ok(out)
    }
}

/// Prefixes every logical line, including bare-CR lines and a trailing empty
/// line. `str::lines()` does not split on a lone CR, which would let data such
/// as `safe\rid: injected` create an unintended SSE field at the client.
fn append_data_lines(out: &mut String, data: &str) {
    let bytes = data.as_bytes();
    let mut start = 0;
    let mut cursor = 0;
    loop {
        if cursor == bytes.len() {
            out.push_str("data: ");
            out.push_str(&data[start..cursor]);
            out.push('\n');
            return;
        }
        if matches!(bytes[cursor], b'\r' | b'\n') {
            out.push_str("data: ");
            out.push_str(&data[start..cursor]);
            out.push('\n');
            if bytes[cursor] == b'\r' && bytes.get(cursor + 1) == Some(&b'\n') {
                cursor += 2;
            } else {
                cursor += 1;
            }
            start = cursor;
        } else {
            cursor += 1;
        }
    }
}

fn validate_single_line(field: &'static str, value: &str) -> Result<(), SseError> {
    if value.contains('\r') || value.contains('\n') {
        Err(SseError::invalid_field(field))
    } else {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SseError {
    message: String,
}

impl SseError {
    fn invalid_field(field: &'static str) -> Self {
        Self {
            message: format!("El campo SSE {field} no puede contener saltos de linea"),
        }
    }
}

impl Display for SseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for SseError {}
