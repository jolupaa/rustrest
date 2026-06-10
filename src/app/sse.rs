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

    pub(super) fn format(&self) -> String {
        let mut out = String::new();
        if let Some(comment) = &self.comment {
            out.push_str(": ");
            out.push_str(comment);
            out.push('\n');
            out.push('\n');
            return out;
        }
        if let Some(id) = &self.id {
            out.push_str("id: ");
            out.push_str(id);
            out.push('\n');
        }
        if let Some(event) = &self.event {
            out.push_str("event: ");
            out.push_str(event);
            out.push('\n');
        }
        if let Some(retry) = self.retry {
            out.push_str("retry: ");
            out.push_str(&retry.to_string());
            out.push('\n');
        }
        for line in self.data.lines() {
            out.push_str("data: ");
            out.push_str(line);
            out.push('\n');
        }
        if self.data.is_empty() {
            out.push_str("data: \n");
        }
        out.push('\n');
        out
    }
}
