use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use hyper::header::{ETAG, LAST_MODIFIED};
use sha1::{Digest, Sha1};

use crate::app::{Middleware, Next, Request};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PreconditionResult {
    Proceed,
    NotModified,
    Failed,
    IgnoreRange,
}

#[derive(Clone, Debug)]
struct ConditionalHeaders {
    method: String,
    if_match: Option<String>,
    if_none_match: Option<String>,
    if_unmodified_since: Option<SystemTime>,
    if_modified_since: Option<SystemTime>,
    if_range: Option<String>,
    has_range: bool,
}

impl ConditionalHeaders {
    fn from_request(req: &Request) -> Self {
        Self {
            method: req.method.clone(),
            if_match: req.header("if-match").map(str::to_string),
            if_none_match: req.header("if-none-match").map(str::to_string),
            if_unmodified_since: req.header("if-unmodified-since").and_then(parse_http_date),
            if_modified_since: req.header("if-modified-since").and_then(parse_http_date),
            if_range: req.header("if-range").map(str::to_string),
            has_range: req.header("range").is_some(),
        }
    }

    fn evaluate(
        &self,
        etag: Option<&str>,
        last_modified: Option<SystemTime>,
        range_requested: bool,
    ) -> PreconditionResult {
        let safe = matches!(self.method.as_str(), "GET" | "HEAD");

        if let Some(if_match) = &self.if_match {
            if !entity_tag_list_matches(if_match, etag, TagComparison::Strong) {
                return PreconditionResult::Failed;
            }
        } else if let (Some(since), Some(last_modified)) = (self.if_unmodified_since, last_modified)
        {
            if modified_after(last_modified, since) {
                return PreconditionResult::Failed;
            }
        }

        if let Some(if_none_match) = &self.if_none_match {
            if entity_tag_list_matches(if_none_match, etag, TagComparison::Weak) {
                return if safe {
                    PreconditionResult::NotModified
                } else {
                    PreconditionResult::Failed
                };
            }
        } else if safe {
            if let (Some(since), Some(last_modified)) = (self.if_modified_since, last_modified) {
                if not_modified_since(last_modified, since) {
                    return PreconditionResult::NotModified;
                }
            }
        }

        if range_requested && self.has_range {
            if let Some(if_range) = &self.if_range {
                if !if_range_matches(if_range, etag, last_modified) {
                    return PreconditionResult::IgnoreRange;
                }
            }
        }

        PreconditionResult::Proceed
    }
}

pub(crate) fn evaluate_preconditions(
    req: &Request,
    etag: Option<&str>,
    last_modified: Option<SystemTime>,
    range_requested: bool,
) -> PreconditionResult {
    ConditionalHeaders::from_request(req).evaluate(etag, last_modified, range_requested)
}

/// Strong-ETag validation for buffered 200 responses: hashes the body
/// (SHA-1), sets `ETag` when the handler did not set one, and evaluates RFC
/// preconditions (`If-Match`, dates, `If-None-Match`) in order. Streaming
/// bodies and non-200 responses pass through untouched.
pub fn etag() -> Middleware {
    Arc::new(|req: Request, next: Next| {
        let conditions = ConditionalHeaders::from_request(&req);
        Box::pin(async move {
            let mut res = next(req).await;
            if res.status != 200 {
                return res;
            }
            let tag = match res.headers.get(ETAG).and_then(|value| value.to_str().ok()) {
                Some(existing) => existing.to_string(),
                None => {
                    let Some(body) = res.body_bytes() else {
                        return res;
                    };
                    if body.is_empty() {
                        return res;
                    }
                    let tag = format!("\"{}\"", sha1_hex(body));
                    res = res.header(ETAG.as_str(), &tag);
                    tag
                }
            };
            let last_modified = res
                .headers
                .get(LAST_MODIFIED)
                .and_then(|value| value.to_str().ok())
                .and_then(parse_http_date);
            match conditions.evaluate(Some(&tag), last_modified, false) {
                PreconditionResult::Proceed | PreconditionResult::IgnoreRange => res,
                PreconditionResult::NotModified => {
                    res.clear_body();
                    res.status(304)
                }
                PreconditionResult::Failed => {
                    res.clear_body();
                    res.status(412)
                }
            }
        })
    })
}

fn sha1_hex(data: &[u8]) -> String {
    use std::fmt::Write as _;

    let digest = Sha1::digest(data);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        let _ = write!(out, "{:02x}", byte);
    }
    out
}

#[derive(Clone, Copy)]
enum TagComparison {
    Strong,
    Weak,
}

fn entity_tag_list_matches(raw: &str, current: Option<&str>, comparison: TagComparison) -> bool {
    raw.split(',').any(|candidate| {
        let candidate = candidate.trim();
        candidate == "*"
            || current.is_some_and(|current| match comparison {
                TagComparison::Strong => strong_tag_matches(candidate, current),
                TagComparison::Weak => weak_tag_matches(candidate, current),
            })
    })
}

fn strong_tag_matches(left: &str, right: &str) -> bool {
    !is_weak_tag(left) && !is_weak_tag(right) && left.trim() == right.trim()
}

fn weak_tag_matches(left: &str, right: &str) -> bool {
    opaque_tag(left) == opaque_tag(right)
}

fn is_weak_tag(tag: &str) -> bool {
    tag.trim().starts_with("W/")
}

fn opaque_tag(tag: &str) -> &str {
    tag.trim().strip_prefix("W/").unwrap_or(tag.trim())
}

fn if_range_matches(
    raw: &str,
    current_etag: Option<&str>,
    last_modified: Option<SystemTime>,
) -> bool {
    let raw = raw.trim();
    if raw.starts_with('"') || raw.starts_with("W/") {
        return current_etag.is_some_and(|etag| strong_tag_matches(raw, etag));
    }
    match (parse_http_date(raw), last_modified) {
        (Some(if_range), Some(last_modified)) => not_modified_since(last_modified, if_range),
        _ => false,
    }
}

fn parse_http_date(raw: &str) -> Option<SystemTime> {
    httpdate::parse_http_date(raw).ok()
}

fn modified_after(last_modified: SystemTime, since: SystemTime) -> bool {
    seconds(last_modified) > seconds(since)
}

fn not_modified_since(last_modified: SystemTime, since: SystemTime) -> bool {
    seconds(last_modified) <= seconds(since)
}

fn seconds(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}
