use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use hyper::header::{
    ACCEPT_RANGES, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_RANGE, ETAG, LAST_MODIFIED,
};
use hyper::{HeaderMap, header::HeaderName};
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;

use crate::app::{HttpError, Middleware, Next, Request, Response};

const BLOCKING_HASH_THRESHOLD: usize = 64 * 1024;

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
    if_range: SingletonRequestHeader,
    has_range: bool,
}

#[derive(Clone, Debug)]
enum SingletonRequestHeader {
    Missing,
    Value(String),
    Invalid,
}

impl ConditionalHeaders {
    fn from_request(req: &Request) -> Self {
        Self {
            method: req.method.clone(),
            if_match: combined_request_header(req, "if-match"),
            if_none_match: combined_request_header(req, "if-none-match"),
            if_unmodified_since: singleton_request_header(req, "if-unmodified-since")
                .and_then(parse_http_date),
            if_modified_since: singleton_request_header(req, "if-modified-since")
                .and_then(parse_http_date),
            if_range: singleton_request_header_state(req, "if-range"),
            has_range: req.header("range").is_some(),
        }
    }

    fn evaluate(
        &self,
        etag: Option<&str>,
        last_modified: Option<SystemTime>,
        range_requested: bool,
    ) -> PreconditionResult {
        self.evaluate_with_response_validators(etag, true, last_modified, range_requested)
    }

    fn evaluate_with_response_validators(
        &self,
        etag: Option<&str>,
        etag_is_unambiguous: bool,
        last_modified: Option<SystemTime>,
        range_requested: bool,
    ) -> PreconditionResult {
        let safe = matches!(self.method.as_str(), "GET" | "HEAD");

        if let Some(if_match) = &self.if_match {
            if entity_tag_list_matches(if_match, etag, etag_is_unambiguous, TagComparison::Strong)
                == Some(false)
            {
                return PreconditionResult::Failed;
            }
        } else if let (Some(since), Some(last_modified)) = (self.if_unmodified_since, last_modified)
        {
            if modified_after(last_modified, since) {
                return PreconditionResult::Failed;
            }
        }

        if let Some(if_none_match) = &self.if_none_match {
            if entity_tag_list_matches(
                if_none_match,
                etag,
                etag_is_unambiguous,
                TagComparison::Weak,
            ) == Some(true)
            {
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
            match &self.if_range {
                SingletonRequestHeader::Invalid => {
                    return PreconditionResult::IgnoreRange;
                }
                SingletonRequestHeader::Value(if_range)
                    if !if_range_matches(if_range, etag, last_modified) =>
                {
                    return PreconditionResult::IgnoreRange;
                }
                SingletonRequestHeader::Missing | SingletonRequestHeader::Value(_) => {}
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
/// (SHA-256), sets `ETag` when the handler did not set one, and evaluates RFC
/// preconditions (`If-Match`, dates, `If-None-Match`) in order for `GET` and
/// `HEAD`. Unsafe-method preconditions require a pre-handler guard; evaluating
/// them here would be too late to prevent a mutation. Streaming bodies and
/// non-200 responses pass through untouched.
pub fn etag() -> Middleware {
    Arc::new(|req: Request, next: Next| {
        // This middleware only sees the response after the handler has run.
        // Enforcing an unsafe-method precondition here would report failure
        // after the mutation already happened, so automatic evaluation is
        // deliberately limited to retrieval methods.
        let conditions = matches!(req.method.as_str(), "GET" | "HEAD")
            .then(|| ConditionalHeaders::from_request(&req));
        Box::pin(async move {
            let mut res = next(req).await;
            if res.status != 200 {
                return res;
            }
            let (tag, etag_is_unambiguous) =
                match singleton_response_header(&res.headers, &ETAG, |value| {
                    parse_entity_tag(value).map(|_| value.to_string())
                }) {
                    SingletonResponseHeader::Valid(existing) => (Some(existing), true),
                    SingletonResponseHeader::Missing => {
                        let Some(body) = res.body_bytes() else {
                            return res;
                        };
                        let digest = if body.len() >= BLOCKING_HASH_THRESHOLD {
                            let permit = match Arc::clone(etag_hash_permits()).acquire_owned().await
                            {
                                Ok(permit) => permit,
                                Err(error) => {
                                    return Response::from_error(
                                        HttpError::internal_server_error(
                                            "No se pudo programar el validador de la respuesta",
                                        )
                                        .with_source(error),
                                    );
                                }
                            };
                            // Do not allocate the offload copy until this request
                            // has been admitted to the bounded blocking workload.
                            let body = res
                                .body_bytes()
                                .expect("the buffered response body is still present")
                                .to_vec();
                            match tokio::task::spawn_blocking(move || {
                                let _permit = permit;
                                sha256_hex(&body)
                            })
                            .await
                            {
                                Ok(digest) => digest,
                                Err(error) => {
                                    return Response::from_error(
                                        HttpError::internal_server_error(
                                            "No se pudo calcular el validador de la respuesta",
                                        )
                                        .with_source(error),
                                    );
                                }
                            }
                        } else {
                            sha256_hex(body)
                        };
                        let tag = format!("\"{digest}\"");
                        res = res.header(ETAG.as_str(), &tag);
                        (Some(tag), true)
                    }
                    // A malformed or repeated response ETag is not equivalent to
                    // a representation without an ETag. In particular, treating
                    // it as absent would make every If-Match tag fail with 412.
                    // Preserve the response metadata, but do not select one of
                    // the ambiguous values for conditional evaluation.
                    SingletonResponseHeader::Invalid => (None, false),
                };
            let (last_modified, last_modified_is_unambiguous) =
                match singleton_response_header(&res.headers, &LAST_MODIFIED, parse_http_date) {
                    SingletonResponseHeader::Valid(last_modified) => (Some(last_modified), true),
                    // Last-Modified has singleton field syntax. A repeated or
                    // malformed value cannot safely participate in date
                    // preconditions, so it is treated as unavailable.
                    SingletonResponseHeader::Missing => (None, true),
                    SingletonResponseHeader::Invalid => (None, false),
                };
            let Some(conditions) = conditions else {
                return res;
            };
            match conditions.evaluate_with_response_validators(
                tag.as_deref(),
                etag_is_unambiguous,
                last_modified,
                false,
            ) {
                PreconditionResult::Proceed | PreconditionResult::IgnoreRange => res,
                PreconditionResult::NotModified => {
                    // Never turn a selected representation into a 304 that
                    // carries ambiguous singleton validators. A wildcard or
                    // the other validator can still legitimately select the
                    // representation, but the malformed metadata must not be
                    // forwarded on the bodyless response.
                    if !etag_is_unambiguous {
                        res.headers.remove(ETAG);
                    }
                    if !last_modified_is_unambiguous {
                        res.headers.remove(LAST_MODIFIED);
                    }
                    // Validate or derive representation metadata before the
                    // selected bytes are discarded for the 304 response.
                    res.prepare_for_not_modified();
                    res.status(304)
                }
                PreconditionResult::Failed => {
                    res.clear_body_and_trailers();
                    remove_failed_representation_headers(&mut res);
                    res.status(412)
                }
            }
        })
    })
}

fn combined_request_header(req: &Request, name: &str) -> Option<String> {
    let values = req.headers_all(name);
    if values.is_empty() {
        req.header(name).map(str::to_string)
    } else {
        Some(values.join(","))
    }
}

fn singleton_request_header<'a>(req: &'a Request, name: &str) -> Option<&'a str> {
    let values = req.headers_all(name);
    match values.as_slice() {
        [] => req.header(name),
        [value] => Some(*value),
        // Date and If-Range fields do not use list syntax. Treat repeated
        // instances as invalid rather than inventing an ordering rule.
        _ => None,
    }
}

fn singleton_request_header_state(req: &Request, name: &str) -> SingletonRequestHeader {
    let values = req.headers_all(name);
    match values.as_slice() {
        [] => req
            .header(name)
            .map(|value| SingletonRequestHeader::Value(value.to_string()))
            .unwrap_or(SingletonRequestHeader::Missing),
        [value] => SingletonRequestHeader::Value((*value).to_string()),
        // If-Range has no list syntax. Applying a range after an ambiguous
        // duplicate would be an unsafe interpretation, so fail closed to the
        // complete representation.
        _ => SingletonRequestHeader::Invalid,
    }
}

fn etag_hash_permits() -> &'static Arc<Semaphore> {
    static PERMITS: OnceLock<Arc<Semaphore>> = OnceLock::new();
    PERMITS.get_or_init(|| {
        let parallelism = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1);
        Arc::new(Semaphore::new(parallelism.saturating_mul(2).max(1)))
    })
}

fn remove_failed_representation_headers(res: &mut Response) {
    for name in [
        CONTENT_LENGTH.as_str(),
        CONTENT_ENCODING.as_str(),
        ACCEPT_RANGES.as_str(),
        CONTENT_RANGE.as_str(),
        "content-md5",
        "digest",
        "content-digest",
        "repr-digest",
    ] {
        res.headers.remove(name);
    }
}

fn sha256_hex(data: &[u8]) -> String {
    use std::fmt::Write as _;

    let digest = Sha256::digest(data);
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

fn entity_tag_list_matches(
    raw: &str,
    current: Option<&str>,
    current_is_unambiguous: bool,
    comparison: TagComparison,
) -> Option<bool> {
    let candidates = entity_tag_list(raw);
    if candidates.iter().any(|candidate| candidate.trim() == "*") {
        return Some(true);
    }
    if !current_is_unambiguous {
        return None;
    }
    Some(candidates.into_iter().any(|candidate| {
        let candidate = candidate.trim();
        current.is_some_and(|current| match comparison {
            TagComparison::Strong => strong_tag_matches(candidate, current),
            TagComparison::Weak => weak_tag_matches(candidate, current),
        })
    }))
}

enum SingletonResponseHeader<T> {
    Missing,
    Valid(T),
    Invalid,
}

fn singleton_response_header<T>(
    headers: &HeaderMap,
    name: &HeaderName,
    parse: impl FnOnce(&str) -> Option<T>,
) -> SingletonResponseHeader<T> {
    let mut values = headers.get_all(name).iter();
    let Some(value) = values.next() else {
        return SingletonResponseHeader::Missing;
    };
    if values.next().is_some() {
        return SingletonResponseHeader::Invalid;
    }
    let Ok(value) = value.to_str() else {
        return SingletonResponseHeader::Invalid;
    };
    parse(value)
        .map(SingletonResponseHeader::Valid)
        .unwrap_or(SingletonResponseHeader::Invalid)
}

fn strong_tag_matches(left: &str, right: &str) -> bool {
    matches!(
        (parse_entity_tag(left), parse_entity_tag(right)),
        (Some((false, left)), Some((false, right))) if left == right
    )
}

fn weak_tag_matches(left: &str, right: &str) -> bool {
    matches!(
        (parse_entity_tag(left), parse_entity_tag(right)),
        (Some((_, left)), Some((_, right))) if left == right
    )
}

/// Splits an entity-tag list only on commas outside quoted opaque tags. A
/// comma is legal inside an ETag (for example `"release,1"`).
fn entity_tag_list(raw: &str) -> Vec<&str> {
    let mut values = Vec::new();
    let mut start = 0;
    let mut quoted = false;
    for (index, byte) in raw.bytes().enumerate() {
        match byte {
            b'"' => quoted = !quoted,
            b',' if !quoted => {
                values.push(&raw[start..index]);
                start = index + 1;
            }
            _ => {}
        }
    }
    values.push(&raw[start..]);
    values
}

fn parse_entity_tag(raw: &str) -> Option<(bool, &str)> {
    let raw = raw.trim();
    let (weak, tag) = match raw.strip_prefix("W/") {
        Some(tag) => (true, tag),
        None => (false, raw),
    };
    let opaque = tag.strip_prefix('"')?.strip_suffix('"')?;
    if opaque
        .bytes()
        .all(|byte| byte == b'!' || (b'#'..=b'~').contains(&byte) || byte >= 0x80)
    {
        Some((weak, opaque))
    } else {
        None
    }
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
        // Unlike If-Modified-Since, If-Range's date validator must match the
        // selected representation's modification time exactly at HTTP-date
        // (whole-second) precision.
        (Some(if_range), Some(last_modified)) => seconds(last_modified) == seconds(if_range),
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
