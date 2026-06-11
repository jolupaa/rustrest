//! Form bodies: `application/x-www-form-urlencoded` deserialization and a
//! hand-written, binary-safe `multipart/form-data` parser over the buffered
//! request body.

use hyper::body::Bytes;
use serde::de::DeserializeOwned;

use super::{HttpError, Request};

/// One part of a `multipart/form-data` body: a form field or an uploaded file.
#[derive(Debug, Clone)]
pub struct MultipartPart {
    /// The `name` from `Content-Disposition`.
    pub name: String,
    /// The `filename` from `Content-Disposition`, when the part is a file.
    pub filename: Option<String>,
    /// The part's `Content-Type`, if declared.
    pub content_type: Option<String>,
    /// Raw part data (binary-safe).
    pub data: Bytes,
}

impl MultipartPart {
    /// Returns the part data as a lossy UTF-8 string (for text fields).
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.data).into_owned()
    }
}

impl Request {
    /// Deserializes an `application/x-www-form-urlencoded` body into `T`.
    /// Repeated keys map onto `Vec` fields.
    pub fn form<T: DeserializeOwned>(&self) -> Result<T, HttpError> {
        serde_html_form::from_bytes(self.bytes())
            .map_err(|err| HttpError::bad_request(format!("Invalid form body: {}", err)))
    }

    /// Parses a `multipart/form-data` body into its parts. The whole body is
    /// already buffered (bounded by `max_body_size`), so parsing is in-memory.
    pub fn multipart(&self) -> Result<Vec<MultipartPart>, HttpError> {
        let content_type = self
            .header("content-type")
            .ok_or_else(|| HttpError::bad_request("Expected multipart/form-data"))?;
        let boundary = multipart_boundary(content_type)
            .ok_or_else(|| HttpError::bad_request("Missing multipart boundary"))?;
        parse_multipart(self.bytes(), &boundary)
    }
}

/// Extracts the boundary parameter from a `multipart/form-data` content type.
fn multipart_boundary(content_type: &str) -> Option<String> {
    let (kind, params) = content_type.split_once(';')?;
    if !kind.trim().eq_ignore_ascii_case("multipart/form-data") {
        return None;
    }
    for param in params.split(';') {
        let (key, value) = param.split_once('=')?;
        if key.trim().eq_ignore_ascii_case("boundary") {
            return Some(value.trim().trim_matches('"').to_string());
        }
    }
    None
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn parse_multipart(body: &[u8], boundary: &str) -> Result<Vec<MultipartPart>, HttpError> {
    let delimiter = format!("--{}", boundary).into_bytes();
    let invalid = || HttpError::bad_request("Malformed multipart body");

    let mut parts = Vec::new();
    let mut pos = find(body, &delimiter).ok_or_else(invalid)?;

    loop {
        pos += delimiter.len();
        // `--` after the delimiter closes the body.
        if body[pos..].starts_with(b"--") {
            break;
        }
        if body[pos..].starts_with(b"\r\n") {
            pos += 2;
        }

        let headers_end = find(&body[pos..], b"\r\n\r\n").ok_or_else(invalid)? + pos;
        let data_start = headers_end + 4;
        let next_delimiter =
            find(&body[data_start..], &delimiter).ok_or_else(invalid)? + data_start;
        // Part data ends before the CRLF that precedes the next delimiter.
        let data_end = next_delimiter.saturating_sub(2).max(data_start);

        let mut name = None;
        let mut filename = None;
        let mut content_type = None;
        for line in String::from_utf8_lossy(&body[pos..headers_end]).lines() {
            let Some((header, value)) = line.split_once(':') else {
                continue;
            };
            let value = value.trim();
            if header.eq_ignore_ascii_case("content-disposition") {
                for param in value.split(';') {
                    let Some((key, param_value)) = param.split_once('=') else {
                        continue;
                    };
                    let param_value = param_value.trim().trim_matches('"').to_string();
                    match key.trim().to_ascii_lowercase().as_str() {
                        "name" => name = Some(param_value),
                        "filename" => filename = Some(param_value),
                        _ => {}
                    }
                }
            } else if header.eq_ignore_ascii_case("content-type") {
                content_type = Some(value.to_string());
            }
        }

        parts.push(MultipartPart {
            name: name.ok_or_else(invalid)?,
            filename,
            content_type,
            data: Bytes::copy_from_slice(&body[data_start..data_end]),
        });
        pos = next_delimiter;
    }

    Ok(parts)
}
