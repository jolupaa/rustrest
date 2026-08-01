//! Form bodies: `application/x-www-form-urlencoded` deserialization and a
//! hand-written, binary-safe `multipart/form-data` parser over the buffered
//! request body.

use hyper::StatusCode;
use hyper::body::Bytes;
use serde::de::DeserializeOwned;

use super::{HttpError, Request};

const DEFAULT_MAX_MULTIPART_PARTS: usize = 128;
const DEFAULT_MAX_MULTIPART_HEADERS_PER_PART: usize = 32;
const DEFAULT_MAX_MULTIPART_HEADER_BYTES: usize = 16 * 1024;
const DEFAULT_MAX_MULTIPART_PART_BYTES: usize = 8 * 1024 * 1024;
const MAX_BOUNDARY_BYTES: usize = 70;

/// Resource limits applied while parsing a buffered `multipart/form-data`
/// request. The request's overall body limit is still enforced first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MultipartLimits {
    max_parts: usize,
    max_headers_per_part: usize,
    max_header_bytes: usize,
    max_part_bytes: usize,
}

impl MultipartLimits {
    /// Creates the default limits: 128 parts, 32 headers and 16 KiB of header
    /// data per part, and 8 MiB per part.
    pub const fn new() -> Self {
        Self {
            max_parts: DEFAULT_MAX_MULTIPART_PARTS,
            max_headers_per_part: DEFAULT_MAX_MULTIPART_HEADERS_PER_PART,
            max_header_bytes: DEFAULT_MAX_MULTIPART_HEADER_BYTES,
            max_part_bytes: DEFAULT_MAX_MULTIPART_PART_BYTES,
        }
    }

    pub const fn max_parts(mut self, max_parts: usize) -> Self {
        self.max_parts = max_parts;
        self
    }

    pub const fn max_headers_per_part(mut self, max_headers: usize) -> Self {
        self.max_headers_per_part = max_headers;
        self
    }

    pub const fn max_header_bytes(mut self, max_bytes: usize) -> Self {
        self.max_header_bytes = max_bytes;
        self
    }

    pub const fn max_part_bytes(mut self, max_bytes: usize) -> Self {
        self.max_part_bytes = max_bytes;
        self
    }
}

impl Default for MultipartLimits {
    fn default() -> Self {
        Self::new()
    }
}

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
    pub async fn form<T: DeserializeOwned>(&mut self) -> Result<T, HttpError> {
        let body = self.bytes().await?;
        deserialize_form(&body)
    }

    /// Parses a `multipart/form-data` body into its parts. The whole body is
    /// collected with the request body limit before parsing in memory.
    pub async fn multipart(&mut self) -> Result<Vec<MultipartPart>, HttpError> {
        self.multipart_with_limits(MultipartLimits::default()).await
    }

    /// Parses `multipart/form-data` with explicit per-message and per-part
    /// limits. These limits cannot bypass the request's overall body limit.
    pub async fn multipart_with_limits(
        &mut self,
        limits: MultipartLimits,
    ) -> Result<Vec<MultipartPart>, HttpError> {
        let content_type = self
            .singleton_header("content-type")?
            .ok_or_else(|| HttpError::bad_request("Expected multipart/form-data"))?;
        let boundary = multipart_boundary(content_type)
            .ok_or_else(|| HttpError::bad_request("Missing multipart boundary"))?;
        let body = self.bytes().await?;
        parse_multipart(&body, &boundary, limits)
    }
}

pub(crate) fn deserialize_form<T: DeserializeOwned>(body: &[u8]) -> Result<T, HttpError> {
    serde_html_form::from_bytes(body).map_err(|error| {
        HttpError::new(
            StatusCode::BAD_REQUEST,
            "invalid_form",
            "El cuerpo no contiene un formulario valido",
        )
        .with_source(error)
    })
}

/// Extracts the boundary parameter from a `multipart/form-data` content type.
fn multipart_boundary(content_type: &str) -> Option<String> {
    let (kind, params) = parse_parameterized_value(content_type)?;
    if !kind.eq_ignore_ascii_case("multipart/form-data") {
        return None;
    }
    let boundary = unique_parameter(&params, "boundary")?;
    is_valid_boundary(boundary).then(|| boundary.to_string())
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

#[derive(Clone, Copy)]
struct Delimiter {
    closing: bool,
    next: usize,
}

fn parse_multipart(
    body: &[u8],
    boundary: &str,
    limits: MultipartLimits,
) -> Result<Vec<MultipartPart>, HttpError> {
    let delimiter = format!("--{}", boundary).into_bytes();
    let invalid = || HttpError::bad_request("Malformed multipart body");

    let mut parts = Vec::new();
    let first = find_initial_delimiter(body, &delimiter).ok_or_else(invalid)?;
    if first.closing {
        return Ok(parts);
    }
    let mut pos = first.next;

    loop {
        if parts.len() >= limits.max_parts {
            return Err(multipart_limit("Demasiadas partes multipart"));
        }

        let Some(relative_headers_end) = find(&body[pos..], b"\r\n\r\n") else {
            return Err(invalid());
        };
        if relative_headers_end > limits.max_header_bytes {
            return Err(multipart_limit(
                "Las cabeceras de una parte multipart son demasiado grandes",
            ));
        }
        let headers_end = pos + relative_headers_end;
        let data_start = headers_end + 4;
        let (delimiter_start, next) =
            find_next_delimiter(body, data_start, &delimiter).ok_or_else(invalid)?;
        let data_end = delimiter_start
            .checked_sub(2)
            .filter(|end| *end >= data_start)
            .ok_or_else(invalid)?;
        if data_end - data_start > limits.max_part_bytes {
            return Err(multipart_limit("Una parte multipart es demasiado grande"));
        }

        let (name, filename, content_type) =
            parse_part_headers(&body[pos..headers_end], limits.max_headers_per_part)?;

        parts.push(MultipartPart {
            name,
            filename,
            content_type,
            data: Bytes::copy_from_slice(&body[data_start..data_end]),
        });
        if next.closing {
            break;
        }
        pos = next.next;
    }

    Ok(parts)
}

fn multipart_limit(message: &'static str) -> HttpError {
    HttpError::payload_too_large(message)
}

fn find_initial_delimiter(body: &[u8], delimiter: &[u8]) -> Option<Delimiter> {
    let mut offset = 0;
    while offset <= body.len().saturating_sub(delimiter.len()) {
        let relative = find(&body[offset..], delimiter)?;
        let position = offset + relative;
        let at_line_start =
            position == 0 || position >= 2 && body.get(position - 2..position) == Some(b"\r\n");
        if at_line_start {
            if let Some(parsed) = parse_delimiter_line(body, position, delimiter) {
                return Some(parsed);
            }
        }
        offset = position + 1;
    }
    None
}

fn find_next_delimiter(
    body: &[u8],
    data_start: usize,
    delimiter: &[u8],
) -> Option<(usize, Delimiter)> {
    let mut offset = data_start;
    while offset <= body.len().saturating_sub(delimiter.len()) {
        let relative = find(&body[offset..], delimiter)?;
        let position = offset + relative;
        if position >= 2 && body.get(position - 2..position) == Some(b"\r\n") {
            if let Some(parsed) = parse_delimiter_line(body, position, delimiter) {
                return Some((position, parsed));
            }
        }
        offset = position + 1;
    }
    None
}

fn parse_delimiter_line(body: &[u8], position: usize, delimiter: &[u8]) -> Option<Delimiter> {
    if body.get(position..position + delimiter.len())? != delimiter {
        return None;
    }
    let mut cursor = position + delimiter.len();
    let closing = body.get(cursor..cursor + 2) == Some(b"--");
    if closing {
        cursor += 2;
    }
    while matches!(body.get(cursor), Some(b' ' | b'\t')) {
        cursor += 1;
    }
    if body.get(cursor..cursor + 2) == Some(b"\r\n") {
        cursor += 2;
    } else if !(closing && cursor == body.len()) {
        return None;
    }
    Some(Delimiter {
        closing,
        next: cursor,
    })
}

fn parse_part_headers(
    raw_headers: &[u8],
    max_headers: usize,
) -> Result<(String, Option<String>, Option<String>), HttpError> {
    let invalid = || HttpError::bad_request("Malformed multipart headers");
    let raw_headers = std::str::from_utf8(raw_headers).map_err(|_| invalid())?;
    let mut disposition = None;
    let mut content_type = None;
    let mut count = 0;

    for line in raw_headers.split("\r\n") {
        count += 1;
        if count > max_headers {
            return Err(multipart_limit(
                "Una parte multipart contiene demasiadas cabeceras",
            ));
        }
        if line.starts_with([' ', '\t']) {
            return Err(invalid());
        }
        let (name, value) = line.split_once(':').ok_or_else(invalid)?;
        if name.is_empty() || !name.bytes().all(is_token_byte) {
            return Err(invalid());
        }
        let value = value.trim();
        if value
            .chars()
            .any(|character| character == '\u{7f}' || character.is_control() && character != '\t')
        {
            return Err(invalid());
        }
        if name.eq_ignore_ascii_case("content-disposition") {
            if disposition.replace(value).is_some() {
                return Err(invalid());
            }
        } else if name.eq_ignore_ascii_case("content-type")
            && (value.is_empty() || content_type.replace(value.to_string()).is_some())
        {
            return Err(invalid());
        }
    }

    let (kind, params) =
        parse_parameterized_value(disposition.ok_or_else(invalid)?).ok_or_else(invalid)?;
    if !kind.eq_ignore_ascii_case("form-data") {
        return Err(invalid());
    }
    let name = unique_parameter(&params, "name")
        .filter(|name| !name.is_empty())
        .ok_or_else(invalid)?
        .to_string();
    let filename = unique_parameter(&params, "filename").map(str::to_string);
    Ok((name, filename, content_type))
}

fn parse_parameterized_value(value: &str) -> Option<(String, Vec<(String, String)>)> {
    let bytes = value.as_bytes();
    let mut cursor = 0;
    while cursor < bytes.len() && bytes[cursor] != b';' {
        cursor += 1;
    }
    let base = value[..cursor].trim();
    if base.is_empty() {
        return None;
    }
    let mut parameters = Vec::new();

    while cursor < bytes.len() {
        cursor += 1;
        skip_optional_whitespace(bytes, &mut cursor);
        if cursor == bytes.len() {
            break;
        }
        let name_start = cursor;
        while cursor < bytes.len() && is_token_byte(bytes[cursor]) {
            cursor += 1;
        }
        if cursor == name_start {
            return None;
        }
        let name = value[name_start..cursor].to_ascii_lowercase();
        skip_optional_whitespace(bytes, &mut cursor);
        if bytes.get(cursor) != Some(&b'=') {
            return None;
        }
        cursor += 1;
        skip_optional_whitespace(bytes, &mut cursor);

        let parameter_value = if bytes.get(cursor) == Some(&b'"') {
            cursor += 1;
            let mut decoded = Vec::new();
            let mut closed = false;
            while cursor < bytes.len() {
                match bytes[cursor] {
                    b'"' => {
                        cursor += 1;
                        closed = true;
                        break;
                    }
                    b'\\' => {
                        cursor += 1;
                        let escaped = *bytes.get(cursor)?;
                        if !is_quoted_text_byte(escaped) {
                            return None;
                        }
                        decoded.push(escaped);
                        cursor += 1;
                    }
                    byte if !is_quoted_text_byte(byte) => return None,
                    byte => {
                        decoded.push(byte);
                        cursor += 1;
                    }
                }
            }
            if !closed {
                return None;
            }
            skip_optional_whitespace(bytes, &mut cursor);
            String::from_utf8(decoded).ok()?
        } else {
            let value_start = cursor;
            while cursor < bytes.len() && bytes[cursor] != b';' {
                cursor += 1;
            }
            let unquoted = value[value_start..cursor].trim();
            if unquoted.is_empty() || !unquoted.bytes().all(is_token_byte) {
                return None;
            }
            unquoted.to_string()
        };
        if cursor < bytes.len() && bytes[cursor] != b';' {
            return None;
        }
        parameters.push((name, parameter_value));
    }

    Some((base.to_string(), parameters))
}

fn unique_parameter<'a>(params: &'a [(String, String)], name: &str) -> Option<&'a str> {
    let mut matching = params
        .iter()
        .filter(|(candidate, _)| candidate.eq_ignore_ascii_case(name));
    let value = matching.next()?.1.as_str();
    matching.next().is_none().then_some(value)
}

fn skip_optional_whitespace(bytes: &[u8], cursor: &mut usize) {
    while matches!(bytes.get(*cursor), Some(b' ' | b'\t')) {
        *cursor += 1;
    }
}

fn is_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

fn is_quoted_text_byte(byte: u8) -> bool {
    byte == b'\t' || byte >= b' ' && byte != 0x7f
}

fn is_valid_boundary(boundary: &str) -> bool {
    let bytes = boundary.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= MAX_BOUNDARY_BYTES
        && bytes.iter().copied().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'\''
                        | b'('
                        | b')'
                        | b'+'
                        | b'_'
                        | b','
                        | b'-'
                        | b'.'
                        | b'/'
                        | b':'
                        | b'='
                        | b'?'
                        | b' '
                )
        })
        && !matches!(bytes.last(), Some(b' '))
}
