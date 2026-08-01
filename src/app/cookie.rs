//! Response cookies: a `Set-Cookie` builder with the standard attributes, and
//! HMAC-SHA256 helpers to sign/verify cookie values.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hmac::{Hmac, Mac};
use hyper::header::SET_COOKIE;
use sha2::Sha256;

use super::Response;

type HmacSha256 = Hmac<Sha256>;

/// `SameSite` cookie attribute.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SameSite {
    Strict,
    Lax,
    None,
}

impl SameSite {
    fn as_str(self) -> &'static str {
        match self {
            SameSite::Strict => "Strict",
            SameSite::Lax => "Lax",
            SameSite::None => "None",
        }
    }
}

/// A response cookie with the standard attributes. Defaults to `Path=/` with
/// no other attributes; finish with [`Response::set_cookie`].
#[derive(Clone, Debug)]
pub struct Cookie {
    name: String,
    value: String,
    path: String,
    domain: Option<String>,
    max_age_secs: Option<i64>,
    secure: bool,
    http_only: bool,
    same_site: Option<SameSite>,
}

impl Cookie {
    pub fn new(name: &str, value: &str) -> Self {
        Self {
            name: sanitize_cookie_name(name),
            value: sanitize_cookie_value(value),
            path: "/".to_string(),
            domain: None,
            max_age_secs: None,
            secure: false,
            http_only: false,
            same_site: None,
        }
    }

    pub fn path(mut self, path: &str) -> Self {
        let path = sanitize_cookie_path(path);
        self.path = if path.is_empty() {
            "/".to_string()
        } else {
            path
        };
        self
    }

    pub fn domain(mut self, domain: &str) -> Self {
        let domain = sanitize_cookie_domain(domain);
        self.domain = (!domain.is_empty()).then_some(domain);
        self
    }

    /// Sets `Max-Age` in seconds. `0` removes the cookie immediately.
    pub fn max_age_secs(mut self, seconds: i64) -> Self {
        self.max_age_secs = Some(seconds);
        self
    }

    pub fn secure(mut self, secure: bool) -> Self {
        self.secure = secure;
        self
    }

    pub fn http_only(mut self, http_only: bool) -> Self {
        self.http_only = http_only;
        self
    }

    pub fn same_site(mut self, same_site: SameSite) -> Self {
        self.same_site = Some(same_site);
        self
    }

    /// Renders the `Set-Cookie` header value.
    pub fn to_header_value(&self) -> String {
        let host_prefix = self.name.starts_with("__Host-");
        let secure_prefix = self.name.starts_with("__Secure-");
        let secure =
            self.secure || self.same_site == Some(SameSite::None) || host_prefix || secure_prefix;
        let path = if host_prefix { "/" } else { &self.path };
        let domain = if host_prefix {
            None
        } else {
            self.domain.as_deref()
        };

        let mut out = format!("{}={}; Path={path}", self.name, self.value);
        if let Some(domain) = domain {
            out.push_str("; Domain=");
            out.push_str(domain);
        }
        if let Some(max_age) = self.max_age_secs {
            out.push_str("; Max-Age=");
            out.push_str(&max_age.to_string());
        }
        if secure {
            out.push_str("; Secure");
        }
        if self.http_only {
            out.push_str("; HttpOnly");
        }
        if let Some(same_site) = self.same_site {
            out.push_str("; SameSite=");
            out.push_str(same_site.as_str());
        }
        out
    }
}

impl Response {
    /// Appends a `Set-Cookie` header built from [`Cookie`].
    pub fn set_cookie(self, cookie: Cookie) -> Self {
        self.append_header(SET_COOKIE.as_str(), &cookie.to_header_value())
    }

    /// Tells the client to delete a cookie (`Max-Age=0`).
    pub fn clear_cookie(self, name: &str) -> Self {
        self.clear_cookie_with(Cookie::new(name, ""))
    }

    /// Deletes a cookie while preserving the supplied scope and security
    /// attributes. The cookie value is discarded and `Max-Age=0` is forced.
    ///
    /// Use the same `Path` and `Domain` values that were used when the cookie
    /// was created; deleting a root-path cookie does not remove a narrower one.
    pub fn clear_cookie_with(self, mut cookie: Cookie) -> Self {
        cookie.value.clear();
        cookie.max_age_secs = Some(0);
        self.set_cookie(cookie)
    }
}

fn sanitize_cookie_name(value: &str) -> String {
    value
        .bytes()
        .filter(|byte| {
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
        })
        .map(char::from)
        .collect()
}

fn sanitize_cookie_value(value: &str) -> String {
    value
        .bytes()
        .filter(|byte| matches!(byte, 0x21 | 0x23..=0x2b | 0x2d..=0x3a | 0x3c..=0x5b | 0x5d..=0x7e))
        .map(char::from)
        .collect()
}

fn sanitize_cookie_path(value: &str) -> String {
    value
        .chars()
        .filter(|character| *character != ';' && !character.is_control())
        .collect()
}

fn sanitize_cookie_domain(value: &str) -> String {
    value
        .bytes()
        .filter(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b':' | b'[' | b']')
        })
        .map(char::from)
        .collect()
}

/// Signs `value` with HMAC-SHA256, returning `value.signature` (the signature
/// is base64url). Verify with [`verify_value`].
pub fn sign_value(secret: &str, value: &str) -> String {
    format!("{}.{}", value, signature(secret, value))
}

/// Verifies a value produced by [`sign_value`], returning the original value
/// when the signature matches.
pub fn verify_value(secret: &str, signed: &str) -> Option<String> {
    let (value, signature_b64) = signed.rsplit_once('.')?;
    let decoded = URL_SAFE_NO_PAD.decode(signature_b64).ok()?;
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).ok()?;
    mac.update(value.as_bytes());
    // Constant-time comparison via the hmac crate.
    mac.verify_slice(&decoded).ok()?;
    Some(value.to_string())
}

fn signature(secret: &str, value: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(value.as_bytes());
    URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
}
