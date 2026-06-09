//! Minimal in-memory sessions: a signed session-id cookie managed by a
//! middleware, with a shared key-value store per session.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use super::cookie::{Cookie, SameSite, sign_value, verify_value};
use super::{Middleware, Next, Request};

const SESSION_ID_HEADER: &str = "x-session-id";
static SESSION_COUNTER: AtomicU64 = AtomicU64::new(1);

/// A minimal in-memory session store. Clone it freely (clones share storage):
/// keep one clone for your handlers and register `.middleware()` on the app.
///
/// The middleware guarantees every request carries a verified session id
/// (creating one and setting a signed, HttpOnly cookie when absent or
/// tampered). Handlers read it with [`Request::session_id`] and use
/// `get`/`set`/`remove`/`clear` for the session's data.
#[derive(Clone)]
pub struct Sessions {
    secret: String,
    cookie_name: String,
    store: Arc<Mutex<HashMap<String, HashMap<String, String>>>>,
}

impl Sessions {
    pub fn new(secret: &str) -> Self {
        Self {
            secret: secret.to_string(),
            cookie_name: "rustrest_session".to_string(),
            store: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Uses a custom cookie name (default `rustrest_session`).
    pub fn cookie_name(mut self, name: &str) -> Self {
        self.cookie_name = name.to_string();
        self
    }

    pub fn get(&self, session_id: &str, key: &str) -> Option<String> {
        self.store
            .lock()
            .expect("session store lock")
            .get(session_id)
            .and_then(|data| data.get(key).cloned())
    }

    pub fn set(&self, session_id: &str, key: &str, value: &str) {
        self.store
            .lock()
            .expect("session store lock")
            .entry(session_id.to_string())
            .or_default()
            .insert(key.to_string(), value.to_string());
    }

    pub fn remove(&self, session_id: &str, key: &str) {
        if let Some(data) = self
            .store
            .lock()
            .expect("session store lock")
            .get_mut(session_id)
        {
            data.remove(key);
        }
    }

    /// Drops all data for a session.
    pub fn clear(&self, session_id: &str) {
        self.store
            .lock()
            .expect("session store lock")
            .remove(session_id);
    }

    /// The middleware that assigns/verifies the session cookie. Register it
    /// globally (`app.layer(sessions.middleware())`) or on a router.
    pub fn middleware(&self) -> Middleware {
        let sessions = self.clone();
        Arc::new(move |mut req: Request, next: Next| {
            let sessions = sessions.clone();
            Box::pin(async move {
                let existing = req
                    .cookie(&sessions.cookie_name)
                    .and_then(|signed| verify_value(&sessions.secret, signed));
                let (id, is_new) = match existing {
                    Some(id) => (id, false),
                    None => (sessions.generate_id(), true),
                };
                req.headers
                    .insert(SESSION_ID_HEADER.to_string(), id.clone());

                let res = next(req).await;

                if is_new {
                    let cookie =
                        Cookie::new(&sessions.cookie_name, &sign_value(&sessions.secret, &id))
                            .http_only(true)
                            .same_site(SameSite::Lax);
                    res.set_cookie(cookie)
                } else {
                    res
                }
            })
        })
    }

    /// Generates an unguessable id: an HMAC (keyed by the secret) over a
    /// timestamp + process-wide counter.
    fn generate_id(&self) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        let count = SESSION_COUNTER.fetch_add(1, Ordering::Relaxed);
        let signed = sign_value(&self.secret, &format!("{}-{}", nanos, count));
        signed
            .rsplit_once('.')
            .map(|(_, signature)| signature.to_string())
            .unwrap_or(signed)
    }
}

impl Request {
    /// Returns the session id assigned by the [`Sessions`] middleware.
    pub fn session_id(&self) -> Option<&str> {
        self.header(SESSION_ID_HEADER)
    }
}
