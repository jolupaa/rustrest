//! Bounded in-memory sessions: a signed session-id cookie managed by a
//! middleware, with an expiring, sharded key-value store.

use std::collections::{BTreeSet, HashMap};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use hyper::StatusCode;
use hyper::header::{CACHE_CONTROL, SET_COOKIE};

use super::cookie::{Cookie, SameSite, sign_value, verify_value};
use super::{HttpError, Middleware, Next, Request, Response};

const SESSION_ID_HEADER: &str = "x-session-id";
const MIN_SECRET_BYTES: usize = 32;
const DEFAULT_MAX_SESSIONS: usize = 10_000;
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);
const DEFAULT_MAX_ENTRIES_PER_SESSION: usize = 64;
const DEFAULT_MAX_SESSION_KEY_BYTES: usize = 256;
const DEFAULT_MAX_SESSION_VALUE_BYTES: usize = 16 * 1024;
const DEFAULT_MAX_SESSION_DATA_BYTES: usize = 64 * 1024;
const SESSION_STORE_SHARDS: usize = 16;
const STORE_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);
const STORE_CLEANUP_BATCH: usize = 256;
const CONFIGURATION_PANIC: &str =
    "session configuration failed; configure Sessions before cloning it or registering middleware";
static SESSION_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Invalid session security or cookie configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionConfigError {
    message: String,
}

impl SessionConfigError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl Display for SessionConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for SessionConfigError {}

/// The bounded-resource rule rejected by [`Sessions::set`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionDataErrorKind {
    EmptyKey,
    KeyTooLarge,
    ValueTooLarge,
    TooManyEntries,
    SessionTooLarge,
    SessionUnavailable,
}

/// A deterministic per-session storage-limit error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionDataError {
    kind: SessionDataErrorKind,
    limit: Option<usize>,
}

impl SessionDataError {
    fn new(kind: SessionDataErrorKind, limit: Option<usize>) -> Self {
        Self { kind, limit }
    }

    pub fn kind(&self) -> SessionDataErrorKind {
        self.kind
    }

    pub fn limit(&self) -> Option<usize> {
        self.limit
    }
}

impl Display for SessionDataError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match (self.kind, self.limit) {
            (SessionDataErrorKind::EmptyKey, _) => {
                formatter.write_str("La clave de sesion no puede estar vacia")
            }
            (SessionDataErrorKind::KeyTooLarge, Some(limit)) => write!(
                formatter,
                "La clave de sesion supera el limite de {limit} bytes"
            ),
            (SessionDataErrorKind::ValueTooLarge, Some(limit)) => write!(
                formatter,
                "El valor de sesion supera el limite de {limit} bytes"
            ),
            (SessionDataErrorKind::TooManyEntries, Some(limit)) => {
                write!(formatter, "La sesion supera el limite de {limit} entradas")
            }
            (SessionDataErrorKind::SessionTooLarge, Some(limit)) => write!(
                formatter,
                "Los datos de sesion superan el limite de {limit} bytes"
            ),
            (SessionDataErrorKind::SessionUnavailable, _) => {
                formatter.write_str("La sesion no esta disponible para la escritura")
            }
            _ => formatter.write_str("Los datos de sesion no son validos"),
        }
    }
}

impl Error for SessionDataError {}

#[derive(Debug, Clone, Copy)]
struct SessionDataLimits {
    entries: usize,
    key_bytes: usize,
    value_bytes: usize,
    total_bytes: usize,
}

#[derive(Debug)]
struct SessionEntry {
    data: HashMap<String, String>,
    expires_at: Instant,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ShardExpiryKey {
    expires_at: Instant,
    session_id: String,
}

#[derive(Debug, Default)]
struct SessionShard {
    entries: HashMap<String, SessionEntry>,
    expiry: BTreeSet<ShardExpiryKey>,
}

#[derive(Debug)]
struct StoreMaintenance {
    next_cleanup: Instant,
    cleanup_shard: usize,
}

#[derive(Debug)]
struct SessionStore {
    shards: Vec<Mutex<SessionShard>>,
    entries: AtomicUsize,
    maintenance: Mutex<StoreMaintenance>,
}

impl SessionStore {
    fn new() -> Self {
        Self {
            shards: (0..SESSION_STORE_SHARDS)
                .map(|_| Mutex::new(SessionShard::default()))
                .collect(),
            entries: AtomicUsize::new(0),
            maintenance: Mutex::new(StoreMaintenance {
                next_cleanup: Instant::now() + STORE_CLEANUP_INTERVAL,
                cleanup_shard: 0,
            }),
        }
    }

    fn shard_index(&self, session_id: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        session_id.hash(&mut hasher);
        hasher.finish() as usize % self.shards.len()
    }

    fn expiry(now: Instant, idle_timeout: Duration) -> Instant {
        now.checked_add(idle_timeout).unwrap_or(now)
    }

    fn expiry_key(session_id: &str, entry: &SessionEntry) -> ShardExpiryKey {
        ShardExpiryKey {
            expires_at: entry.expires_at,
            session_id: session_id.to_string(),
        }
    }

    fn renew(
        shard: &mut SessionShard,
        session_id: &str,
        now: Instant,
        idle_timeout: Duration,
    ) -> bool {
        let Some(previous) = shard
            .entries
            .get(session_id)
            .map(|entry| Self::expiry_key(session_id, entry))
        else {
            return false;
        };
        shard.expiry.remove(&previous);
        let replacement = {
            let entry = shard
                .entries
                .get_mut(session_id)
                .expect("the session entry was checked above");
            entry.expires_at = Self::expiry(now, idle_timeout);
            Self::expiry_key(session_id, entry)
        };
        shard.expiry.insert(replacement);
        true
    }

    fn insert_entry(shard: &mut SessionShard, session_id: &str, entry: SessionEntry) -> bool {
        let key = Self::expiry_key(session_id, &entry);
        let previous = shard.entries.insert(session_id.to_string(), entry);
        if let Some(previous) = previous.as_ref() {
            shard.expiry.remove(&Self::expiry_key(session_id, previous));
        }
        shard.expiry.insert(key);
        previous.is_none()
    }

    fn remove_entry(shard: &mut SessionShard, session_id: &str) -> Option<SessionEntry> {
        let entry = shard.entries.remove(session_id)?;
        shard.expiry.remove(&Self::expiry_key(session_id, &entry));
        Some(entry)
    }

    fn remove_expired_entries(shard: &mut SessionShard, now: Instant, maximum: usize) -> usize {
        let mut removed = 0;
        while removed < maximum {
            let Some(candidate) = shard.expiry.first().cloned() else {
                break;
            };
            if candidate.expires_at > now {
                break;
            }
            shard.expiry.remove(&candidate);
            let matches_index = shard
                .entries
                .get(&candidate.session_id)
                .is_some_and(|entry| entry.expires_at == candidate.expires_at);
            if matches_index {
                shard.entries.remove(&candidate.session_id);
                removed += 1;
            } else {
                debug_assert!(false, "the session expiry index must remain exact");
            }
        }
        removed
    }

    fn resume(&self, session_id: &str, idle_timeout: Duration) -> bool {
        let now = Instant::now();
        let index = self.shard_index(session_id);
        let mut shard = lock_unpoisoned(&self.shards[index]);
        let expired = shard
            .entries
            .get(session_id)
            .is_some_and(|entry| entry.expires_at <= now);
        if expired {
            Self::remove_entry(&mut shard, session_id);
            self.entries.fetch_sub(1, Ordering::Relaxed);
            return false;
        }
        Self::renew(&mut shard, session_id, now, idle_timeout)
    }

    fn create(&self, session_id: &str, idle_timeout: Duration, max_sessions: usize) -> bool {
        let now = Instant::now();
        let mut maintenance = lock_unpoisoned(&self.maintenance);
        let index = self.shard_index(session_id);
        {
            let mut shard = lock_unpoisoned(&self.shards[index]);
            if shard
                .entries
                .get(session_id)
                .is_some_and(|entry| entry.expires_at <= now)
            {
                Self::remove_entry(&mut shard, session_id);
                self.entries.fetch_sub(1, Ordering::Relaxed);
            }
            if Self::renew(&mut shard, session_id, now, idle_timeout) {
                return true;
            }
        }

        let cleanup_attempted = self.cleanup_if_needed(now, &mut maintenance);
        if self.entries.load(Ordering::Relaxed) >= max_sessions && !cleanup_attempted {
            // Admission at capacity probes only the oldest expiry in each
            // shard. It may reclaim expired entries, but never a live one.
            self.cleanup_expired_batch(now, &mut maintenance);
        }

        if self.entries.load(Ordering::Relaxed) >= max_sessions {
            return false;
        }

        let mut shard = lock_unpoisoned(&self.shards[index]);
        if Self::insert_entry(
            &mut shard,
            session_id,
            SessionEntry {
                data: HashMap::new(),
                expires_at: Self::expiry(now, idle_timeout),
            },
        ) {
            self.entries.fetch_add(1, Ordering::Relaxed);
        }
        true
    }

    fn cleanup_if_needed(&self, now: Instant, maintenance: &mut StoreMaintenance) -> bool {
        if now < maintenance.next_cleanup {
            return false;
        }

        self.cleanup_expired_batch(now, maintenance);
        true
    }

    fn cleanup_expired_batch(&self, now: Instant, maintenance: &mut StoreMaintenance) {
        let mut remaining = STORE_CLEANUP_BATCH;
        let mut visited = 0;
        let mut removed = 0;
        while remaining > 0 && visited < self.shards.len() {
            let index = (maintenance.cleanup_shard + visited) % self.shards.len();
            let mut shard = lock_unpoisoned(&self.shards[index]);
            let shard_removed = Self::remove_expired_entries(&mut shard, now, remaining);
            removed += shard_removed;
            remaining -= shard_removed;
            if remaining == 0 {
                maintenance.cleanup_shard = index;
                break;
            }
            visited += 1;
        }
        if removed > 0 {
            self.entries.fetch_sub(removed, Ordering::Relaxed);
        }
        if remaining == 0 {
            // Continue a large expiration wave on the next creation without
            // making one request scan the complete store.
            maintenance.next_cleanup = now;
        } else {
            maintenance.cleanup_shard = (maintenance.cleanup_shard + 1) % self.shards.len();
            maintenance.next_cleanup = now.checked_add(STORE_CLEANUP_INTERVAL).unwrap_or(now);
        }
    }

    fn get(&self, session_id: &str, key: &str, idle_timeout: Duration) -> Option<String> {
        let now = Instant::now();
        let index = self.shard_index(session_id);
        let mut shard = lock_unpoisoned(&self.shards[index]);
        if shard
            .entries
            .get(session_id)
            .is_some_and(|entry| entry.expires_at <= now)
        {
            Self::remove_entry(&mut shard, session_id);
            self.entries.fetch_sub(1, Ordering::Relaxed);
            return None;
        }
        if !Self::renew(&mut shard, session_id, now, idle_timeout) {
            return None;
        }
        shard.entries.get(session_id)?.data.get(key).cloned()
    }

    fn set(
        &self,
        session_id: &str,
        key: &str,
        value: &str,
        idle_timeout: Duration,
        max_sessions: usize,
        limits: SessionDataLimits,
    ) -> Result<(), SessionDataError> {
        validate_session_value(key, value, limits)?;
        if !self.resume(session_id, idle_timeout)
            && !self.create(session_id, idle_timeout, max_sessions)
        {
            return Err(SessionDataError::new(
                SessionDataErrorKind::SessionUnavailable,
                None,
            ));
        }
        let index = self.shard_index(session_id);
        let mut shard = lock_unpoisoned(&self.shards[index]);
        if shard
            .entries
            .get(session_id)
            .is_some_and(|entry| entry.expires_at <= Instant::now())
        {
            Self::remove_entry(&mut shard, session_id);
            self.entries.fetch_sub(1, Ordering::Relaxed);
            return Err(SessionDataError::new(
                SessionDataErrorKind::SessionUnavailable,
                None,
            ));
        }
        let Some(entry) = shard.entries.get(session_id) else {
            return Err(SessionDataError::new(
                SessionDataErrorKind::SessionUnavailable,
                None,
            ));
        };

        let is_new_key = !entry.data.contains_key(key);
        if is_new_key && entry.data.len() >= limits.entries {
            return Err(SessionDataError::new(
                SessionDataErrorKind::TooManyEntries,
                Some(limits.entries),
            ));
        }
        let current_bytes = entry
            .data
            .iter()
            .try_fold(0_usize, |total, (stored_key, stored_value)| {
                total
                    .checked_add(stored_key.len())
                    .and_then(|total| total.checked_add(stored_value.len()))
            })
            .ok_or_else(|| {
                SessionDataError::new(
                    SessionDataErrorKind::SessionTooLarge,
                    Some(limits.total_bytes),
                )
            })?;
        let replaced_bytes = entry
            .data
            .get(key)
            .map_or(0, |old_value| key.len() + old_value.len());
        let next_bytes = current_bytes
            .checked_sub(replaced_bytes)
            .and_then(|total| total.checked_add(key.len()))
            .and_then(|total| total.checked_add(value.len()))
            .filter(|total| *total <= limits.total_bytes)
            .ok_or_else(|| {
                SessionDataError::new(
                    SessionDataErrorKind::SessionTooLarge,
                    Some(limits.total_bytes),
                )
            })?;
        debug_assert!(next_bytes <= limits.total_bytes);

        shard
            .entries
            .get_mut(session_id)
            .expect("the session entry was checked above")
            .data
            .insert(key.to_string(), value.to_string());
        Ok(())
    }

    fn remove(&self, session_id: &str, key: &str, idle_timeout: Duration) {
        let now = Instant::now();
        let index = self.shard_index(session_id);
        let mut shard = lock_unpoisoned(&self.shards[index]);
        if shard
            .entries
            .get(session_id)
            .is_some_and(|entry| entry.expires_at <= now)
        {
            Self::remove_entry(&mut shard, session_id);
            self.entries.fetch_sub(1, Ordering::Relaxed);
            return;
        }
        if Self::renew(&mut shard, session_id, now, idle_timeout) {
            shard
                .entries
                .get_mut(session_id)
                .expect("the session entry was renewed above")
                .data
                .remove(key);
        }
    }

    fn clear(&self, session_id: &str) {
        let index = self.shard_index(session_id);
        if Self::remove_entry(&mut lock_unpoisoned(&self.shards[index]), session_id).is_some() {
            self.entries.fetch_sub(1, Ordering::Relaxed);
        }
    }

    fn live_len(&self) -> usize {
        let now = Instant::now();
        let mut removed = 0_usize;
        for shard in &self.shards {
            removed += Self::remove_expired_entries(&mut lock_unpoisoned(shard), now, usize::MAX);
        }
        if removed > 0 {
            self.entries.fetch_sub(removed, Ordering::Relaxed);
        }
        self.entries.load(Ordering::Relaxed)
    }
}

/// A bounded in-memory session store. Clone it freely after configuration
/// (clones share storage): keep one clone for your handlers and register
/// `.middleware()` on the app.
///
/// Sessions expire after 24 hours of inactivity and the store keeps at most
/// 10,000 sessions by default. Anonymous requests are admitted lazily on the
/// first successful [`Sessions::set`]; reaching capacity never evicts a live
/// session. Configuration changes are rejected while this value has live
/// clones or middleware, preventing handles that share storage from silently
/// using incompatible cookie, expiry, or size settings.
#[derive(Clone)]
pub struct Sessions {
    secret: String,
    cookie_name: String,
    same_site: SameSite,
    force_secure_cookie: bool,
    idle_timeout: Duration,
    max_sessions: usize,
    max_entries_per_session: usize,
    max_session_key_bytes: usize,
    max_session_value_bytes: usize,
    max_session_data_bytes: usize,
    store: Arc<SessionStore>,
}

impl Sessions {
    /// Creates a session store and panics when the HMAC secret is shorter than
    /// 32 bytes. Use [`Sessions::try_new`] for fallible configuration.
    pub fn new(secret: &str) -> Self {
        Self::try_new(secret).expect("session secret must contain at least 32 bytes")
    }

    /// Creates a session store after validating the HMAC secret.
    pub fn try_new(secret: &str) -> Result<Self, SessionConfigError> {
        if secret.len() < MIN_SECRET_BYTES {
            return Err(SessionConfigError::new(
                "El secreto de sesion debe contener al menos 32 bytes",
            ));
        }
        Ok(Self {
            secret: secret.to_string(),
            cookie_name: "rustrest_session".to_string(),
            same_site: SameSite::Lax,
            force_secure_cookie: false,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            max_sessions: DEFAULT_MAX_SESSIONS,
            max_entries_per_session: DEFAULT_MAX_ENTRIES_PER_SESSION,
            max_session_key_bytes: DEFAULT_MAX_SESSION_KEY_BYTES,
            max_session_value_bytes: DEFAULT_MAX_SESSION_VALUE_BYTES,
            max_session_data_bytes: DEFAULT_MAX_SESSION_DATA_BYTES,
            store: Arc::new(SessionStore::new()),
        })
    }

    /// Uses a custom cookie name (default `rustrest_session`). Invalid cookie
    /// token names fail fast; use [`Sessions::try_cookie_name`] to avoid panic.
    pub fn cookie_name(self, name: &str) -> Self {
        self.try_cookie_name(name)
            .unwrap_or_else(|error| panic!("{CONFIGURATION_PANIC}: {error}"))
    }

    pub fn try_cookie_name(mut self, name: &str) -> Result<Self, SessionConfigError> {
        if !is_valid_cookie_name(name) {
            return Err(SessionConfigError::new(
                "El nombre de la cookie de sesion no es valido",
            ));
        }
        self.ensure_configurable()?;
        self.cookie_name = name.to_string();
        Ok(self)
    }

    /// Sets the sliding inactivity timeout used by the server-side store.
    pub fn idle_timeout(self, timeout: Duration) -> Self {
        self.try_idle_timeout(timeout)
            .unwrap_or_else(|error| panic!("{CONFIGURATION_PANIC}: {error}"))
    }

    /// Fallible variant of [`Sessions::idle_timeout`].
    pub fn try_idle_timeout(mut self, timeout: Duration) -> Result<Self, SessionConfigError> {
        if timeout.is_zero() || Instant::now().checked_add(timeout).is_none() {
            return Err(SessionConfigError::new(
                "El tiempo de inactividad de sesion debe ser positivo y representable",
            ));
        }
        self.ensure_configurable()?;
        self.idle_timeout = timeout;
        Ok(self)
    }

    /// Sets the maximum number of sessions retained in memory. New sessions
    /// are rejected at this bound until an existing entry expires or clears.
    pub fn max_sessions(self, max_sessions: usize) -> Self {
        self.try_max_sessions(max_sessions)
            .unwrap_or_else(|error| panic!("{CONFIGURATION_PANIC}: {error}"))
    }

    /// Fallible variant of [`Sessions::max_sessions`].
    pub fn try_max_sessions(mut self, max_sessions: usize) -> Result<Self, SessionConfigError> {
        if max_sessions == 0 {
            return Err(SessionConfigError::new(
                "El maximo de sesiones debe ser mayor que cero",
            ));
        }
        self.ensure_configurable()?;
        self.max_sessions = max_sessions;
        Ok(self)
    }

    /// Sets the maximum number of key/value entries retained by one session.
    pub fn max_entries_per_session(self, maximum: usize) -> Self {
        self.try_max_entries_per_session(maximum)
            .unwrap_or_else(|error| panic!("{CONFIGURATION_PANIC}: {error}"))
    }

    /// Fallible variant of [`Sessions::max_entries_per_session`].
    pub fn try_max_entries_per_session(
        mut self,
        maximum: usize,
    ) -> Result<Self, SessionConfigError> {
        if maximum == 0 {
            return Err(SessionConfigError::new(
                "El maximo de entradas por sesion debe ser mayor que cero",
            ));
        }
        self.ensure_configurable()?;
        self.max_entries_per_session = maximum;
        Ok(self)
    }

    /// Sets the maximum UTF-8 byte length of one session key.
    pub fn max_session_key_bytes(self, maximum: usize) -> Self {
        self.try_max_session_key_bytes(maximum)
            .unwrap_or_else(|error| panic!("{CONFIGURATION_PANIC}: {error}"))
    }

    /// Fallible variant of [`Sessions::max_session_key_bytes`].
    pub fn try_max_session_key_bytes(mut self, maximum: usize) -> Result<Self, SessionConfigError> {
        if maximum == 0 {
            return Err(SessionConfigError::new(
                "El maximo de bytes por clave de sesion debe ser mayor que cero",
            ));
        }
        self.ensure_configurable()?;
        self.max_session_key_bytes = maximum;
        Ok(self)
    }

    /// Sets the maximum UTF-8 byte length of one session value.
    pub fn max_session_value_bytes(self, maximum: usize) -> Self {
        self.try_max_session_value_bytes(maximum)
            .unwrap_or_else(|error| panic!("{CONFIGURATION_PANIC}: {error}"))
    }

    /// Fallible variant of [`Sessions::max_session_value_bytes`].
    pub fn try_max_session_value_bytes(
        mut self,
        maximum: usize,
    ) -> Result<Self, SessionConfigError> {
        if maximum == 0 {
            return Err(SessionConfigError::new(
                "El maximo de bytes por valor de sesion debe ser mayor que cero",
            ));
        }
        self.ensure_configurable()?;
        self.max_session_value_bytes = maximum;
        Ok(self)
    }

    /// Sets the maximum aggregate key + value bytes retained by one session.
    pub fn max_session_data_bytes(self, maximum: usize) -> Self {
        self.try_max_session_data_bytes(maximum)
            .unwrap_or_else(|error| panic!("{CONFIGURATION_PANIC}: {error}"))
    }

    /// Fallible variant of [`Sessions::max_session_data_bytes`].
    pub fn try_max_session_data_bytes(
        mut self,
        maximum: usize,
    ) -> Result<Self, SessionConfigError> {
        if maximum == 0 {
            return Err(SessionConfigError::new(
                "El maximo agregado de datos de sesion debe ser mayor que cero",
            ));
        }
        self.ensure_configurable()?;
        self.max_session_data_bytes = maximum;
        Ok(self)
    }

    /// Always emits the session cookie with `Secure`, including when the
    /// immediate request is plaintext behind a TLS-terminating proxy.
    pub fn secure_cookies(self, secure: bool) -> Self {
        self.try_secure_cookies(secure)
            .unwrap_or_else(|error| panic!("{CONFIGURATION_PANIC}: {error}"))
    }

    /// Fallible variant of [`Sessions::secure_cookies`].
    pub fn try_secure_cookies(mut self, secure: bool) -> Result<Self, SessionConfigError> {
        self.ensure_configurable()?;
        self.force_secure_cookie = secure;
        Ok(self)
    }

    pub fn same_site(self, same_site: SameSite) -> Self {
        self.try_same_site(same_site)
            .unwrap_or_else(|error| panic!("{CONFIGURATION_PANIC}: {error}"))
    }

    /// Fallible variant of [`Sessions::same_site`].
    pub fn try_same_site(mut self, same_site: SameSite) -> Result<Self, SessionConfigError> {
        self.ensure_configurable()?;
        self.same_site = same_site;
        Ok(self)
    }

    /// Number of live entries currently retained by this process.
    pub fn len(&self) -> usize {
        self.store.live_len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, session_id: &str, key: &str) -> Option<String> {
        self.store.get(session_id, key, self.idle_timeout)
    }

    /// Inserts or replaces a session value while enforcing all configured
    /// per-session bounds. A transient middleware id is admitted here; at
    /// capacity this returns [`SessionDataErrorKind::SessionUnavailable`]
    /// without evicting another session.
    pub fn set(&self, session_id: &str, key: &str, value: &str) -> Result<(), SessionDataError> {
        self.store.set(
            session_id,
            key,
            value,
            self.idle_timeout,
            self.max_sessions,
            self.data_limits(),
        )
    }

    pub fn remove(&self, session_id: &str, key: &str) {
        self.store.remove(session_id, key, self.idle_timeout);
    }

    /// Drops all data for a session and invalidates its signed id.
    pub fn clear(&self, session_id: &str) {
        self.store.clear(session_id);
    }

    /// The middleware that assigns/verifies the session cookie. Anonymous
    /// requests receive a transient [`Request::session_id`] for handlers, but
    /// consume no store capacity and receive no cookie unless a handler
    /// persists data with [`Sessions::set`]. Register it globally
    /// (`app.layer(sessions.middleware())`) or on a router.
    pub fn middleware(&self) -> Middleware {
        // Share immutable configuration as well as the store; cloning a
        // `Sessions` value per request would copy the secret and cookie name.
        let sessions = Arc::new(self.clone());
        Arc::new(move |mut req: Request, next: Next| {
            let sessions = Arc::clone(&sessions);
            Box::pin(async move {
                let secure_transport = req.is_secure();
                let request_path = req.path.clone();
                if request_cookie_occurrences(&req, &sessions.cookie_name) > 1 {
                    let mut response = Response::from_error(HttpError::new(
                        StatusCode::BAD_REQUEST,
                        "duplicate_session_cookie",
                        "La solicitud contiene varias cookies de sesion con el mismo nombre",
                    ));
                    ensure_private_cache_control(&mut response);
                    return response;
                }
                let presented_cookie = req.cookie(&sessions.cookie_name);
                let had_presented_cookie = presented_cookie.is_some();
                let existing = presented_cookie
                    .and_then(|signed| verify_value(&sessions.secret, signed))
                    .filter(|id| sessions.store.resume(id, sessions.idle_timeout));
                let id = match existing {
                    Some(id) => id,
                    None => sessions.generate_id(),
                };
                if let Err(error) = req.set_header(SESSION_ID_HEADER, &id) {
                    let mut response = Response::from_error(error);
                    ensure_private_cache_control(&mut response);
                    return response;
                }

                let mut res = next(req).await;
                ensure_private_cache_control(&mut res);

                if !response_sets_root_host_cookie(&res, &sessions.cookie_name, &request_path) {
                    let secure = secure_transport
                        || sessions.force_secure_cookie
                        || sessions.cookie_name.starts_with("__Host-")
                        || sessions.cookie_name.starts_with("__Secure-")
                        || sessions.same_site == SameSite::None;
                    let alive = sessions.store.resume(&id, sessions.idle_timeout);
                    if !alive && !had_presented_cookie {
                        return res;
                    }
                    let signed = alive.then(|| sign_value(&sessions.secret, &id));
                    let cookie =
                        Cookie::new(&sessions.cookie_name, signed.as_deref().unwrap_or_default())
                            .max_age_secs(if alive {
                                duration_max_age(sessions.idle_timeout)
                            } else {
                                0
                            })
                            .secure(secure)
                            .http_only(true)
                            .same_site(sessions.same_site);
                    res.set_cookie(cookie)
                } else {
                    res
                }
            })
        })
    }

    fn data_limits(&self) -> SessionDataLimits {
        SessionDataLimits {
            entries: self.max_entries_per_session,
            key_bytes: self.max_session_key_bytes,
            value_bytes: self.max_session_value_bytes,
            total_bytes: self.max_session_data_bytes,
        }
    }

    fn ensure_configurable(&self) -> Result<(), SessionConfigError> {
        if Arc::strong_count(&self.store) == 1 {
            Ok(())
        } else {
            Err(SessionConfigError::new(
                "La configuracion de Sessions debe completarse antes de clonarla o registrar middleware",
            ))
        }
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

fn validate_session_value(
    key: &str,
    value: &str,
    limits: SessionDataLimits,
) -> Result<(), SessionDataError> {
    if key.is_empty() {
        return Err(SessionDataError::new(SessionDataErrorKind::EmptyKey, None));
    }
    if key.len() > limits.key_bytes {
        return Err(SessionDataError::new(
            SessionDataErrorKind::KeyTooLarge,
            Some(limits.key_bytes),
        ));
    }
    if value.len() > limits.value_bytes {
        return Err(SessionDataError::new(
            SessionDataErrorKind::ValueTooLarge,
            Some(limits.value_bytes),
        ));
    }
    if key
        .len()
        .checked_add(value.len())
        .is_none_or(|size| size > limits.total_bytes)
    {
        return Err(SessionDataError::new(
            SessionDataErrorKind::SessionTooLarge,
            Some(limits.total_bytes),
        ));
    }
    Ok(())
}

fn request_cookie_occurrences(request: &Request, cookie_name: &str) -> usize {
    request
        .headers_all("cookie")
        .into_iter()
        .flat_map(|value| value.split(';'))
        .filter(|pair| {
            pair.split_once('=')
                .is_some_and(|(name, _)| name.trim() == cookie_name)
        })
        .count()
}

fn duration_max_age(duration: Duration) -> i64 {
    let rounded = duration
        .as_secs()
        .saturating_add(u64::from(duration.subsec_nanos() != 0));
    rounded.min(i64::MAX as u64) as i64
}

fn response_sets_root_host_cookie(
    response: &Response,
    cookie_name: &str,
    request_path: &str,
) -> bool {
    response.headers.get_all(SET_COOKIE).iter().any(|value| {
        let Ok(value) = value.to_str() else {
            return false;
        };
        let mut segments = value.split(';');
        let Some((name, _)) = segments.next().and_then(|pair| pair.split_once('=')) else {
            return false;
        };
        if name.trim() != cookie_name {
            return false;
        }

        let default_path = default_cookie_path(request_path);
        let mut effective_path = default_path;
        let mut has_domain = false;
        for attribute in segments {
            let (name, value) = attribute
                .trim()
                .split_once('=')
                .map_or((attribute.trim(), ""), |(name, value)| {
                    (name.trim(), value.trim())
                });
            if name.eq_ignore_ascii_case("path") {
                effective_path = if value.starts_with('/') {
                    value
                } else {
                    default_path
                };
            } else if name.eq_ignore_ascii_case("domain") {
                has_domain = true;
            }
        }
        effective_path == "/" && !has_domain
    })
}

fn default_cookie_path(request_path: &str) -> &str {
    if !request_path.starts_with('/') {
        return "/";
    }
    match request_path.rfind('/') {
        Some(0) | None => "/",
        Some(index) => &request_path[..index],
    }
}

fn ensure_private_cache_control(response: &mut Response) {
    let mut private = false;
    let mut invalid = false;
    for value in response.headers.get_all(CACHE_CONTROL) {
        let Ok(value) = value.to_str() else {
            invalid = true;
            break;
        };
        private |= value.split(',').any(|directive| {
            let name = directive
                .trim()
                .split_once('=')
                .map_or(directive.trim(), |(name, _)| name.trim());
            name.eq_ignore_ascii_case("private") || name.eq_ignore_ascii_case("no-store")
        });
    }
    if invalid {
        response.headers.remove(CACHE_CONTROL);
        response.headers.insert(
            CACHE_CONTROL,
            hyper::header::HeaderValue::from_static("private, no-store"),
        );
    } else if !private {
        response.headers.append(
            CACHE_CONTROL,
            hyper::header::HeaderValue::from_static("private"),
        );
    }
}

impl Request {
    /// Returns the session id assigned by the [`Sessions`] middleware. For an
    /// anonymous request this id remains transient until a successful
    /// [`Sessions::set`] call persists it.
    pub fn session_id(&self) -> Option<&str> {
        self.header(SESSION_ID_HEADER)
    }
}

fn is_valid_cookie_name(name: &str) -> bool {
    !name.is_empty()
        && name.bytes().all(|byte| {
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
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_store_rejects_churn_without_evicting_live_session_data() {
        const CAPACITY: usize = 256;
        const REJECTIONS: usize = 1_024;
        let sessions = Sessions::new("a-test-session-secret-that-is-at-least-thirty-two-bytes")
            .max_sessions(CAPACITY);

        for index in 0..CAPACITY {
            sessions
                .set(&format!("session-{index}"), "owner", &index.to_string())
                .unwrap();
        }
        for index in 0..REJECTIONS {
            assert_eq!(
                sessions
                    .set(&format!("attacker-{index}"), "owner", "attacker")
                    .unwrap_err()
                    .kind(),
                SessionDataErrorKind::SessionUnavailable,
            );
        }

        assert_eq!(sessions.len(), CAPACITY);
        for index in 0..CAPACITY {
            assert_eq!(
                sessions.get(&format!("session-{index}"), "owner"),
                Some(index.to_string()),
            );
        }

        let (stored, indexed) = sessions
            .store
            .shards
            .iter()
            .map(|shard| {
                let shard = lock_unpoisoned(shard);
                (shard.entries.len(), shard.expiry.len())
            })
            .fold((0, 0), |(stored, indexed), (entries, expiry)| {
                (stored + entries, indexed + expiry)
            });
        assert_eq!(stored, CAPACITY);
        assert_eq!(indexed, stored, "the expiry index must remain exact");
    }

    #[test]
    fn expired_entry_recovers_capacity_without_evicting_a_live_entry() {
        let sessions = Sessions::new("a-test-session-secret-that-is-at-least-thirty-two-bytes")
            .max_sessions(2);
        sessions.set("expired", "owner", "old").unwrap();
        sessions.set("live", "owner", "resident").unwrap();

        let index = sessions.store.shard_index("expired");
        {
            let mut shard = lock_unpoisoned(&sessions.store.shards[index]);
            let old_key = SessionStore::expiry_key(
                "expired",
                shard.entries.get("expired").expect("stored session"),
            );
            shard.expiry.remove(&old_key);
            let entry = shard.entries.get_mut("expired").expect("stored session");
            entry.expires_at = Instant::now();
            let new_key = SessionStore::expiry_key("expired", entry);
            shard.expiry.insert(new_key);
        }

        sessions.set("replacement", "owner", "new").unwrap();
        assert_eq!(sessions.len(), 2);
        assert_eq!(sessions.get("expired", "owner"), None);
        assert_eq!(sessions.get("live", "owner").as_deref(), Some("resident"));
        assert_eq!(sessions.get("replacement", "owner").as_deref(), Some("new"));
    }
}
