use std::collections::HashSet;
use std::time::{Duration, Instant};

use hyper::Uri;
use hyper::http::uri::Authority;

#[cfg(test)]
use super::Request;
use super::WsError;

const DEFAULT_MAX_MESSAGE_SIZE: usize = 1024 * 1024;
const DEFAULT_MAX_FRAME_SIZE: usize = 256 * 1024;
const DEFAULT_WRITE_BUFFER_SIZE: usize = 128 * 1024;
const DEFAULT_MAX_WRITE_BUFFER_SIZE: usize = 1024 * 1024;
const DEFAULT_CHANNEL_CAPACITY: usize = 16;
const DEFAULT_SEND_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_PING_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_PONG_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(120);
const DEFAULT_MAX_CONNECTION_LIFETIME: Duration = Duration::from_secs(24 * 60 * 60);
const DEFAULT_CLOSE_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_MAX_CONNECTIONS: usize = 2_000;
const DEFAULT_MAX_CONNECTIONS_PER_IP: usize = 20;
const DEFAULT_MAX_MESSAGES_PER_INTERVAL: u32 = 100;
const DEFAULT_MESSAGE_RATE_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_MAX_ROOMS: usize = 32;
const DEFAULT_MAX_ROOM_NAME_BYTES: usize = 128;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum BackpressurePolicy {
    #[default]
    Wait,
    Reject,
    Disconnect,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum OriginPolicy {
    Any {
        allow_missing: bool,
    },
    SameHost {
        allow_missing: bool,
    },
    AllowList {
        origins: Vec<String>,
        allow_missing: bool,
    },
}

impl OriginPolicy {
    pub fn any() -> Self {
        Self::Any {
            allow_missing: true,
        }
    }

    pub fn same_host() -> Self {
        Self::SameHost {
            allow_missing: true,
        }
    }

    pub fn allow<I, S>(origins: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::AllowList {
            origins: origins.into_iter().map(Into::into).collect(),
            allow_missing: true,
        }
    }

    pub fn allow_missing(self, allow_missing: bool) -> Self {
        match self {
            Self::Any { .. } => Self::Any { allow_missing },
            Self::SameHost { .. } => Self::SameHost { allow_missing },
            Self::AllowList { origins, .. } => Self::AllowList {
                origins,
                allow_missing,
            },
        }
    }

    pub fn allows(&self, origin: Option<&str>, host: &str) -> bool {
        let Some(origin) = origin else {
            return self.allows_missing();
        };
        let Some(origin) = NormalizedOrigin::parse(origin) else {
            return false;
        };

        let host_default_port = origin.scheme.default_port();
        self.allows_normalized(origin, host, host_default_port)
    }

    pub(crate) fn allows_for_transport(
        &self,
        origin: Option<&str>,
        host: &str,
        secure_transport: bool,
    ) -> bool {
        let Some(origin) = origin else {
            return self.allows_missing();
        };
        let Some(origin) = NormalizedOrigin::parse(origin) else {
            return false;
        };

        if matches!(self, Self::SameHost { .. }) && origin.scheme.is_secure() != secure_transport {
            return false;
        }
        let host_default_port = if secure_transport { 443 } else { 80 };
        self.allows_normalized(origin, host, host_default_port)
    }

    fn allows_normalized(
        &self,
        origin: NormalizedOrigin,
        host: &str,
        host_default_port: u16,
    ) -> bool {
        match self {
            Self::Any { .. } => true,
            Self::SameHost { .. } => normalize_host(host, host_default_port).is_some_and(
                |(expected_host, expected_port)| {
                    expected_host == origin.host && expected_port == origin.port
                },
            ),
            Self::AllowList { origins, .. } => origins.iter().any(|allowed| {
                NormalizedOrigin::parse(allowed).is_some_and(|allowed| allowed == origin)
            }),
        }
    }

    fn allows_missing(&self) -> bool {
        match self {
            Self::Any { allow_missing }
            | Self::SameHost { allow_missing }
            | Self::AllowList { allow_missing, .. } => *allow_missing,
        }
    }

    fn is_valid(&self) -> bool {
        match self {
            Self::AllowList { origins, .. } => origins
                .iter()
                .all(|origin| NormalizedOrigin::parse(origin).is_some()),
            Self::Any { .. } | Self::SameHost { .. } => true,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OriginScheme {
    Http,
    Https,
    Ws,
    Wss,
}

impl OriginScheme {
    fn parse(value: &str) -> Option<Self> {
        if value.eq_ignore_ascii_case("http") {
            Some(Self::Http)
        } else if value.eq_ignore_ascii_case("https") {
            Some(Self::Https)
        } else if value.eq_ignore_ascii_case("ws") {
            Some(Self::Ws)
        } else if value.eq_ignore_ascii_case("wss") {
            Some(Self::Wss)
        } else {
            None
        }
    }

    fn default_port(self) -> u16 {
        match self {
            Self::Http | Self::Ws => 80,
            Self::Https | Self::Wss => 443,
        }
    }

    fn is_secure(self) -> bool {
        matches!(self, Self::Https | Self::Wss)
    }
}

#[derive(Debug, PartialEq, Eq)]
struct NormalizedOrigin {
    scheme: OriginScheme,
    host: String,
    port: u16,
}

impl NormalizedOrigin {
    fn parse(value: &str) -> Option<Self> {
        let uri = value.parse::<Uri>().ok()?;
        let scheme = OriginScheme::parse(uri.scheme_str()?)?;
        let authority = uri.authority()?;
        if authority.as_str().contains('@') {
            return None;
        }
        if uri
            .path_and_query()
            .is_some_and(|path_and_query| path_and_query.as_str() != "/")
        {
            return None;
        }
        let host = authority.host();
        if host.is_empty() {
            return None;
        }
        Some(Self {
            scheme,
            host: host.to_ascii_lowercase(),
            port: effective_port(authority, scheme.default_port())?,
        })
    }
}

fn normalize_host(host: &str, default_port: u16) -> Option<(String, u16)> {
    let uri = format!("http://{host}").parse::<Uri>().ok()?;
    let authority = uri.authority()?;
    if authority.as_str().contains('@') {
        return None;
    }
    if let Some(path_and_query) = uri.path_and_query() {
        if path_and_query.as_str() != "/" {
            return None;
        }
    }
    let host = authority.host();
    if host.is_empty() {
        return None;
    }
    Some((
        host.to_ascii_lowercase(),
        effective_port(authority, default_port)?,
    ))
}

fn effective_port(authority: &Authority, default_port: u16) -> Option<u16> {
    if let Some(port) = authority.port_u16() {
        return Some(port);
    }

    let raw = authority.as_str();
    let has_explicit_port = if raw.starts_with('[') {
        raw.find(']').is_some_and(|end| end + 1 < raw.len())
    } else {
        raw.contains(':')
    };
    (!has_explicit_port).then_some(default_port)
}

#[derive(Clone, Debug)]
pub struct MessageRateLimit {
    pub max_messages: u32,
    pub interval: Duration,
}

/// WebSocket configuration overrides. Unset values inherit from the App layer
/// and then fall back to bounded built-in defaults.
#[derive(Clone, Debug, Default)]
pub struct WebSocketConfig {
    pub(crate) protocols: Option<Vec<String>>,
    pub(crate) require_protocol: Option<bool>,
    pub(crate) max_message_size: Option<Option<usize>>,
    pub(crate) max_frame_size: Option<Option<usize>>,
    pub(crate) write_buffer_size: Option<usize>,
    pub(crate) max_write_buffer_size: Option<usize>,
    pub(crate) inbound_capacity: Option<usize>,
    pub(crate) outbound_capacity: Option<usize>,
    pub(crate) backpressure_policy: Option<BackpressurePolicy>,
    pub(crate) send_timeout: Option<Duration>,
    pub(crate) ping_interval: Option<Option<Duration>>,
    pub(crate) pong_timeout: Option<Duration>,
    pub(crate) idle_timeout: Option<Option<Duration>>,
    pub(crate) max_connection_lifetime: Option<Option<Duration>>,
    pub(crate) close_timeout: Option<Duration>,
    pub(crate) origin_policy: Option<OriginPolicy>,
    pub(crate) max_connections: Option<Option<usize>>,
    pub(crate) max_connections_per_ip: Option<Option<usize>>,
    pub(crate) message_rate_limit: Option<Option<MessageRateLimit>>,
    pub(crate) max_rooms_per_connection: Option<usize>,
    pub(crate) max_room_name_bytes: Option<usize>,
}

impl WebSocketConfig {
    pub fn new() -> Self {
        Self::default()
    }

    /// Declares the subprotocols the server supports. The first
    /// client-offered protocol in this list is selected and echoed in the
    /// `Sec-WebSocket-Protocol` response header.
    pub fn protocols(mut self, protocols: &[&str]) -> Self {
        self.protocols = Some(
            protocols
                .iter()
                .map(|protocol| protocol.to_string())
                .collect(),
        );
        self
    }

    pub fn require_protocol(mut self, require: bool) -> Self {
        self.require_protocol = Some(require);
        self
    }

    /// Caps the size of incoming messages; larger ones error the connection.
    pub fn max_message_size(mut self, bytes: usize) -> Self {
        self.max_message_size = Some(Some(bytes));
        self
    }

    /// Removes the incoming message-size ceiling. Prefer a finite limit for
    /// internet-facing endpoints.
    pub fn disable_max_message_size(mut self) -> Self {
        self.max_message_size = Some(None);
        self
    }

    pub fn max_frame_size(mut self, bytes: usize) -> Self {
        self.max_frame_size = Some(Some(bytes));
        self
    }

    /// Removes the incoming frame-size ceiling. Prefer a finite limit for
    /// internet-facing endpoints.
    pub fn disable_max_frame_size(mut self) -> Self {
        self.max_frame_size = Some(None);
        self
    }

    pub fn write_buffer_size(mut self, bytes: usize) -> Self {
        self.write_buffer_size = Some(bytes);
        self
    }

    pub fn max_write_buffer_size(mut self, bytes: usize) -> Self {
        self.max_write_buffer_size = Some(bytes);
        self
    }

    pub fn inbound_capacity(mut self, capacity: usize) -> Self {
        self.inbound_capacity = Some(capacity);
        self
    }

    pub fn outbound_capacity(mut self, capacity: usize) -> Self {
        self.outbound_capacity = Some(capacity);
        self
    }

    pub fn backpressure_policy(mut self, policy: BackpressurePolicy) -> Self {
        self.backpressure_policy = Some(policy);
        self
    }

    pub fn send_timeout(mut self, timeout: Duration) -> Self {
        self.send_timeout = Some(timeout);
        self
    }

    /// Sends a Ping whenever the connection has been idle in
    /// [`WebSocket::recv`](super::WebSocket::recv) for `interval`.
    pub fn ping_interval(mut self, interval: Duration) -> Self {
        self.ping_interval = Some(Some(interval));
        self
    }

    pub fn disable_ping(mut self) -> Self {
        self.ping_interval = Some(None);
        self
    }

    pub fn pong_timeout(mut self, timeout: Duration) -> Self {
        self.pong_timeout = Some(timeout);
        self
    }

    pub fn idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = Some(Some(timeout));
        self
    }

    pub fn disable_idle_timeout(mut self) -> Self {
        self.idle_timeout = Some(None);
        self
    }

    pub fn max_connection_lifetime(mut self, lifetime: Duration) -> Self {
        self.max_connection_lifetime = Some(Some(lifetime));
        self
    }

    pub fn disable_max_connection_lifetime(mut self) -> Self {
        self.max_connection_lifetime = Some(None);
        self
    }

    pub fn close_timeout(mut self, timeout: Duration) -> Self {
        self.close_timeout = Some(timeout);
        self
    }

    pub fn origin_policy(mut self, policy: OriginPolicy) -> Self {
        self.origin_policy = Some(policy);
        self
    }

    pub fn max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = Some(Some(max_connections));
        self
    }

    /// Disables the process-wide limit when used as an App default, or the
    /// route-specific limit when used on a route.
    pub fn disable_max_connections(mut self) -> Self {
        self.max_connections = Some(None);
        self
    }

    pub fn max_connections_per_ip(mut self, max_connections: usize) -> Self {
        self.max_connections_per_ip = Some(Some(max_connections));
        self
    }

    pub fn disable_max_connections_per_ip(mut self) -> Self {
        self.max_connections_per_ip = Some(None);
        self
    }

    pub fn message_rate_limit(mut self, max_messages: u32, interval: Duration) -> Self {
        self.message_rate_limit = Some(Some(MessageRateLimit {
            max_messages,
            interval,
        }));
        self
    }

    pub fn disable_message_rate_limit(mut self) -> Self {
        self.message_rate_limit = Some(None);
        self
    }

    pub fn max_rooms_per_connection(mut self, max_rooms: usize) -> Self {
        self.max_rooms_per_connection = Some(max_rooms);
        self
    }

    pub fn max_room_name_bytes(mut self, max_bytes: usize) -> Self {
        self.max_room_name_bytes = Some(max_bytes);
        self
    }

    pub fn validate(&self) -> Result<(), WsError> {
        let resolved = ResolvedWebSocketConfig::from_layers(&WebSocketConfig::default(), self);
        resolved.validate()
    }

    /// Picks the first client-offered subprotocol the server supports.
    #[cfg(test)]
    pub(super) fn negotiate(&self, req: &Request) -> Option<String> {
        let protocols = self.protocols.as_deref().unwrap_or_default();
        super::negotiate_protocol(req, protocols).ok().flatten()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ResolvedWebSocketConfig {
    pub protocols: Vec<String>,
    pub require_protocol: bool,
    pub max_message_size: Option<usize>,
    pub max_frame_size: Option<usize>,
    pub write_buffer_size: usize,
    pub max_write_buffer_size: usize,
    pub inbound_capacity: usize,
    pub outbound_capacity: usize,
    pub backpressure_policy: BackpressurePolicy,
    pub send_timeout: Duration,
    pub ping_interval: Option<Duration>,
    pub pong_timeout: Duration,
    pub idle_timeout: Option<Duration>,
    pub max_connection_lifetime: Option<Duration>,
    pub close_timeout: Duration,
    pub origin_policy: OriginPolicy,
    pub process_max_connections: Option<usize>,
    pub route_max_connections: Option<usize>,
    pub max_connections_per_ip: Option<usize>,
    pub message_rate_limit: Option<MessageRateLimit>,
    pub max_rooms_per_connection: usize,
    pub max_room_name_bytes: usize,
}

impl ResolvedWebSocketConfig {
    pub(crate) fn from_layers(app: &WebSocketConfig, route: &WebSocketConfig) -> Self {
        Self {
            protocols: route
                .protocols
                .clone()
                .or_else(|| app.protocols.clone())
                .unwrap_or_default(),
            require_protocol: route
                .require_protocol
                .or(app.require_protocol)
                .unwrap_or(false),
            max_message_size: route
                .max_message_size
                .or(app.max_message_size)
                .unwrap_or(Some(DEFAULT_MAX_MESSAGE_SIZE)),
            max_frame_size: route
                .max_frame_size
                .or(app.max_frame_size)
                .unwrap_or(Some(DEFAULT_MAX_FRAME_SIZE)),
            write_buffer_size: route
                .write_buffer_size
                .or(app.write_buffer_size)
                .unwrap_or(DEFAULT_WRITE_BUFFER_SIZE),
            max_write_buffer_size: route
                .max_write_buffer_size
                .or(app.max_write_buffer_size)
                .unwrap_or(DEFAULT_MAX_WRITE_BUFFER_SIZE),
            inbound_capacity: route
                .inbound_capacity
                .or(app.inbound_capacity)
                .unwrap_or(DEFAULT_CHANNEL_CAPACITY),
            outbound_capacity: route
                .outbound_capacity
                .or(app.outbound_capacity)
                .unwrap_or(DEFAULT_CHANNEL_CAPACITY),
            backpressure_policy: route
                .backpressure_policy
                .or(app.backpressure_policy)
                .unwrap_or_default(),
            send_timeout: route
                .send_timeout
                .or(app.send_timeout)
                .unwrap_or(DEFAULT_SEND_TIMEOUT),
            ping_interval: route
                .ping_interval
                .or(app.ping_interval)
                .unwrap_or(Some(DEFAULT_PING_INTERVAL)),
            pong_timeout: route
                .pong_timeout
                .or(app.pong_timeout)
                .unwrap_or(DEFAULT_PONG_TIMEOUT),
            idle_timeout: route
                .idle_timeout
                .or(app.idle_timeout)
                .unwrap_or(Some(DEFAULT_IDLE_TIMEOUT)),
            max_connection_lifetime: route
                .max_connection_lifetime
                .or(app.max_connection_lifetime)
                .unwrap_or(Some(DEFAULT_MAX_CONNECTION_LIFETIME)),
            close_timeout: route
                .close_timeout
                .or(app.close_timeout)
                .unwrap_or(DEFAULT_CLOSE_TIMEOUT),
            origin_policy: route
                .origin_policy
                .clone()
                .or_else(|| app.origin_policy.clone())
                .unwrap_or_else(|| OriginPolicy::same_host().allow_missing(false)),
            process_max_connections: app.max_connections.unwrap_or(Some(DEFAULT_MAX_CONNECTIONS)),
            route_max_connections: route.max_connections.unwrap_or(None),
            max_connections_per_ip: route
                .max_connections_per_ip
                .or(app.max_connections_per_ip)
                .unwrap_or(Some(DEFAULT_MAX_CONNECTIONS_PER_IP)),
            message_rate_limit: route
                .message_rate_limit
                .clone()
                .or_else(|| app.message_rate_limit.clone())
                .unwrap_or({
                    Some(MessageRateLimit {
                        max_messages: DEFAULT_MAX_MESSAGES_PER_INTERVAL,
                        interval: DEFAULT_MESSAGE_RATE_INTERVAL,
                    })
                }),
            max_rooms_per_connection: route
                .max_rooms_per_connection
                .or(app.max_rooms_per_connection)
                .unwrap_or(DEFAULT_MAX_ROOMS),
            max_room_name_bytes: route
                .max_room_name_bytes
                .or(app.max_room_name_bytes)
                .unwrap_or(DEFAULT_MAX_ROOM_NAME_BYTES),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), WsError> {
        let mut protocols = HashSet::with_capacity(self.protocols.len());
        for protocol in &self.protocols {
            if !super::is_valid_protocol_token(protocol) {
                return Err(WsError::InvalidConfiguration(
                    "los subprotocolos WebSocket deben ser tokens HTTP validos".into(),
                ));
            }
            if !protocols.insert(protocol.as_str()) {
                return Err(WsError::InvalidConfiguration(
                    "los subprotocolos WebSocket no pueden estar duplicados".into(),
                ));
            }
        }
        if self.require_protocol && self.protocols.is_empty() {
            return Err(WsError::InvalidConfiguration(
                "require_protocol necesita al menos un subprotocolo WebSocket".into(),
            ));
        }
        if self.max_message_size == Some(0) || self.max_frame_size == Some(0) {
            return Err(WsError::InvalidConfiguration(
                "los limites de mensaje y frame WebSocket deben ser mayores que cero".into(),
            ));
        }
        if self.inbound_capacity == 0 || self.outbound_capacity == 0 {
            return Err(WsError::InvalidConfiguration(
                "las capacidades de los canales WebSocket deben ser mayores que cero".into(),
            ));
        }
        if self.max_write_buffer_size <= self.write_buffer_size {
            return Err(WsError::InvalidConfiguration(
                "max_write_buffer_size debe ser mayor que write_buffer_size".into(),
            ));
        }
        if self
            .ping_interval
            .is_some_and(|interval| interval <= self.pong_timeout)
        {
            return Err(WsError::InvalidConfiguration(
                "ping_interval debe ser mayor que pong_timeout".into(),
            ));
        }
        let now = Instant::now();
        let deadlines = [
            self.ping_interval,
            Some(self.pong_timeout),
            self.idle_timeout,
            self.max_connection_lifetime,
            Some(self.close_timeout),
        ];
        if self.send_timeout.is_zero()
            || deadlines
                .into_iter()
                .flatten()
                .any(|timeout| timeout.is_zero() || now.checked_add(timeout).is_none())
        {
            return Err(WsError::InvalidConfiguration(
                "los tiempos limite WebSocket deben ser positivos y representables".into(),
            ));
        }
        if self.max_rooms_per_connection == 0 || self.max_room_name_bytes == 0 {
            return Err(WsError::InvalidConfiguration(
                "los limites de rooms WebSocket deben ser mayores que cero".into(),
            ));
        }
        if self.process_max_connections == Some(0)
            || self.route_max_connections == Some(0)
            || self.max_connections_per_ip == Some(0)
        {
            return Err(WsError::InvalidConfiguration(
                "los limites de conexiones WebSocket deben ser mayores que cero".into(),
            ));
        }
        if let Some(limit) = &self.message_rate_limit {
            if limit.max_messages == 0 || limit.interval.is_zero() {
                return Err(WsError::InvalidConfiguration(
                    "los limites de frecuencia de mensajes WebSocket deben ser mayores que cero"
                        .into(),
                ));
            }
        }
        if !self.origin_policy.is_valid() {
            return Err(WsError::InvalidConfiguration(
                "la politica de origen WebSocket contiene un origen no valido".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn tungstenite_config(
        &self,
    ) -> tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
        tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
            .write_buffer_size(self.write_buffer_size)
            .max_write_buffer_size(self.max_write_buffer_size)
            .max_message_size(self.max_message_size)
            .max_frame_size(self.max_frame_size)
    }
}
