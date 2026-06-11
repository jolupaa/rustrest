mod config;
mod error;
mod runtime;
mod socket;
#[cfg(test)]
mod tests;
mod types;

use base64::Engine;
use hyper::upgrade::OnUpgrade;
use hyper_util::rt::TokioIo;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::protocol::Role;

use super::{HttpError, Request, Response};

pub(crate) use config::ResolvedWebSocketConfig;
pub use config::{BackpressurePolicy, OriginPolicy, WebSocketConfig};
pub use error::{WebSocketCapacityError, WebSocketError, WebSocketTimeout, WsError};
pub use runtime::WebSocketRuntimeHandle;
pub use socket::{
    IntoWebSocketHandler, WebSocket, WebSocketEvent, WebSocketHandler, WebSocketMessage,
};
pub use types::{
    WebSocketConnectionSnapshot, WebSocketErrorCategory, WebSocketId, WebSocketObservation,
    WebSocketObserver, WebSocketStats,
};

pub(crate) use runtime::{AdmissionError, ConnectionPermit};

pub(crate) struct HandshakeRejection {
    status: u16,
    message: &'static str,
    headers: Vec<(&'static str, &'static str)>,
}

impl HandshakeRejection {
    fn new(status: u16, message: &'static str) -> Self {
        Self {
            status,
            message,
            headers: Vec::new(),
        }
    }

    fn with_header(mut self, name: &'static str, value: &'static str) -> Self {
        self.headers.push((name, value));
        self
    }

    fn into_response(self) -> Response {
        self.headers.into_iter().fold(
            Response::send(self.message).status(self.status),
            |response, (name, value)| response.header(name, value),
        )
    }

    pub(crate) fn into_http_error(self) -> HttpError {
        HttpError::new(self.status, self.message)
    }
}

pub(crate) fn header_value_contains_token(value: &str, expected: &str) -> bool {
    value
        .split(',')
        .any(|token| token.trim().eq_ignore_ascii_case(expected))
}

pub(crate) fn is_valid_websocket_key(value: &str) -> bool {
    base64::engine::general_purpose::STANDARD
        .decode(value)
        .is_ok_and(|decoded| decoded.len() == 16)
}

pub(crate) fn request_header_contains_token(req: &Request, name: &str, expected: &str) -> bool {
    req.headers_all(name)
        .into_iter()
        .any(|value| header_value_contains_token(value, expected))
        || req
            .header(name)
            .is_some_and(|value| header_value_contains_token(value, expected))
}

pub(crate) fn singleton_header<'a>(req: &'a Request, name: &str) -> Option<&'a str> {
    let values = req.headers_all(name);
    match values.as_slice() {
        [value] => Some(*value),
        _ => None,
    }
}

fn negotiate_protocol(req: &Request, protocols: &[String]) -> Option<String> {
    for raw in req.headers_all("sec-websocket-protocol") {
        for candidate in raw.split(',') {
            let candidate = candidate.trim();
            if protocols
                .iter()
                .any(|supported| supported.eq_ignore_ascii_case(candidate))
            {
                return Some(candidate.to_string());
            }
        }
    }

    req.header("sec-websocket-protocol").and_then(|raw| {
        raw.split(',').find_map(|candidate| {
            let candidate = candidate.trim();
            protocols
                .iter()
                .any(|supported| supported.eq_ignore_ascii_case(candidate))
                .then(|| candidate.to_string())
        })
    })
}

pub(crate) fn validate_handshake(
    req: &Request,
    config: &ResolvedWebSocketConfig,
) -> Result<Option<String>, HandshakeRejection> {
    if req.version() != hyper::Version::HTTP_11 {
        return Err(HandshakeRejection::new(
            400,
            "La actualizacion WebSocket requiere HTTP/1.1",
        ));
    }
    let Some(host) = singleton_header(req, "host").map(str::trim) else {
        return Err(HandshakeRejection::new(
            400,
            "La cabecera Host debe aparecer exactamente una vez y no estar vacia",
        ));
    };
    if host.is_empty() {
        return Err(HandshakeRejection::new(
            400,
            "La cabecera Host debe aparecer exactamente una vez y no estar vacia",
        ));
    }
    if !req.method.eq_ignore_ascii_case("GET") {
        return Err(HandshakeRejection::new(
            400,
            "La actualizacion WebSocket requiere el metodo GET",
        ));
    }
    if !request_header_contains_token(req, "upgrade", "websocket") {
        return Err(HandshakeRejection::new(
            400,
            "La cabecera Upgrade debe incluir websocket",
        ));
    }
    if !request_header_contains_token(req, "connection", "upgrade") {
        return Err(HandshakeRejection::new(
            400,
            "La cabecera Connection debe incluir Upgrade",
        ));
    }
    let Some(version) = singleton_header(req, "sec-websocket-version") else {
        return Err(HandshakeRejection::new(
            400,
            "Sec-WebSocket-Version debe aparecer exactamente una vez",
        ));
    };
    if version.trim() != "13" {
        return Err(
            HandshakeRejection::new(426, "La version WebSocket debe ser 13")
                .with_header("sec-websocket-version", "13"),
        );
    }
    let Some(key) = singleton_header(req, "sec-websocket-key") else {
        return Err(HandshakeRejection::new(
            400,
            "Sec-WebSocket-Key debe aparecer exactamente una vez",
        ));
    };
    if !is_valid_websocket_key(key) {
        return Err(HandshakeRejection::new(
            400,
            "Sec-WebSocket-Key debe codificar exactamente 16 bytes",
        ));
    }

    let origins = req.headers_all("origin");
    if origins.len() > 1 {
        return Err(HandshakeRejection::new(
            400,
            "La cabecera Origin no puede aparecer mas de una vez",
        ));
    }
    if !config
        .origin_policy
        .allows_for_transport(origins.first().copied(), host, req.is_secure())
    {
        return Err(HandshakeRejection::new(
            403,
            "El origen WebSocket no esta permitido",
        ));
    }

    let protocol = negotiate_protocol(req, &config.protocols);
    if config.require_protocol && protocol.is_none() {
        return Err(HandshakeRejection::new(
            400,
            "Se requiere un subprotocolo WebSocket compatible",
        ));
    }

    Ok(protocol)
}

impl Request {
    pub fn websocket<H>(self, handler: H) -> Response
    where
        H: IntoWebSocketHandler,
    {
        self.websocket_with(WebSocketConfig::default(), handler)
    }

    /// Like [`Request::websocket`], with subprotocol negotiation, message
    /// size limits, and keepalive pings from `config`.
    pub fn websocket_with<H>(self, config: WebSocketConfig, handler: H) -> Response
    where
        H: IntoWebSocketHandler,
    {
        let config = self.resolved_websocket_config.clone().unwrap_or_else(|| {
            ResolvedWebSocketConfig::from_layers(&WebSocketConfig::default(), &config)
        });
        let protocol = match validate_handshake(&self, &config) {
            Ok(protocol) => protocol,
            Err(rejection) => return rejection.into_response(),
        };

        self.into_websocket_response(config, protocol, handler.into_websocket_handler())
    }

    fn into_websocket_response(
        self,
        config: ResolvedWebSocketConfig,
        protocol: Option<String>,
        handler: WebSocketHandler,
    ) -> Response {
        let route = self.route_pattern().unwrap_or(&self.path).to_string();
        let permit = match self.websocket_runtime.admit(
            &route,
            self.remote_addr,
            protocol.as_deref(),
            &config,
        ) {
            Ok(permit) => permit,
            Err(error) => return error.into_response(),
        };
        let mut response = match Response::websocket(&self) {
            Ok(response) => response,
            Err(error) => return Response::from_error(error),
        };
        if let Some(protocol) = &protocol {
            response = response.header("sec-websocket-protocol", protocol);
        }
        let Some(upgrade) = self.upgrade else {
            return Response::from_error(HttpError::bad_request(
                "La actualizacion WebSocket no esta disponible",
            ));
        };
        spawn_websocket(upgrade, config, protocol, handler, permit);
        response
    }
}

impl AdmissionError {
    fn into_response(self) -> Response {
        match self {
            Self::Shutdown => Response::send("El runtime WebSocket se esta cerrando").status(503),
            Self::ProcessCapacity => {
                Response::send("La capacidad global de conexiones WebSocket esta agotada")
                    .status(503)
            }
            Self::RouteCapacity => {
                Response::send("La capacidad de conexiones WebSocket para esta ruta esta agotada")
                    .status(503)
            }
            Self::IpCapacity => Response::send(
                "El limite de conexiones WebSocket para esta direccion IP esta agotado",
            )
            .status(429)
            .header("retry-after", "1"),
        }
    }
}

fn spawn_websocket(
    upgrade: OnUpgrade,
    config: ResolvedWebSocketConfig,
    protocol: Option<String>,
    handler: WebSocketHandler,
    permit: ConnectionPermit,
) {
    tokio::spawn(async move {
        let _permit = permit;
        match upgrade.await {
            Ok(upgraded) => {
                let io = TokioIo::new(upgraded);
                let stream = WebSocketStream::from_raw_socket(
                    io,
                    Role::Server,
                    Some(config.tungstenite_config()),
                )
                .await;
                handler(WebSocket::new(stream, protocol, config.ping_interval)).await;
            }
            Err(err) => {
                eprintln!("WebSocket upgrade failed: {}", err);
            }
        }
    });
}

/// A clonable fan-out channel for WebSocket rooms: handlers `subscribe()`
/// and forward received messages to their socket, while any holder of the
/// `WsBroadcast` can `send` to every current subscriber. Backed by
/// `tokio::sync::broadcast` (lagging subscribers skip the oldest messages).
#[derive(Clone)]
pub struct WsBroadcast {
    sender: tokio::sync::broadcast::Sender<WebSocketMessage>,
}

impl WsBroadcast {
    /// Creates a channel buffering up to `capacity` in-flight messages.
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = tokio::sync::broadcast::channel(capacity);
        Self { sender }
    }

    /// Sends to every current subscriber, returning how many received it.
    pub fn send(&self, message: WebSocketMessage) -> usize {
        self.sender.send(message).unwrap_or(0)
    }

    pub fn send_text(&self, text: &str) -> usize {
        self.send(WebSocketMessage::text(text))
    }

    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<WebSocketMessage> {
        self.sender.subscribe()
    }

    pub fn receiver_count(&self) -> usize {
        self.sender.receiver_count()
    }
}
