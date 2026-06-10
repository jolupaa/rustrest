use hyper::header::SEC_WEBSOCKET_ACCEPT;

use crate::RequestBuilder;

use super::*;

fn handshake_request_without_host() -> RequestBuilder {
    Request::builder()
        .method("GET")
        .header("upgrade", "websocket")
        .header("connection", "Upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
}

fn handshake_request() -> RequestBuilder {
    handshake_request_without_host().header("host", "localhost")
}

fn assert_websocket_error_status(req: &Request, expected: u16) {
    let error = Response::websocket(req)
        .err()
        .expect("the handshake should be rejected");
    assert_eq!(error.status(), expected);
}

#[test]
fn websocket_handshake_sets_upgrade_headers() {
    let req = handshake_request().build();

    assert!(req.is_websocket_upgrade());

    let res = Response::websocket(&req).unwrap().into_hyper();

    assert_eq!(res.status(), 101);
    assert_eq!(
        res.headers().get(SEC_WEBSOCKET_ACCEPT).unwrap(),
        "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
    );
}

#[test]
fn websocket_handshake_parses_upgrade_headers_as_tokens() {
    let req = Request::builder()
        .method("GET")
        .header("host", "localhost")
        .header("upgrade", "h2c")
        .header("upgrade", "WebSocket")
        .header("connection", "keep-alive")
        .header("connection", "keep-alive, UpGrAdE")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .build();

    assert!(Response::websocket(&req).is_ok());
}

#[test]
fn websocket_handshake_requires_one_non_empty_host() {
    let missing = handshake_request_without_host().build();
    assert_websocket_error_status(&missing, 400);

    let empty = handshake_request_without_host().header("host", "").build();
    assert_websocket_error_status(&empty, 400);

    let duplicate = handshake_request().header("host", "example.com").build();
    assert_websocket_error_status(&duplicate, 400);
}

#[test]
fn websocket_handshake_rejects_duplicate_key() {
    let req = handshake_request()
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .build();

    assert_websocket_error_status(&req, 400);
}

#[test]
fn websocket_handshake_rejects_duplicate_version() {
    let req = handshake_request()
        .header("sec-websocket-version", "13")
        .build();

    assert_websocket_error_status(&req, 400);
}

#[test]
fn websocket_handshake_rejects_duplicate_origin() {
    let req = handshake_request()
        .header("origin", "https://app.example.com")
        .header("origin", "https://app.example.com")
        .build();

    assert_websocket_error_status(&req, 400);
}

#[test]
fn websocket_with_rejects_disallowed_origin_with_403() {
    let req = Request::builder()
        .method("GET")
        .header("host", "app.example.com")
        .header("origin", "https://evil.example.com")
        .header("upgrade", "websocket")
        .header("connection", "Upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .secure(true)
        .build();
    let config =
        WebSocketConfig::new().origin_policy(OriginPolicy::same_host().allow_missing(false));

    let response = req.websocket_with(config, |_socket| async move {});

    assert_eq!(response.status, 403);
}

#[test]
fn websocket_with_requires_subprotocol_overlap_when_configured() {
    let req = handshake_request()
        .header("sec-websocket-protocol", "graphql-ws")
        .build();
    let config = WebSocketConfig::new()
        .protocols(&["chat"])
        .require_protocol(true);

    let response = req.websocket_with(config, |_socket| async move {});

    assert_eq!(response.status, 400);
}

#[test]
fn websocket_config_negotiates_first_supported_subprotocol() {
    let req = Request::builder()
        .header("sec-websocket-protocol", "other")
        .header("sec-websocket-protocol", "chat, superchat")
        .build();

    // Client preference order wins among the server-supported protocols.
    let config = WebSocketConfig::new().protocols(&["superchat", "chat"]);
    assert_eq!(config.negotiate(&req).as_deref(), Some("chat"));

    let config = WebSocketConfig::new().protocols(&["superchat"]);
    assert_eq!(config.negotiate(&req).as_deref(), Some("superchat"));

    // No overlap (or no offer) -> no protocol echoed.
    let config = WebSocketConfig::new().protocols(&["graphql-ws"]);
    assert_eq!(config.negotiate(&req), None);
    assert_eq!(WebSocketConfig::new().negotiate(&req), None);
}

#[tokio::test]
async fn ws_broadcast_fans_out_to_subscribers() {
    let room = WsBroadcast::new(8);
    let mut a = room.subscribe();
    let mut b = room.subscribe();

    assert_eq!(room.receiver_count(), 2);
    assert_eq!(room.send_text("hola"), 2);

    assert_eq!(a.recv().await.unwrap(), WebSocketMessage::text("hola"));
    assert_eq!(b.recv().await.unwrap(), WebSocketMessage::text("hola"));

    // Without subscribers nothing is delivered (and nothing panics).
    drop(a);
    drop(b);
    assert_eq!(room.send_text("nadie"), 0);
}
