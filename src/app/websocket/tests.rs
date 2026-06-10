use hyper::header::SEC_WEBSOCKET_ACCEPT;

use super::*;

#[test]
fn websocket_handshake_sets_upgrade_headers() {
    let req = Request::builder()
        .method("GET")
        .header("upgrade", "websocket")
        .header("connection", "Upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .build();

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
        .header("upgrade", "h2c, WebSocket")
        .header("connection", "keep-alive, UpGrAdE")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .build();

    assert!(Response::websocket(&req).is_ok());
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
    let req = Request::builder()
        .method("GET")
        .header("upgrade", "websocket")
        .header("connection", "Upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
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
