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
