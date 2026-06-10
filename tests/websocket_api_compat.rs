use std::time::Duration;

use rustrest::{
    App, Request, Router, WebSocket, WebSocketConfig, WebSocketError, WebSocketMessage, WsBroadcast,
};

fn exhaustive_existing_error(error: WebSocketError) -> &'static str {
    match error {
        WebSocketError::Protocol(_) => "protocol",
        WebSocketError::Json(_) => "json",
    }
}

fn accepts_websocket(_: WebSocket) {}

#[test]
fn existing_websocket_surface_still_compiles() {
    let mut app = App::new();
    app.websocket("/ws", |mut socket| async move {
        let _ = socket.protocol();
        let _ = socket.send_text("hola").await;
        let _ = socket.send_binary([1_u8, 2, 3].as_slice()).await;
        let _ = socket.send_json(&serde_json::json!({ "ok": true })).await;
        let _ = socket.send_event("ready", &true).await;
        let _ = socket.ping(Vec::<u8>::new()).await;
        let _ = socket.pong(Vec::<u8>::new()).await;
        let _ = socket.close().await;
    });

    app.ws("/short", |_socket| async move {});
    app.websocket_with(
        "/configured",
        WebSocketConfig::new()
            .protocols(&["chat"])
            .max_message_size(1024)
            .ping_interval(Duration::from_secs(30)),
        |_socket| async move {},
    );

    let mut router = Router::new();
    router.websocket("/nested", |_socket| async move {});
    router.ws("/nested-short", |_socket| async move {});
    router.websocket_with(
        "/nested-configured",
        WebSocketConfig::new(),
        |_socket| async move {},
    );

    let request = Request::builder().path("/ws").build();
    let _response = request.websocket(|_socket| async move {});

    let broadcast = WsBroadcast::new(8);
    let mut receiver = broadcast.subscribe();
    assert_eq!(broadcast.send_text("hola"), 1);
    let _ = broadcast.receiver_count();
    drop(receiver.try_recv());

    let _message = WebSocketMessage::text("hola");
    let _error_match: fn(WebSocketError) -> &'static str = exhaustive_existing_error;
    let _socket_consumer: fn(WebSocket) = accepts_websocket;
}
