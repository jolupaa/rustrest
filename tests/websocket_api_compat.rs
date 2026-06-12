use std::sync::Arc;
use std::time::Duration;

use rustrest::{
    App, IntoWebSocketHandler, Request, Response, Router, WebSocket, WebSocketCloseInfo,
    WebSocketCloseInitiator, WebSocketConfig, WebSocketError, WebSocketEvent, WebSocketHandler,
    WebSocketMessage, WebSocketObservation, WebSocketObserver, WebSocketReceiver,
    WebSocketRuntimeHandle, WebSocketSender, WebSocketStats, WsBroadcast, WsError, WsHub,
};

struct Observer;

impl WebSocketObserver for Observer {
    fn observe(&self, _event: &WebSocketObservation<'_>) {}
}

fn exhaustive_existing_error(error: WebSocketError) -> &'static str {
    match error {
        WebSocketError::Protocol(_) => "protocol",
        WebSocketError::Json(_) => "json",
    }
}

fn accepts_websocket(_: WebSocket) {}
fn accepts_close_info(_: WebSocketCloseInfo) {}
fn accepts_close_initiator(_: WebSocketCloseInitiator) {}

#[test]
fn existing_websocket_surface_still_compiles() {
    let mut app = App::new();
    let hub = WsHub::builder()
        .max_rooms_per_connection(32)
        .max_room_name_bytes(128)
        .broadcast_concurrency(64)
        .broker_operation_timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let _: &mut App = app.websocket_hub(hub.clone());
    let _: WsHub = app.websocket_hub_handle();
    let _: WsHub = WsHub::local();
    let runtime: WebSocketRuntimeHandle = app.websocket_runtime();
    let _stats: WebSocketStats = runtime.stats();
    let _connections = runtime.connections();
    let _: &mut App = app.websocket_defaults(WebSocketConfig::new());
    let _: &mut App = app.websocket_observer(Arc::new(Observer));
    let handler: WebSocketHandler = (|mut socket: WebSocket| async move {
        let _: Option<&str> = socket.protocol();
        let _: Result<Option<WebSocketMessage>, WebSocketError> = socket.recv().await;
        let _: Result<(), WebSocketError> = socket.send(WebSocketMessage::text("directo")).await;
        let _: Result<(), WebSocketError> = socket.send_text("hola").await;
        let _: Result<(), WebSocketError> = socket.send_binary([1_u8, 2, 3].as_slice()).await;
        let _: Result<(), WebSocketError> =
            socket.send_json(&serde_json::json!({ "ok": true })).await;
        let _: Result<Option<serde_json::Value>, WebSocketError> = socket.recv_json().await;
        let _: Result<(), WebSocketError> = socket.send_event("ready", &true).await;
        let _: Result<Option<WebSocketEvent<bool>>, WebSocketError> = socket.recv_event().await;
        let _: Result<(), WebSocketError> = socket.ping(Vec::<u8>::new()).await;
        let _: Result<(), WebSocketError> = socket.pong(Vec::<u8>::new()).await;
        let _: Result<(), WebSocketError> = socket.close().await;
        let _: Result<(), WebSocketError> = socket.close_with(1000, "finalizado").await;
        let _: WebSocketCloseInfo = socket.closed().await;
        let _: Result<(), WsError> = socket.join("general").await;
        let _: Result<(), WsError> = socket.join_many(["general", "equipo-7"]).await;
        let _: Result<(), WsError> = socket.leave("general").await;
        let _: Result<(), WsError> = socket.leave_many(["general", "equipo-7"]).await;
        let _: Result<(), WsError> = socket.leave_all().await;
        let _: Result<Vec<String>, WsError> = socket.rooms().await;
        let _ = socket.id();
        let _: Option<std::net::SocketAddr> = socket.remote_addr();
        let _: &str = socket.route();
        let (mut receiver, sender): (WebSocketReceiver, WebSocketSender) = socket.split();
        let cloned_sender: WebSocketSender = sender.clone();
        let _ = sender.id();
        let _: Option<std::net::SocketAddr> = sender.remote_addr();
        let _: &str = sender.route();
        let _: Option<&str> = sender.protocol();
        let _: Result<(), WsError> = sender.send(WebSocketMessage::text("split")).await;
        let _: Result<(), WsError> = sender.try_send(WebSocketMessage::text("inmediato"));
        let _: Result<(), WsError> = cloned_sender.send_text("desde clone").await;
        let _: Result<(), WsError> = sender.join("general").await;
        let _: Result<(), WsError> = sender.join_many(["general", "equipo-7"]).await;
        let _: Result<(), WsError> = sender.leave("general").await;
        let _: Result<(), WsError> = sender.leave_many(["general", "equipo-7"]).await;
        let _: Result<(), WsError> = sender.leave_all().await;
        let _: Result<Vec<String>, WsError> = sender.rooms().await;
        let _: Result<(), WsError> = sender.close_with(1000, "finalizado").await;
        let _: WebSocketCloseInfo = sender.closed().await;
        let _: Result<Option<WebSocketMessage>, WebSocketError> = receiver.recv().await;
        let _: WebSocketCloseInfo = receiver.closed().await;
    })
    .into_websocket_handler();
    let _: () = app.websocket("/ws", |_socket| async move {});
    let _: () = app.websocket("/ws-result", |_socket| async move {
        Ok::<(), WebSocketError>(())
    });
    let _: () = app.websocket("/ws-result-precise", |_socket| async move {
        Ok::<(), WsError>(())
    });

    let _: () = app.ws("/short", |_socket| async move {});
    let _: () = app.websocket_with(
        "/configured",
        WebSocketConfig::new()
            .protocols(&["chat"])
            .max_message_size(1024)
            .ping_interval(Duration::from_secs(30)),
        |_socket| async move {},
    );

    let mut router = Router::new();
    let _: () = router.websocket("/nested", |_socket| async move {});
    let _: () = router.websocket("/nested-result", |_socket| async move {
        Ok::<(), WebSocketError>(())
    });
    let _: () = router.websocket("/nested-result-precise", |_socket| async move {
        Ok::<(), WsError>(())
    });
    let _: () = router.ws("/nested-short", |_socket| async move {});
    let _: () = router.websocket_with(
        "/nested-configured",
        WebSocketConfig::new(),
        |_socket| async move {},
    );

    let request = Request::builder().path("/ws").build();
    let _response: Response = request.websocket(|_socket| async move {});
    let request = Request::builder().path("/configured").build();
    let _response: Response = request.websocket_with(WebSocketConfig::new(), handler.clone());

    let broadcast: WsBroadcast = WsBroadcast::new(8);
    let mut receiver: tokio::sync::broadcast::Receiver<WebSocketMessage> = broadcast.subscribe();
    let delivered: usize = broadcast.send_text("hola");
    assert_eq!(delivered, 1);
    let delivered: usize = broadcast.send(WebSocketMessage::text("directo"));
    assert_eq!(delivered, 1);
    let _receiver_count: usize = broadcast.receiver_count();
    drop(receiver.try_recv());

    let _message: WebSocketMessage = WebSocketMessage::text("hola");
    let _event: WebSocketEvent<bool> = WebSocketEvent {
        event: "ready".to_string(),
        data: true,
    };
    let _error_match: fn(WebSocketError) -> &'static str = exhaustive_existing_error;
    let _socket_consumer: fn(WebSocket) = accepts_websocket;
    let _close_info_consumer: fn(WebSocketCloseInfo) = accepts_close_info;
    let _close_initiator_consumer: fn(WebSocketCloseInitiator) = accepts_close_initiator;
}
