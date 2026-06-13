use std::sync::Arc;
use std::time::Duration;

use rustrest::{
    App, InMemoryWsBroker, IntoWebSocketHandler, Request, Response, Router, WebSocket,
    WebSocketCloseInfo, WebSocketCloseInitiator, WebSocketConfig, WebSocketError, WebSocketEvent,
    WebSocketHandler, WebSocketLifecycleState, WebSocketMessage, WebSocketObservation,
    WebSocketObserver, WebSocketReceiver, WebSocketRuntimeHandle, WebSocketSender, WebSocketStats,
    WsBroadcast, WsBroadcastError, WsBroadcastReport, WsBroker, WsBrokerError,
    WsBrokerErrorCategory, WsBrokerPayload, WsBrokerPublication, WsBrokerStream, WsBrokerTarget,
    WsError, WsHub, WsLocalSocket, WsNodeId, WsPublicationId, WsRemotePublish, WsRoute, WsTarget,
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
fn assert_broker_object_safe(_: Arc<dyn WsBroker>) {}

#[test]
fn existing_websocket_surface_still_compiles() {
    let mut app = App::new();
    let broker = Arc::new(InMemoryWsBroker::new(64));
    assert_broker_object_safe(broker.clone());
    let hub = WsHub::builder()
        .max_rooms_per_connection(32)
        .max_room_name_bytes(128)
        .broadcast_concurrency(64)
        .broker_operation_timeout(Duration::from_secs(2))
        .broker(broker)
        .node_id(WsNodeId::new(42))
        .build()
        .unwrap();
    assert_eq!(hub.node_id().get(), 42);
    let _: &mut App = app.websocket_hub(hub.clone());
    let _: WsHub = app.websocket_hub_handle();
    let _: WsHub = WsHub::local();
    let route: WsRoute = hub.route("/ws");
    let _: WsTarget = route.to("general");
    let _: WsTarget = route.to_many(["general", "equipo-7"]);
    let _: WsTarget = route.all();
    let _: WsTarget = hub.all();
    let runtime: WebSocketRuntimeHandle = app.websocket_runtime();
    let _stats: WebSocketStats = runtime.stats();
    let _connections = runtime.connections();
    let _local_socket_lookup: fn(&WsHub, rustrest::WebSocketId) -> Option<WsLocalSocket> =
        WsHub::local_socket;
    let _local_connection_count: fn(&WsHub) -> usize = WsHub::local_connection_count;
    let production_config = WebSocketConfig::new()
        .protocols(&["chat"])
        .require_protocol(true)
        .max_message_size(1024 * 1024)
        .max_frame_size(256 * 1024)
        .write_buffer_size(128 * 1024)
        .max_write_buffer_size(2 * 1024 * 1024)
        .inbound_capacity(64)
        .outbound_capacity(64)
        .backpressure_policy(rustrest::BackpressurePolicy::Wait)
        .send_timeout(Duration::from_secs(5))
        .ping_interval(Duration::from_secs(30))
        .pong_timeout(Duration::from_secs(10))
        .idle_timeout(Duration::from_secs(120))
        .max_connection_lifetime(Duration::from_secs(86_400))
        .close_timeout(Duration::from_secs(5))
        .origin_policy(rustrest::OriginPolicy::same_host().allow_missing(false))
        .max_connections(2_000)
        .max_connections_per_ip(20)
        .message_rate_limit(100, Duration::from_secs(1))
        .max_rooms_per_connection(32)
        .max_room_name_bytes(128);
    production_config.validate().unwrap();
    let relaxed_route = WebSocketConfig::new()
        .disable_ping()
        .disable_idle_timeout()
        .disable_max_connection_lifetime()
        .disable_max_connections_per_ip()
        .disable_message_rate_limit();
    relaxed_route.validate().unwrap();
    let _: &mut App = app.websocket_defaults(production_config);
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
        let _: Result<WsBroadcastReport, WsBroadcastError> =
            socket.to("general").send_text("hola").await;
        let _: Result<WsBroadcastReport, WsBroadcastError> = socket
            .to_many(["general", "equipo-7"])
            .send_json(&serde_json::json!({ "ok": true }))
            .await;
        let _: WsTarget = socket.broadcast().except(socket.id());
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
        let _: Result<WsBroadcastReport, WsBroadcastError> = sender
            .to("general")
            .send_binary([1_u8, 2, 3].as_slice())
            .await;
        let _: Result<WsBroadcastReport, WsBroadcastError> =
            sender.broadcast().send_event("ready", &true).await;
        let _: Result<(), WsError> = sender.close_with(1000, "finalizado").await;
        let _: WebSocketCloseInfo = sender.closed().await;
        let _: Result<Option<WebSocketMessage>, WebSocketError> = receiver.recv().await;
        let _: WebSocketCloseInfo = receiver.closed().await;
    })
    .into_websocket_handler();
    let admin_hub = hub.clone();
    let admin_runtime = runtime.clone();
    let _: () = app.websocket("/ws-admin-api", move |socket| {
        let admin_hub = admin_hub.clone();
        let admin_runtime = admin_runtime.clone();
        async move {
            let local = admin_hub.local_socket(socket.id());
            if let Some(local) = local {
                let _: rustrest::WebSocketId = local.id();
                let _: &str = local.route();
                let _: Option<std::net::SocketAddr> = local.remote_addr();
                let _: Option<&str> = local.protocol();
                let _: std::time::SystemTime = local.opened_at();
                let _: &[String] = local.rooms();
                let _: WebSocketLifecycleState = local.lifecycle();
                let _: Result<(), WsError> = local.send_text("administrado").await;
                let _: Result<(), WsError> = local.send_event("ready", &true).await;
                let _: Result<(), WsError> = local.close_with(1000, "finalizado").await;
                let _: WebSocketCloseInfo = local.closed().await;
            }
            let _: Result<(), WsError> = admin_hub
                .disconnect_local(socket.id(), 1008, "no autorizado")
                .await;
            let _snapshot = admin_runtime.connection(socket.id());
            let _: Result<(), WsError> = admin_runtime
                .close(socket.id(), 1008, "no autorizado")
                .await;
            let _: Result<(), WsError> = admin_runtime.shutdown().await;
        }
    });
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
    let _remote: WsRemotePublish = WsRemotePublish::NotConfigured;
    let _broker_stream: Option<WsBrokerStream> = None;
    let _broker_error: Option<WsBrokerError> = None;
    let _broker_error_category: WsBrokerErrorCategory = WsBrokerError::Unavailable.category();
    let _publication = WsBrokerPublication::new(
        WsPublicationId::new(1),
        WsNodeId::new(42),
        WsBrokerTarget::AllRoutes,
        WsBrokerPayload::Text("hola".into()),
    );
    let _error_match: fn(WebSocketError) -> &'static str = exhaustive_existing_error;
    let _socket_consumer: fn(WebSocket) = accepts_websocket;
    let _close_info_consumer: fn(WebSocketCloseInfo) = accepts_close_info;
    let _close_initiator_consumer: fn(WebSocketCloseInitiator) = accepts_close_initiator;
}
