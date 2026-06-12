use hyper::header::SEC_WEBSOCKET_ACCEPT;
use std::net::SocketAddr;
use std::time::Duration;

use crate::RequestBuilder;

use super::socket::{LocalEnqueueOutcome, SocketMetadata, channel_pair};
use super::*;

fn resolved_config(app: WebSocketConfig, route: WebSocketConfig) -> ResolvedWebSocketConfig {
    ResolvedWebSocketConfig::from_layers(&app, &route)
}

#[test]
fn websocket_config_resolves_process_route_and_disable_overrides() {
    let app = WebSocketConfig::new()
        .max_connections(10)
        .max_connections_per_ip(4)
        .message_rate_limit(8, Duration::from_secs(1))
        .idle_timeout(Duration::from_secs(30))
        .max_connection_lifetime(Duration::from_secs(300));
    let route = WebSocketConfig::new()
        .max_connections(2)
        .disable_max_connections_per_ip()
        .disable_message_rate_limit()
        .disable_idle_timeout()
        .disable_max_connection_lifetime();

    let resolved = resolved_config(app, route);

    assert_eq!(resolved.process_max_connections, Some(10));
    assert_eq!(resolved.route_max_connections, Some(2));
    assert_eq!(resolved.max_connections_per_ip, None);
    assert!(resolved.message_rate_limit.is_none());
    assert_eq!(resolved.idle_timeout, None);
    assert_eq!(resolved.max_connection_lifetime, None);
}

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
fn websocket_upgrade_predicate_rejects_duplicates_and_trims_version() {
    let whitespace_version = Request::builder()
        .method("GET")
        .header("host", "localhost")
        .header("upgrade", "websocket")
        .header("connection", "Upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", " 13 ")
        .build();
    assert!(whitespace_version.is_websocket_upgrade());

    let duplicate_key = handshake_request()
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .build();
    assert!(!duplicate_key.is_websocket_upgrade());

    let duplicate_version = handshake_request()
        .header("sec-websocket-version", "13")
        .build();
    assert!(!duplicate_version.is_websocket_upgrade());
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

#[tokio::test]
async fn ws_broadcast_reports_lag() {
    let room = WsBroadcast::new(1);
    let mut receiver = room.subscribe();

    assert_eq!(room.send_text("primero"), 1);
    assert_eq!(room.send_text("segundo"), 1);
    assert!(matches!(
        receiver.recv().await,
        Err(tokio::sync::broadcast::error::RecvError::Lagged(1))
    ));
    assert_eq!(
        receiver.recv().await.unwrap(),
        WebSocketMessage::text("segundo")
    );
}

#[test]
fn websocket_runtime_accounts_for_permits() {
    let runtime = WebSocketRuntimeHandle::local();
    let config = resolved_config(WebSocketConfig::new(), WebSocketConfig::new());
    let first_addr: SocketAddr = "127.0.0.1:4001".parse().unwrap();
    let second_addr: SocketAddr = "127.0.0.2:4002".parse().unwrap();

    let first = runtime
        .admit("/chat/:room", Some(first_addr), Some("chat"), &config)
        .unwrap();
    let first_id = first.id();
    let _second = runtime
        .admit("/chat/:room", Some(second_addr), None, &config)
        .unwrap();
    runtime
        .join(first_id, &["zeta".into(), "general".into()])
        .unwrap();

    assert_eq!(runtime.stats().active_connections, 2);
    let first_snapshot = runtime.connection(first_id).unwrap();
    assert_eq!(first_snapshot.id.to_string(), "1");
    assert_eq!(first_snapshot.route, "/chat/:room");
    assert_eq!(first_snapshot.remote_addr, Some(first_addr));
    assert_eq!(first_snapshot.protocol.as_deref(), Some("chat"));
    assert_eq!(first_snapshot.rooms, ["general", "zeta"]);
    assert_eq!(
        first_snapshot.lifecycle,
        WebSocketLifecycleState::Connecting
    );
    let connections = runtime.connections();
    assert_eq!(connections.len(), 2);
    assert!(connections[0].id.0 < connections[1].id.0);
    drop(first);
    assert_eq!(runtime.stats().active_connections, 1);
    assert_eq!(runtime.stats().accepted_connections, 2);
}

#[test]
fn websocket_runtime_rejects_capacity_without_partial_registration() {
    let process_runtime = WebSocketRuntimeHandle::local();
    let process_config = resolved_config(
        WebSocketConfig::new().max_connections(1),
        WebSocketConfig::new()
            .max_connections(1)
            .max_connections_per_ip(1),
    );
    let process_permit = process_runtime
        .admit(
            "/process",
            Some("127.0.0.1:4101".parse().unwrap()),
            None,
            &process_config,
        )
        .unwrap();
    assert!(matches!(
        process_runtime.admit(
            "/process",
            Some("127.0.0.1:4102".parse().unwrap()),
            None,
            &process_config,
        ),
        Err(AdmissionError::ProcessCapacity)
    ));
    assert_eq!(process_runtime.stats().active_connections, 1);
    assert_eq!(process_runtime.stats().accepted_connections, 1);
    assert_eq!(process_runtime.stats().rejected_connections, 1);
    drop(process_permit);

    let route_runtime = WebSocketRuntimeHandle::local();
    let route_config = resolved_config(
        WebSocketConfig::new(),
        WebSocketConfig::new()
            .max_connections(1)
            .max_connections_per_ip(1),
    );
    let route_permit = route_runtime
        .admit(
            "/route/:id",
            Some("127.0.0.3:4201".parse().unwrap()),
            None,
            &route_config,
        )
        .unwrap();
    assert!(matches!(
        route_runtime.admit(
            "/route/:id",
            Some("127.0.0.3:4202".parse().unwrap()),
            None,
            &route_config,
        ),
        Err(AdmissionError::RouteCapacity)
    ));
    assert_eq!(route_runtime.stats().active_connections, 1);
    assert_eq!(route_runtime.stats().accepted_connections, 1);
    assert_eq!(route_runtime.stats().rejected_connections, 1);
    drop(route_permit);

    let ip_runtime = WebSocketRuntimeHandle::local();
    let ip_config = resolved_config(
        WebSocketConfig::new(),
        WebSocketConfig::new().max_connections_per_ip(1),
    );
    let ip_permit = ip_runtime
        .admit(
            "/first",
            Some("127.0.0.5:4301".parse().unwrap()),
            None,
            &ip_config,
        )
        .unwrap();
    assert!(matches!(
        ip_runtime.admit(
            "/second",
            Some("127.0.0.5:4302".parse().unwrap()),
            None,
            &ip_config,
        ),
        Err(AdmissionError::IpCapacity)
    ));
    assert_eq!(ip_runtime.stats().active_connections, 1);
    assert_eq!(ip_runtime.stats().accepted_connections, 1);
    assert_eq!(ip_runtime.stats().rejected_connections, 1);
    drop(ip_permit);
}

#[test]
fn websocket_runtime_rejects_after_accepting_stops() {
    let runtime = WebSocketRuntimeHandle::local();
    let config = resolved_config(WebSocketConfig::new(), WebSocketConfig::new());

    runtime.stop_accepting();

    assert!(matches!(
        runtime.admit("/ws", None, None, &config),
        Err(AdmissionError::Shutdown)
    ));
    assert_eq!(runtime.stats().active_connections, 0);
    assert_eq!(runtime.stats().accepted_connections, 0);
    assert_eq!(runtime.stats().rejected_connections, 1);
}

#[tokio::test]
async fn websocket_runtime_retains_shutdown_for_late_driver_subscribers() {
    let runtime = WebSocketRuntimeHandle::local();

    runtime.begin_shutdown().await;
    let shutdown_rx = runtime.subscribe_shutdown();

    assert!(*shutdown_rx.borrow());
}

#[test]
fn websocket_runtime_isolates_observer_panics_during_admission() {
    struct PanickingObserver;

    impl WebSocketObserver for PanickingObserver {
        fn observe(&self, _event: &WebSocketObservation<'_>) {
            panic!("observer panic");
        }
    }

    let runtime = WebSocketRuntimeHandle::local();
    runtime.set_observer(std::sync::Arc::new(PanickingObserver));
    let config = resolved_config(WebSocketConfig::new(), WebSocketConfig::new());

    let permit = runtime.admit("/ws", None, None, &config).unwrap();

    assert_eq!(runtime.stats().active_connections, 1);
    drop(permit);
    assert_eq!(runtime.stats().active_connections, 0);
}

#[test]
fn websocket_rooms_are_route_scoped_atomic_and_cleaned_on_release() {
    let runtime = WebSocketRuntimeHandle::local();
    let config = resolved_config(
        WebSocketConfig::new(),
        WebSocketConfig::new()
            .max_rooms_per_connection(2)
            .max_room_name_bytes(16),
    );
    let chat = runtime
        .admit("/chat/:channel", None, None, &config)
        .unwrap();
    let admin = runtime
        .admit("/admin/chat/:channel", None, None, &config)
        .unwrap();

    runtime
        .join(chat.id(), &["general".into(), "equipo-7".into()])
        .unwrap();
    runtime.join(chat.id(), &["general".into()]).unwrap();
    runtime.join(admin.id(), &["general".into()]).unwrap();

    assert_eq!(
        runtime.rooms(chat.id()).unwrap(),
        vec!["equipo-7", "general"]
    );
    assert_eq!(runtime.local_room_size("/chat/:channel", "general"), 1);
    assert_eq!(
        runtime.local_room_size("/admin/chat/:channel", "general"),
        1
    );

    assert!(
        runtime
            .join(chat.id(), &["a".into(), "b".into(), "c".into()])
            .is_err()
    );
    assert!(
        runtime
            .join(chat.id(), &["valida".into(), "".into()])
            .is_err()
    );
    assert!(runtime.join(chat.id(), &["con\0nul".into()]).is_err());
    assert!(runtime.join(chat.id(), &["á".repeat(9)]).is_err());
    assert_eq!(
        runtime.rooms(chat.id()).unwrap(),
        vec!["equipo-7", "general"]
    );

    runtime.leave(chat.id(), &["general".into()]).unwrap();
    runtime.leave(chat.id(), &["general".into()]).unwrap();
    assert_eq!(runtime.rooms(chat.id()).unwrap(), vec!["equipo-7"]);
    runtime.leave_all(chat.id()).unwrap();
    assert!(runtime.rooms(chat.id()).unwrap().is_empty());

    runtime.join(chat.id(), &["general".into()]).unwrap();
    drop(chat);
    assert!(runtime.rooms(WebSocketId(1)).is_none());
    assert_eq!(runtime.local_room_size("/chat/:channel", "general"), 0);
    assert_eq!(
        runtime.local_room_size("/admin/chat/:channel", "general"),
        1
    );
    drop(admin);
}

#[test]
fn websocket_hub_room_limits_are_hard_ceilings() {
    let hub = WsHub::builder()
        .max_rooms_per_connection(1)
        .max_room_name_bytes(4)
        .build()
        .unwrap();
    let runtime = hub.runtime();
    let config = resolved_config(
        WebSocketConfig::new(),
        WebSocketConfig::new()
            .max_rooms_per_connection(10)
            .max_room_name_bytes(100),
    );
    let permit = runtime.admit("/ws", None, None, &config).unwrap();

    runtime.join(permit.id(), &["sala".into()]).unwrap();
    assert!(matches!(
        runtime.join(permit.id(), &["otra".into()]),
        Err(WsError::RoomLimit)
    ));
    runtime.leave_all(permit.id()).unwrap();
    assert!(matches!(
        runtime.join(permit.id(), &["larga".into()]),
        Err(WsError::InvalidRoom(_))
    ));
}

#[tokio::test]
async fn websocket_broadcast_report_represents_every_partial_outcome() {
    let hub = WsHub::local();
    let runtime = hub.runtime();
    let config = resolved_config(
        WebSocketConfig::new(),
        WebSocketConfig::new()
            .outbound_capacity(1)
            .backpressure_policy(BackpressurePolicy::Reject),
    );
    let first = runtime.admit("/chat", None, None, &config).unwrap();
    let second = runtime.admit("/chat", None, None, &config).unwrap();
    let third = runtime.admit("/chat", None, None, &config).unwrap();
    let (_first_socket, first_sender, _first_channels) = channel_pair(
        SocketMetadata {
            id: first.id(),
            remote_addr: None,
            route: "/chat".into(),
            protocol: None,
        },
        &config,
        runtime.clone(),
    );
    let (_second_socket, second_sender, _second_channels) = channel_pair(
        SocketMetadata {
            id: second.id(),
            remote_addr: None,
            route: "/chat".into(),
            protocol: None,
        },
        &config,
        runtime.clone(),
    );
    let first_driver = tokio::spawn(std::future::pending::<()>());
    let second_driver = tokio::spawn(std::future::pending::<()>());
    assert!(runtime.register_driver(
        first.id(),
        first_sender.clone(),
        first_driver.abort_handle()
    ));
    assert!(runtime.register_driver(second.id(), second_sender, second_driver.abort_handle()));
    assert!(matches!(
        first_sender
            .enqueue(WebSocketMessage::text("ocupa-cola"))
            .await,
        LocalEnqueueOutcome::Enqueued
    ));

    let report = hub
        .route("/chat")
        .all()
        .send_text("broadcast")
        .await
        .unwrap();

    assert_eq!(report.matched, 3);
    assert_eq!(report.enqueued, 1);
    assert_eq!(report.rejected, 1);
    assert_eq!(report.disconnected, 1);
    assert_eq!(
        report.matched,
        report.enqueued + report.rejected + report.disconnected
    );
    assert_eq!(report.remote, WsRemotePublish::NotConfigured);
    assert!(matches!(
        hub.all()
            .send(WebSocketMessage::Ping(Vec::new().into()))
            .await,
        Err(WsBroadcastError::InvalidMessage)
    ));

    first_driver.abort();
    second_driver.abort();
    drop((first, second, third));
}

#[test]
fn websocket_admission_errors_map_before_upgrade() {
    for error in [
        AdmissionError::Shutdown,
        AdmissionError::ProcessCapacity,
        AdmissionError::RouteCapacity,
    ] {
        assert_eq!(error.into_response().status, 503);
    }

    let response = AdmissionError::IpCapacity.into_response();
    assert_eq!(response.status, 429);
    assert_eq!(response.headers.get("retry-after").unwrap(), "1");
}
