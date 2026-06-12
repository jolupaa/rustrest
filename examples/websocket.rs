use std::sync::Arc;
use std::time::Duration;

use rustrest::{
    App, BackpressurePolicy, Next, OriginPolicy, Request, Response, WebSocketConfig,
    WebSocketError, WebSocketObservation, WebSocketObserver, WsError, WsHub,
};
use serde_json::{Value, json};

const DEMO_TOKEN: &str = "demo-token";

struct MetadataObserver;

impl WebSocketObserver for MetadataObserver {
    fn observe(&self, event: &WebSocketObservation<'_>) {
        eprintln!("WebSocket: {event:?}");
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut app = App::new();
    app.websocket_hub(WsHub::local());
    app.websocket_observer(Arc::new(MetadataObserver));
    app.websocket_defaults(WebSocketConfig::new().max_connections(12_000));

    // Token fijo solo para demostrar que la autenticacion ocurre antes del upgrade.
    app.layer(|req: Request, next: Next| async move {
        if req.path.starts_with("/chat/") && req.query("token") != Some(DEMO_TOKEN) {
            return Response::send("Token WebSocket no valido").status(401);
        }
        next(req).await
    });

    let chat_config = WebSocketConfig::new()
        .protocols(&["chat"])
        .require_protocol(true)
        .max_message_size(1024 * 1024)
        .max_frame_size(256 * 1024)
        .inbound_capacity(64)
        .outbound_capacity(64)
        .backpressure_policy(BackpressurePolicy::Wait)
        .send_timeout(Duration::from_secs(5))
        .ping_interval(Duration::from_secs(30))
        .pong_timeout(Duration::from_secs(10))
        .idle_timeout(Duration::from_secs(120))
        .close_timeout(Duration::from_secs(5))
        .origin_policy(OriginPolicy::same_host().allow_missing(false))
        .max_connections(2_000)
        .max_connections_per_ip(20)
        .message_rate_limit(100, Duration::from_secs(1))
        .max_rooms_per_connection(32)
        .max_room_name_bytes(128);

    app.websocket_with("/chat/:channel", chat_config, |mut socket| async move {
        let Some(join) = socket.recv_event::<String>().await? else {
            return Ok::<(), WsError>(());
        };
        if join.event != "room:join" {
            socket.close_with(1008, "se requiere room:join").await?;
            return Ok(());
        }
        let room = join.data;
        socket.join(room.clone()).await?;
        socket
            .send_event("room:joined", &json!({ "room": room }))
            .await?;

        let (mut receiver, sender) = socket.split();
        let background = sender.clone();
        tokio::spawn(async move {
            if let Err(error) = background
                .send_event("server:ready", &json!({ "background": true }))
                .await
            {
                eprintln!("No se pudo enviar server:ready: {error}");
            }
        });

        while let Some(event) = receiver.recv_event::<Value>().await? {
            match event.event.as_str() {
                "chat:message" => {
                    match sender
                        .to(room.clone())
                        .send_event("chat:message", &event.data)
                        .await
                    {
                        Ok(report) if report.rejected == 0 && report.disconnected == 0 => {}
                        Ok(report) => eprintln!("Broadcast parcial: {report:?}"),
                        Err(error) => eprintln!("Fallo de broadcast: {error}"),
                    }
                }
                "room:leave" => {
                    sender.leave(room.clone()).await?;
                    sender.close_with(1000, "sala abandonada").await?;
                    break;
                }
                _ => {
                    sender
                        .send_event(
                            "server:error",
                            &json!({ "message": "evento no reconocido" }),
                        )
                        .await?;
                }
            }
        }
        Ok::<(), WsError>(())
    });

    app.websocket_with(
        "/autobahn",
        WebSocketConfig::new()
            .max_message_size(64 * 1024 * 1024)
            .max_frame_size(16 * 1024 * 1024)
            .origin_policy(OriginPolicy::any().allow_missing(true))
            .disable_ping()
            .disable_idle_timeout()
            .disable_max_connection_lifetime()
            .disable_max_connections_per_ip()
            .disable_message_rate_limit(),
        |mut socket| async move {
            while let Some(message) = socket.recv().await? {
                if message.is_text() || message.is_binary() {
                    socket.send(message).await?;
                } else if message.is_close() {
                    break;
                }
            }
            Ok::<(), WebSocketError>(())
        },
    );

    app.websocket_with(
        "/load",
        WebSocketConfig::new()
            .max_connections(12_000)
            .inbound_capacity(64)
            .outbound_capacity(64)
            .max_message_size(1024 * 1024)
            .origin_policy(OriginPolicy::any().allow_missing(true))
            .disable_max_connections_per_ip()
            .disable_message_rate_limit(),
        |mut socket| async move {
            while let Some(message) = socket.recv().await? {
                if message.is_text() || message.is_binary() {
                    socket.send(message).await?;
                } else if message.is_close() {
                    break;
                }
            }
            Ok::<(), WebSocketError>(())
        },
    );

    app.get("/", |_req| {
        Response::send(
            r##"<!doctype html>
<html lang="es">
  <body>
    <button id="send">Enviar mensaje</button>
    <pre id="log"></pre>
    <script>
      const log = (line) => document.querySelector("#log").textContent += `${line}\n`;
      const socket = new WebSocket(
        "ws://127.0.0.1:3000/chat/general?token=demo-token",
        "chat"
      );
      socket.addEventListener("open", () => {
        socket.send(JSON.stringify({ event: "room:join", data: "general" }));
      });
      socket.addEventListener("message", (event) => log(event.data));
      socket.addEventListener("close", (event) => log(`close ${event.code}: ${event.reason}`));
      socket.addEventListener("error", () => log("error"));
      document.querySelector("#send").addEventListener("click", () => {
        socket.send(JSON.stringify({
          event: "chat:message",
          data: { text: "hola desde el navegador" }
        }));
      });
    </script>
  </body>
</html>"##,
        )
        .content_type("text/html; charset=utf-8")
    });

    println!("Ejemplo WebSocket en http://127.0.0.1:3000");
    app.listen_with_shutdown("127.0.0.1:3000", async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await
}
