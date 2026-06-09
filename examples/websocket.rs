use rustrest::App;
use serde_json::json;

#[tokio::main]
async fn main() {
    let mut app = App::new();

    app.websocket("/ws", |mut socket| async move {
        socket
            .send_event("server:ready", &json!({ "message": "connected" }))
            .await
            .ok();

        while let Ok(Some(message)) = socket.recv().await {
            if message.is_text() {
                let text = message.into_text().unwrap().to_string();
                socket.send_text(&format!("echo:{}", text)).await.ok();
                socket
                    .send_event("chat:message", &json!({ "text": text }))
                    .await
                    .ok();
            } else if message.is_close() {
                break;
            }
        }
    });

    app.websocket("/events", |mut socket| async move {
        while let Ok(Some(event)) = socket.recv_event::<serde_json::Value>().await {
            match event.event.as_str() {
                "client:ping" => {
                    socket
                        .send_event("server:pong", &json!({ "received": event.data }))
                        .await
                        .ok();
                }
                "client:close" => {
                    socket.close().await.ok();
                    break;
                }
                _ => {
                    socket
                        .send_event(
                            "server:error",
                            &json!({ "message": "unknown event", "event": event.event }),
                        )
                        .await
                        .ok();
                }
            }
        }
    });

    app.get("/", |_req| {
        rustrest::Response::send(
            r##"<!doctype html>
<html>
  <body>
    <button id="send">Send WebSocket message</button>
    <pre id="log"></pre>
    <script>
      const log = (line) => document.querySelector("#log").textContent += `${line}\n`;
      const socket = new WebSocket("ws://127.0.0.1:3000/ws");

      socket.addEventListener("open", () => log("open"));
      socket.addEventListener("message", (event) => log(`message: ${event.data}`));
      socket.addEventListener("close", () => log("close"));
      socket.addEventListener("error", () => log("error"));

      document.querySelector("#send").addEventListener("click", () => {
        socket.send("hello from browser");
      });
    </script>
  </body>
</html>"##,
        )
        .content_type("text/html; charset=utf-8")
    });

    println!("Open http://127.0.0.1:3000 and click the button.");
    app.listen("127.0.0.1:3000").await;
}
