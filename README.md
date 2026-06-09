# RustRest

RustRest is a minimal Express-style HTTP framework for Rust, built on top of `hyper` 1.x and `tokio`.

The goal is to provide a small, direct, easy-to-understand API for building HTTP servers and APIs without hiding the transport layer completely. RustRest includes routes, mountable routers, onion-style middleware, typed extractors, shared state, JSON responses, static files, SSE, cookies, redirects, and WebSocket routes.

> Status: `0.1.0`. The API is still evolving. It is best suited for learning, prototyping, and controlled framework development.

## Features

- HTTP server built on `hyper` 1.x and `tokio`.
- Synchronous and asynchronous handlers.
- Route helpers for `GET`, `POST`, `PUT`, `DELETE`, `PATCH`, `OPTIONS`, and `HEAD`.
- Mountable `Router` with nested prefixes.
- Route parameters with `:id`.
- Route wildcards with `*path`.
- Global and router-scoped onion middleware with `next`.
- Router guards.
- App and router fallbacks.
- Parsed query strings.
- Request and response cookies.
- Arbitrary response headers.
- `Result<Response, HttpError>` handlers.
- Global error handler.
- Typed shared state.
- Extractors: `Json<T>`, `Path<T>`, `Query<T>`, `State<T>`.
- Static files with extension-based content types.
- Response streaming.
- Server-Sent Events.
- WebSocket routes with frame send/receive helpers.
- JSON WebSocket events with `{ "event": "...", "data": ... }` envelopes.
- Built-in middleware: CORS, request id, gzip, and tracing.
- Unit tests and a real HTTP integration test.

## Installation

### Local path dependency

```toml
[dependencies]
rustrest = { path = "/path/to/rustrest" }
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
```

### Git dependency

```toml
[dependencies]
rustrest = { git = "https://github.com/your-user/rustrest.git" }
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
```

### crates.io dependency

After the crate is published:

```toml
[dependencies]
rustrest = "0.1"
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
```

RustRest uses Rust edition 2024 and requires Rust `1.85` or newer.

## Quick Start

```rust
use rustrest::{App, Request, Response};

#[tokio::main]
async fn main() {
    let mut app = App::new();

    app.get("/", |_req: Request| {
        Response::send("Hello from RustRest")
    });

    app.get("/users/:id", |req: Request| {
        let id = req.param("id").unwrap_or("?");
        Response::send(&format!("Requested user: {}", id))
    });

    app.listen("127.0.0.1:3000").await;
}
```

Run:

```bash
cargo run
```

Try:

```bash
curl http://127.0.0.1:3000/
curl http://127.0.0.1:3000/users/42
```

## Included Examples

```bash
cargo run --example basic
cargo run --example api
cargo run --example websocket
```

The `basic` example demonstrates simple routes and path parameters. The `api` example demonstrates routers, guards, extractors, shared state, built-in middleware, and errors. The `websocket` example demonstrates browser WebSocket clients, text messages, and JSON event envelopes.

## Mental Model

RustRest is built around four primary types:

```rust
use rustrest::{App, Router, Request, Response};
```

- `App`: the root application. It owns the root router, global middleware, shared state, and the global error handler.
- `Router`: a mountable group of routes. It can have scoped middleware, guards, and a fallback.
- `Request`: normalized request data passed to handlers.
- `Response`: the framework response type, converted internally into a Hyper response.

Request flow:

1. `App::listen` accepts TCP connections.
2. Hyper receives an HTTP request.
3. RustRest builds a `Request`.
4. `Router` finds the first matching route in registration order.
5. RustRest builds the middleware chain.
6. The handler returns `Response` or `Result<Response, E>`.
7. `Response` is converted into a Hyper response.

## Routes

```rust
let mut app = App::new();

app.get("/", |_req: Request| Response::send("home"));
app.post("/users", |_req: Request| Response::send("create"));
app.put("/users/:id", |_req: Request| Response::send("update"));
app.patch("/users/:id", |_req: Request| Response::send("patch"));
app.delete("/users/:id", |_req: Request| Response::send("delete"));
app.options("/users", |_req: Request| Response::send("options"));
app.head("/health", |_req: Request| Response::send("ok"));
app.all("/any", |_req: Request| Response::send("any method"));
```

Routes are evaluated in registration order. Register concrete routes before parameterized routes:

```rust
app.get("/users/me", |_req: Request| Response::send("me"));
app.get("/users/:id", |_req: Request| Response::send("by id"));
```

### Path Parameters

```rust
app.get("/users/:id/posts/:post_id", |req: Request| {
    let user_id = req.param("id").unwrap_or("?");
    let post_id = req.param("post_id").unwrap_or("?");
    Response::send(&format!("user={} post={}", user_id, post_id))
});
```

### Wildcards

Patterns such as `*name` capture the rest of the path. They are used internally for static files and fallbacks.

```rust
app.get("/files/*path", |req: Request| {
    Response::send(req.param("path").unwrap_or(""))
});
```

## Routers

Routers let you organize routes by module.

```rust
use rustrest::{Request, Response, Router};

fn users_router() -> Router {
    let mut router = Router::new();

    router.get("/", |_req: Request| Response::send("user list"));
    router.get("/:id", |req: Request| {
        Response::send(req.param("id").unwrap_or("?"))
    });

    router
}

let mut app = App::new();
app.mount("/users", users_router());
```

This creates:

- `GET /users`
- `GET /users/:id`

Routers can be mounted inside other routers:

```rust
let mut api = Router::new();
api.mount("/users", users_router());

let mut app = App::new();
app.mount("/api", api);
```

Result:

- `GET /api/users`
- `GET /api/users/:id`

## Handlers

A handler can be synchronous:

```rust
app.get("/", |_req: Request| {
    Response::send("sync")
});
```

Or asynchronous:

```rust
app.get("/async", |_req: Request| async move {
    Response::send("async")
});
```

A handler can also return `Result<Response, E>` when `E` implements `IntoHttpError`:

```rust
use rustrest::{HttpError, Request, Response};

app.get("/fallible", |_req: Request| -> Result<Response, HttpError> {
    Err(HttpError::bad_request("Invalid parameters"))
});
```

If a handler panics, RustRest catches it and returns `500`.

## Request

Main public fields:

```rust
pub struct Request {
    pub method: String,
    pub path: String,
    pub raw_query: Option<String>,
    pub query: HashMap<String, Vec<String>>,
    pub headers: HashMap<String, String>,
    pub cookies: HashMap<String, String>,
    pub body: String,
    pub params: HashMap<String, String>,
}
```

Useful methods:

```rust
req.param("id");
req.query("page");
req.query_all("tag");
req.header("authorization");
req.cookie("sid");
req.json::<MyType>();
req.extract::<Json<MyType>>();
req.state::<Config>();
req.is_websocket_upgrade();
req.websocket(|socket| async move { ... });
```

The request body is fully buffered into a `String`, with an internal 64 KB limit.

## Typed Extractors

RustRest includes extractors used through `Request::extract`.

```rust
use rustrest::{Json, Path, Query, Request, Response, State};
use serde::Deserialize;

#[derive(Deserialize)]
struct UserPath {
    id: u32,
}

#[derive(Deserialize)]
struct UserQuery {
    active: Option<bool>,
    tag: Vec<String>,
}

#[derive(Deserialize)]
struct CreateUser {
    name: String,
}

struct Config {
    app_name: &'static str,
}

app.get("/users/:id", |req: Request| -> Result<Response, rustrest::HttpError> {
    let Path(path) = req.extract::<Path<UserPath>>()?;
    let Query(query) = req.extract::<Query<UserQuery>>()?;
    let State(config) = req.extract::<State<Config>>()?;

    Ok(Response::send(&format!(
        "{} id={} active={:?} tags={:?}",
        config.app_name,
        path.id,
        query.active,
        query.tag
    )))
});

app.post("/users", |req: Request| -> Result<Response, rustrest::HttpError> {
    let Json(user) = req.extract::<Json<CreateUser>>()?;
    Ok(Response::send(&format!("Creating {}", user.name)).status(201))
});
```

## Shared State

Register state by type:

```rust
struct Config {
    database_url: String,
}

let mut app = App::new();
app.state(Config {
    database_url: "postgres://localhost/app".to_string(),
});

app.get("/config", |req: Request| {
    let config = req.state::<Config>().expect("Config registered");
    Response::send(&config.database_url)
});
```

Internally, state is stored in `Arc`, so `req.state::<T>()` returns `Option<Arc<T>>`.

## Response

### Text

```rust
Response::send("hello")
```

Returns `200` with `text/plain; charset=utf-8`.

### JSON

```rust
#[derive(serde::Serialize)]
struct User {
    id: u32,
    name: String,
}

Response::json(&User {
    id: 1,
    name: "Ada".to_string(),
})
```

### Status

```rust
Response::send("created").status(201)
```

### Content-Type

```rust
Response::send("<h1>Hello</h1>").content_type("text/html; charset=utf-8")
```

### Headers

```rust
Response::send("ok")
    .header("x-trace-id", "abc")
    .append_header("vary", "accept-encoding")
```

### Cookies

```rust
Response::send("ok").cookie("sid", "abc123")
```

Currently, `cookie` generates `Path=/; HttpOnly`.

### Redirects

```rust
Response::redirect("/login")
Response::redirect_with_status("/new", 301)
```

### Common Errors

```rust
Response::not_found()
Response::bad_request()
Response::internal_server_error()
Response::from_error(HttpError::forbidden("Access denied"))
```

### Bytes

```rust
use hyper::body::Bytes;

Response::bytes(Bytes::from_static(b"binary"), "application/octet-stream")
```

### Streaming

```rust
use futures_util::stream;
use hyper::body::Bytes;
use std::convert::Infallible;

let chunks = stream::iter(vec![
    Ok::<_, Infallible>(Bytes::from_static(b"hello ")),
    Ok::<_, Infallible>(Bytes::from_static(b"stream")),
]);

Response::stream(chunks).content_type("text/plain; charset=utf-8")
```

## Middleware

Middleware receives `Request` and `Next`.

```rust
use rustrest::{Next, Request, Response};

app.layer(|req: Request, next: Next| async move {
    println!("--> {} {}", req.method, req.path);
    let res = next(req).await;
    println!("<-- {}", res.status);
    res
});
```

It can short-circuit the chain without calling `next`:

```rust
app.layer(|_req: Request, _next: Next| async move {
    Response::send("blocked").status(403)
});
```

### Scoped Middleware

```rust
let mut router = Router::new();

router.layer(|req: Request, next: Next| async move {
    println!("[api] {}", req.path);
    next(req).await
});

router.get("/health", |_req: Request| Response::send("ok"));
app.mount("/api", router);
```

That middleware only runs for routes under `/api`.

### Built-In Middleware

```rust
use rustrest::middleware;

app.layer(middleware::tracing());
app.layer(middleware::request_id());
app.layer(middleware::cors());
app.layer(middleware::gzip());
```

- `tracing`: prints method, path, and status.
- `request_id`: propagates or generates `x-request-id`.
- `cors`: adds permissive CORS headers.
- `gzip`: compresses byte responses when the client accepts `gzip`.

## Guards

A guard blocks requests before they reach the router's routes.

```rust
let mut api = Router::new();

api.guard(|req: &Request| {
    req.header("x-api-key") == Some("secret")
});

api.get("/private", |_req: Request| Response::send("private"));
app.mount("/api", api);
```

If the guard fails, RustRest returns `403 Access denied`.

## Fallbacks

Global fallback:

```rust
app.fallback(|_req: Request| {
    Response::send("Not found").status(404)
});
```

Scoped fallback:

```rust
let mut api = Router::new();

api.get("/health", |_req: Request| Response::send("ok"));
api.fallback(|_req: Request| {
    Response::send("API route not found").status(404)
});

app.mount("/api", api);
```

## Static Files

```rust
let mut app = App::new();
app.static_files("/assets", "public");
```

Examples:

- `/assets/app.css` serves `public/app.css`.
- `/assets/images/logo.png` serves `public/images/logo.png`.

RustRest blocks path traversal with `..` and assigns content types for common file extensions.

## Error Handling

`HttpError` represents HTTP errors:

```rust
HttpError::bad_request("Invalid request");
HttpError::unauthorized("Unauthenticated");
HttpError::forbidden("Access denied");
HttpError::not_found("Not found");
HttpError::internal_server_error("Internal failure");
```

Handlers with `Result`:

```rust
app.get("/users/:id", |req: Request| -> Result<Response, HttpError> {
    let id = req.param("id").ok_or_else(|| {
        HttpError::bad_request("Missing id")
    })?;

    Ok(Response::send(id))
});
```

Global error handler:

```rust
app.error_handler(|err: HttpError| {
    Response::json(&serde_json::json!({
        "error": err.message(),
        "status": err.status(),
    }))
    .status(err.status())
});
```

## Server-Sent Events

```rust
use futures_util::stream;
use rustrest::{Response, SseEvent};

app.get("/events", |_req: Request| {
    let events = stream::iter(vec![
        SseEvent::new("hello").event("greeting").id("1"),
        SseEvent::new("goodbye"),
    ]);

    Response::sse(events)
});
```

The response uses `text/event-stream`, `Cache-Control: no-cache`, and `Connection: keep-alive`.

## WebSocket

RustRest supports native WebSocket routes:

```rust
use rustrest::{App, WebSocketEvent};
use serde_json::json;

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
```

`Router` has the same API:

```rust
let mut router = Router::new();
router.websocket("/ws", |mut socket| async move {
    socket.send_text("hello").await.ok();
});
app.mount("/api", router);
```

There is also a short alias:

```rust
app.ws("/ws", |mut socket| async move {
    socket.close().await.ok();
});
```

### WebSocket Methods

```rust
socket.recv().await;
socket.send(WebSocketMessage::text("hello")).await;
socket.send_text("hello").await;
socket.send_binary(bytes).await;
socket.send_json(&value).await;
socket.recv_json::<T>().await;
socket.send_event("event:name", &data).await;
socket.recv_event::<T>().await;
socket.ping(bytes).await;
socket.pong(bytes).await;
socket.close().await;
```

`recv()` returns `Result<Option<WebSocketMessage>, WebSocketError>`. `Ok(None)` means the peer closed the stream.

### Event Envelopes

WebSocket itself has no named events. RustRest provides a small JSON convention:

```json
{
  "event": "chat:message",
  "data": {
    "text": "hello"
  }
}
```

Server side:

```rust
socket
    .send_event("chat:message", &serde_json::json!({ "text": "hello" }))
    .await?;

if let Some(event) = socket.recv_event::<serde_json::Value>().await? {
    println!("event={} data={}", event.event, event.data);
}
```

Client side with the browser WebSocket API:

```html
<script>
  const socket = new WebSocket("ws://127.0.0.1:3000/ws");

  socket.addEventListener("open", () => {
    socket.send(JSON.stringify({
      event: "client:hello",
      data: { text: "hello from browser" }
    }));
  });

  socket.addEventListener("message", (message) => {
    const parsed = JSON.parse(message.data);
    switch (parsed.event) {
      case "server:ready":
        console.log("ready", parsed.data);
        break;
      case "chat:message":
        console.log("chat", parsed.data.text);
        break;
      default:
        console.log("raw message", message.data);
    }
  });

  socket.addEventListener("close", () => console.log("closed"));
  socket.addEventListener("error", (error) => console.error(error));
</script>
```

### WebSocket vs Socket.IO

RustRest implements native WebSocket, not the Socket.IO protocol.

Socket.IO adds its own protocol on top of HTTP/WebSocket: named events, acknowledgements, fallback transports, rooms, namespaces, reconnection behavior, heartbeats, and a Socket.IO-specific client. A Socket.IO JavaScript client cannot connect directly to a plain WebSocket server unless the server implements the Socket.IO protocol.

With RustRest, use the browser `WebSocket` API or any WebSocket client. For named events, use `send_event` / `recv_event`, which are plain JSON messages and can be consumed from any language.

### Manual Handshake Helper

RustRest includes a handshake helper:

```rust
app.get("/ws", |req: Request| -> Result<Response, HttpError> {
    Response::websocket(&req)
});
```

This validates upgrade headers and returns `101 Switching Protocols` with `Sec-WebSocket-Accept`. Prefer `app.websocket` for normal server-side WebSocket handlers because it also owns the upgraded stream and frame loop.

## Serving with an Existing TcpListener

Besides `listen`, you can use `serve` for tests or custom bootstrap code:

```rust
use tokio::net::TcpListener;

let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
app.serve(listener).await;
```

## Testing

Main commands:

```bash
cargo fmt --check
cargo check
cargo clippy --all-targets
cargo test
```

The project includes:

- Core unit tests in `src/app/tests.rs`.
- A real HTTP integration test in `tests/http_integration.rs`.

## Publishing Preparation

This repository already includes basic crates.io metadata:

- `description`
- `license`
- `readme`
- `documentation`
- `keywords`
- `categories`
- `rust-version`

Before publishing:

```bash
cargo package --list
cargo publish --dry-run
```

To publish for real:

```bash
cargo login
cargo publish
```

This README documents the process only. It does not publish the crate.

## Project Structure

```text
src/
  lib.rs                 # Public crate API
  main.rs                # Demo server for cargo run
  api.rs                 # Demo router used by main.rs
  users.rs               # Demo router used by main.rs
  app.rs                 # Core: App, Router, Request, Response
  app/
    error.rs             # HttpError and IntoHttpError
    extract.rs           # Json, Path, Query, State
    middleware.rs        # Built-in middleware
    sse.rs               # SseEvent
    tests.rs             # Framework unit tests
examples/
  basic.rs               # Minimal example
  api.rs                 # Full API example
  websocket.rs           # WebSocket and browser client example
tests/
  http_integration.rs    # Real HTTP integration test
```

## Current Limitations

- Request bodies are fully buffered with a 64 KB limit.
- Request streaming is not implemented yet.
- TLS is not integrated; use a proxy or external listener if you need HTTPS.
- Routing is registration-order based, without specificity ranking.
- Built-in CORS is permissive; customize it for stricter policies.
- Handler argument macros are not implemented; extractors are used through `req.extract::<T>()`.

## License

MIT. See `LICENSE`.
