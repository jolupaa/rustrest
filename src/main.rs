#![forbid(unsafe_code)]

mod api;
mod users;

use rustrest::{App, HttpError, Json, Response, middleware};
use serde::Deserialize;

#[derive(Deserialize)]
struct Greeting {
    nombre: String,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut app = App::new();

    app.layer(middleware::cors());

    // Global middleware (onion model): runs before and after the handler.
    // `from_fn` infers the `Request`/`Next` parameter types.
    app.layer(middleware::from_fn(|req, next| async move {
        println!("--> {} {}", req.method, req.path);
        let res = next(req).await;
        println!("<-- {} ({})", res.status, res.content_type);
        res
    }));

    // Synchronous handler.
    app.get("/", |_req: rustrest::Request| {
        Response::send("Hola desde RustRest")
    })?;

    // Asynchronous handler with a typed JSON body: a missing or malformed
    // body becomes a structured 4xx problem response instead of a default.
    app.post("/saludo", |Json(greeting): Json<Greeting>| async move {
        Ok::<_, HttpError>(Response::send(&format!("Hola, {}", greeting.nombre)))
    })?;

    // Routes and sub-routes organized in files: `api` mounts `users`.
    // Result: /api/users, /api/users/:id, ...
    app.mount("/api", api::router())?;

    app.listen("127.0.0.1:3000").await
}
