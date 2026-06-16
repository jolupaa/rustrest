use rustrest::{App, Request, Response};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut app = App::new();

    app.get("/", |_req: Request| Response::send("Hello from RustRest"))
        .unwrap();

    app.get("/hello/:name", |req: Request| {
        let name = req.param("name").unwrap_or("world");
        Response::send(&format!("Hello, {}", name))
    })
    .unwrap();

    app.listen("127.0.0.1:3000").await
}
