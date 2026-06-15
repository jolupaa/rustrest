use rustrest::{App, HttpError, Next, Path, Query, Request, Response, Router, State, middleware};
use serde::{Deserialize, Serialize};

#[derive(Clone)]
struct Config {
    app_name: &'static str,
}

#[derive(Deserialize)]
struct UserPath {
    id: u32,
}

#[derive(Deserialize)]
struct ListQuery {
    active: Option<bool>,
}

#[derive(Deserialize, Serialize)]
struct User {
    id: u32,
    name: String,
}

fn users_router() -> Router {
    let mut router = Router::new();

    router.guard(|req: &Request| req.header("x-api-key") == Some("secret"));

    router
        .get("/", |req: Request| -> Result<Response, HttpError> {
            let Query(query) = req.extract::<Query<ListQuery>>()?;
            let State(config) = req.extract::<State<Config>>()?;
            let users = vec![User {
                id: 1,
                name: format!(
                    "{} - Ada ({})",
                    config.app_name,
                    query.active.unwrap_or(true)
                ),
            }];
            Ok(Response::json(&users))
        })
        .unwrap();

    router
        .get("/:id", |req: Request| -> Result<Response, HttpError> {
            let Path(path) = req.extract::<Path<UserPath>>()?;
            Ok(Response::json(&User {
                id: path.id,
                name: "Ada".to_string(),
            }))
        })
        .unwrap();

    router
        .post("/", |mut req: Request| async move {
            let mut user: User = req.json().await?;
            user.id = 100;
            Ok::<_, HttpError>(Response::json(&user).status(201))
        })
        .unwrap();

    router
        .fallback(|_req: Request| {
            Response::from_error(HttpError::not_found("User resource not found"))
        })
        .unwrap();

    router
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut app = App::new();

    app.state(Config {
        app_name: "RustRest API",
    });

    app.layer(middleware::tracing());
    app.layer(middleware::request_id());
    app.layer(middleware::cors());
    app.layer(|req: Request, next: Next| async move {
        println!("Custom middleware: {} {}", req.method, req.path);
        next(req).await
    });

    app.mount("/users", users_router()).unwrap();
    app.fallback(|_req: Request| Response::not_found()).unwrap();

    app.listen("127.0.0.1:3000").await
}
