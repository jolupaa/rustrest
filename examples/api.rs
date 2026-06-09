use rustrest::{
    App, HttpError, Json, Next, Path, Query, Request, Response, Router, State, middleware,
};
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

    router.get("/", |req: Request| -> Result<Response, HttpError> {
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
    });

    router.get("/:id", |req: Request| -> Result<Response, HttpError> {
        let Path(path) = req.extract::<Path<UserPath>>()?;
        Ok(Response::json(&User {
            id: path.id,
            name: "Ada".to_string(),
        }))
    });

    router.post("/", |req: Request| -> Result<Response, HttpError> {
        let Json(mut user) = req.extract::<Json<User>>()?;
        user.id = 100;
        Ok(Response::json(&user).status(201))
    });

    router.fallback(|_req: Request| {
        Response::from_error(HttpError::not_found("User resource not found"))
    });

    router
}

#[tokio::main]
async fn main() {
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

    app.mount("/users", users_router());
    app.fallback(|_req: Request| Response::not_found());

    app.listen("127.0.0.1:3000").await;
}
