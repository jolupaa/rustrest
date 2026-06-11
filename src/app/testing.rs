//! In-process test client: drives an [`App`] through its full middleware and
//! routing pipeline without opening a TCP socket.

use hyper::body::Bytes;
use serde::Serialize;

use super::{App, HttpError, Request, RequestBuilder, Response};

/// Drives an [`App`] in-process for tests:
///
/// ```rust,no_run
/// # use rustrest::{App, Request, Response, TestClient};
/// # async fn demo() {
/// let mut app = App::new();
/// app.get("/ping", |_req: Request| Response::send("pong"));
///
/// let client = TestClient::new(app);
/// let res = client.get("/ping").send().await;
/// assert_eq!(res.status, 200);
/// assert_eq!(res.body_text(), "pong");
/// # }
/// ```
pub struct TestClient {
    app: App,
}

impl TestClient {
    pub fn new(app: App) -> Self {
        Self { app }
    }

    pub fn get(&self, path: &str) -> TestRequest<'_> {
        self.request("GET", path)
    }

    pub fn post(&self, path: &str) -> TestRequest<'_> {
        self.request("POST", path)
    }

    pub fn put(&self, path: &str) -> TestRequest<'_> {
        self.request("PUT", path)
    }

    pub fn delete(&self, path: &str) -> TestRequest<'_> {
        self.request("DELETE", path)
    }

    pub fn patch(&self, path: &str) -> TestRequest<'_> {
        self.request("PATCH", path)
    }

    pub fn options(&self, path: &str) -> TestRequest<'_> {
        self.request("OPTIONS", path)
    }

    pub fn head(&self, path: &str) -> TestRequest<'_> {
        self.request("HEAD", path)
    }

    /// Starts a request with an arbitrary method.
    pub fn request(&self, method: &str, path: &str) -> TestRequest<'_> {
        TestRequest {
            client: self,
            builder: Request::builder().method(method).path(path),
        }
    }
}

/// A request under construction against a [`TestClient`]. Finish with `send`.
pub struct TestRequest<'c> {
    client: &'c TestClient,
    builder: RequestBuilder,
}

impl TestRequest<'_> {
    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.builder = self.builder.header(name, value);
        self
    }

    pub fn cookie(mut self, name: &str, value: &str) -> Self {
        self.builder = self.builder.cookie(name, value);
        self
    }

    pub fn body(mut self, body: impl Into<Bytes>) -> Self {
        self.builder = self.builder.body(body);
        self
    }

    /// Serializes `value` as the JSON body and sets the content type.
    pub fn json<T: Serialize>(mut self, value: &T) -> Self {
        self.builder = self.builder.json(value);
        self
    }

    /// Runs the request through the app (global middleware, routing, scoped
    /// middleware, handler, error handler), honoring the configured body
    /// limit (413) and request timeout (408) like the real server.
    pub async fn send(self) -> Response {
        let request = self.builder.build();
        if request.bytes().len() > self.client.app.config.max_body_size {
            return self
                .client
                .app
                .error_response(HttpError::new(413, "Payload Too Large"));
        }
        self.client.app.run_request(request).await
    }
}
