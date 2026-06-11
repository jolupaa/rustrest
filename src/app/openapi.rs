//! Minimal OpenAPI 3.0 generation from the registered routes, plus the
//! Swagger UI page served by `App::serve_docs`. Path parameters are derived
//! from `:param` / `*wildcard` placeholders (typed as strings); request/
//! response schemas are not introspected.

use serde_json::{Map, Value, json};

use super::RouteInfo;

/// Builds an OpenAPI 3.0.3 document for the given routes. `all()` routes
/// (method `*`) have no single HTTP method and are skipped.
pub(crate) fn build_document(title: &str, version: &str, routes: &[RouteInfo]) -> Value {
    let mut paths = Map::new();
    for route in routes {
        if route.method == "*" {
            continue;
        }
        let (path, params) = openapi_path(&route.path);

        let mut operation = Map::new();
        if let Some(summary) = &route.summary {
            operation.insert("summary".to_string(), json!(summary));
        }
        if let Some(description) = &route.description {
            operation.insert("description".to_string(), json!(description));
        }
        if !route.tags.is_empty() {
            operation.insert("tags".to_string(), json!(route.tags));
        }
        if !params.is_empty() {
            let params: Vec<Value> = params
                .iter()
                .map(|name| {
                    json!({
                        "name": name,
                        "in": "path",
                        "required": true,
                        "schema": { "type": "string" },
                    })
                })
                .collect();
            operation.insert("parameters".to_string(), json!(params));
        }
        operation.insert(
            "responses".to_string(),
            json!({ "200": { "description": "OK" } }),
        );

        let item = paths
            .entry(path)
            .or_insert_with(|| Value::Object(Map::new()));
        if let Value::Object(item) = item {
            item.insert(route.method.to_lowercase(), Value::Object(operation));
        }
    }

    json!({
        "openapi": "3.0.3",
        "info": { "title": title, "version": version },
        "paths": paths,
    })
}

/// Converts a route pattern (`/users/:id/files/*rest`) into an OpenAPI path
/// (`/users/{id}/files/{rest}`), returning the path parameter names.
fn openapi_path(pattern: &str) -> (String, Vec<String>) {
    let mut path = String::new();
    let mut params = Vec::new();
    for segment in pattern.split('/').filter(|s| !s.is_empty()) {
        path.push('/');
        match segment
            .strip_prefix(':')
            .or_else(|| segment.strip_prefix('*'))
        {
            Some(name) => {
                path.push('{');
                path.push_str(name);
                path.push('}');
                params.push(name.to_string());
            }
            None => path.push_str(segment),
        }
    }
    if path.is_empty() {
        path.push('/');
    }
    (path, params)
}

/// The Swagger UI page (assets from the unpkg CDN) pointing at `spec_url`.
pub(crate) fn swagger_ui_html(title: &str, spec_url: &str) -> String {
    format!(
        r##"<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{title} — API docs</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css" />
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>
    window.onload = () => {{
      SwaggerUIBundle({{ url: "{spec_url}", dom_id: "#swagger-ui" }});
    }};
  </script>
</body>
</html>
"##
    )
}
