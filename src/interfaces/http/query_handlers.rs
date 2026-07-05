use crate::AppState;
use axum::{extract::State, response::IntoResponse, Json};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tracing::error;
use uuid::Uuid;

#[derive(Deserialize)]
pub struct QueryRequest {
    pub query: String,
}

#[derive(Serialize)]
pub struct QueryResponse {
    pub results: Vec<QueryResult>,
}

#[derive(Serialize)]
pub struct QueryResult {
    pub content: String,
    pub source: String,
    pub source_path: String,
    pub score: f32,
}

pub async fn search_handler(
    State(state): State<AppState>,
    Json(payload): Json<QueryRequest>,
) -> impl IntoResponse {
    let start = std::time::Instant::now();

    let (response, results_count) = match state
        .retrieval_use_case
        .execute(&payload.query, &state.collection_name)
        .await
    {
        Ok(chunks) => {
            let results: Vec<QueryResult> = chunks
                .into_iter()
                .map(|c| QueryResult {
                    content: c.content,
                    source: c.metadata.file_name,
                    source_path: c.metadata.source_path,
                    score: c.score.unwrap_or(0.0),
                })
                .collect();
            let count = results.len() as i32;
            (Json(QueryResponse { results }).into_response(), count)
        }
        Err(e) => {
            error!("Search error: {}", e);
            (
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": e.to_string()})),
                )
                    .into_response(),
                0,
            )
        }
    };

    let duration_ms = start.elapsed().as_millis() as i32;
    let _ = state
        .pipeline_repo
        .log_search(Uuid::new_v4(), &payload.query, results_count, duration_ms)
        .await;

    response
}

pub async fn ollama_generate_proxy_handler(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    body_bytes: axum::body::Bytes,
) -> impl IntoResponse {
    let client = reqwest::Client::new();
    let url = format!("{}/api/generate", state.ollama_url.trim_end_matches('/'));

    let mut req_builder = client.post(&url).body(body_bytes);

    if let Some(ct) = headers.get(axum::http::header::CONTENT_TYPE) {
        req_builder = req_builder.header(axum::http::header::CONTENT_TYPE, ct);
    }

    match req_builder.send().await {
        Ok(res) => {
            let status = axum::http::StatusCode::from_u16(res.status().as_u16())
                .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
            let mut res_headers = axum::http::HeaderMap::new();
            if let Some(ct) = res.headers().get(reqwest::header::CONTENT_TYPE) {
                if let Ok(val) = axum::http::HeaderValue::from_bytes(ct.as_bytes()) {
                    res_headers.insert(axum::http::header::CONTENT_TYPE, val);
                }
            }

            let stream = res
                .bytes_stream()
                .map(|chunk| chunk.map_err(std::io::Error::other));

            (status, res_headers, axum::body::Body::from_stream(stream)).into_response()
        }
        Err(e) => (
            axum::http::StatusCode::BAD_GATEWAY,
            format!("Ollama proxy error: {}", e),
        )
            .into_response(),
    }
}
