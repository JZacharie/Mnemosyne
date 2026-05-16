use crate::AppState;
use axum::{extract::State, response::IntoResponse, Json};
use serde::{Deserialize, Serialize};
use tracing::error;

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
    pub score: f32,
}

pub async fn search_handler(
    State(state): State<AppState>,
    Json(payload): Json<QueryRequest>,
) -> impl IntoResponse {
    match state
        .retrieval_use_case
        .execute(&payload.query, &state.collection_name)
        .await
    {
        Ok(chunks) => {
            let results = chunks
                .into_iter()
                .map(|c| QueryResult {
                    content: c.content,
                    source: c.metadata.file_name,
                    score: c.score.unwrap_or(0.0),
                })
                .collect();
            Json(QueryResponse { results }).into_response()
        }
        Err(e) => {
            error!("Search error: {}", e);
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response()
        }
    }
}
