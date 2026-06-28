use crate::AppState;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::Deserialize;
use uuid::Uuid;
use tracing::{error, info};

#[derive(Deserialize)]
pub struct RetryRequest {
    pub id: Uuid,
    pub chunk_size: Option<usize>,
    pub chunk_overlap: Option<usize>,
    pub custom_text: Option<String>,
}

pub async fn list_runs_handler(State(state): State<AppState>) -> impl IntoResponse {
    match state.pipeline_repo.list_runs().await {
        Ok(runs) => Json(runs).into_response(),
        Err(e) => {
            error!("Failed to list pipeline runs: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": e.to_string() })),
            )
                .into_response()
        }
    }
}

pub async fn get_run_handler(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> impl IntoResponse {
    match state.pipeline_repo.get_run(id).await {
        Ok(Some(run)) => Json(run).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "Run not found" })),
        )
            .into_response(),
        Err(e) => {
            error!("Failed to get pipeline run: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": e.to_string() })),
            )
                .into_response()
        }
    }
}

pub async fn retry_run_handler(
    State(state): State<AppState>,
    Json(payload): Json<RetryRequest>,
) -> impl IntoResponse {
    // 1. Fetch the existing run to get file_path
    let run = match state.pipeline_repo.get_run(payload.id).await {
        Ok(Some(r)) => r,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "Run not found" })),
            )
                .into_response();
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": e.to_string() })),
            )
                .into_response();
        }
    };

    let chunk_size = payload.chunk_size.unwrap_or(1000);
    let chunk_overlap = payload.chunk_overlap.unwrap_or(0);
    let custom_text = payload.custom_text;
    let collection_name = state.collection_name.clone();
    let indexing_use_case = state.indexing_use_case.clone();
    let file_path = run.file_path.clone();

    info!("Queueing manual re-run / correction for file: {} (ID: {})", file_path, payload.id);

    // Spawn a background thread/task to process the pipeline asynchronously
    tokio::spawn(async move {
        if let Err(e) = indexing_use_case
            .process_file_with_params(
                &file_path,
                &collection_name,
                chunk_size,
                chunk_overlap,
                custom_text,
            )
            .await
        {
            error!("Failed pipeline correction re-run for {}: {}", file_path, e);
        }
    });

    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({ "status": "re-indexing queued" })),
    )
        .into_response()
}
