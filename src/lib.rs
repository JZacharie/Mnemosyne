use std::sync::Arc;
pub mod application;
pub mod domain;
pub mod infrastructure;
pub mod interfaces;

use axum::extract::FromRef;

#[derive(Clone)]
pub struct AppState {
    pub auth_use_case: Arc<application::use_cases::auth::AuthUseCase>,
    pub retrieval_use_case: Arc<application::use_cases::retrieval::RetrievalUseCase>,
    pub indexing_use_case: Arc<application::use_cases::indexing::IndexingUseCase>,
    pub pipeline_repo: Arc<dyn domain::ports::PipelineRepository>,
    pub collection_name: String,
    pub db_pool: sqlx::PgPool,
    pub vector_store: Arc<dyn domain::ports::VectorStore>,
    pub ollama_url: String,
    pub nfs_paths: Vec<String>,
}

impl FromRef<AppState> for Arc<application::use_cases::auth::AuthUseCase> {
    fn from_ref(state: &AppState) -> Self {
        state.auth_use_case.clone()
    }
}

impl FromRef<AppState> for Arc<application::use_cases::retrieval::RetrievalUseCase> {
    fn from_ref(state: &AppState) -> Self {
        state.retrieval_use_case.clone()
    }
}
