use crate::domain::entities::{AuditLog, Document, DocumentChunk, PipelineRun, User};
use anyhow::Result;
use async_trait::async_trait;
use uuid::Uuid;

#[async_trait]
pub trait VectorStore: Send + Sync {
    async fn save_chunks(&self, chunks: Vec<DocumentChunk>, collection_name: &str) -> Result<()>;
    async fn search(
        &self,
        query_text: &str,
        query_vector: Vec<f32>,
        limit: usize,
        collection_name: &str,
    ) -> Result<Vec<DocumentChunk>>;
    async fn health_check(&self) -> Result<()>;
}

#[async_trait]
pub trait EmbeddingService: Send + Sync {
    async fn generate_embeddings(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>>;
}

#[async_trait]
pub trait RerankingService: Send + Sync {
    async fn rerank(
        &self,
        query: &str,
        documents: Vec<DocumentChunk>,
        top_n: usize,
    ) -> Result<Vec<DocumentChunk>>;
}

#[async_trait]
pub trait FileScanner: Send + Sync {
    async fn scan_directory(&self, path: &str) -> Result<Vec<String>>;
    async fn load_document(&self, file_path: &str) -> Result<Document>;
}

#[async_trait]
pub trait UserRepository: Send + Sync {
    async fn get_by_username(&self, username: &str) -> Result<Option<User>>;
    #[allow(dead_code)]
    async fn create(&self, user: User) -> Result<User>;
}

#[async_trait]
pub trait AuditRepository: Send + Sync {
    async fn log(&self, entry: AuditLog) -> Result<()>;
    #[allow(dead_code)]
    async fn get_by_user(&self, user_id: Uuid) -> Result<Vec<AuditLog>>;
}

#[async_trait]
pub trait PipelineRepository: Send + Sync {
    async fn create_run(&self, run: PipelineRun) -> Result<()>;
    async fn update_run(&self, run: PipelineRun) -> Result<()>;
    async fn get_run(&self, id: Uuid) -> Result<Option<PipelineRun>>;
    async fn get_run_by_file_path(&self, file_path: &str) -> Result<Option<PipelineRun>>;
    async fn list_runs(&self) -> Result<Vec<PipelineRun>>;
}
