use crate::domain::entities::{AuditLog, Document, DocumentChunk, User};
use anyhow::Result;
use async_trait::async_trait;
use uuid::Uuid;

#[async_trait]
pub trait VectorStore: Send + Sync {
    async fn save_chunks(&self, chunks: Vec<DocumentChunk>, collection_name: &str) -> Result<()>;
}

#[async_trait]
pub trait EmbeddingService: Send + Sync {
    async fn generate_embeddings(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>>;
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
