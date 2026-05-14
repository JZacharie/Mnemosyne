use crate::domain::entities::{DocumentChunk, Document};
use async_trait::async_trait;
use anyhow::Result;

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
