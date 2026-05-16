use crate::domain::entities::DocumentChunk;
use crate::domain::ports::{EmbeddingService, FileScanner, VectorStore};
use anyhow::Result;
use std::sync::Arc;
use tracing::{debug, error, info};
use futures::StreamExt;

#[derive(Clone)]
pub struct IndexingUseCase {
    file_scanner: Arc<dyn FileScanner>,
    embedding_service: Arc<dyn EmbeddingService>,
    vector_store: Arc<dyn VectorStore>,
}

impl IndexingUseCase {
    pub fn new(
        file_scanner: Arc<dyn FileScanner>,
        embedding_service: Arc<dyn EmbeddingService>,
        vector_store: Arc<dyn VectorStore>,
    ) -> Self {
        Self {
            file_scanner,
            embedding_service,
            vector_store,
        }
    }

    pub async fn execute(&self, path: &str, collection_name: &str) -> Result<()> {
        info!("Starting indexing process for path: {}", path);

        let file_paths = self.file_scanner.scan_directory(path).await?;
        info!("Found {} files to process", file_paths.len());

        let concurrency = 8; // Process 8 files in parallel
        let collection_name = collection_name.to_string();

        futures::stream::iter(file_paths)
            .map(|file_path| {
                let this = self.clone();
                let col_name = collection_name.clone();
                async move {
                    match this.process_file(&file_path, &col_name).await {
                        Ok(_) => info!("Successfully indexed: {}", file_path),
                        Err(e) => error!("Failed to index {}: {}", file_path, e),
                    }
                }
            })
            .buffer_unordered(concurrency)
            .collect::<Vec<()>>()
            .await;

        Ok(())
    }

    async fn process_file(&self, file_path: &str, collection_name: &str) -> Result<()> {
        debug!("Processing file: {}", file_path);
        let doc = self.file_scanner.load_document(file_path).await?;

        // Simple chunking logic (could be moved to a domain service)
        let chunks_content: Vec<String> = doc
            .content
            .chars()
            .collect::<Vec<char>>()
            .chunks(1000)
            .map(|c| c.iter().collect())
            .collect();

        let mut embeddings = Vec::new();
        for batch in chunks_content.chunks(32) {
            let batch_embeddings = self
                .embedding_service
                .generate_embeddings(batch.to_vec())
                .await?;
            embeddings.extend(batch_embeddings);
        }
        
        let doc_chunks: Vec<DocumentChunk> = chunks_content
            .into_iter()
            .zip(embeddings)
            .map(|(content, embedding)| DocumentChunk {
                content,
                metadata: doc.metadata.clone(),
                embedding: Some(embedding),
                score: None,
            })
            .collect();

        self.vector_store
            .save_chunks(doc_chunks, collection_name)
            .await?;

        Ok(())
    }
}
