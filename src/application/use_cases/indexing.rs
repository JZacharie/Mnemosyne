use crate::domain::ports::{VectorStore, EmbeddingService, FileScanner};
use crate::domain::entities::DocumentChunk;
use std::sync::Arc;
use anyhow::Result;
use tracing::{info, error, debug};

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

        for file_path in file_paths {
            match self.process_file(&file_path, collection_name).await {
                Ok(_) => info!("Successfully indexed: {}", file_path),
                Err(e) => error!("Failed to index {}: {}", file_path, e),
            }
        }

        Ok(())
    }

    async fn process_file(&self, file_path: &str, collection_name: &str) -> Result<()> {
        debug!("Processing file: {}", file_path);
        let doc = self.file_scanner.load_document(file_path).await?;
        
        // Simple chunking logic (could be moved to a domain service)
        let chunks_content: Vec<String> = doc.content
            .chars()
            .collect::<Vec<char>>()
            .chunks(1000)
            .map(|c| c.iter().collect())
            .collect();

        let embeddings = self.embedding_service.generate_embeddings(chunks_content.clone()).await?;

        let doc_chunks: Vec<DocumentChunk> = chunks_content
            .into_iter()
            .zip(embeddings)
            .map(|(content, embedding)| DocumentChunk {
                content,
                metadata: doc.metadata.clone(),
                embedding: Some(embedding),
            })
            .collect();

        self.vector_store.save_chunks(doc_chunks, collection_name).await?;

        Ok(())
    }
}
