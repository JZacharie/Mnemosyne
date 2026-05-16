use crate::domain::entities::DocumentChunk;
use crate::domain::ports::{VectorStore, EmbeddingService, RerankingService};
use anyhow::Result;
use std::sync::Arc;
use tracing::info;

pub struct RetrievalUseCase {
    vector_store: Arc<dyn VectorStore>,
    embedding_service: Arc<dyn EmbeddingService>,
    reranking_service: Arc<dyn RerankingService>,
}

impl RetrievalUseCase {
    pub fn new(
        vector_store: Arc<dyn VectorStore>,
        embedding_service: Arc<dyn EmbeddingService>,
        reranking_service: Arc<dyn RerankingService>,
    ) -> Self {
        Self {
            vector_store,
            embedding_service,
            reranking_service,
        }
    }

    pub async fn execute(&self, query: &str, collection_name: &str) -> Result<Vec<DocumentChunk>> {
        info!("Retrieving documents for query: {}", query);

        // 1. Generate query embedding
        let query_embeddings = self.embedding_service.generate_embeddings(vec![query.to_string()]).await?;
        let query_vector = query_embeddings.into_iter().next().ok_or_else(|| anyhow::anyhow!("No embedding generated"))?;

        // 2. Search vector store (retrieve top 50 as recommended)
        let initial_results = self.vector_store.search(query_vector, 50, collection_name).await?;

        if initial_results.is_empty() {
            return Ok(vec![]);
        }

        // 3. Rerank results (keep top 5)
        let reranked_results = self.reranking_service.rerank(query, initial_results, 5).await?;

        Ok(reranked_results)
    }
}
