use crate::domain::entities::DocumentChunk;
use crate::domain::ports::{EmbeddingService, RerankingService, VectorStore};
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
        let query_embeddings = self
            .embedding_service
            .generate_embeddings(vec![query.to_string()])
            .await?;
        let query_vector = query_embeddings
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("No embedding generated"))?;

        // 2. Search vector store (Hybrid Search: Vector + Full-Text)
        let initial_results = self
            .vector_store
            .search(query, query_vector, 50, collection_name)
            .await?;

        if initial_results.is_empty() {
            return Ok(vec![]);
        }

        // 3. Rerank results (keep top 5)
        let reranked_results = self
            .reranking_service
            .rerank(query, initial_results, 5)
            .await?;

        Ok(reranked_results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::entities::{DocumentChunk, DocumentMetadata};
    use crate::domain::ports::{EmbeddingService, RerankingService, VectorStore};
    use anyhow::Result;
    use async_trait::async_trait;
    use mockall::mock;

    mock! {
        pub VectorStoreImpl {}
        #[async_trait]
        impl VectorStore for VectorStoreImpl {
            async fn save_chunks(&self, chunks: Vec<DocumentChunk>, collection_name: &str) -> Result<()>;
            async fn search(&self, query_text: &str, query_vector: Vec<f32>, limit: usize, collection_name: &str) -> Result<Vec<DocumentChunk>>;
            async fn health_check(&self) -> Result<()>;
            async fn get_collection_info(&self, collection_name: &str) -> Result<serde_json::Value>;
        }
    }

    mock! {
        pub EmbeddingServiceImpl {}
        #[async_trait]
        impl EmbeddingService for EmbeddingServiceImpl {
            async fn generate_embeddings(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>>;
        }
    }

    mock! {
        pub RerankingServiceImpl {}
        #[async_trait]
        impl RerankingService for RerankingServiceImpl {
            async fn rerank(&self, query: &str, documents: Vec<DocumentChunk>, top_n: usize) -> Result<Vec<DocumentChunk>>;
        }
    }

    fn make_doc(content: &str, score: f32) -> DocumentChunk {
        DocumentChunk {
            content: content.to_string(),
            metadata: DocumentMetadata {
                source_path: format!("path/{}", content),
                file_name: content.to_string(),
                pvc_name: "pvc".to_string(),
                file_size: 100,
                last_modified: 0,
                creation_date: 0,
                file_hash: "hash".to_string(),
                folder_tags: vec![],
                inferred_tags: None,
                document_summary: None,
                detected_entities: None,
            },
            embedding: None,
            score: Some(score),
        }
    }

    #[tokio::test]
    async fn test_retrieval_pipeline() {
        let mut mock_vs = MockVectorStoreImpl::new();
        let mut mock_es = MockEmbeddingServiceImpl::new();
        let mut mock_rs = MockRerankingServiceImpl::new();

        mock_es
            .expect_generate_embeddings()
            .returning(|_| Ok(vec![vec![0.1, 0.2]]));

        mock_vs
            .expect_search()
            .returning(|_, _, _, _| Ok(vec![make_doc("doc1", 0.8)]));

        mock_rs.expect_rerank().returning(|_, docs, _| Ok(docs));

        let use_case =
            RetrievalUseCase::new(Arc::new(mock_vs), Arc::new(mock_es), Arc::new(mock_rs));

        let results = use_case.execute("query", "col").await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].content, "doc1");
    }

    #[tokio::test]
    async fn test_retrieval_empty_results() {
        let mut mock_vs = MockVectorStoreImpl::new();
        let mut mock_es = MockEmbeddingServiceImpl::new();
        let mut mock_rs = MockRerankingServiceImpl::new();

        mock_es
            .expect_generate_embeddings()
            .returning(|_| Ok(vec![vec![0.1, 0.2]]));

        mock_vs.expect_search().returning(|_, _, _, _| Ok(vec![]));

        // rerank should NOT be called when vector store returns empty
        mock_rs.expect_rerank().never();

        let use_case =
            RetrievalUseCase::new(Arc::new(mock_vs), Arc::new(mock_es), Arc::new(mock_rs));

        let results = use_case.execute("query", "col").await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_retrieval_embedding_failure() {
        let mut mock_es = MockEmbeddingServiceImpl::new();
        mock_es
            .expect_generate_embeddings()
            .returning(|_| Err(anyhow::anyhow!("Embedding service unavailable")));

        let use_case = RetrievalUseCase::new(
            Arc::new(MockVectorStoreImpl::new()),
            Arc::new(mock_es),
            Arc::new(MockRerankingServiceImpl::new()),
        );

        let result = use_case.execute("query", "col").await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Embedding service unavailable"));
    }

    #[tokio::test]
    async fn test_retrieval_no_embedding_generated() {
        let mut mock_es = MockEmbeddingServiceImpl::new();
        mock_es
            .expect_generate_embeddings()
            .returning(|_| Ok(vec![]));

        let use_case = RetrievalUseCase::new(
            Arc::new(MockVectorStoreImpl::new()),
            Arc::new(mock_es),
            Arc::new(MockRerankingServiceImpl::new()),
        );

        let result = use_case.execute("query", "col").await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("No embedding generated"));
    }

    #[tokio::test]
    async fn test_retrieval_rerank_failure() {
        let mut mock_vs = MockVectorStoreImpl::new();
        let mut mock_es = MockEmbeddingServiceImpl::new();
        let mut mock_rs = MockRerankingServiceImpl::new();

        mock_es
            .expect_generate_embeddings()
            .returning(|_| Ok(vec![vec![0.1, 0.2]]));

        mock_vs
            .expect_search()
            .returning(|_, _, _, _| Ok(vec![make_doc("doc1", 0.8)]));

        mock_rs
            .expect_rerank()
            .returning(|_, _, _| Err(anyhow::anyhow!("Reranker unavailable")));

        let use_case =
            RetrievalUseCase::new(Arc::new(mock_vs), Arc::new(mock_es), Arc::new(mock_rs));

        let result = use_case.execute("query", "col").await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Reranker unavailable"));
    }

    #[tokio::test]
    async fn test_retrieval_multiple_results_reranked() {
        let mut mock_vs = MockVectorStoreImpl::new();
        let mut mock_es = MockEmbeddingServiceImpl::new();
        let mut mock_rs = MockRerankingServiceImpl::new();

        mock_es
            .expect_generate_embeddings()
            .returning(|_| Ok(vec![vec![0.1, 0.2]]));

        mock_vs.expect_search().returning(|_, _, _, _| {
            Ok(vec![
                make_doc("a", 0.5),
                make_doc("b", 0.7),
                make_doc("c", 0.3),
            ])
        });

        mock_rs
            .expect_rerank()
            .withf(|_, docs, top_n| docs.len() == 3 && *top_n == 5)
            .returning(|_, mut docs, _| {
                docs.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
                Ok(docs)
            });

        let use_case =
            RetrievalUseCase::new(Arc::new(mock_vs), Arc::new(mock_es), Arc::new(mock_rs));

        let results = use_case.execute("query", "col").await.unwrap();
        assert_eq!(results.len(), 3);
    }
}
