use crate::domain::entities::DocumentChunk;
use crate::domain::ports::{EmbeddingService, RerankingService};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{debug, error};

pub struct TEIService {
    client: Client,
    embedder_url: String,
    reranker_url: String,
}

impl TEIService {
    pub fn new(embedder_url: String, reranker_url: String) -> Self {
        let client = Client::builder()
            .http1_only()
            .timeout(std::time::Duration::from_secs(60))
            .build()
            .expect("Failed to build TEI HTTP client");
        Self {
            client,
            embedder_url,
            reranker_url,
        }
    }
}

#[derive(Serialize)]
struct TEIEmbeddingRequest {
    inputs: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    truncate: Option<bool>,
}

#[derive(Serialize)]
struct TEIRerankRequest {
    query: String,
    texts: Vec<String>,
}

#[derive(Deserialize)]
struct TEIRerankResult {
    index: usize,
    score: f32,
}

#[async_trait]
impl EmbeddingService for TEIService {
    async fn generate_embeddings(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>> {
        debug!("TEI: Generating embeddings for {} texts", texts.len());

        let response = self
            .client
            .post(format!("{}/embed", self.embedder_url))
            .json(&TEIEmbeddingRequest {
                inputs: texts,
                truncate: Some(true),
            })
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            error!("TEI Embedder error: {}", error_text);
            return Err(anyhow!("TEI Embedder error: {}", error_text));
        }

        let embeddings: Vec<Vec<f32>> = response.json().await?;
        Ok(embeddings)
    }
}

#[async_trait]
impl RerankingService for TEIService {
    async fn rerank(
        &self,
        query: &str,
        mut documents: Vec<DocumentChunk>,
        top_n: usize,
    ) -> Result<Vec<DocumentChunk>> {
        if documents.is_empty() {
            return Ok(documents);
        }

        if self.reranker_url.is_empty() {
            debug!("TEI: Reranker URL is empty, bypassing reranking step");
            documents.truncate(top_n);
            return Ok(documents);
        }

        debug!("TEI: Reranking {} documents for query", documents.len());

        let texts: Vec<String> = documents.iter().map(|d| d.content.clone()).collect();

        let response = self
            .client
            .post(format!("{}/rerank", self.reranker_url))
            .json(&TEIRerankRequest {
                query: query.to_string(),
                texts,
            })
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            error!("TEI Reranker error: {}", error_text);
            return Err(anyhow!("TEI Reranker error: {}", error_text));
        }

        let results: Vec<TEIRerankResult> = response.json().await?;

        // Update scores and sort
        for res in &results {
            if let Some(doc) = documents.get_mut(res.index) {
                doc.score = Some(res.score);
            }
        }

        // Sort by score descending
        documents.sort_by(|a, b| {
            b.score
                .unwrap_or(0.0)
                .partial_cmp(&a.score.unwrap_or(0.0))
                .unwrap()
        });

        // Keep top_n
        documents.truncate(top_n);

        Ok(documents)
    }
}
