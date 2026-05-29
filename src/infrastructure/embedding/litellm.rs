use crate::domain::ports::EmbeddingService;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::debug;

pub struct LiteLLMEmbeddingService {
    client: Client,
    api_url: String,
    api_key: String,
    model: String,
}

impl LiteLLMEmbeddingService {
    pub fn new(api_url: String, api_key: String, model: String) -> Self {
        Self {
            client: Client::new(),
            api_url,
            api_key,
            model,
        }
    }
}

#[derive(Serialize)]
struct EmbeddingRequest {
    model: String,
    input: Vec<String>,
}

#[derive(Deserialize)]
struct EmbeddingResponse {
    data: Vec<EmbeddingData>,
}

#[derive(Deserialize)]
struct EmbeddingData {
    embedding: Vec<f32>,
}

#[async_trait]
impl EmbeddingService for LiteLLMEmbeddingService {
    async fn generate_embeddings(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>> {
        debug!("Generating embeddings for {} texts", texts.len());

        let url = if self.api_url.ends_with("/v1") {
            format!("{}/embeddings", self.api_url)
        } else if self.api_url.ends_with("/v1/") {
            format!("{}embeddings", self.api_url)
        } else {
            format!("{}/v1/embeddings", self.api_url.trim_end_matches('/'))
        };

        let response = self
            .client
            .post(url)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .json(&EmbeddingRequest {
                model: self.model.clone(),
                input: texts,
            })
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(anyhow!("LiteLLM error: {}", error_text));
        }

        let embedding_res: EmbeddingResponse = response.json().await?;
        Ok(embedding_res
            .data
            .into_iter()
            .map(|d| d.embedding)
            .collect())
    }
}
