use crate::domain::ports::{EmbeddingService, LLMService};
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

pub struct LiteLLMTextService {
    client: Client,
    api_url: String,
    api_key: String,
    model: String,
}

impl LiteLLMTextService {
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
struct ChatCompletionRequest {
    model: String,
    messages: Vec<ChatMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
}

#[derive(Serialize, Clone)]
struct ChatMessage {
    role: String,
    content: String,
}

#[derive(Deserialize)]
struct ChatCompletionResponse {
    choices: Vec<ChatChoice>,
}

#[derive(Deserialize)]
struct ChatChoice {
    message: ChatResponseMessage,
}

#[derive(Deserialize)]
struct ChatResponseMessage {
    content: String,
}

#[async_trait]
impl LLMService for LiteLLMTextService {
    async fn generate_text(&self, system_prompt: &str, user_prompt: &str) -> Result<String> {
        let url = if self.api_url.ends_with("/v1") {
            format!("{}/chat/completions", self.api_url)
        } else if self.api_url.ends_with("/v1/") {
            format!("{}chat/completions", self.api_url)
        } else {
            format!("{}/v1/chat/completions", self.api_url.trim_end_matches('/'))
        };

        let response = self
            .client
            .post(url)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .json(&ChatCompletionRequest {
                model: self.model.clone(),
                messages: vec![
                    ChatMessage {
                        role: "system".to_string(),
                        content: system_prompt.to_string(),
                    },
                    ChatMessage {
                        role: "user".to_string(),
                        content: user_prompt.to_string(),
                    },
                ],
                temperature: Some(0.2),
            })
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(anyhow!("LiteLLM text generation error: {}", error_text));
        }

        let res: ChatCompletionResponse = response.json().await?;
        let text = res
            .choices
            .into_iter()
            .next()
            .map(|c| c.message.content)
            .ok_or_else(|| anyhow!("No choices returned from LLM"))?;

        Ok(text)
    }
}
