use std::sync::Arc;
use clap::Parser;
use dotenvy::dotenv;
use sqlx::PgPool;
use tracing::{info, error};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

mod domain;
mod application;
mod infrastructure;

use crate::infrastructure::repositories::file_scanner::LocalFileScanner;
use crate::infrastructure::repositories::postgres_vector::PostgresVectorStore;
use crate::infrastructure::embedding::litellm::LiteLLMEmbeddingService;
use crate::application::use_cases::indexing::IndexingUseCase;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, env = "NFS_PATH", default_value = "/data/nfs")]
    paths: Vec<String>,

    #[arg(long, env = "DATABASE_URL")]
    database_url: String,

    #[arg(long, env = "COLLECTION_NAME", default_value = "mnemosyne_docs")]
    collection_name: String,

    #[arg(long, env = "EMBEDDING_MODEL", default_value = "zembed-132k")]
    embedding_model: String,

    #[arg(long, env = "LITELLM_URL", default_value = "http://litellm.litellm.svc.cluster.local:4000")]
    litellm_url: String,

    #[arg(long, env = "LITELLM_API_KEY")]
    litellm_api_key: String,

    #[arg(long, env = "PVC_NAME", default_value = "unknown")]
    pvc_name: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load .env file if it exists
    dotenv().ok();

    // Setup logging
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let args = Args::parse();

    info!("🧠 Mnemosyne Indexer starting...");

    // Setup database pool
    let pool = PgPool::connect(&args.database_url).await?;

    // Initialize adapters
    let file_scanner = Arc::new(LocalFileScanner::new(args.pvc_name));
    let embedding_service = Arc::new(LiteLLMEmbeddingService::new(
        args.litellm_url,
        args.litellm_api_key,
        args.embedding_model,
    ));
    let vector_store = Arc::new(PostgresVectorStore::new(pool));

    // Initialize use case
    let indexing_use_case = IndexingUseCase::new(
        file_scanner,
        embedding_service,
        vector_store,
    );

    // Run indexing for each path
    for path in args.paths {
        if let Err(e) = indexing_use_case.execute(&path, &args.collection_name).await {
            error!("Error indexing path {}: {}", path, e);
        }
    }

    info!("✨ Indexing job complete!");

    Ok(())
}
