use std::sync::Arc;
use clap::Parser;
use dotenvy::dotenv;
use sqlx::PgPool;
use tracing::{info, error};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};
use axum::{
    routing::post,
    Router,
};
use std::net::SocketAddr;

mod domain;
mod application;
mod infrastructure;
mod interfaces;

use crate::infrastructure::repositories::file_scanner::LocalFileScanner;
use crate::infrastructure::repositories::postgres_vector::PostgresVectorStore;
use crate::infrastructure::repositories::postgres_account::PostgresAccountRepository;
use crate::infrastructure::embedding::litellm::LiteLLMEmbeddingService;
use crate::application::use_cases::indexing::IndexingUseCase;
use crate::application::use_cases::auth::AuthUseCase;
use crate::interfaces::http::auth_handlers::login_handler;

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

    #[arg(long, env = "HTTP_PORT", default_value = "8080")]
    http_port: u16,
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

    info!("🧠 Mnemosyne Service starting...");

    // Setup database pool
    let pool = PgPool::connect(&args.database_url).await?;

    // Run migrations (in a real app, use sqlx-cli or sqlx::migrate!)
    // sqlx::migrate!("./migrations").run(&pool).await?;

    // Initialize adapters
    let file_scanner = Arc::new(LocalFileScanner::new(args.pvc_name));
    let embedding_service = Arc::new(LiteLLMEmbeddingService::new(
        args.litellm_url,
        args.litellm_api_key,
        args.embedding_model,
    ));
    let vector_store = Arc::new(PostgresVectorStore::new(pool.clone()));
    let account_repo = Arc::new(PostgresAccountRepository::new(pool.clone()));

    // Initialize use cases
    let indexing_use_case = Arc::new(IndexingUseCase::new(
        file_scanner,
        embedding_service,
        vector_store,
    ));
    let auth_use_case = Arc::new(AuthUseCase::new(
        account_repo.clone(),
        account_repo.clone(),
    ));

    // Build Axum router
    let app = Router::new()
        .route("/api/auth/login", post(login_handler))
        .with_state(auth_use_case);

    // Start HTTP server in background
    let addr = SocketAddr::from(([0, 0, 0, 0], args.http_port));
    info!("🌐 HTTP server listening on {}", addr);
    
    let server_handle = tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
        axum::serve(listener, app).await.unwrap();
    });

    // Run initial indexing for each path
    for path in args.paths {
        if let Err(e) = indexing_use_case.execute(&path, &args.collection_name).await {
            error!("Error indexing path {}: {}", path, e);
        }
    }

    info!("✨ Initial indexing job complete!");
    
    // Keep the service running
    server_handle.await?;

    Ok(())
}
