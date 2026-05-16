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

use mnemosyne::infrastructure::repositories::file_scanner::LocalFileScanner;
use mnemosyne::infrastructure::repositories::qdrant::QdrantVectorStore;
use mnemosyne::infrastructure::repositories::postgres_account::PostgresAccountRepository;
use mnemosyne::infrastructure::embedding::tei::TEIService;
use mnemosyne::application::use_cases::indexing::IndexingUseCase;
use mnemosyne::application::use_cases::auth::AuthUseCase;
use mnemosyne::application::use_cases::retrieval::RetrievalUseCase;
use mnemosyne::interfaces::http::auth_handlers::login_handler;
use mnemosyne::interfaces::http::query_handlers::search_handler;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, env = "NFS_PATH", default_value = "/data/nfs", value_delimiter = ',')]
    paths: Vec<String>,

    #[arg(long, env = "DATABASE_URL")]
    database_url: String,

    #[arg(long, env = "QDRANT_URL", default_value = "http://qdrant.qdrant.svc.cluster.local:6333")]
    qdrant_url: String,

    #[arg(long, env = "COLLECTION_NAME", default_value = "mnemosyne_docs")]
    collection_name: String,

    #[arg(long, env = "EMBEDDING_MODEL", default_value = "BAAI/bge-m3")]
    embedding_model: String,

    #[arg(long, env = "TEI_EMBEDDER_URL", default_value = "http://mnemosyne-tei-embedder.mnemosyne.svc.cluster.local")]
    tei_embedder_url: String,

    #[arg(long, env = "TEI_RERANKER_URL", default_value = "http://mnemosyne-tei-reranker.mnemosyne.svc.cluster.local")]
    tei_reranker_url: String,

    #[arg(long, env = "PVC_NAME", default_value = "unknown")]
    pvc_name: String,

    #[arg(long, env = "HTTP_PORT", default_value = "8080")]
    http_port: u16,

    #[arg(long, env = "ONESHOT", default_value = "false")]
    oneshot: bool,
}

use mnemosyne::AppState;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load .env file if it exists
    dotenv().ok();

    // Setup logging
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,lopdf=error,pdf_extract=error"));

    tracing_subscriber::registry()
        .with(fmt::layer().with_writer(std::io::stderr))
        .with(filter)
        .init();

    let args = Args::parse();

    info!("🧠 Mnemosyne Service starting...");
    info!("🚀 Using Qdrant at: {}", args.qdrant_url);
    info!("📡 Using TEI Embedder at: {}", args.tei_embedder_url);
    info!("📡 Using TEI Reranker at: {}", args.tei_reranker_url);

    // Setup database pool for account management
    let pool = PgPool::connect(&args.database_url).await?;

    // Initialize adapters
    let file_scanner = Arc::new(LocalFileScanner::new(args.pvc_name));
    
    let tei_service = Arc::new(TEIService::new(
        args.tei_embedder_url,
        args.tei_reranker_url,
    ));

    let vector_store = Arc::new(QdrantVectorStore::new(&args.qdrant_url).await?);
    let account_repo = Arc::new(PostgresAccountRepository::new(pool.clone()));

    // Initialize use cases
    let indexing_use_case = Arc::new(IndexingUseCase::new(
        file_scanner,
        tei_service.clone(),
        vector_store.clone(),
    ));
    
    let auth_use_case = Arc::new(AuthUseCase::new(
        account_repo.clone(),
        account_repo.clone(),
    ));

    let retrieval_use_case = Arc::new(RetrievalUseCase::new(
        vector_store.clone(),
        tei_service.clone(),
        tei_service.clone(),
    ));

    let state = AppState {
        auth_use_case,
        retrieval_use_case,
        collection_name: args.collection_name.clone(),
    };

    // Build Axum router
    let app = Router::new()
        .route("/api/auth/login", post(login_handler))
        .route("/api/search", post(search_handler))
        .with_state(state);

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
    
    if args.oneshot {
        info!("🛑 Oneshot mode enabled, exiting...");
        return Ok(());
    }

    // Keep the service running
    server_handle.await?;

    Ok(())
}
