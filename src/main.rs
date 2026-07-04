use axum::{extract::State, http::StatusCode, routing::post, Router};
use clap::Parser;
use dotenvy::dotenv;
use sqlx::PgPool;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{error, info};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use axum::response::IntoResponse;
use axum::Json;
use mnemosyne::application::use_cases::auth::AuthUseCase;
use mnemosyne::application::use_cases::indexing::IndexingUseCase;
use mnemosyne::application::use_cases::retrieval::RetrievalUseCase;
use mnemosyne::infrastructure::embedding::tei::TEIService;
use mnemosyne::infrastructure::repositories::file_scanner::LocalFileScanner;
use mnemosyne::infrastructure::repositories::postgres_account::PostgresAccountRepository;
use mnemosyne::infrastructure::repositories::qdrant::QdrantVectorStore;
use mnemosyne::interfaces::http::auth_handlers::login_handler;
use mnemosyne::interfaces::http::pipeline_handlers::{
    get_run_handler, list_runs_handler, retry_run_handler,
};
use mnemosyne::interfaces::http::query_handlers::search_handler;
use tower_http::cors::{Any, CorsLayer};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(
        long,
        env = "NFS_PATH",
        default_value = "/data/nfs",
        value_delimiter = ','
    )]
    paths: Vec<String>,

    #[arg(long, env = "DATABASE_URL")]
    database_url: String,

    #[arg(
        long,
        env = "QDRANT_URL",
        default_value = "http://qdrant.qdrant.svc.cluster.local:6333"
    )]
    qdrant_url: String,

    #[arg(long, env = "COLLECTION_NAME", default_value = "mnemosyne_docs")]
    collection_name: String,

    #[arg(long, env = "EMBEDDING_MODEL", default_value = "BAAI/bge-m3")]
    embedding_model: String,

    #[arg(
        long,
        env = "TEI_EMBEDDER_URL",
        default_value = "http://mnemosyne-tei-embedder.mnemosyne.svc.cluster.local"
    )]
    tei_embedder_url: String,

    #[arg(
        long,
        env = "TEI_RERANKER_URL",
        default_value = "http://mnemosyne-tei-reranker.mnemosyne.svc.cluster.local"
    )]
    tei_reranker_url: String,

    #[arg(long, env = "PYLOS_URL")]
    pylos_url: Option<String>,

    #[arg(long, env = "PYLOS_API_KEY")]
    pylos_api_key: Option<String>,

    #[arg(long, env = "PVC_NAME", default_value = "unknown")]
    pvc_name: String,

    #[arg(long, env = "HTTP_PORT", default_value = "8080")]
    http_port: u16,

    #[arg(long, env = "ONESHOT", default_value = "false")]
    oneshot: bool,
}

use mnemosyne::domain::ports::EmbeddingService;
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

    let pylos_url = args.pylos_url.or_else(|| std::env::var("LITELLM_URL").ok());
    let pylos_api_key = args
        .pylos_api_key
        .or_else(|| std::env::var("LITELLM_API_KEY").ok());

    info!("🧠 Mnemosyne Service starting...");
    info!("🚀 Using Qdrant at: {}", args.qdrant_url);
    if let Some(ref url) = pylos_url {
        info!("📡 Using Pylos/LiteLLM for embeddings at: {}", url);
    } else {
        info!("📡 Using TEI Embedder at: {}", args.tei_embedder_url);
    }
    info!("📡 Using TEI Reranker at: {}", args.tei_reranker_url);

    // Setup database pool for account management
    let pool = PgPool::connect(&args.database_url).await?;

    info!("⚙️ Running database migrations...");
    sqlx::migrate!("./migrations").run(&pool).await?;

    // Initialize adapters
    let file_scanner = Arc::new(LocalFileScanner::new(args.pvc_name));

    let tei_service = Arc::new(TEIService::new(
        args.tei_embedder_url,
        args.tei_reranker_url,
    ));

    let embedding_service: Arc<dyn EmbeddingService> = if let Some(url) = pylos_url {
        let api_key = pylos_api_key.unwrap_or_default();
        Arc::new(
            mnemosyne::infrastructure::embedding::litellm::LiteLLMEmbeddingService::new(
                url,
                api_key,
                args.embedding_model.clone(),
            ),
        )
    } else {
        tei_service.clone()
    };

    let vector_store = Arc::new(QdrantVectorStore::new(&args.qdrant_url).await?);
    let account_repo = Arc::new(PostgresAccountRepository::new(pool.clone()));

    // Initialize use cases
    let indexing_use_case = Arc::new(IndexingUseCase::new(
        file_scanner,
        embedding_service.clone(),
        vector_store.clone(),
        account_repo.clone(),
    ));

    let auth_use_case = Arc::new(AuthUseCase::new(account_repo.clone(), account_repo.clone()));

    let retrieval_use_case = Arc::new(RetrievalUseCase::new(
        vector_store.clone(),
        embedding_service.clone(),
        tei_service.clone(),
    ));

    let state = AppState {
        auth_use_case,
        retrieval_use_case,
        indexing_use_case: indexing_use_case.clone(),
        pipeline_repo: account_repo.clone(),
        collection_name: args.collection_name.clone(),
        db_pool: pool.clone(),
        vector_store: vector_store.clone(),
    };

    // Build Axum router
    let app = Router::new()
        .route("/health", axum::routing::get(health_handler))
        .route("/api/auth/login", post(login_handler))
        .route("/api/search", post(search_handler))
        .route("/api/pipeline/runs", axum::routing::get(list_runs_handler))
        .route(
            "/api/pipeline/runs/:id",
            axum::routing::get(get_run_handler),
        )
        .route("/api/pipeline/retry", post(retry_run_handler))
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
        .with_state(state);

    // Start HTTP server in background
    let addr = SocketAddr::from(([0, 0, 0, 0], args.http_port));
    info!("🌐 HTTP server listening on {}", addr);

    let server_handle = tokio::spawn(async move {
        match tokio::net::TcpListener::bind(&addr).await {
            Ok(listener) => {
                if let Err(e) = axum::serve(listener, app).await {
                    error!("Axum server error: {}", e);
                }
            }
            Err(e) => {
                error!("Failed to bind TCP listener to {}: {}", addr, e);
            }
        }
    });

    // Run initial indexing for each path
    for path in args.paths {
        if let Err(e) = indexing_use_case
            .execute(&path, &args.collection_name)
            .await
        {
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

async fn health_handler(State(state): State<AppState>) -> impl IntoResponse {
    let db_status = match sqlx::query("SELECT 1").execute(&state.db_pool).await {
        Ok(_) => "up",
        Err(e) => {
            error!("Database health check failed: {}", e);
            "down"
        }
    };

    let qdrant_status = match state.vector_store.health_check().await {
        Ok(_) => "up",
        Err(e) => {
            error!("Qdrant health check failed: {}", e);
            "down"
        }
    };

    let status = if db_status == "up" && qdrant_status == "up" {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (
        status,
        Json(serde_json::json!({
            "status": if status == StatusCode::OK { "ok" } else { "error" },
            "service": "mnemosyne",
            "dependencies": {
                "database": db_status,
                "qdrant": qdrant_status,
            }
        })),
    )
}
