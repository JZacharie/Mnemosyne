# Mnemosyne — Documentation Complète

> Mnemosyne est un moteur d'indexation **RAG** (Retrieval-Augmented Generation) et de **recherche hybride** haute performance, écrit en Rust. Il indexe des documents (PDF, Markdown, TXT, Log) dans une base vectorielle Qdrant et expose une API REST pour la recherche sémantique avec reranking.

---

## Table des matières

1. [Présentation](#1-présentation)
2. [Architecture](#2-architecture)
3. [Modules et Composants](#3-modules-et-composants)
4. [Pipeline d'indexation](#4-pipeline-dindexation)
5. [Pipeline de recherche](#5-pipeline-de-recherche)
6. [Configuration](#6-configuration)
7. [API REST](#7-api-rest)
8. [Base de données](#8-base-de-données)
9. [Interface Utilisateur](#9-interface-utilisateur)
10. [Déploiement](#10-déploiement)
11. [Développement](#11-développement)
12. [Tests](#12-tests)
13. [CI/CD](#13-cicd)
14. [Sécurité](#14-sécurité)
15. [Déploiement Legacy Python](#15-déploiement-legacy-python)
16. [Changelog](#16-changelog)
17. [Glossaire](#17-glossaire)

---

## 1. Présentation

**Mnemosyne** (déesse grecque de la mémoire) est un service d'indexation et de recherche RAG conçu pour les lacs de données à grande échelle. Il transforme des documents bruts en chunks vectorisés, les stocke dans **Qdrant**, et permet une **recherche hybride** (vectorielle + texte intégral) avec **reranking** via TEI (Text Embeddings Inference).

### Objectifs

- Indexer des millions de documents avec un débit élevé (concurrence configurable)
- Fournir une recherche sémantique contextuelle avec des résultats pertinents
- S'intégrer dans une infrastructure Kubernetes avec GitOps (Helm, ArgoCD)
- Assurer l'observabilité via des logs structurés et une API de monitoring

---

## 2. Architecture

### Architecture Hexagonale (Ports & Adapters)

```
┌─────────────────────────────────────────────────────────┐
│                    INTERFACES                           │
│  ┌────────────────────────────────────────────────────┐ │
│  │                   HTTP (Axum)                      │ │
│  │  ┌──────────┐ ┌──────────┐ ┌───────────────────┐  │ │
│  │  │ Auth     │ │ Search   │ │ Pipeline Monitor  │  │ │
│  │  │ Handlers │ │ Handlers │ │ Handlers          │  │ │
│  │  └────┬─────┘ └────┬─────┘ └────────┬──────────┘  │ │
│  └───────┼─────────────┼────────────────┼──────────────┘ │
└──────────┼─────────────┼────────────────┼────────────────┘
           │             │                │
┌──────────┼─────────────┼────────────────┼────────────────┐
│          ▼             ▼                ▼                │
│                 APPLICATION (Use Cases)                  │
│  ┌────────────────┐ ┌──────────────┐ ┌───────────────┐  │
│  │ Authentication │ │  Indexing    │ │  Retrieval    │  │
│  │ Use Case       │ │  Use Case    │ │  Use Case     │  │
│  └───────┬────────┘ └──────┬───────┘ └───────┬───────┘  │
└──────────┼─────────────────┼─────────────────┼──────────┘
           │                 │                 │
┌──────────┼─────────────────┼─────────────────┼──────────┐
│          ▼                 ▼                 ▼          │
│                   DOMAIN (Ports)                        │
│  ┌──────────┐ ┌────────────┐ ┌─────────────┐           │
│  │ Vector   │ │ Embedding  │ │ Reranking   │           │
│  │ Store    │ │ Service    │ │ Service     │           │
│  └────┬─────┘ └─────┬──────┘ └──────┬──────┘           │
│  ┌────┴────┐ ┌──────┴───────┐       │                  │
│  │ File    │ │ User/Audit/ │       │                  │
│  │ Scanner │ │ Pipeline    │       │                  │
│  └─────────┘ │ Repository  │       │                  │
│              └─────────────┘       │                  │
└────────────────────────────────────┼──────────────────┘
                                     │
┌────────────────────────────────────┼──────────────────┐
│           INFRASTRUCTURE (Adapters)▼                  │
│  ┌──────────┐ ┌──────────────┐ ┌──────────────┐      │
│  │ Qdrant   │ │ TEI Service  │ │ LiteLLM      │      │
│  │ Vector   │ │ (Embed +     │ │ Embedding    │      │
│  │ Store    │ │  Rerank)     │ │ Service      │      │
│  └──────────┘ └──────────────┘ └──────────────┘      │
│  ┌──────────┐ ┌────────────────────────────────┐      │
│  │ Local    │ │ PostgresAccountRepository      │      │
│  │ File     │ │ (User, Audit, Pipeline)        │      │
│  │ Scanner  │ └────────────────────────────────┘      │
│  └──────────┘                                         │
│  ┌────────────────────────────────────────────┐       │
│  │ PostgresVectorStore (Legacy, pgvector)     │       │
│  └────────────────────────────────────────────┘       │
└───────────────────────────────────────────────────────┘
```

### Flux de démarrage

```
main()
  │
  ├─ dotenv() ──────────────── Chargement .env
  ├─ tracing_subscriber ─────── Logs structurés
  ├─ PgPool::connect ────────── Connexion PostgreSQL
  ├─ sqlx::migrate ──────────── Migrations DB
  │
  ├─ Initialisation adapters
  │   ├─ LocalFileScanner
  │   ├─ TEIService (embed + rerank)
  │   ├─ LiteLLMEmbeddingService (optionnel)
  │   ├─ QdrantVectorStore
  │   └─ PostgresAccountRepository
  │
  ├─ Initialisation use cases
  │   ├─ IndexingUseCase
  │   ├─ AuthUseCase
  │   └─ RetrievalUseCase
  │
  ├─ AppState ───────────────── État partagé
  ├─ Axum Router ─────────────── Routes REST
  ├─ tokio::spawn HTTP server ── Serveur asynchrone
  │
  └─ Indexation initiale
      └─ IndexingUseCase::execute() pour chaque path
```

---

## 3. Modules et Composants

### 3.1 `src/domain/` — Cœur métier

| Fichier | Description |
|---------|-------------|
| `entities.rs` | Entités du domaine : `Document`, `DocumentMetadata`, `DocumentChunk`, `User`, `AuditLog`, `Session`, `PipelineRun` |
| `ports.rs` | Traits (interfaces) : `VectorStore`, `EmbeddingService`, `RerankingService`, `FileScanner`, `UserRepository`, `AuditRepository`, `PipelineRepository` |

### 3.2 `src/application/use_cases/` — Cas d'usage

| Fichier | Description |
|---------|-------------|
| `indexing.rs` | Orchestration du pipeline d'indexation (scan → parse → chunk → embed → store) |
| `retrieval.rs` | Recherche hybride : embed → vector search (top 50) → rerank (top 5) |
| `auth.rs` | Authentification utilisateur avec audit logging |

### 3.3 `src/infrastructure/` — Adaptateurs

| Fichier | Description |
|---------|-------------|
| `repositories/qdrant.rs` | Implémentation `VectorStore` pour Qdrant (collection, upsert, search, health, infos) |
| `repositories/file_scanner.rs` | Scan disque local via `walkdir`, extraction PDF via `pdf_oxide`, hash SHA256 |
| `repositories/postgres_account.rs` | Implémente `UserRepository`, `AuditRepository`, `PipelineRepository` via sqlx |
| `repositories/postgres_vector.rs` | Legacy `VectorStore` via pgvector (en migration vers Qdrant) |
| `embedding/tei.rs` | Services d'embedding et reranking via TEI (HuggingFace) |
| `embedding/litellm.rs` | Service d'embedding via LiteLLM (compatible OpenAI) |

### 3.4 `src/interfaces/http/` — API REST

| Fichier | Description |
|---------|-------------|
| `auth_handlers.rs` | `POST /api/auth/login` |
| `query_handlers.rs` | `POST /api/search` |
| `pipeline_handlers.rs` | `GET /api/pipeline/runs`, `GET .../:id`, `POST .../retry`, `GET /api/indexing/stats` |

---

## 4. Pipeline d'indexation

```
                     IndexingUseCase::execute(path)
                               │
                               ▼
                    scan_directory(path)
                    ┌──────────────────────┐
                    │  walkdir récursif    │
                    │  Extensions: pdf,    │
                    │  md, txt, log        │
                    └──────────┬───────────┘
                               │ Vec<file_paths>
                               ▼
              futures::stream::iter(file_paths)
              .buffer_unordered(8)     ← Concurrence = 8
                               │
              ┌────────────────┼────────────────┐
              ▼                ▼                ▼
         process_file()   process_file()   process_file()
              │                │                │
              ▼                ▼                ▼
         PipelineRun CREATE / UPDATE (PostgreSQL)
         status: "IN_PROGRESS"
              │
              ▼
    ┌─────────────────────────────────────────┐
    │         process_file_internal()         │
    │                                         │
    │  Étape 1: PARSING                       │
    │  ┌─────────────────────────────────┐    │
    │  │ load_document(file_path)       │    │
    │  │ - SHA256 hash                  │    │
    │  │ - PDF: pdf_oxide (30s timeout) │    │
    │  │ - Autres: UTF-8 read          │    │
    │  │ - Métadonnées: size, dates,   │    │
    │  │   folder_tags, pvc_name      │    │
    │  │ - OCR status: SUCCESS/FAILED  │    │
    │  └──────────────┬──────────────────┘    │
    │                 ▼                       │
    │  Étape 2: CHUNKING                      │
    │  ┌─────────────────────────────────┐    │
    │  │ split_text(content, chunk_size, │    │
    │  │              chunk_overlap)    │    │
    │  │ Séparateurs: \n\n → \n → " " → "" │  │
    │  │ Défaut: chunk_size=1000,       │    │
    │  │         chunk_overlap=0        │    │
    │  └──────────────┬──────────────────┘    │
    │                 ▼                       │
    │  Étape 3: EMBEDDING                     │
    │  ┌─────────────────────────────────┐    │
    │  │ Par batch de 32 chunks         │    │
    │  │ EmbeddingService               │    │
    │  │  → TEI (POST /embed)           │    │
    │  │  → ou LiteLLM (POST /v1/emb.) │    │
    │  └──────────────┬──────────────────┘    │
    │                 ▼                       │
    │  Étape 4: STORING                       │
    │  ┌─────────────────────────────────┐    │
    │  │ VectorStore::save_chunks()     │    │
    │  │ → Qdrant upsert_points          │    │
    │  │ Payload: content, source_path, │    │
    │  │ file_name, pvc_name, file_size, │    │
    │  │ dates, file_hash, folder_tags  │    │
    │  └─────────────────────────────────┘    │
    └─────────────────────────────────────────┘
              │
              ▼
         PipelineRun UPDATE
         status: "COMPLETED" ou "FAILED"
```

### Détail du Text Splitter (`split_text`)

```
Fonction split_recursive(text, séparateurs, chunk_size, chunk_overlap)

  1. Si text ≤ chunk_size → ajouter aux chunks, return
  2. Sinon:
     a. Prendre le séparateur courant (hiérarchie: \n\n → \n → " " → "")
     b. Fractionner le texte par ce séparateur
     c. Grouper les fragments jusqu'à atteindre chunk_size
     d. Quand un fragment dépasse:
        - Sauvegarder le groupe actuel comme chunk
        - Calculer l'overlap en prenant les derniers fragments
        - Continuer avec le fragment restant
     e. Si un fragment seul dépasse chunk_size:
        - Récursion avec le séparateur suivant
  3. Ajouter le dernier groupe s'il n'est pas vide
```

---

## 5. Pipeline de recherche

```
         POST /api/search { "query": "..." }
                    │
                    ▼
          RetrievalUseCase::execute(query, collection)
                    │
                    ▼
     Étape 1: EMBEDDING DE LA REQUÊTE
     ┌─────────────────────────────────┐
     │ generate_embeddings([query])   │
     │ → TEI / LiteLLM                 │
     └──────────────┬──────────────────┘
                    ▼
     Étape 2: RECHERCHE VECTORIELLE
     ┌─────────────────────────────────┐
     │ VectorStore::search()          │
     │ → Qdrant query_points          │
     │ → Limite: 50 résultats         │
     │ → Similarité cosinus           │
     └──────────────┬──────────────────┘
                    ▼
     Étape 3: RERANKING
     ┌─────────────────────────────────┐
     │ RerankingService::rerank()     │
     │ → TEI (POST /rerank)           │
     │ → Tri par score descendant     │
     │ → Top 5 résultats              │
     └──────────────┬──────────────────┘
                    ▼
          Réponse: Vec<DocumentChunk>
```

---

## 6. Configuration

Toutes les variables peuvent être définies via variables d'environnement ou arguments CLI (clap).

| Variable | CLI | Défaut | Description |
|----------|-----|--------|-------------|
| `DATABASE_URL` | `--database-url` | **Requis** | Connexion PostgreSQL |
| `QDRANT_URL` | `--qdrant-url` | `http://qdrant.qdrant.svc.cluster.local:6333` | URL du cluster Qdrant |
| `NFS_PATH` | `--paths` | `/data/nfs` | Chemins à indexer (séparés par des virgules) |
| `COLLECTION_NAME` | `--collection-name` | `mnemosyne_docs` | Nom de la collection Qdrant |
| `EMBEDDING_MODEL` | `--embedding-model` | `BAAI/bge-m3` | Modèle d'embedding (métadonnée) |
| `TEI_EMBEDDER_URL` | `--tei-embedder-url` | Service K8s TEI embedder | URL du service TEI embedding |
| `TEI_RERANKER_URL` | `--tei-reranker-url` | Service K8s TEI reranker | URL du service TEI reranking |
| `PYLOS_URL` / `LITELLM_URL` | `--pylos-url` | `None` | URL alternative LiteLLM |
| `PYLOS_API_KEY` / `LITELLM_API_KEY` | `--pylos-api-key` | `None` | Clé API LiteLLM |
| `PVC_NAME` | `--pvc-name` | `unknown` | Identifiant du volume PVC |
| `HTTP_PORT` | `--http-port` | `8080` | Port du serveur HTTP |
| `ONESHOT` | `--oneshot` | `false` | Sortir après indexation (mode CronJob) |
| `RUST_LOG` | — | `info` | Niveau de log (tracing) |

---

## 7. API REST

### 7.1 Health Check

```
GET /health
```

Réponse :
```json
{
  "status": "ok",
  "service": "mnemosyne",
  "dependencies": {
    "database": "up",
    "qdrant": "up"
  }
}
```

### 7.2 Authentification

```
POST /api/auth/login
Content-Type: application/json

{
  "username": "admin",
  "password": "password"
}
```

Réponse (200) :
```json
{
  "token": "dummy-jwt-token",
  "username": "admin"
}
```

Réponse (401) :
```json
{
  "error": "Invalid password"
}
```

### 7.3 Recherche

```
POST /api/search
Content-Type: application/json

{
  "query": "Qu'est-ce que la mémoire ?"
}
```

Réponse (200) :
```json
{
  "results": [
    {
      "content": "Le chunk de document pertinent...",
      "source": "document.pdf",
      "score": 0.95
    }
  ]
}
```

### 7.4 Pipeline Runs (Monitoring)

```
GET /api/pipeline/runs
```

Réponse : tableau de `PipelineRun` trié par `started_at` DESC.

```
GET /api/pipeline/runs/:id
```

Réponse : `PipelineRun` unique.

### 7.5 Statistiques d'Indexation

```
GET /api/indexing/stats
```

Réponse (200) :
```json
{
  "indexing": {
    "total_files": 150,
    "completed_files": 142,
    "failed_files": 5,
    "in_progress_files": 3,
    "total_chunks": 2840,
    "total_file_size_bytes": 524288000
  },
  "vector_database": {
    "exists": true,
    "collection_name": "mnemosyne_docs",
    "points_count": 2840,
    "indexed_vectors_count": 2840,
    "segments_count": 3,
    "status": "Green"
  }
}
```

### 7.6 Ré-indexation / Correction

```
POST /api/pipeline/retry
Content-Type: application/json

{
  "id": "uuid-de-la-run",
  "chunk_size": 1000,
  "chunk_overlap": 50,
  "custom_text": "Texte corrigé (optionnel)"
}
```

Réponse (202) :
```json
{
  "status": "re-indexing queued"
}
```

---

## 8. Base de données

### 8.1 Schéma PostgreSQL

#### Migration `001_init.sql` — Tables principales

```sql
-- Extensions vectorielles (pgvector / vectorscale)
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS vectorscale CASCADE;

-- Compatibilité LangChain
langchain_pg_collection (name, cmetadata, uuid)
langchain_pg_embedding  (collection_id, embedding, document, cmetadata, custom_id, uuid)

-- Utilisateurs
users (id, username, password_hash, email, created_at)

-- Audit logs
audit_logs (id, user_id, action, resource, timestamp, metadata)
```

#### Migration `002_observability.sql` — Monitoring

```sql
pipeline_runs (
    id            UUID PRIMARY KEY,
    file_path     VARCHAR(1024) NOT NULL,
    file_name     VARCHAR(255) NOT NULL,
    file_size     BIGINT NOT NULL,
    status        VARCHAR(50) NOT NULL,  -- IN_PROGRESS / COMPLETED / FAILED
    current_step  VARCHAR(50) NOT NULL,  -- PARSING / CHUNKING / EMBEDDING / STORING / COMPLETE
    ocr_status    VARCHAR(50) NOT NULL,  -- NONE / SUCCESS / FAILED
    error_message TEXT,
    chunks_count  INT,
    extracted_text TEXT,
    chunks        JSONB,
    started_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at  TIMESTAMPTZ,
    parameters    JSONB                   -- { chunk_size, chunk_overlap }
)
```

### 8.2 Collection Qdrant

Configuration de la collection par défaut `mnemosyne_docs` :

| Paramètre | Valeur |
|-----------|--------|
| Distance | Cosinus |
| Shards | 3 |
| Facteur de réplication | 2 |
| Stockage payload | On-disk |
| Index plein texte | Sur le champ `content` |

Payload des points :
```
{
  "content":        "texte du chunk",
  "source_path":    "/data/nfs/doc.pdf",
  "file_name":      "doc.pdf",
  "pvc_name":       "nfs-data",
  "file_size":      123456,
  "last_modified":  1700000000,
  "creation_date":  1700000000,
  "file_hash":      "sha256hex",
  "folder_tags":    ["data", "nfs", "rapports"]
}
```

---

## 9. Interface Utilisateur

L'interface web se trouve dans `ui/` et est servie par un conteneur nginx séparé (`Dockerfile.ui`).

### Fonctionnalités

| Fonction | Description |
|----------|-------------|
| **Recherche** | Barre de recherche avec résultats enrichis (score, source) |
| **Synthèse AI** | Bouton flottant pour générer une réponse via Ollama |
| **Pipeline Monitor** | Tableau des runs d'indexation avec statuts, étapes, OCR |
| **Détail d'une run** | Panneau latéral avec métadonnées, timeline, texte extrait, chunks |
| **Correction** | Ré-indexation avec chunk_size/overlap personnalisés ou texte corrigé |
| **Paramètres** | Configuration de l'URL API Mnemosyne, Ollama, modèle |

### Stack UI
- **HTML** : Page unique avec onglets (Search / Pipeline Monitor)
- **CSS** : Thème dark mode, verre morphism, dégradés violet/bleu, animations
- **JS** : Appels API fetch, gestion d'état localStorage, streaming Ollama

---

## 10. Déploiement

### 10.1 Kubernetes (Production)

Deux modes de fonctionnement, configurables via Helm (`deploy/helm/mnemosyne/values.yaml`) :

#### Mode Service Continu (Deployment)

```yaml
deployment:
  enabled: true
  replicaCount: 1
```

- Démarre, indexe tous les chemins configurés
- Reste actif pour servir l'API REST
- Utilisé pour les environnements nécessitant une disponibilité permanente

#### Mode Indexation Planifiée (CronJob)

```yaml
cronjob:
  enabled: false  # true pour activer
  schedule: "0 2 * * *"  # Tous les jours à 2h
```

- Utilise `ONESHOT=true` → indexe puis s'arrête
- Monte le PVC NFS en read-only
- Utilise les secrets Vault pour les credentials

### 10.2 Architecture K8s

```
                    ┌─────────────┐
                    │   Ingress   │
                    └──────┬──────┘
                           │
              ┌────────────┴────────────┐
              │                         │
        ┌─────▼──────┐          ┌──────▼─────┐
        │  Mnemosyne │          │   Nginx    │
        │  (Service) │          │   (UI)     │
        │  :8080     │          │   :8080    │
        └─────┬──────┘          └──────┬─────┘
              │                        │
              ▼                        ▼
        ┌──────────┐           ┌──────────────┐
        │ Qdrant   │           │  PostgreSQL  │
        │ :6333    │           │  :5432       │
        └──────────┘           └──────────────┘

        ┌──────────┐
        │ TEI      │
        │ Embedder │
        └──────────┘

        ┌──────────┐
        │ TEI      │
        │ Reranker │
        └──────────┘
```

### 10.3 Dépendances Externes

| Service | Version | Description |
|---------|---------|-------------|
| Qdrant | ≥ 1.17 | Base vectorielle |
| PostgreSQL | ≥ 14 | Métadonnées, comptes, monitoring |
| TEI Embedder | — | HuggingFace Text Embeddings Inference (embedding) |
| TEI Reranker | — | HuggingFace TEI (reranking) |
| LiteLLM (optionnel) | — | Alternative d'embedding (compatible OpenAI) |

### 10.4 Vault

Les secrets sont gérés via **External Secrets Operator** avec Vault comme backend :

```bash
./scripts/setup_vault.sh
# Stoque DATABASE_URL et LITELLM_API_KEY dans Vault
```

Chemin Vault : `ai/mnemosyne`

---

## 11. Développement

### 11.1 Prérequis

- Rust 1.80+
- Tesseract OCR (headers dev) : `libtesseract-dev`, `libleptonica-dev`
- PostgreSQL accessible
- Cluster Qdrant (ou Docker local)
- Service TEI (ou LiteLLM)

### 11.2 Commandes

```bash
# Build
cargo build --release

# Lancer (indexation + serveur)
cargo run -- --paths /data/documents --database-url postgres://...

# Mode oneshot (CronJob)
cargo run -- --paths /data/documents --database-url postgres://... --oneshot

# Créer l'utilisateur admin
cargo run --bin seed

# Tests
cargo test

# Linting
make lint          # = cargo fmt --check + cargo clippy -- -D warnings
bash local-ci.sh   # CI complète (fmt → clippy → test → build)

# Docker
make docker-build
```

### 11.3 Structure du Projet

```
mnemosyne/
├── Cargo.toml                    # Manifest Rust
├── Makefile                      # Commandes make
├── Dockerfile                    # Build Docker mnemosyne
├── Dockerfile.ui                 # Build Docker UI (nginx)
├── local-ci.sh                   # CI locale
├── .github/workflows/ci.yml      # CI/CD GitHub Actions
├── src/
│   ├── main.rs                   # Point d'entrée, CLI, routes
│   ├── lib.rs                    # AppState, modules
│   ├── bin/seed.rs               # Utilitaire création admin
│   ├── domain/
│   │   ├── entities.rs           # Entités du domaine
│   │   └── ports.rs              # Traits (interfaces)
│   ├── application/
│   │   └── use_cases/
│   │       ├── indexing.rs       # Pipeline d'indexation
│   │       ├── retrieval.rs      # Pipeline de recherche
│   │       └── auth.rs           # Authentification
│   ├── infrastructure/
│   │   ├── repositories/
│   │   │   ├── file_scanner.rs   # Scan disque + extraction
│   │   │   ├── qdrant.rs         # Adaptateur Qdrant
│   │   │   ├── postgres_account.rs # Comptes + monitoring
│   │   │   └── postgres_vector.rs # Legacy pgvector
│   │   └── embedding/
│   │       ├── tei.rs            # TEI (embed + rerank)
│   │       └── litellm.rs        # LiteLLM (embed)
│   └── interfaces/
│       └── http/
│           ├── auth_handlers.rs
│           ├── query_handlers.rs
│           └── pipeline_handlers.rs
├── migrations/
│   ├── 001_init.sql              # Schéma initial
│   └── 002_observability.sql     # Table pipeline_runs
├── ui/
│   ├── index.html                # Interface web
│   ├── style.css                 # Styles dark mode
│   └── script.js                 # Logique front-end
├── deploy/
│   └── helm/mnemosyne/
│       └── values.yaml           # Helm chart values
├── kubernetes/
│   ├── cronjob.yaml              # CronJob K8s
│   └── check-index-job.yaml      # Job d'audit K8s
├── scripts/
│   ├── check_non_indexed.py      # Audit fichiers non indexés
│   └── setup_vault.sh            # Configuration Vault
└── python-version/               # Version Python legacy
    ├── indexer.py
    ├── setup_db.py
    ├── Dockerfile
    ├── Makefile
    └── requirements.txt
```

---

## 12. Tests

### 12.1 Tests Unitaires

| Module | Fichier | Tests |
|--------|---------|-------|
| `indexing` | `src/application/use_cases/indexing.rs` | 3 tests sur `split_text` |
| `retrieval` | `src/application/use_cases/retrieval.rs` | 1 test sur le pipeline complet |
| `auth` | `src/application/use_cases/auth.rs` | 2 tests (login success/failure) |

Les tests utilisent `mockall` pour simuler les dépendances asynchrones.

### 12.2 Exécution

```bash
# Tous les tests
cargo test

# Test spécifique
cargo test test_retrieval_pipeline

# Avec output
cargo test -- --nocapture
```

### 12.3 Audit d'Indexation

Un script Python (`scripts/check_non_indexed.py`) compare les fichiers indexés dans Qdrant avec le système de fichiers local :

```bash
python3 scripts/check_non_indexed.py
```

Un job Kubernetes (`kubernetes/check-index-job.yaml`) effectue la même vérification dans le cluster.

---

## 13. CI/CD

### GitHub Actions (`.github/workflows/ci.yml`)

| Job | Description |
|-----|-------------|
| `security` | Scan Gitleaks pour détection de secrets |
| `lint-and-format` | `cargo fmt` + `cargo clippy` avec auto-commit des corrections |
| `build-and-push` | Build Docker multi-stage + push vers `ghcr.io` |

Déclencheurs :
- `push` sur `main`
- `pull_request` sur `main`

Tags Docker : `sha-<commit>`, `<branchname>`, `latest`.

### CI Locale (`local-ci.sh`)

```bash
./local-ci.sh
# 1. cargo fmt --check
# 2. cargo clippy -- -D warnings
# 3. cargo test
# 4. cargo build
```

---

## 14. Sécurité

| Mesure | Détail |
|--------|--------|
| **Non-root** | Le conteneur Docker exécute avec l'utilisateur `mnemosyne` (UID 1000) |
| **Pod Security Context** | `runAsNonRoot: true`, `allowPrivilegeEscalation: false`, drop de toutes les capacités |
| **Secrets** | Gestion via Vault + External Secrets Operator (pas de secrets en dur) |
| **Logs sensibles** | Aucun secret dans les logs (tracing structuré) |
| **CORS** | Configuration permissive en développement (à restreindre en production) |
| **Auth** | Authentification basique (à renforcer avec JWT valide en production) |

---

## 15. Déploiement Legacy Python

Une version Python de l'indexeur est préservée dans `python-version/` :

```
python-version/
├── indexer.py         # Indexeur LangChain + LiteLLM + PGVector
├── setup_db.py        # Active pgvector/vectorscale
├── Dockerfile         # Image Python 3.11
├── Makefile           # Commandes make
└── requirements.txt   # Dépendances Python
```

### Usage

```bash
cd python-version
make setup
make db-setup   # Active les extensions PostgreSQL
make run        # Lance l'indexation
```

---

## 16. Changelog

### v0.1.0 (2025-05-16)

#### Ajouts
- Pipeline d'indexation complet : scan → parse → chunk → embed → store
- Recherche hybride : embeddings vectoriels + reranking TEI
- API REST avec Axum (health, auth, search, pipeline monitoring)
- Monitoring d'indexation avec table `pipeline_runs`
- Interface web dark mode (recherche + monitoring + correction)
- Support PDF avec extraction `pdf_oxide` (timeout 30s, spawn_blocking)
- Support Markdown, TXT, Log
- Hachage SHA256 pour déduplication
- Tags hiérarchiques basés sur les dossiers
- Métadonnées temporelles (création, modification)
- Embedding via TEI ou LiteLLM (compatible OpenAI)
- Reranking via TEI
- Mode oneshot pour CronJob
- Configuration 12-Factor (env vars + CLI)
- Docker multi-stage (cargo-chef)
- Helm chart pour déploiement Kubernetes
- Workflow CI/CD GitHub Actions
- Audit des fichiers non indexés (script Python + Job K8s)
- Gestion des secrets via Vault
- Logs structurés JSON (tracing)
- Tests unitaires avec mockall
- Legacy Python version préservée

#### Technique
- Architecture hexagonale (ports & adapters)
- Async Rust avec Tokio
- Collection Qdrant : Cosinus, 3 shards, replication 2, on-disk payload, full-text index
- Concurrence configurable (buffer_unordered 8)
- Batch embedding (32 chunks)
- Text splitter récursif multi-niveaux

### v0.1.1 (2025-07-04)

#### Ajouts
- Nouvel endpoint `GET /api/indexing/stats` :
  - Statistiques d'indexation (total, completed, failed, in_progress, chunks, taille)
  - Informations de la collection Qdrant (points, vecteurs, segments, statut)
- Méthode `get_collection_info` sur `VectorStore` trait
- Méthode `get_indexing_stats` sur `PipelineRepository` trait

#### Technique
- Utilisation de l'API `collection_info` de Qdrant
- Requêtes SQL `COUNT` / `SUM` sur `pipeline_runs`
- CI locale (`local-ci.sh`) validée : fmt, clippy, tests, build

---

## 17. Glossaire

| Terme | Définition |
|-------|------------|
| **RAG** | Retrieval-Augmented Generation — Génération augmentée par la récupération d'information |
| **Chunk** | Fragment de texte extrait d'un document, prêt à être vectorisé |
| **Embedding** | Représentation vectorielle d'un texte (tableau de flottants) |
| **Reranking** | Seconde phase de classement qui réordonne les résultats d'une recherche initiale |
| **TEI** | Text Embeddings Inference — Service HuggingFace pour embeddings et reranking |
| **Qdrant** | Base de données vectorielle utilisée comme stockage principal |
| **pgvector** | Extension PostgreSQL pour les recherches vectorielles (utilisée en legacy) |
| **LiteLLM** | Proxy compatible OpenAI API pour les modèles de langage |
| **Pipeline Run** | Enregistrement du traitement d'un fichier individuel (de l'extraction au stockage) |
| **Dense Vector** | Vecteur sémantique dense (non creux) représentant le sens d'un texte |
| **Hybrid Search** | Combinaison de la recherche vectorielle (sens) et textuelle (mots-clés) |
| **RRF** | Reciprocal Rank Fusion — Méthode de fusion de classements multiples |
| **Cosine Distance** | Mesure de similarité entre deux vecteurs (angle) |
| **On-disk payload** | Stockage des métadonnées sur disque plutôt qu'en mémoire (économise RAM) |
| **SHA256** | Fonction de hachage cryptographique pour l'intégrité des fichiers |
