# Mnemosyne — Documentation Complète

> Mnemosyne est un moteur d'indexation **RAG** (Retrieval-Augmented Generation) et de **recherche hybride** haute performance, écrit en Rust. Il indexe des documents (PDF, Markdown, TXT, Log) dans une base vectorielle Qdrant, enrichit les chunks avec du contexte via LLM, et expose une API REST pour la recherche sémantique avec reranking.

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

**Mnemosyne** (déesse grecque de la mémoire) est un service d'indexation et de recherche RAG conçu pour les lacs de données à grande échelle. Il transforme des documents bruts en chunks vectorisés enrichis par LLM, les stocke dans **Qdrant**, et permet une **recherche hybride** (vectorielle + texte intégral) avec **reranking** via TEI (Text Embeddings Inference).

### Objectifs

- Indexer des millions de documents avec un débit élevé (concurrence configurable)
- Enrichir les chunks avec le résumé et le contexte du document via LLM
- Extraire automatiquement des métadonnées structurées (tags, entités, résumé)
- Fournir une recherche sémantique contextuelle avec des résultats pertinents
- S'intégrer dans une infrastructure Kubernetes avec GitOps (Helm, ArgoCD)
- Assurer l'observabilité via des logs structurés et une API de monitoring

---

## 2. Architecture

### Architecture Hexagonale (Ports & Adapters)

```
┌──────────────────────────────────────────────────────────────────┐
│                        INTERFACES                                │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │                     HTTP (Axum)                             │ │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────────────────────┐    │ │
│  │  │ Auth     │ │ Search   │ │ Pipeline / Stats / Retry │    │ │
│  │  │ Handlers │ │ Handlers │ │ Handlers                 │    │ │
│  │  └────┬─────┘ └────┬─────┘ └───────────┬──────────────┘    │ │
│  └───────┼─────────────┼───────────────────┼───────────────────┘ │
└──────────┼─────────────┼───────────────────┼─────────────────────┘
           │             │                   │
┌──────────┼─────────────┼───────────────────┼─────────────────────┐
│          ▼             ▼                   ▼                     │
│                    APPLICATION (Use Cases)                       │
│  ┌────────────────┐ ┌──────────────┐ ┌──────────────────────┐   │
│  │ Authentication │ │  Indexing    │ │  Retrieval           │   │
│  │ Use Case       │ │  Use Case    │ │  Use Case            │   │
│  │                │ │  (LLM enrich)│ │  (embed→search→rerank)│  │
│  └───────┬────────┘ └──────┬───────┘ └──────────┬───────────┘   │
└──────────┼─────────────────┼────────────────────┼───────────────┘
           │                 │                    │
┌──────────┼─────────────────┼────────────────────┼───────────────┐
│          ▼                 ▼                    ▼               │
│                        DOMAIN (Ports)                           │
│  ┌──────────┐ ┌────────────┐ ┌──────────────┐                  │
│  │ Vector   │ │ Embedding  │ │ LLMService   │                  │
│  │ Store    │ │ Service    │ │ (TEXT GEN)   │                  │
│  └────┬─────┘ └─────┬──────┘ └──────┬───────┘                  │
│  ┌────┴────┐ ┌──────┴───────┐       │                         │
│  │ File    │ │ User/Audit/ │       │  ┌────────────┐        │
│  │ Scanner │ │ Pipeline    │       │  │ Reranking  │        │
│  └─────────┘ │ Repository  │       │  │ Service    │        │
│              └─────────────┘       │  └────────────┘        │
└────────────────────────────────────┼──────────────────────────┘
                                     │
┌────────────────────────────────────┼──────────────────────────┐
│           INFRASTRUCTURE (Adapters)▼                          │
│  ┌──────────┐ ┌──────────────┐ ┌─────────────────────┐       │
│  │ Qdrant   │ │ TEI Service  │ │ LiteLLMTextService  │       │
│  │ Vector   │ │ (Embed +     │ │ (LLM: résumé, tags, │       │
│  │ Store    │ │  Rerank)     │ │  entités)           │       │
│  └──────────┘ └──────────────┘ └─────────────────────┘       │
│  ┌──────────┐ ┌──────────────────────────────────────┐       │
│  │ Local    │ │ PostgresAccountRepository            │       │
│  │ File     │ │ (User, Audit, Pipeline, Stats)       │       │
│  │ Scanner  │ └──────────────────────────────────────┘       │
│  └──────────┘                                                │
│  ┌─────────────────────────────────────────────────┐        │
│  │ LiteLLMEmbeddingService (embedding OpenAI-compat)│        │
│  └─────────────────────────────────────────────────┘        │
└──────────────────────────────────────────────────────────────┘
```

### Flux de démarrage

```
main()
  │
  ├─ dotenv() ──────────────── Chargement .env
  ├─ tracing_subscriber ─────── Logs structurés stderr
  ├─ PgPool::connect ────────── Connexion PostgreSQL
  ├─ sqlx::migrate ──────────── Migrations DB automatiques
  │
  ├─ Initialisation adapters
  │   ├─ LocalFileScanner(pvc_name)
  │   ├─ TEIService(embed_url, rerank_url)
  │   ├─ LiteLLMEmbeddingService (si PYLOS_URL défini)
  │   ├─ QdrantVectorStore(qdrant_url)
  │   ├─ PostgresAccountRepository(pool)
  │   └─ LiteLLMTextService(pylos_url, api_key, LLM_MODEL)
  │
  ├─ Initialisation use cases
  │   ├─ IndexingUseCase (scanner, embed, vector, repo, llm)
  │   ├─ AuthUseCase(user_repo, audit_repo)
  │   └─ RetrievalUseCase(vector, embed, rerank)
  │
  ├─ AppState ───────────────── État partagé (tous les use cases)
  ├─ Axum Router ─────────────── Routes REST
  ├─ tokio::spawn HTTP server ── Serveur asynchrone (background)
  │
  └─ Indexation initiale synchrone
      └─ IndexingUseCase::execute(path) pour chaque --paths
```

---

## 3. Modules et Composants

### 3.1 `src/domain/` — Cœur métier

| Fichier | Description |
|---------|-------------|
| `entities.rs` | Entités : `Document`, `DocumentMetadata` (avec `inferred_tags`, `document_summary`, `detected_entities`), `DocumentChunk`, `User`, `AuditLog`, `Session`, `PipelineRun` |
| `ports.rs` | Traits : `VectorStore`, `EmbeddingService`, `RerankingService`, `FileScanner`, `UserRepository`, `AuditRepository`, `PipelineRepository`, **`LLMService`** |

### 3.2 `src/application/use_cases/` — Cas d'usage

| Fichier | Description |
|---------|-------------|
| `indexing.rs` | Pipeline d'indexation complet : scan → parse → LLM enrich (résumé + métadonnées) → chunk → enrich context → embed → store |
| `retrieval.rs` | Recherche hybride : embed → vector search (top 50) → rerank TEI (top 5) |
| `auth.rs` | Authentification utilisateur avec audit logging |

### 3.3 `src/infrastructure/` — Adaptateurs

| Fichier | Description |
|---------|-------------|
| `repositories/qdrant.rs` | `VectorStore` pour Qdrant : création collection, upsert points (avec metadata enrichie), search, health, collection_info |
| `repositories/file_scanner.rs` | Scan disque via `walkdir`, extraction PDF via `pdf_oxide` (timeout 30s, spawn_blocking), hash SHA256, métadonnées |
| `repositories/postgres_account.rs` | Implémente 3 traits : `UserRepository`, `AuditRepository`, `PipelineRepository` (CRUD + `get_indexing_stats`) |
| `repositories/postgres_vector.rs` | Legacy `VectorStore` via pgvector (en migration) |
| `embedding/tei.rs` | Services TEI : `POST /embed` (embedding) + `POST /rerank` (reranking) |
| `embedding/litellm.rs` | **Deux services** : `LiteLLMEmbeddingService` (OpenAI-compatible embeddings) + **`LiteLLMTextService`** (chat completion pour enrichissement LLM) |

### 3.4 `src/interfaces/http/` — API REST

| Fichier | Description |
|---------|-------------|
| `auth_handlers.rs` | `POST /api/auth/login` |
| `query_handlers.rs` | `POST /api/search` |
| `pipeline_handlers.rs` | `GET /api/pipeline/runs`, `GET .../:id`, `POST .../retry`, **`GET /api/indexing/stats`** |

---

## 4. Pipeline d'indexation

```
                    IndexingUseCase::execute(path)
                              │
                              ▼
                   scan_directory(path)
                   ┌─────────────────────────┐
                   │ walkdir récursif        │
                   │ Extensions: pdf, md,    │
                   │ txt, log                │
                   └───────────┬─────────────┘
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
   ┌────────────────────────────────────────────────────┐
   │              process_file_internal()               │
   │                                                    │
   │  Étape 1: PARSING                                  │
   │  ┌────────────────────────────────────────────┐   │
   │  │ load_document(file_path)                   │   │
   │  │ - SHA256 hash (déduplication)              │   │
   │  │ - PDF: pdf_oxide spawn_blocking (30s max)  │   │
   │  │        [OCR PENDING] si vide               │   │
   │  │        [ERROR] si échec                    │   │
   │  │        [TIMEOUT] si >30s                   │   │
   │  │ - Autres: UTF-8 read                       │   │
   │  │ - Métadonnées: size, dates, folder_tags,   │   │
   │  │   pvc_name, inferred_tags=None, etc.       │   │
   │  │ - OCR status: SUCCESS/FAILED/NONE          │   │
   │  └───────────────────┬────────────────────────┘   │
   │                      ▼                            │
   │  Étape 2: ENRICHISSEMENT LLM (NOUVEAU)            │
   │  ┌────────────────────────────────────────────┐   │
   │  │ Contenu tronqué à 10000 caractères max     │   │
   │  │                                            │   │
   │  │ 2a. RÉSUMÉ CONTEXTUEL                      │   │
   │  │ ┌──────────────────────────────────────┐   │   │
   │  │ │ System: "You are a precise context   │   │   │
   │  │ │          summarizer."                │   │   │
   │  │ │ User: "Provide 1-2 sentence summary  │   │   │
   │  │ │        of this document..."          │   │   │
   │  │ │ LLM → doc_context (summary string)   │   │   │
   │  │ └──────────────────────────────────────┘   │   │
   │  │                                            │   │
   │  │ 2b. MÉTADONNÉES STRUCTURÉES                │   │
   │  │ ┌──────────────────────────────────────┐   │   │
   │  │ │ System: "You are a metadata          │   │   │
   │  │ │          extractor. Output ONLY JSON" │   │   │
   │  │ │ User: "Extract: inferred_tags (list),│   │   │
   │  │ │        document_summary (3 sentences)│   │   │
   │  │ │        detected_entities (list)"     │   │   │
   │  │ │ LLM → Parsed JSON → stocké dans     │   │   │
   │  │ │        doc.metadata.*                │   │   │
   │  │ │ Fallback: nettoie ```json / ```      │   │   │
   │  │ └──────────────────────────────────────┘   │   │
   │  └───────────────────┬────────────────────────┘   │
   │                      ▼                            │
   │  Étape 3: CHUNKING                                │
   │  ┌────────────────────────────────────────────┐   │
   │  │ split_text(content, chunk_size,            │   │
   │  │              chunk_overlap)                │   │
   │  │ Séparateurs: \n\n → \n → " " → ""         │   │
   │  │                                            │   │
   │  │ ENRICHISSEMENT CONTEXTUEL DES CHUNKS       │   │
   │  │ enrichi = format!(                          │   │
   │  │   "Document: {name}\n                      │   │
   │  │    Context: {summary}\n                    │   │
   │  │    Chunk Content: {chunk}")                │   │
   │  │                                            │   │
   │  │ Les chunks enrichis remplacent les chunks  │   │
   │  │ bruts pour l'embedding et le stockage      │   │
   │  │                                            │   │
   │  │ Valeurs par défaut :                       │   │
   │  │   chunk_size = 1000, chunk_overlap = 0     │   │
   │  └───────────────────┬────────────────────────┘   │
   │                      ▼                            │
   │  Étape 4: EMBEDDING                               │
   │  ┌────────────────────────────────────────────┐   │
   │  │ Par batch de 32 chunks enrichis            │   │
   │  │ EmbeddingService::generate_embeddings()    │   │
   │  │  → TEI (POST /embed)                       │   │
   │  │  → ou LiteLLM (POST /v1/embeddings)        │   │
   │  └───────────────────┬────────────────────────┘   │
   │                      ▼                            │
   │  Étape 5: STORING                                 │
   │  ┌────────────────────────────────────────────┐   │
   │  │ VectorStore::save_chunks(chunks, collection)│   │
   │  │ → Qdrant upsert_points                      │   │
   │  │                                              │   │
   │  │ Payload stocké :                             │   │
   │  │ - content (texte enrichi)                    │   │
   │  │ - source_path, file_name, pvc_name           │   │
   │  │ - file_size, last_modified, creation_date    │   │
   │  │ - file_hash (SHA256)                         │   │
   │  │ - folder_tags (hiérarchiques)                │   │
   │  │ - inferred_tags (list, du LLM)               │   │
   │  │ - document_summary (string, du LLM)          │   │
   │  │ - detected_entities (list, du LLM)           │   │
   │  └─────────────────────────────────────────────┘   │
   └────────────────────────────────────────────────────┘
             │
             ▼
        PipelineRun UPDATE
        status: "COMPLETED" ou "FAILED"
```

### Détail du Text Splitter (`split_text`)

```
Fonction split_recursive(text, séparateurs, chunk_size, chunk_overlap)

  Hiérarchie des séparateurs :
    niveau 0: "\n\n" (paragraphes)
    niveau 1: "\n"   (lignes)
    niveau 2: " "    (mots)
    niveau 3: ""     (caractères — fallback)

  Algorithme :
  1. Si text ≤ chunk_size → ajouter aux chunks, return
  2. Prendre le séparateur courant
  3. Splitter le texte par ce séparateur
  4. Grouper les fragments jusqu'à chunk_size
  5. Quand un fragment dépasse → sauvegarder le groupe,
     appliquer l'overlap (reprendre derniers N fragments),
     récursion avec séparateur suivant si nécessaire
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
     ┌────────────────────────────────────┐
     │ generate_embeddings([query])      │
     │ → TEI (POST /embed)               │
     │ → ou LiteLLM (POST /v1/embeddings)│
     └──────────────┬─────────────────────┘
                    ▼
     Étape 2: RECHERCHE VECTORIELLE
     ┌────────────────────────────────────┐
     │ VectorStore::search()             │
     │ → Qdrant query_points             │
     │   .query(query_vector)            │
     │   .limit(50)                      │
     │   .with_payload(true)             │
     │ → Similarité cosinus              │
     │ → Retourne 50 candidats           │
     └──────────────┬─────────────────────┘
                    ▼
     Étape 3: RERANKING
     ┌────────────────────────────────────┐
     │ RerankingService::rerank()        │
     │ → TEI (POST /rerank)              │
     │   query + 50 textes               │
     │ → Tri par score descendant        │
     │ → Garde top 5                     │
     │ → Retourne DocumentChunk[]        │
     └──────────────┬─────────────────────┘
                    ▼
          Réponse JSON: results[]
          { content, source, score }
```

---

## 6. Configuration

Variables d'environnement (12-Factor App) — toutes surchargeables en CLI via clap :

| Variable | CLI | Défaut | Description |
|----------|-----|--------|-------------|
| `DATABASE_URL` | `--database-url` | **Requis** | Connexion PostgreSQL |
| `QDRANT_URL` | `--qdrant-url` | `http://qdrant.qdrant.svc.cluster.local:6333` | URL cluster Qdrant |
| `NFS_PATH` | `--paths` | `/data/nfs` | Chemins à indexer (séparés par `,`) |
| `COLLECTION_NAME` | `--collection-name` | `mnemosyne_docs` | Nom collection Qdrant |
| `EMBEDDING_MODEL` | `--embedding-model` | `BAAI/bge-m3` | Modèle embedding (métadonnée) |
| `TEI_EMBEDDER_URL` | `--tei-embedder-url` | Service K8s mnemosyne-tei-embedder | URL TEI embedding |
| `TEI_RERANKER_URL` | `--tei-reranker-url` | Service K8s mnemosyne-tei-reranker | URL TEI reranking |
| `PYLOS_URL` / `LITELLM_URL` | `--pylos-url` | `None` | URL LiteLLM (embeddings + LLM) |
| `PYLOS_API_KEY` / `LITELLM_API_KEY` | `--pylos-api-key` | `None` | Clé API LiteLLM |
| `LLM_MODEL` | — | `gemini-3-flash` | Modèle LLM pour enrichissement (résumé, tags, entités) |
| `PVC_NAME` | `--pvc-name` | `unknown` | Identifiant PVC pour métadonnées |
| `HTTP_PORT` | `--http-port` | `8080` | Port serveur HTTP |
| `ONESHOT` | `--oneshot` | `false` | Sortir après indexation (mode CronJob) |
| `RUST_LOG` | — | `info` | Niveau de log tracing |

---

## 7. API REST

### 7.1 Health Check

```
GET /health
```

Réponse (200) :
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

{ "username": "admin", "password": "password" }
```

Réponse (200) :
```json
{ "token": "dummy-jwt-token", "username": "admin" }
```

### 7.3 Recherche

```
POST /api/search
Content-Type: application/json

{ "query": "Qu'est-ce que la mémoire ?" }
```

Réponse (200) :
```json
{
  "results": [
    {
      "content": "Document: doc.pdf\nContext: Ce document traite de...\nChunk Content: ...",
      "source": "doc.pdf",
      "score": 0.95
    }
  ]
}
```

**Note** : Le `content` retourné inclut le préfixe contextuel (`Document: ... Context: ... Chunk Content: ...`) généré lors de l'indexation enrichie.

### 7.4 Pipeline Runs — Monitoring

```
GET /api/pipeline/runs
```

Retourne le tableau des `PipelineRun` trié par `started_at` DESC.

```
GET /api/pipeline/runs/:id
```

Retourne une `PipelineRun` unique.

### 7.5 Métriques Globales

```
GET /api/indexing/stats
```

Point d'entrée unique pour toutes les métriques : indexation, base vectorielle et utilisation.

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
  },
  "usage": {
    "total_searches": 1250,
    "searches_today": 42,
    "searches_this_week": 310,
    "average_duration_ms": 185.5,
    "total_results_returned": 5230,
    "zero_result_searches": 15,
    "top_queries": [
      { "query": "rapport financier 2024", "count": 28 },
      { "query": "procédure sécurité", "count": 22 }
    ]
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
{ "status": "re-indexing queued" }
```

La ré-indexation s'exécute en arrière-plan via `tokio::spawn`.

---

## 8. Base de données

### 8.1 Schéma PostgreSQL

#### Migration `001_init.sql` — Tables principales

```sql
-- Extensions vectorielles
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS vectorscale CASCADE;

-- Compatibilité LangChain (legacy)
langchain_pg_collection (name VARCHAR, cmetadata JSONB, uuid UUID PRIMARY KEY)
langchain_pg_embedding  (collection_id UUID, embedding VECTOR(1536),
                         document VARCHAR, cmetadata JSONB,
                         custom_id VARCHAR, uuid UUID PRIMARY KEY)

-- Utilisateurs
users (id UUID PRIMARY KEY, username VARCHAR(255) UNIQUE NOT NULL,
       password_hash VARCHAR(255) NOT NULL, email VARCHAR(255) UNIQUE NOT NULL,
       created_at TIMESTAMPTZ DEFAULT NOW())

-- Audit logs
audit_logs (id UUID PRIMARY KEY, user_id UUID REFERENCES users(id),
            action VARCHAR(255), resource VARCHAR(255),
            timestamp TIMESTAMPTZ DEFAULT NOW(), metadata JSONB)
```

#### Migration `002_observability.sql` — Monitoring

```sql
pipeline_runs (
    id             UUID PRIMARY KEY,
    file_path      VARCHAR(1024) NOT NULL,
    file_name      VARCHAR(255) NOT NULL,
    file_size      BIGINT NOT NULL,
    status         VARCHAR(50) NOT NULL,   -- IN_PROGRESS / COMPLETED / FAILED
    current_step   VARCHAR(50) NOT NULL,   -- PARSING / CHUNKING / EMBEDDING / STORING / COMPLETE
    ocr_status     VARCHAR(50) NOT NULL,   -- NONE / SUCCESS / FAILED
    error_message  TEXT,
    chunks_count   INT,
    extracted_text TEXT,
    chunks         JSONB,                  -- Liste des chunks bruts
    started_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at   TIMESTAMPTZ,
    parameters     JSONB                   -- { chunk_size, chunk_overlap }
)
```

#### Migration `003_usage_stats.sql` — Statistiques d'utilisation

```sql
search_logs (
    id                 UUID PRIMARY KEY,
    query              TEXT NOT NULL,          -- Texte de la requête
    results_count      INT NOT NULL DEFAULT 0, -- Nombre de résultats retournés
    search_duration_ms INT NOT NULL DEFAULT 0, -- Temps d'exécution en ms
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    INDEX idx_search_logs_created_at ON (created_at)
)
```

Chaque appel à `POST /api/search` enregistre automatiquement la requête, son temps d'exécution et le nombre de résultats dans cette table.

### 8.2 Collection Qdrant

Configuration de la collection `mnemosyne_docs` (créée automatiquement si inexistante) :

| Paramètre | Valeur |
|-----------|--------|
| Distance | Cosinus |
| Shards | 3 |
| Facteur de réplication | 2 |
| Stockage payload | On-disk |
| Index plein texte | Sur le champ `content` |

Payload des points — métadonnées complètes enrichies :

```json
{
  "content":           "Document: doc.pdf\nContext: Ce document...\nChunk Content: ...",
  "source_path":       "/data/nfs/rapports/doc.pdf",
  "file_name":         "doc.pdf",
  "pvc_name":          "nfs-data",
  "file_size":         123456,
  "last_modified":     1700000000,
  "creation_date":     1700000000,
  "file_hash":         "abc123def456...",
  "folder_tags":       ["data", "nfs", "rapports"],
  "inferred_tags":     ["finance", "report", "Q4"],
  "document_summary":  "Ce rapport présente les résultats financiers du Q4...",
  "detected_entities": ["SARL Exemple", "Jean Dupont", "2024"]
}
```

---

## 9. Interface Utilisateur

L'interface web se trouve dans `ui/` et est servie par un conteneur nginx séparé (`Dockerfile.ui`).

### Fonctionnalités

| Fonction | Description |
|----------|-------------|
| **Recherche** | Barre de recherche avec résultats enrichis (score %, source) |
| **Synthèse AI** | Bouton flottant → interroge Ollama avec les résultats comme contexte |
| **Pipeline Monitor** | Tableau des runs d'indexation (statuts, étapes, OCR, chunks) |
| **Détail d'une run** | Panneau latéral : métadonnées, timeline visuelle (5 étapes), texte extrait, chunks |
| **Correction** | Ré-indexation avec chunk_size/overlap personnalisés ou texte corrigé |
| **Paramètres** | Configuration URL API Mnemosyne, Ollama URL, modèle |

### Stack UI
- **HTML** : Page unique avec onglets (Search / Pipeline Monitor)
- **CSS** : Thème dark, glass-morphism, dégradés violet/bleu, animations
- **JS** : Appels fetch REST, streaming Ollama, localStorage, state management

---

## 10. Déploiement

### 10.1 Kubernetes (Production)

Deux modes mutuellement exclusifs via Helm (`deploy/helm/mnemosyne/values.yaml`) :

#### Mode Service Continu (Deployment)

```yaml
deployment:
  enabled: true
  replicaCount: 1
```

- Démarre → indexe tous les chemins → reste actif pour servir l'API REST
- Utilisé pour les environnements nécessitant une disponibilité permanente

#### Mode Indexation Planifiée (CronJob)

```yaml
cronjob:
  enabled: true
  schedule: "0 2 * * *"   # Tous les jours à 2h du matin
  concurrencyPolicy: Forbid
  failedJobsHistoryLimit: 3
  successfulJobsHistoryLimit: 1
```

- `ONESHOT=true` → indexe puis s'arrête
- Monte le PVC NFS en read-only
- Secrets via Vault External Secrets Operator

### 10.2 Architecture K8s

```
                    ┌──────────────┐
                    │   Ingress    │
                    └──────┬───────┘
                           │
              ┌────────────┴────────────┐
              │                         │
        ┌─────▼──────┐          ┌──────▼──────┐
        │  Mnemosyne │          │   Nginx     │
        │  (Service) │          │   (UI)      │
        │  :8080     │          │   :8080     │
        └─────┬──────┘          └──────┬──────┘
              │                        │
              ▼                        │
        ┌──────────┐                   │
        │ Qdrant   │                   │
        │ :6333    │                   │
        └──────────┘                   │
                                       │
        ┌──────────────┐               │
        │  PostgreSQL  │◄──────────────┘
        │  :5432       │
        └──────────────┘

        ┌──────────────────┐     ┌──────────────────┐
        │ TEI Embedder     │     │ TEI Reranker     │
        │ (BAAI/bge-m3)    │     │ (BGE-reranker-v2)│
        └──────────────────┘     └──────────────────┘

        ┌──────────────────────────────────┐
        │ LiteLLM (proxy OpenAI-compat)    │
        │ → embeddings + LLM text gen     │
        └──────────────────────────────────┘
```

### 10.3 Dépendances Externes

| Service | Rôle | Version |
|---------|------|---------|
| **Qdrant** | Base vectorielle | ≥ 1.17 |
| **PostgreSQL** | Métadonnées, comptes, monitoring | ≥ 14 |
| **TEI Embedder** | Embedding (BAAI/bge-m3) | — |
| **TEI Reranker** | Reranking (BGE-reranker-v2-m3) | — |
| **LiteLLM** | Proxy LLM (embedding + text gen) | — |

### 10.4 Vault (Secrets)

```bash
./scripts/setup_vault.sh
# Stoque DATABASE_URL et LITELLM_API_KEY
# Chemin Vault : ai/mnemosyne
# Sync via External Secrets Operator
```

---

## 11. Développement

### 11.1 Prérequis

- Rust 1.80+
- Tesseract OCR headers : `libtesseract-dev`, `libleptonica-dev`, `clang`, `pkg-config`
- PostgreSQL
- Qdrant (Docker ou cluster)
- TEI Embedder + Reranker (ou LiteLLM)

### 11.2 Commandes

```bash
# Build release
cargo build --release

# Lancer (indexation + serveur)
cargo run -- --paths /data/docs --database-url postgres://user:pass@localhost/mnemosyne

# Mode oneshot (CronJob)
cargo run -- --paths /data/docs --database-url postgres://... --oneshot

# Créer l'utilisateur admin
cargo run --bin seed

# Tests
cargo test

# CI locale complète
bash local-ci.sh

# Linting
make lint

# Docker
make docker-build
```

### 11.3 Structure du Projet

```
mnemosyne/
├── Cargo.toml                        # Manifest Rust, 2 bins
├── Makefile                          # build, run, test, lint, docker
├── Dockerfile                        # Multi-stage (cargo-chef)
├── Dockerfile.ui                     # nginx pour UI
├── local-ci.sh                       # fmt → clippy → test → build
├── DOCUMENTATION.md                  # Ce fichier
├── README.md                         # Présentation rapide
├── .github/workflows/ci.yml          # GitHub Actions CI/CD
│
├── src/
│   ├── main.rs                       # CLI (clap), startup, routes
│   ├── lib.rs                        # AppState, module declarations
│   ├── bin/seed.rs                   # Seed admin user
│   │
│   ├── domain/
│   │   ├── entities.rs               # Document, Metadata, Chunk, User, PipelineRun
│   │   └── ports.rs                  # Traits VectorStore, LLMService, etc.
│   │
│   ├── application/use_cases/
│   │   ├── indexing.rs               # Pipeline complet (LLM enrich)
│   │   ├── retrieval.rs              # Search + rerank
│   │   └── auth.rs                   # Login + audit
│   │
│   ├── infrastructure/
│   │   ├── repositories/
│   │   │   ├── file_scanner.rs       # WalkDir + pdf_oxide + SHA256
│   │   │   ├── qdrant.rs             # Qdrant upsert/search/info
│   │   │   ├── postgres_account.rs   # User + Audit + Pipeline + Stats
│   │   │   └── postgres_vector.rs    # Legacy pgvector
│   │   └── embedding/
│   │       ├── tei.rs                # TEI embed + rerank
│   │       └── litellm.rs            # LiteLLM embed + LLM text gen
│   │
│   └── interfaces/http/
│       ├── auth_handlers.rs
│       ├── query_handlers.rs
│       └── pipeline_handlers.rs      # Runs + Stats + Retry
│
├── migrations/
│   ├── 001_init.sql                  # pgvector, users, audit_logs
│   └── 002_observability.sql         # pipeline_runs
│
├── ui/
│   ├── index.html                    # SPA : Search + Pipeline Monitor
│   ├── style.css                     # Dark theme, glassmorphism
│   └── script.js                     # Fetch, Ollama, state
│
├── deploy/helm/mnemosyne/
│   └── values.yaml                   # Helm chart
│
├── kubernetes/
│   ├── cronjob.yaml                  # CronJob indexation nightly
│   └── check-index-job.yaml          # Job audit fichiers non indexés
│
├── scripts/
│   ├── check_non_indexed.py          # Audit Python
│   └── setup_vault.sh               # Configuration Vault
│
└── python-version/                   # Version Python legacy
    ├── indexer.py, setup_db.py
    ├── Dockerfile, Makefile
    └── requirements.txt
```

---

## 12. Tests

### 12.1 Tests Unitaires

| Module | Fichier | Tests | Description |
|--------|---------|-------|-------------|
| `indexing` | `indexing.rs` | 21 | `split_text` (10), `safe_truncate` (3), `enrich_chunks_with_context` (4), `parse_llm_metadata` (7) |
| `retrieval` | `retrieval.rs` | 6 | Pipeline, empty results, embedding fail, no embedding, rerank fail, multi-résultats |
| `auth` | `auth.rs` | 2 | Login success + invalid password |
| `litellm` | `litellm.rs` | 6 | `build_api_url` : plain, trailing slash, `/v1`, `/v1/`, chat, empty |
| **Total** | | **38** | |

Framework de mock : **mockall** (génération automatique de mocks async).

### 12.2 Exécution

```bash
cargo test                          # Tous
cargo test test_retrieval_pipeline  # Un seul
cargo test -- --nocapture           # Avec stdout
```

### 12.3 Audit d'Indexation

Script Python (`scripts/check_non_indexed.py`) ou Job K8s (`kubernetes/check-index-job.yaml`) :

```bash
python3 scripts/check_non_indexed.py
# Compare Qdrant (source_path) vs filesystem → liste les non indexés
```

---

## 13. CI/CD

### GitHub Actions (`.github/workflows/ci.yml`)

```yaml
# Déclencheurs : push/PR sur main
jobs:
  security:           # Gitleaks (scan de secrets)
  lint-and-format:    # cargo fmt + clippy → auto-commit
  build-and-push:     # Docker buildx → push ghcr.io
                       # Tags : sha-<commit>, branche, latest
```

### CI Locale (`local-ci.sh`)

```bash
./local-ci.sh
# [1/4] cargo fmt --all --check
# [2/4] cargo clippy -- -D warnings
# [3/4] cargo test
# [4/4] cargo build
```

### Makefile

```makefile
build           cargo build --release
run             cargo run
seed            cargo run --bin seed
test            cargo test
lint            cargo fmt --check + cargo clippy -- -D warnings
docker-build    docker build -t mnemosyne:latest .
clean           cargo clean
```

---

## 14. Sécurité

| Mesure | Détail |
|--------|--------|
| **Non-root** | Conteneur exécute avec user `mnemosyne` (UID 1000) |
| **Pod Security** | `runAsNonRoot: true`, `allowPrivilegeEscalation: false`, drop ALL capabilities |
| **Secrets** | External Secrets Operator + Vault |
| **Logs** | Aucun secret dans les logs (tracing structuré vers stderr) |
| **CORS** | `AllowOrigin(Any)` — à restreindre en production |
| **Auth API** | Token JWT (actuellement placeholder `dummy-jwt-token`) |
| **PDF timeout** | 30s max avec `spawn_blocking` pour éviter le blocage Tokio |
| **LLM fallback** | Parsing JSON avec fallback pour les réponses LLM non conformes |

---

## 15. Déploiement Legacy Python

Version Python préservée dans `python-version/` (indexeur LangChain + LiteLLM + PGVector) :

```bash
cd python-version
make setup            # pip install -r requirements.txt
make db-setup         # Active pgvector/vectorscale
make run              # Lance indexation Python
```

Stack : Python 3.11, LangChain, LiteLLM, pgvector, Rich (barres de progression).

---

## 16. Changelog

### v0.1.1 (2025-07-04)

#### 🚀 Nouvelles fonctionnalités

- **Enrichissement contextuel des chunks via LLM** (`LLMService` trait + `LiteLLMTextService`) :
  - Résumé automatique du document (1-2 phrases) ajouté comme préfixe contextuel à chaque chunk
  - Extraction de métadonnées structurées : `inferred_tags` (tags thématiques), `document_summary`, `detected_entities`
  - Chunks stockés avec le format : `"Document: {name}\nContext: {summary}\nChunk Content: {chunk}"`
  - Nouvelle variable d'environnement : `LLM_MODEL` (défaut `gemini-3-flash`)
  - Parsing JSON avec fallback pour les réponses LLM non conformes
- **Endpoint `GET /api/indexing/stats`** :
  - Statistiques PostgreSQL : total/completed/failed/in_progress/chunks/file_size
  - Informations collection Qdrant : points_count, indexed_vectors, segments, status
  - Statistiques d'utilisation : total_searches, searches_today/week, avg_duration, zero_result_searches, top_queries
- **Statistiques d'utilisation des recherches** (table `search_logs`) :
  - Chaque requête `POST /api/search` est automatiquement loggée avec durée et nombre de résultats
  - Endpoint `/api/indexing/stats` expose les métriques d'usage
  - Top 10 des requêtes les plus fréquentes

#### 🔧 Technique

- Nouveau trait `LLMService` dans `domain/ports.rs`
- Nouveau `LiteLLMTextService` (chat completions OpenAI-compatible) dans `litellm.rs`
- `IndexingUseCase` : nouvelle dépendance `LLMService`, 2 appels LLM par fichier (résumé + métadonnées)
- `DocumentMetadata` : 3 nouveaux champs optionnels (`inferred_tags`, `document_summary`, `detected_entities`)
- `QdrantVectorStore` : stocke et lit les nouveaux champs dans le payload Qdrant
- `main.rs` : initialisation du LLM service avec fallback URL et clé
- Migration `003_usage_stats.sql` : table `search_logs` avec index temporel
- `PipelineRepository` : nouvelles méthodes `log_search` et `get_usage_stats`
- `query_handlers.rs` : logging automatique des recherches avec timing
- `pipeline_handlers.rs` : métriques d'usage intégrées dans `/api/indexing/stats`
- CI locale (`local-ci.sh`) : fmt → clippy → test → build

### v0.1.2 (2025-07-05)

#### 🔧 Refactoring & Qualité

- **Découpage de `process_file_internal`** : extraction de `extract_content()`, `enrich_with_llm()`, `generate_embeddings_batched()` — orchestration réduite de 120 à 40 lignes
- **Parallélisation des appels LLM** : résumé + métadonnées via `tokio::join!` (÷2 le temps d'enrichissement)
- **Élimination du clone `chunks_content`** : `enrich_chunks_with_context()` prend désormais `&[String]`
- **Extraction `build_api_url()`** : URL construction dupliquée (2×) factorisée dans `litellm.rs` — 6 tests unitaires
- **Extraction `row_to_run()`** : mapping `PipelineRun` dupliqué (3×) factorisé dans `postgres_account.rs`
- **Extraction `extract_pdf_text()`** : triple `Ok(Ok(Ok()))` isolé dans `file_scanner.rs`
- **Extraction helpers Qdrant** : `get_str()`, `get_i64()`, `get_str_list()`, `get_optional_str()`, `get_optional_str_list()` — parsing réduit de 60 à 15 lignes
- **Constantes nommées** : `EMBEDDING_BATCH_SIZE` (32), `FILE_SCAN_CONCURRENCY` (8), `LLM_TRUNCATION_LIMIT` (10000)

#### 🧪 Tests

- **32 nouveaux tests** (6 → 38) :
  - Splitter : empty, whitespace, small, overlap, unicode, large
  - Enrichissement : format, empty context, empty chunks, special chars
  - Metadata LLM : valid JSON, markdown fence, plain fence, partial, empty, invalid, fallback
  - `safe_truncate` : ascii, unicode boundary, empty
  - Retrieval : empty results, embedding fail, no embedding, rerank fail, multi-résultats
  - `build_api_url` : plain, trailing slash, `/v1`, `/v1/`, chat completions, empty

### v0.1.0 (2025-05-16)

#### 🚀 Nouvelles fonctionnalités

- Pipeline d'indexation complet : scan → parse (PDF/md/txt/log) → chunk → embed → store
- Recherche hybride : embeddings vectoriels + reranking TEI
- API REST Axum : health, auth, search, pipeline monitoring
- Monitoring avec table `pipeline_runs` (statut, étape, OCR, chunks, durée)
- Interface web dark mode : search + pipeline monitor + correction
- Support PDF avec extraction `pdf_oxide` (timeout 30s, spawn_blocking)
- Support Markdown, TXT, Log
- Hachage SHA256 pour déduplication et suivi des modifications
- Tags hiérarchiques basés sur les dossiers
- Métadonnées temporelles (création, modification)
- Embedding via TEI ou LiteLLM (OpenAI-compatible)
- Reranking via TEI
- Mode oneshot pour CronJob Kubernetes
- Configuration 12-Factor (env vars + CLI clap)
- Docker multi-stage (cargo-chef)
- Helm chart pour déploiement Kubernetes
- Workflow CI/CD GitHub Actions (gitleaks, fmt, clippy, build, push)
- Audit des fichiers non indexés (script Python + Job K8s)
- Gestion des secrets via Vault
- Logs structurés JSON (tracing)
- Tests unitaires avec mockall
- Legacy Python version préservée

---

## 17. Glossaire

| Terme | Définition |
|-------|------------|
| **RAG** | Retrieval-Augmented Generation — Génération augmentée par la récupération d'information |
| **Chunk** | Fragment de texte extrait d'un document, prêt à être vectorisé |
| **Enriched Chunk** | Chunk enrichi avec le contexte du document : `"Document: {name}\nContext: {summary}\nContent: {chunk}"` |
| **Embedding** | Représentation vectorielle d'un texte (tableau de flottants) |
| **Reranking** | Seconde phase de classement qui réordonne les résultats d'une recherche initiale |
| **TEI** | Text Embeddings Inference — Service HuggingFace pour embeddings et reranking |
| **Qdrant** | Base de données vectorielle utilisée comme stockage principal |
| **pgvector** | Extension PostgreSQL pour les recherches vectorielles (legacy) |
| **LiteLLM** | Proxy compatible OpenAI API pour les modèles de langage |
| **LLM Enrichment** | Processus d'appel à un LLM pour générer résumé, tags et entités d'un document |
| **Pipeline Run** | Enregistrement du traitement d'un fichier (extraction → enrichissement → stockage) |
| **Dense Vector** | Vecteur sémantique dense représentant le sens d'un texte |
| **Hybrid Search** | Combinaison recherche vectorielle (sens) + textuelle (mots-clés) |
| **Cosine Distance** | Mesure de similarité entre deux vecteurs (angle) |
| **On-disk payload** | Stockage des métadonnées Qdrant sur disque plutôt qu'en mémoire |
| **SHA256** | Fonction de hachage cryptographique pour l'intégrité des fichiers |
| **RRF** | Reciprocal Rank Fusion — Méthode de fusion de classements multiples |
| **Oneshot** | Mode où Mnemosyne indexe puis s'arrête (pour CronJob) |
