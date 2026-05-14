-- Enable pgvector
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS vectorscale CASCADE;

-- Langchain tables (compatibility)
CREATE TABLE IF NOT EXISTS langchain_pg_collection (
    name VARCHAR,
    cmetadata JSONB,
    uuid UUID PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS langchain_pg_embedding (
    collection_id UUID REFERENCES langchain_pg_collection(uuid),
    embedding VECTOR(1536), -- Adjust based on model
    document VARCHAR,
    cmetadata JSONB,
    custom_id VARCHAR,
    uuid UUID PRIMARY KEY
);

-- Mnemosyne User Accounts
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY,
    username VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Mnemosyne Audit Logs
CREATE TABLE IF NOT EXISTS audit_logs (
    id UUID PRIMARY KEY,
    user_id UUID REFERENCES users(id),
    action VARCHAR(255) NOT NULL,
    resource VARCHAR(255) NOT NULL,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    metadata JSONB
);
