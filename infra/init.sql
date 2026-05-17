-- Enable pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;

-- Incidents table — stores every resolved incident permanently
CREATE TABLE IF NOT EXISTS incidents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    incident_type VARCHAR(100) NOT NULL,
    service_name VARCHAR(100) NOT NULL,
    raw_event JSONB NOT NULL,
    anomaly_classification TEXT,
    root_cause TEXT,
    remediation_actions JSONB,
    resolution_status VARCHAR(50) NOT NULL DEFAULT 'open',
    retry_count INTEGER NOT NULL DEFAULT 0,
    escalated BOOLEAN NOT NULL DEFAULT FALSE,
    resolution_time_seconds INTEGER,
    embedding vector(1536),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMPTZ
);

-- Index for fast similarity search on embeddings
CREATE INDEX IF NOT EXISTS incidents_embedding_idx
    ON incidents USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);

-- Index for filtering by incident type and service
CREATE INDEX IF NOT EXISTS incidents_type_service_idx
    ON incidents (incident_type, service_name);

-- Index for filtering unresolved / escalated incidents
CREATE INDEX IF NOT EXISTS incidents_status_idx
    ON incidents (resolution_status, escalated);

-- Runbooks table — stores vectorized runbook chunks
CREATE TABLE IF NOT EXISTS runbooks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title VARCHAR(200) NOT NULL,
    incident_type VARCHAR(100) NOT NULL,
    content TEXT NOT NULL,
    embedding vector(1536),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for runbook similarity search
CREATE INDEX IF NOT EXISTS runbooks_embedding_idx
    ON runbooks USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 50);