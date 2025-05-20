-- Initial schema is already created by init-postgres.sql
-- This is a reference migration to establish a baseline

-- Create migrations table to track applied migrations
CREATE TABLE IF NOT EXISTS migrations (
    id SERIAL PRIMARY KEY,
    migration_name VARCHAR(255) NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Insert record for initial migration
INSERT INTO migrations (migration_name) 
VALUES ('001_initial_schema.sql')
ON CONFLICT DO NOTHING;
