-- Create table for raw TV Shows data
-- This script should be executed manually once in Neon database

CREATE TABLE IF NOT EXISTS raw.tv_shows_raw (
    id SERIAL PRIMARY KEY,
    show_data JSONB,
    source_file TEXT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

