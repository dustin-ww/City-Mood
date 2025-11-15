CREATE TABLE IF NOT EXISTS mood_aggregates (
    city TEXT,
    avg_mood_score DOUBLE PRECISION,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
