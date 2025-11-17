DROP TABLE IF EXISTS public.mood_aggregates;

CREATE TABLE mood_aggregates (
    avg_score DOUBLE PRECISION,
    updated_at TIMESTAMP,
    metric_name VARCHAR(50)
);