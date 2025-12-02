-- DROP TABLE IF EXISTS public.mood_aggregates;

CREATE TABLE IF NOT EXISTS mood_aggregates (
    avg_score DOUBLE PRECISION,
    updated_at TIMESTAMP,
    metric_name VARCHAR(50)
);

CREATE TABLE cities (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    country VARCHAR(100),
    longitude DOUBLE PRECISION NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO cities (name, country, longitude, latitude)
VALUES ('Hamburg', 'Germany', 9.993682, 53.551086);