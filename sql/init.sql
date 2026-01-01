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




-- NEUE TABELLE: Tägliche Zählungen pro Quelle/Kategorie
CREATE TABLE IF NOT EXISTS daily_source_counts (
    day_date DATE,
    source VARCHAR(50),
    category VARCHAR(100),
    event_count INT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (day_date, source, category)
);

-- NEUE TABELLE: Täglicher Mood Score
CREATE TABLE IF NOT EXISTS daily_mood_score (
    day_date DATE PRIMARY KEY,
    mood_score DOUBLE PRECISION, -- Skala z.B. -1.0 (negativ) bis +1.0 (positiv)
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);