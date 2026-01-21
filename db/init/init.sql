-- DROP TABLE IF EXISTS public.mood_aggregates;



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

-- ============================================================================
-- City Mood Database Schema
-- ============================================================================

-- Main Table: Current City Mood Scores
-- ============================================================================
CREATE TABLE IF NOT EXISTS city_mood_scores (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    city_mood_score DOUBLE PRECISION NOT NULL,
    
    -- Component Scores
    news_score DOUBLE PRECISION,
    air_score DOUBLE PRECISION,
    weather_score DOUBLE PRECISION,
    traffic_score DOUBLE PRECISION,
    alerts_score DOUBLE PRECISION,
    construction_score DOUBLE PRECISION,
    water_score DOUBLE PRECISION,
    
    -- Data Point Counts
    news_count INTEGER DEFAULT 0,
    air_count INTEGER DEFAULT 0,
    weather_count INTEGER DEFAULT 0,
    traffic_count INTEGER DEFAULT 0,
    alert_count INTEGER DEFAULT 0,
    construction_count INTEGER DEFAULT 0,
    water_count INTEGER DEFAULT 0,
    total_data_points INTEGER DEFAULT 0,
    
    -- Metrics
    avg_aqi DOUBLE PRECISION,
    avg_temp DOUBLE PRECISION,
    avg_water_level DOUBLE PRECISION,
    
    -- Timestamps
    computed_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    
    PRIMARY KEY (window_start)
);

-- History Table: All Historical City Mood Scores
-- ============================================================================
CREATE TABLE IF NOT EXISTS city_mood_score_history (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    city_mood_score DOUBLE PRECISION NOT NULL,
    
    -- Component Scores
    news_score DOUBLE PRECISION,
    air_score DOUBLE PRECISION,
    weather_score DOUBLE PRECISION,
    traffic_score DOUBLE PRECISION,
    alerts_score DOUBLE PRECISION,
    construction_score DOUBLE PRECISION,
    water_score DOUBLE PRECISION,
    
    -- Data Point Counts
    news_count INTEGER DEFAULT 0,
    air_count INTEGER DEFAULT 0,
    weather_count INTEGER DEFAULT 0,
    traffic_count INTEGER DEFAULT 0,
    alert_count INTEGER DEFAULT 0,
    construction_count INTEGER DEFAULT 0,
    water_count INTEGER DEFAULT 0,
    total_data_points INTEGER DEFAULT 0,
    
    -- Metrics
    avg_aqi DOUBLE PRECISION,
    avg_temp DOUBLE PRECISION,
    avg_water_level DOUBLE PRECISION,
    
    -- Timestamps
    computed_at TIMESTAMP NOT NULL,
    written_at TIMESTAMP NOT NULL,
    batch_id BIGINT NOT NULL,
    
    -- Validation Metadata
    validation_success BOOLEAN,
    validation_success_percent DOUBLE PRECISION,
    validation_evaluated_expectations INTEGER,
    validation_successful_expectations INTEGER,
    validation_failed_expectations INTEGER,
    validation_failed_list TEXT,
    validation_warnings_list TEXT
);

-- Indexes for Main Table
-- ============================================================================
CREATE INDEX IF NOT EXISTS idx_window_start 
    ON city_mood_scores(window_start DESC);
    
CREATE INDEX IF NOT EXISTS idx_city_mood_score 
    ON city_mood_scores(city_mood_score);
    
CREATE INDEX IF NOT EXISTS idx_updated_at 
    ON city_mood_scores(updated_at DESC);

-- Indexes for History Table
-- ============================================================================
CREATE INDEX IF NOT EXISTS idx_history_window_start 
    ON city_mood_score_history(window_start DESC);
    
CREATE INDEX IF NOT EXISTS idx_history_written_at 
    ON city_mood_score_history(written_at DESC);
    
CREATE INDEX IF NOT EXISTS idx_history_batch_id 
    ON city_mood_score_history(batch_id);
    
CREATE INDEX IF NOT EXISTS idx_history_validation_success 
    ON city_mood_score_history(validation_success);
    
CREATE INDEX IF NOT EXISTS idx_history_validation_success_percent 
    ON city_mood_score_history(validation_success_percent);

-- Optional: View for Latest Scores with Trends
-- ============================================================================
CREATE OR REPLACE VIEW v_city_mood_latest AS
SELECT 
    cms.*,
    LAG(cms.city_mood_score) OVER (ORDER BY cms.window_start) as prev_mood_score,
    cms.city_mood_score - LAG(cms.city_mood_score) OVER (ORDER BY cms.window_start) as mood_change
FROM city_mood_scores cms
ORDER BY cms.window_start DESC
LIMIT 100;

-- Optional: Function to cleanup old history data
-- ============================================================================
CREATE OR REPLACE FUNCTION cleanup_old_history(days_to_keep INTEGER DEFAULT 90)
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM city_mood_score_history
    WHERE written_at < NOW() - (days_to_keep || ' days')::INTERVAL;
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Grant permissions
-- ============================================================================
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO spark;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO spark;