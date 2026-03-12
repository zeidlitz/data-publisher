CREATE SEQUENCE IF NOT EXISTS sentiment_id_seq;

CREATE TABLE IF NOT EXISTS sentiment_analysis (
    created_at TIMESTAMP NOT NULL,
    subreddit VARCHAR,
    title VARCHAR,
    body TEXT,
    categories VARCHAR[],
    sentiment VARCHAR,
    unix_timestamp BIGINT
);

CREATE INDEX IF NOT EXISTS idx_sentiment_timestamp ON sentiment_analysis (created_at);
CREATE INDEX IF NOT EXISTS idx_sentiment_subreddit ON sentiment_analysis (subreddit);

CREATE VIEW IF NOT EXISTS v_category_trends AS
SELECT 
    created_at,
    sentiment,
    unnest(categories) AS category
FROM sentiment_analysis;
