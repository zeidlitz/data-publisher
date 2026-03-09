CREATE SEQUENCE IF NOT EXISTS sentiment_id_seq;

CREATE TABLE IF NOT EXISTS sentiment_analysis (
    id INTEGER DEFAULT nextval('sentiment_id_seq'),
    created_at TIMESTAMP NOT NULL,
    subreddit VARCHAR,
    title VARCHAR,
    body TEXT,
    categories VARCHAR[],
    sentiment VARCHAR,
    unix_timestamp BIGINT,
    
    PRIMARY KEY (id)
);

CREATE INDEX idx_sentiment_timestamp ON sentiment_analysis (created_at);
CREATE INDEX idx_sentiment_subreddit ON sentiment_analysis (subreddit);

CREATE VIEW IF NOT EXISTS v_category_trends AS
SELECT 
    created_at,
    sentiment,
    unnest(categories) AS category
FROM sentiment_analysis;
