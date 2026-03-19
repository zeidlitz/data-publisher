CREATE TYPE IF NOT EXISTS sentiment_type AS ENUM ('POSITIVE', 'NEGATIVE');

CREATE TABLE IF NOT EXISTS analysis_results (
    date TIMESTAMPTZ NOT NULL,
    subreddit VARCHAR,
    title VARCHAR,
    body TEXT,
    topics VARCHAR[],
    sentiment sentiment_type,
);

CREATE TABLE IF NOT EXISTS daily_topics (
  topic VARCHAR,
  date DATE,
  positive_count INTEGER DEFAULT 0,
  negative_count INTEGER DEFAULT 0,
  total_mentions INTEGER AS (positive_count + negative_count),
  PRIMARY KEY(date, topic)
)
