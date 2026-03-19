package database

import (
	"context"
	"github.com/zeidlitz/data-publisher/internal/models"
	"time"
)

func (c *DuckDbClient) InsertAnalysisResults(ctx context.Context, results []models.AnalysisResult) error {
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
	INSERT INTO analysis_results (date, subreddit, title, body, topics, sentiment)
        VALUES (to_timestamp(?), ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, result := range results {
		// date := time.Unix(result.UnixTimestamp, 0).Format("2006-01-02")
		date := result.UnixTimestamp
		_, err := stmt.ExecContext(ctx, date, result.Subreddit, result.Title, result.Body, result.Categories, result.Sentiment)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (c *DuckDbClient) InsertTopics(ctx context.Context, results []models.AnalysisResult) error {
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
        INSERT INTO daily_topics (topic, date, positive_count, negative_count)
        VALUES (?, date_trunc('day', to_timestamp(?)), ?, ?)
        ON CONFLICT (topic, date) DO UPDATE SET
            positive_count = daily_topics.positive_count + EXCLUDED.positive_count,
            negative_count = daily_topics.negative_count + EXCLUDED.negative_count;
    `)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, res := range results {
		// date := time.Unix(res.UnixTimestamp, 0).Format("2006-01-02")
		date := res.UnixTimestamp
		pos, neg := 0, 0
		if res.Sentiment == "POSITIVE" {
			pos = 1
		} else {
			neg = 1
		}

		for _, cat := range res.Categories {
			if _, err := stmt.ExecContext(ctx, cat, date, pos, neg); err != nil {
				return err
			}
		}
	}

	return tx.Commit()
}

func (c *DuckDbClient) GetAnalysisResults(ctx context.Context) ([]models.AnalysisResult, error) {
	var results []models.AnalysisResult
	var rawTopics []any

	query := "SELECT epoch(date)::BIGINT, subreddit, title, body, topics, sentiment FROM analysis_results;"
	rows, err := c.DB.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var a models.AnalysisResult
		err := rows.Scan(&a.UnixTimestamp, &a.Subreddit, &a.Title, &a.Body, &rawTopics, &a.Sentiment)
		if err != nil {
			return nil, err
		}
		a.Categories = make([]string, len(rawTopics))
		for i, v := range rawTopics {
			if v != nil {
				a.Categories[i] = v.(string)
			}
		}
		results = append(results, a)
	}
	return results, nil
}

func (c *DuckDbClient) GetDailyTopics(ctx context.Context, year, month, day int) ([]models.DailyTopic, error) {
	var topics []models.DailyTopic

	query := `
		SELECT topic, date, positive_count, negative_count, total_mentions 
		FROM daily_topics 
		WHERE date = make_date(?, ?, ?);
	`
	rows, err := c.DB.QueryContext(ctx, query, year, month, day)
	if err != nil {
		return nil, err
	}
	defer rows.Next()

	for rows.Next() {
		var t models.DailyTopic
		var dateVal time.Time
		err := rows.Scan(&t.Topic, &dateVal, &t.PositiveCount, &t.NegativeCount, &t.TotalMentions)
		if err != nil {
			return nil, err
		}
		t.Date = dateVal.Format("2006-01-02")
		topics = append(topics, t)
	}
	return topics, nil
}
