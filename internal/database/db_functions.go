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
        VALUES (?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, result := range results {
		date := time.Unix(result.UnixTimestamp, 0).Format("2006-01-02")
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
		VALUES (?, ?, ?, ?)
		ON CONFLICT (topic, date) DO UPDATE SET
			positive_count = positive_count + excluded.positive_count,
			negative_count = negative_count + excluded.negative_count;
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, res := range results {
		date := time.Unix(res.UnixTimestamp, 0).Format("2006-01-02")
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
