package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zeidlitz/data-models/generated/go/analytics"
	"github.com/zeidlitz/data-publisher/internal/config"
	"github.com/zeidlitz/data-publisher/internal/database"
	"github.com/zeidlitz/data-publisher/internal/models"
	"google.golang.org/protobuf/proto"
)

type Worker struct {
	db            database.DuckDbClient
	rdb           *redis.Client
	streamName    string
	groupName     string
	consumerID    string
	batchSize     int
	flushInterval time.Duration
}

func NewWorker(db database.DuckDbClient, rdb *redis.Client, cfg config.Config) *Worker {
	return &Worker{
		db:            db,
		rdb:           rdb,
		streamName:    cfg.RedisStream,
		groupName:     cfg.RedisGroup,
		consumerID:    cfg.RedisConsumer,
		batchSize:     500,
		flushInterval: 5 * time.Second,
	}
}

func (w *Worker) Run(ctx context.Context) error {
	buffer := make([]models.AnalysisResult, 0, w.batchSize)
	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	slog.Info("Starting Redis stream processor")

	for {
		select {
		case <-ctx.Done():
			return w.flush(context.Background(), buffer)

		case <-ticker.C:
			if len(buffer) > 0 {
				if err := w.flush(ctx, buffer); err != nil {
					slog.Error("Failed to flush batch on ticker", "error", err)
					continue
				}
				buffer = buffer[:0]
			}

		default:
			streams, err := w.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    w.groupName,
				Consumer: w.consumerID,
				Streams:  []string{w.streamName, ">"},
				Count:    10,
				Block:    1 * time.Second,
			}).Result()

			if err != nil && err != redis.Nil {
				slog.Error("Redis read error", "error", err)
				continue
			}

			for _, stream := range streams {
				for _, msg := range stream.Messages {
					var protoMsg analytics.AnalysisResult
					data := []byte(msg.Values["data"].(string))
					if err := proto.Unmarshal(data, &protoMsg); err != nil {
						slog.Error("Unmarshal error", "msg_id", msg.ID)
						continue
					}

					res := models.AnalysisResult{
						Subreddit:     protoMsg.RawData.Subreddit,
						Title:         protoMsg.RawData.Title,
						Body:          protoMsg.RawData.Body,
						Categories:    protoMsg.Categories,
						Sentiment:     protoMsg.Sentiment,
						UnixTimestamp: protoMsg.RawData.UnixTimestamp,
					}

					buffer = append(buffer, res)

					w.rdb.XAck(ctx, w.streamName, w.groupName, msg.ID)

					if len(buffer) >= w.batchSize {
						if err := w.flush(ctx, buffer); err != nil {
							slog.Error("Batch insert failed", "error", err)
						}
						buffer = buffer[:0]
					}
				}
			}
		}
	}
}

func (w *Worker) flush(ctx context.Context, data []models.AnalysisResult) error {
	if len(data) == 0 {
		return nil
	}
	slog.Debug("Flushing batch to DuckDB", "count", len(data))
	return w.db.BatchInsert(ctx, data)
}
