package worker

import (
	"context"
	"fmt"
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
	consumerId    string
	batchSize     int
	flushInterval time.Duration
}

func NewWorker(db database.DuckDbClient, rdb *redis.Client, cfg config.Config) *Worker {
	return &Worker{
		db:            db,
		rdb:           rdb,
		streamName:    cfg.RedisStream,
		groupName:     cfg.RedisGroup,
		consumerId:    cfg.RedisConsumer,
		batchSize:     500,
		flushInterval: 5 * time.Second,
	}
}

type TopicStats struct {
	Topic    string
	Date     string
	Positive int
	Negative int
}

type BatchBuffer struct {
	Results    []models.AnalysisResult
	Topics     map[string]struct{}
	DailyStats map[string]*TopicStats
}

func NewBatchBuffer(size int) *BatchBuffer {
	return &BatchBuffer{
		Results:    make([]models.AnalysisResult, 0, size),
		Topics:     make(map[string]struct{}),
		DailyStats: make(map[string]*TopicStats),
	}
}

func (w *Worker) Run(ctx context.Context) error {
	buffer := make([]models.AnalysisResult, 0, w.batchSize)
	messageIDs := make([]string, 0, w.batchSize)
	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	slog.Info("starting redis stream processor")

	for {
		select {
		case <-ctx.Done():
			w.flush(context.Background(), &buffer, &messageIDs)
			return nil

		case <-ticker.C:
			w.flush(ctx, &buffer, &messageIDs)

		default:
			streams, err := w.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    w.groupName,
				Consumer: w.consumerId,
				Streams:  []string{w.streamName, ">"},
				Count:    10,
				Block:    1 * time.Second,
			}).Result()

			if err != nil && err != redis.Nil {
				slog.Error("redis read error", "error", err)
				continue
			}

			for _, stream := range streams {
				for _, msg := range stream.Messages {
					var protoMsg analytics.AnalysisResult
					data := []byte(msg.Values["data"].(string))
					if err := proto.Unmarshal(data, &protoMsg); err != nil {
						slog.Error("unmarshal error", "msg_id", msg.ID)
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
					messageIDs = append(messageIDs, msg.ID)

					w.rdb.XAck(ctx, w.streamName, w.groupName, msg.ID)

					if len(buffer) >= w.batchSize {
						w.flush(ctx, &buffer, &messageIDs)
						buffer = buffer[:0]
					}
				}
			}
		}
	}
}

func (w *Worker) flush(ctx context.Context, buffer *[]models.AnalysisResult, ids *[]string) {
	if len(*buffer) == 0 {
		return
	}

	slog.Info("flusing", "messages", len(*buffer))
	var err error

	err = w.db.InsertAnalysisResults(ctx, *buffer)
	if err != nil {
		slog.Warn("failed to insert analysis results", "err", err)
	}

	err = w.db.InsertTopics(ctx, *buffer)
	if err != nil {
		slog.Warn("failed to insert topics", "err", err)
	}

	if len(*ids) > 0 {
		w.rdb.XAck(ctx, w.streamName, w.groupName, (*ids)...)
	}
	*buffer = (*buffer)[:0]
	*ids = (*ids)[:0]
}

func (w *Worker) EnsureGroup(ctx context.Context) error {
	err := w.rdb.XGroupCreateMkStream(ctx, w.streamName, w.groupName, "$").Err()

	if err != nil {
		if err.Error() == "BUSYGROUP Consumer Group name already exists" {
			slog.Debug("consumer group already exists", "group", w.groupName)
			return nil
		}
		return fmt.Errorf("failed to create consumer group: %w", err)
	}

	slog.Info("created new consumer group", "group", w.groupName, "stream", w.streamName)
	return nil
}
