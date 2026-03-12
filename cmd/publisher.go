package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/v9"
	"github.com/zeidlitz/data-publisher/internal/config"
	"github.com/zeidlitz/data-publisher/internal/database"
	"github.com/zeidlitz/data-publisher/internal/worker"
)

var version = "dev"

func main() {
	slog.Info("starting publisher", "version", version)

	cfg, err := config.New()
	if err != nil {
		slog.Error("failed to create config", "err", err)
		os.Exit(1)
	}

	db, err := database.New(cfg)
	if err != nil {
		slog.Warn("database initialization", "conn", cfg.DuckDbConn, "err", err)
		os.Exit(1)
	}

	redisClient := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	ctx := context.Background()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	worker := worker.NewWorker(db, redisClient, cfg)
	if err := worker.EnsureGroup(ctx); err != nil {
		slog.Error("could not bootstrap Redis group", "error", err)
		os.Exit(1)
	}

	slog.Info("data-publisher running", "version", version)

	if err := worker.Run(ctx); err != nil {
		slog.Error("exiting", "err", err)
	}

	slog.Info("shutting down")
}
