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
	}

	db, err := database.New(cfg)
	if err != nil {
		slog.Error("faield to connect to database", "conn", cfg.DuckDbConn, "err", err)
	}

	redisClient := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	worker := worker.NewWorker(db, redisClient, cfg)

	slog.Info("data-publisher running", "version", version)

	if err := worker.Run(ctx); err != nil {
		slog.Error("exiting", "err", err)
	}

	slog.Info("shutting down")
}
