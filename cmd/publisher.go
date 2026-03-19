package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/redis/go-redis/v9"
	"github.com/zeidlitz/data-publisher/internal/api"
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

	ctx := context.Background()
	db, err := database.New(ctx, cfg)
	if err != nil {
		slog.Warn("database initialization", "conn", cfg.DuckDbConn, "err", err)
		os.Exit(1)
	}

	redisClient := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	worker := worker.NewWorker(*db, redisClient, cfg)
	if err := worker.EnsureGroup(ctx); err != nil {
		slog.Error("could not bootstrap Redis group", "error", err)
		os.Exit(1)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := worker.Run(ctx); err != nil {
			slog.Error("worker failed", "err", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := api.Serve(cfg, *db, ctx); err != nil {
			slog.Error("api failed", "err", err)
		}
	}()

	wg.Wait()
	slog.Info("shutting down")
}
