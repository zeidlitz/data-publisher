package main

import (
	"log/slog"

	"github.com/redis/go-redis/v9"
	"github.com/zeidlitz/data-publisher/internal/config"
	"github.com/zeidlitz/data-publisher/internal/database"
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
}
