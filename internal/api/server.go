package api

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/zeidlitz/data-publisher/internal/config"
	"github.com/zeidlitz/data-publisher/internal/database"
)

type Application struct {
	dbClient database.DuckDbClient
}

func Serve(cfg config.Config, db database.DuckDbClient, ctx context.Context) error {
	app := &Application{dbClient: db}
	srv := &http.Server{Addr: cfg.ServerAddr, Handler: app.Routes()}
	go func() {
		<-ctx.Done()
		slog.Info("shutting down server")
		srv.Shutdown(context.Background())
	}()

	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}
	return nil
}
