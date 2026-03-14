package database

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"log/slog"
	"sort"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/zeidlitz/data-publisher/internal/config"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

type DuckDbClient struct {
	DB        *sql.DB
	Connector *duckdb.Connector
}

func New(ctx context.Context, cfg config.Config) (*DuckDbClient, error) {
	connector, err := duckdb.NewConnector(cfg.DuckDbConn, nil)
	if err != nil {
		return nil, err
	}

	db := sql.OpenDB(connector)
	if err := db.PingContext(ctx); err != nil {
		connector.Close()
		return nil, fmt.Errorf("could not ping database: %w", err)
	}

	if err := runMigrations(db); err != nil {
		db.Close()
		return nil, err
	}

	return &DuckDbClient{
		Connector: connector,
		DB:        db,
	}, nil
}

func runMigrations(db *sql.DB) error {
	entries, err := migrationFiles.ReadDir("migrations")
	if err != nil {
		return err
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	for _, entry := range entries {
		slog.Info("applying migration ", "schema", entry.Name())
		content, err := migrationFiles.ReadFile("migrations/" + entry.Name())
		if err != nil {
			return err
		}

		_, err = db.Exec(string(content))
		if err != nil {
			return fmt.Errorf("error in %s : %w", entry.Name(), err)
		}
	}
	return nil
}

func (c *DuckDbClient) Close() error {
	if err := c.DB.Close(); err != nil {
		return err
	}
	return c.Connector.Close()
}
