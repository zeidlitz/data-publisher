package database

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/zeidlitz/data-publisher/internal/config"
	"github.com/zeidlitz/data-publisher/internal/models"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

type DuckDbClient struct {
	db *sql.DB
}

func New(cfg config.Config) (DuckDbClient, error) {
	var client DuckDbClient
	connector, err := duckdb.NewConnector(cfg.DuckDbConn, nil)
	if err != nil {
		return client, err
	}

	db := sql.OpenDB(connector)
	if err := db.Ping(); err != nil {
		return client, fmt.Errorf("could not ping database %w", err)
	}

	err = runMigrations(db)
	if err != nil {
		return client, err
	}

	client.db = db
	slog.Info("database initialized successfully", "path", cfg.DuckDbConn)
	return client, nil
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

func (c *DuckDbClient) BatchInsert(ctx context.Context, results []models.AnalysisResult) error {
	if len(results) == 0 {
		return nil
	}

	conn, err := c.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	defer conn.Close()

	return conn.Raw(func(driverConn any) error {
		nativeConn, ok := driverConn.(*duckdb.Conn)
		if !ok {
			return fmt.Errorf("invalid driver connection type")
		}

		appender, err := duckdb.NewAppenderFromConn(nativeConn, "", "sentiment_analysis")
		if err != nil {
			return fmt.Errorf("failed to create appender: %w", err)
		}
		defer appender.Close()

		for _, res := range results {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				createdAt := time.Unix(res.UnixTimestamp, 0)

				if err := appender.AppendRow(
					createdAt,
					res.Subreddit,
					res.Title,
					res.Body,
					res.Categories,
					res.Sentiment,
					res.UnixTimestamp,
				); err != nil {
					return fmt.Errorf("append error at subreddit %s: %w", res.Subreddit, err)
				}
			}
		}

		slog.Info("flushing to database")
		return appender.Flush()
	})
}
