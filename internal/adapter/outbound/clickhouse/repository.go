package clickhouse

import (
	"context"
	"fmt"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Config struct {
	Addr     string
	Database string
	Username string
	Password string
}

type Repository struct {
	conn driver.Conn
}

func NewRepository(ctx context.Context, cfg Config) (*Repository, error) {
	if cfg.Database == "" {
		cfg.Database = "default"
	}
	if cfg.Username == "" {
		cfg.Username = "default"
	}

	conn, err := ch.Open(&ch.Options{
		Addr: []string{cfg.Addr},
		Auth: ch.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		Compression:     &ch.Compression{Method: ch.CompressionLZ4},
		DialTimeout:     5 * time.Second,
		MaxOpenConns:    24,
		MaxIdleConns:    12,
		ConnMaxLifetime: 30 * time.Minute,
	})
	if err != nil {
		return nil, fmt.Errorf("open clickhouse: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}
	return &Repository{conn: conn}, nil
}

func (r *Repository) Migrate(ctx context.Context) error {
	for _, stmt := range schemaStatements {
		if err := r.conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("exec migration: %w", err)
		}
	}
	return nil
}

func (r *Repository) Close() error {
	return r.conn.Close()
}
