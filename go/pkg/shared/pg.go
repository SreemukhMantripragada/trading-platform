package shared

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type DB interface {
	Exec(ctx context.Context, sql string, args ...any) error
	Close()
}

type PgxDB struct {
	pool *pgxpool.Pool
}

func NewPgxPool(ctx context.Context, cfg PostgresConfig) (*PgxDB, error) {
	url := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)
	p, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	p.Config().MaxConns = int32(cfg.PoolMax)
	return &PgxDB{pool: p}, nil
}

func (d *PgxDB) Exec(ctx context.Context, sql string, args ...any) error {
	_, err := d.pool.Exec(ctx, sql, args...)
	return err
}

func (d *PgxDB) Acquire() (*pgxpool.Conn, error) { return d.pool.Acquire(context.Background()) }

func (d *PgxDB) Close() { d.pool.Close() }
