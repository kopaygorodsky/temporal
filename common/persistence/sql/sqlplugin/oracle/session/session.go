package session

import (
	"fmt"
	"github.com/iancoleman/strcase"
	"github.com/jmoiron/sqlx"
	go_ora "github.com/sijms/go-ora/v2"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/resolver"
	"strconv"
	"strings"
)

type Session struct {
	DB *sqlx.DB
}

func NewSession(
	dbKind sqlplugin.DbKind,
	cfg *config.SQL,
	resolver resolver.ServiceResolver,
) (*Session, error) {
	db, err := createConnection(cfg, resolver)
	if err != nil {
		return nil, err
	}
	return &Session{DB: db}, nil
}

func createConnection(
	cfg *config.SQL,
	resolver resolver.ServiceResolver,
) (*sqlx.DB, error) {
	dsn, err := buildDSN(cfg, resolver)
	if err != nil {
		return nil, err
	}

	sqlx.BindDriver("oracle", sqlx.NAMED)

	db, err := sqlx.Open("oracle", dsn)
	if err != nil {
		return nil, err
	}
	if cfg.MaxConns > 0 {
		db.SetMaxOpenConns(cfg.MaxConns)
	}
	if cfg.MaxIdleConns > 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConns)
	}
	if cfg.MaxConnLifetime > 0 {
		db.SetConnMaxLifetime(cfg.MaxConnLifetime)
	}

	db.MapperFunc(strcase.ToScreamingSnake)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("unable to connect to oracle database: %v", err)
	}

	return db, nil
}

func buildDSN(
	cfg *config.SQL,
	r resolver.ServiceResolver,
) (string, error) {
	addrParts := strings.Split(r.Resolve(cfg.ConnectAddr)[0], ":")
	if len(addrParts) != 2 {
		return "", fmt.Errorf("invalid connection address: %s", cfg.ConnectAddr)
	}

	port, err := strconv.Atoi(addrParts[1])
	if err != nil {
		return "", fmt.Errorf("invalid connection port: %s", cfg.ConnectAddr)
	}

	return go_ora.BuildUrl(addrParts[0], port, cfg.DatabaseName, cfg.User, cfg.Password, nil), nil
}
