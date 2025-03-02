package oracle

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/oracle/session"
	"go.temporal.io/server/common/resolver"
)

const PluginName = "oracle"

type plugin struct {
}

var _ sqlplugin.Plugin = (*plugin)(nil)

func init() {
	sql.RegisterPlugin(PluginName, &plugin{})
}

func (p plugin) CreateDB(dbKind sqlplugin.DbKind, cfg *config.SQL, r resolver.ServiceResolver, logger log.Logger, metricsHandler metrics.Handler) (sqlplugin.DB, error) {
	connect := func() (*sqlx.DB, error) {
		if cfg.Connect != nil {
			return cfg.Connect(cfg)
		}
		return p.createDBConnection(dbKind, cfg, r)
	}
	handle := sqlplugin.NewDatabaseHandle(connect, isConnNeedsRefreshError, logger, metricsHandler, clock.NewRealTimeSource())
	db := newDB(dbKind, cfg.DatabaseName, handle, nil)
	return db, nil
}

func (p plugin) CreateAdminDB(dbKind sqlplugin.DbKind, cfg *config.SQL, r resolver.ServiceResolver, logger log.Logger, metricsHandler metrics.Handler) (sqlplugin.AdminDB, error) {
	connect := func() (*sqlx.DB, error) {
		if cfg.Connect != nil {
			return cfg.Connect(cfg)
		}
		return p.createDBConnection(dbKind, cfg, r)
	}
	handle := sqlplugin.NewDatabaseHandle(connect, isConnNeedsRefreshError, logger, metricsHandler, clock.NewRealTimeSource())
	db := newDB(dbKind, cfg.DatabaseName, handle, nil)
	return db, nil
}

func (p plugin) createDBConnection(dbKind sqlplugin.DbKind, cfg *config.SQL, resolver resolver.ServiceResolver) (*sqlx.DB, error) {
	oracleSession, err := session.NewSession(dbKind, cfg, resolver)
	if err != nil {
		return nil, fmt.Errorf("failed to create session for oracle database %v: %v", dbKind, err)
	}
	return oracleSession.DB, nil
}

func isConnNeedsRefreshError(err error) bool {
	return false
}
