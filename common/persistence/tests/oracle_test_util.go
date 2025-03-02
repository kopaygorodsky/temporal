package tests

import (
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"testing"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/oracle"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/environment"
	"go.uber.org/zap/zaptest"
)

const (
	testOracleClusterName        = "temporal_oracle_cluster"
	testOracleUser               = "C##temporal"
	testOraclePassword           = "temporal"
	testOracleConnectionProtocol = "tcp"
	testOracleDatabaseName       = "FREE"

	// TODO hard code this dir for now
	//  need to merge persistence test config / initialization in one place
	testOracleExecutionSchema  = "../../../schema/oracle/temporal/schema.sql"
	testOracleVisibilitySchema = "../../../schema/oracle/visibility/schema.sql"
)

type (
	OracleTestData struct {
		Cfg     *config.SQL
		Factory *sql.Factory
		Logger  log.Logger
		Metrics *metricstest.Capture
	}
)

func setUpOracleTest(t *testing.T) (OracleTestData, func()) {
	var testData OracleTestData
	testData.Cfg = NewOracleSQLConfig()
	testData.Logger = log.NewZapLogger(zaptest.NewLogger(t))
	mh := metricstest.NewCaptureHandler()
	testData.Metrics = mh.StartCapture()
	SetupOracleSQLDatabase(t, testData.Cfg)
	SetupOracleSchema(t, testData.Cfg)

	testData.Factory = sql.NewFactory(
		*testData.Cfg,
		resolver.NewNoopResolver(),
		testOracleClusterName,
		testData.Logger,
		mh,
	)

	tearDown := func() {
		testData.Factory.Close()
		mh.StopCapture(testData.Metrics)
		TearDownOracleDatabase(t, testData.Cfg)
	}

	return testData, tearDown
}

func NewOracleSQLConfig() *config.SQL {
	return &config.SQL{
		User:     testOracleUser,
		Password: testOraclePassword,
		ConnectAddr: net.JoinHostPort(
			environment.GetOracleAddress(),
			strconv.Itoa(environment.GetOraclePort()),
		),
		ConnectProtocol: testOracleConnectionProtocol,
		PluginName:      oracle.PluginName,
		DatabaseName:    testOracleDatabaseName,
	}
}

func SetupOracleSQLDatabase(t *testing.T, cfg *config.SQL) {
	adminCfg := *cfg

	db, err := sql.NewSQLAdminDB(sqlplugin.DbKindUnknown, &adminCfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create Oracle admin DB: %v", err)
	}
	defer func() { _ = db.Close() }()

	err = db.CreateDatabase(cfg.DatabaseName)
	if err != nil {
		t.Fatalf("unable to create Oracle database: %v", err)
	}
}

func SetupOracleSchema(t *testing.T, cfg *config.SQL) {
	db, err := sql.NewSQLAdminDB(sqlplugin.DbKindUnknown, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create Oracle admin DB: %v", err)
	}
	defer func() { _ = db.Close() }()

	schemaPath, err := filepath.Abs(testOracleExecutionSchema)
	if err != nil {
		t.Fatal(err)
	}

	statements, err := p.LoadAndSplitQuery([]string{schemaPath})
	if err != nil {
		t.Fatal(err)
	}

	for _, stmt := range statements {
		stmt = prepareOracleSQLStmt(stmt)
		if err = db.Exec(stmt); err != nil {
			defer db.DropAllTables(cfg.DatabaseName)
			t.Fatal(fmt.Errorf("error executing statement %s: %v", stmt, err))
		}
	}

	//@todo visibility will be implemented later
	//statements, err = p.LoadAndSplitQuery([]string{schemaPath})
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//for _, stmt := range statements {
	//	if stmt[len(stmt)-1] == ';' {
	//		stmt = stmt[:len(stmt)-1]
	//	}
	//	if err = db.Exec(stmt); err != nil {
	//		defer db.DropAllTables(cfg.DatabaseName)
	//		t.Fatal(err)
	//	}
	//}
}

func TearDownOracleDatabase(t *testing.T, cfg *config.SQL) {
	db, err := sql.NewSQLAdminDB(sqlplugin.DbKindUnknown, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create Oracle admin DB: %v", err)
	}
	defer func() { _ = db.Close() }()

	err = db.DropAllTables(cfg.DatabaseName)
	if err != nil {
		t.Fatalf("unable to drop Oracle database: %v", err)
	}
	t.Logf("Oracle database teared down")
}

//@todo will be removed once oracle parser is written
func prepareOracleSQLStmt(stmt string) string {
	if stmt[len(stmt)-1] == ';' {
		return stmt[:len(stmt)-1]
	}

	return stmt
}
