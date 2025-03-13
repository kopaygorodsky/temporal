package tests

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/oracle"
	sqltests "go.temporal.io/server/common/persistence/sql/sqlplugin/tests"
	"go.temporal.io/server/common/resolver"
)

func TestOracleShardStoreSuite(t *testing.T) {
	testData, tearDown := setUpOracleTest(t)
	defer tearDown()

	shardStore, err := testData.Factory.NewShardStore()
	if err != nil {
		t.Fatalf("unable to create Oracle DB: %v", err)
	}

	s := NewShardSuite(
		t,
		shardStore,
		serialization.NewSerializer(),
		testData.Logger,
	)
	suite.Run(t, s)
}

func TestOracleExecutionMutableStateStoreSuite(t *testing.T) {
	testData, tearDown := setUpOracleTest(t)
	defer tearDown()

	shardStore, err := testData.Factory.NewShardStore()
	if err != nil {
		t.Fatalf("unable to create Oracle DB: %v", err)
	}
	executionStore, err := testData.Factory.NewExecutionStore()
	if err != nil {
		t.Fatalf("unable to create Oracle DB: %v", err)
	}

	s := NewExecutionMutableStateSuite(
		t,
		shardStore,
		executionStore,
		serialization.NewSerializer(),
		&persistence.HistoryBranchUtilImpl{},
		testData.Logger,
	)
	suite.Run(t, s)
}

func TestOracleExecutionMutableStateTaskStoreSuite(t *testing.T) {
	testData, tearDown := setUpOracleTest(t)
	defer tearDown()

	shardStore, err := testData.Factory.NewShardStore()
	if err != nil {
		t.Fatalf("unable to create Oracle DB: %v", err)
	}
	executionStore, err := testData.Factory.NewExecutionStore()
	if err != nil {
		t.Fatalf("unable to create Oracle DB: %v", err)
	}

	s := NewExecutionMutableStateTaskSuite(
		t,
		shardStore,
		executionStore,
		serialization.NewSerializer(),
		testData.Logger,
	)
	suite.Run(t, s)
}

func TestOracleHistoryStoreSuite(t *testing.T) {
	testData, tearDown := setUpOracleTest(t)
	defer tearDown()

	store, err := testData.Factory.NewExecutionStore()
	if err != nil {
		t.Fatalf("unable to create Oracle DB: %v", err)
	}

	s := NewHistoryEventsSuite(t, store, testData.Logger)
	suite.Run(t, s)
}

func TestOracleTaskQueueSuite(t *testing.T) {
	testData, tearDown := setUpOracleTest(t)
	defer tearDown()

	taskQueueStore, err := testData.Factory.NewTaskStore()
	if err != nil {
		t.Fatalf("unable to create Oracle DB: %v", err)
	}

	s := NewTaskQueueSuite(t, taskQueueStore, testData.Logger)
	suite.Run(t, s)
}

func TestOracleTaskQueueTaskSuite(t *testing.T) {
	testData, tearDown := setUpOracleTest(t)
	defer tearDown()

	taskQueueStore, err := testData.Factory.NewTaskStore()
	if err != nil {
		t.Fatalf("unable to create Oracle DB: %v", err)
	}

	s := NewTaskQueueTaskSuite(t, taskQueueStore, testData.Logger)
	suite.Run(t, s)
}

func TestOracleTaskQueueUserDataSuite(t *testing.T) {
	testData, tearDown := setUpOracleTest(t)
	defer tearDown()

	taskQueueStore, err := testData.Factory.NewTaskStore()
	if err != nil {
		t.Fatalf("unable to create Oracle DB: %v", err)
	}

	s := NewTaskQueueUserDataSuite(t, taskQueueStore, testData.Logger)
	suite.Run(t, s)
}

//func TestOracleVisibilityPersistenceSuite(t *testing.T) {
//	s := &VisibilityPersistenceSuite{
//		TestBase: persistencetests.NewTestBaseWithSQL(persistencetests.GetOracleTestClusterOption()),
//	}
//	suite.Run(t, s)
//}

// TODO: Merge persistence-tests into the tests directory.

func TestOracleHistoryV2PersistenceSuite(t *testing.T) {
	s := new(persistencetests.HistoryV2PersistenceSuite)
	s.TestBase = persistencetests.NewTestBaseWithSQL(persistencetests.GetOracleTestClusterOption())
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

func TestOracleMetadataPersistenceSuiteV2(t *testing.T) {
	s := new(persistencetests.MetadataPersistenceSuiteV2)
	s.TestBase = persistencetests.NewTestBaseWithSQL(persistencetests.GetOracleTestClusterOption())
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

func TestOracleClusterMetadataPersistence(t *testing.T) {
	s := new(persistencetests.ClusterMetadataManagerSuite)
	s.TestBase = persistencetests.NewTestBaseWithSQL(persistencetests.GetOracleTestClusterOption())
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

func TestOracleQueuePersistence(t *testing.T) {
	s := new(persistencetests.QueuePersistenceSuite)
	s.TestBase = persistencetests.NewTestBaseWithSQL(persistencetests.GetOracleTestClusterOption())
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

func TestOracleQueueV2(t *testing.T) {
	testData, tearDown := setUpOracleTest(t)
	t.Cleanup(tearDown)
	RunQueueV2TestSuiteForSQL(t, testData.Factory)
}

// SQL store tests

func TestOracleNamespaceSuite(t *testing.T) {
	cfg := NewOracleSQLConfig()
	SetupOracleSQLDatabase(t, cfg)
	SetupOracleSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create Oracle DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownOracleDatabase(t, cfg)
	}()

	s := sqltests.NewNamespaceSuite(t, store)
	suite.Run(t, s)
}

func TestOracleQueueMessageSuite(t *testing.T) {
	cfg := NewOracleSQLConfig()
	SetupOracleSQLDatabase(t, cfg)
	SetupOracleSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create Oracle DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownOracleDatabase(t, cfg)
	}()

	s := sqltests.NewQueueMessageSuite(t, store)
	suite.Run(t, s)
}

func TestOracleQueueMetadataSuite(t *testing.T) {
	cfg := NewOracleSQLConfig()
	SetupOracleSQLDatabase(t, cfg)
	SetupOracleSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create Oracle DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownOracleDatabase(t, cfg)
	}()

	s := sqltests.NewQueueMetadataSuite(t, store)
	suite.Run(t, s)
}

func TestOracleMatchingTaskSuite(t *testing.T) {
	cfg := NewOracleSQLConfig()
	SetupOracleSQLDatabase(t, cfg)
	SetupOracleSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create Oracle DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownOracleDatabase(t, cfg)
	}()

	s := sqltests.NewMatchingTaskSuite(t, store)
	suite.Run(t, s)
}

func TestOracleMatchingTaskQueueSuite(t *testing.T) {
	cfg := NewOracleSQLConfig()
	SetupOracleSQLDatabase(t, cfg)
	SetupOracleSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create Oracle DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownOracleDatabase(t, cfg)
	}()

	s := sqltests.NewMatchingTaskQueueSuite(t, store)
	suite.Run(t, s)
}

func TestOracleHistoryShardSuite(t *testing.T) {
	cfg := NewOracleSQLConfig()
	SetupOracleSQLDatabase(t, cfg)
	SetupOracleSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create Oracle DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownOracleDatabase(t, cfg)
	}()

	s := sqltests.NewHistoryShardSuite(t, store)
	suite.Run(t, s)
}

func TestOracleHistoryNodeSuite(t *testing.T) {
	cfg := NewOracleSQLConfig()
	SetupOracleSQLDatabase(t, cfg)
	SetupOracleSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create Oracle DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownOracleDatabase(t, cfg)
	}()

	s := sqltests.NewHistoryNodeSuite(t, store)
	suite.Run(t, s)
}

func TestOracleHistoryTreeSuite(t *testing.T) {
	cfg := NewOracleSQLConfig()
	SetupOracleSQLDatabase(t, cfg)
	SetupOracleSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create Oracle DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownOracleDatabase(t, cfg)
	}()

	s := sqltests.NewHistoryTreeSuite(t, store)
	suite.Run(t, s)
}

func TestOracleHistoryCurrentExecutionSuite(t *testing.T) {
	cfg := NewOracleSQLConfig()
	SetupOracleSQLDatabase(t, cfg)
	SetupOracleSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create Oracle DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownOracleDatabase(t, cfg)
	}()

	s := sqltests.NewHistoryCurrentExecutionSuite(t, store)
	suite.Run(t, s)
}

func TestOracleHistoryExecutionSuite(t *testing.T) {
	cfg := NewOracleSQLConfig()
	SetupOracleSQLDatabase(t, cfg)
	SetupOracleSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create Oracle DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownOracleDatabase(t, cfg)
	}()

	s := sqltests.NewHistoryExecutionSuite(t, store)
	suite.Run(t, s)
}

func TestOracleHistoryTransferTaskSuite(t *testing.T) {
	cfg := NewOracleSQLConfig()
	SetupOracleSQLDatabase(t, cfg)
	SetupOracleSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create Oracle DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownOracleDatabase(t, cfg)
	}()

	s := sqltests.NewHistoryTransferTaskSuite(t, store)
	suite.Run(t, s)
}

func TestOracleHistoryTimerTaskSuite(t *testing.T) {
	cfg := NewOracleSQLConfig()
	SetupOracleSQLDatabase(t, cfg)
	SetupOracleSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create Oracle DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownOracleDatabase(t, cfg)
	}()

	s := sqltests.NewHistoryTimerTaskSuite(t, store)
	suite.Run(t, s)
}

func TestOracleHistoryReplicationTaskSuite(t *testing.T) {
	cfg := NewOracleSQLConfig()
	SetupOracleSQLDatabase(t, cfg)
	SetupOracleSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create Oracle DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownOracleDatabase(t, cfg)
	}()

	s := sqltests.NewHistoryReplicationTaskSuite(t, store)
	suite.Run(t, s)
}

//func TestOracleHistoryVisibilityTaskSuite(t *testing.T) {
//	cfg := NewOracleSQLConfig()
//	SetupOracleSQLDatabase(t, cfg)
//	SetupOracleSchema(t, cfg)
//	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
//	if err != nil {
//		t.Fatalf("unable to create Oracle DB: %v", err)
//	}
//	defer func() {
//		_ = store.Close()
//		TearDownOracleDatabase(t, cfg)
//	}()
//
//	s := sqltests.NewHistoryVisibilityTaskSuite(t, store)
//	suite.Run(t, s)
//}

func TestOracleHistoryReplicationDLQTaskSuite(t *testing.T) {
	cfg := NewOracleSQLConfig()
	SetupOracleSQLDatabase(t, cfg)
	SetupOracleSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create Oracle DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownOracleDatabase(t, cfg)
	}()

	s := sqltests.NewHistoryReplicationDLQTaskSuite(t, store)
	suite.Run(t, s)
}

func TestOracleHistoryExecutionBufferSuite(t *testing.T) {
	cfg := NewOracleSQLConfig()
	SetupOracleSQLDatabase(t, cfg)
	SetupOracleSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create Oracle DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownOracleDatabase(t, cfg)
	}()

	s := sqltests.NewHistoryExecutionBufferSuite(t, store)
	suite.Run(t, s)
}

func TestOracleHistoryExecutionActivitySuite(t *testing.T) {
	cfg := NewOracleSQLConfig()
	SetupOracleSQLDatabase(t, cfg)
	SetupOracleSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create Oracle DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownOracleDatabase(t, cfg)
	}()

	s := sqltests.NewHistoryExecutionActivitySuite(t, store)
	suite.Run(t, s)
}

func TestOracleHistoryExecutionChildWorkflowSuite(t *testing.T) {
	cfg := NewOracleSQLConfig()
	SetupOracleSQLDatabase(t, cfg)
	SetupOracleSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create Oracle DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownOracleDatabase(t, cfg)
	}()

	s := sqltests.NewHistoryExecutionChildWorkflowSuite(t, store)
	suite.Run(t, s)
}

func TestOracleHistoryExecutionTimerSuite(t *testing.T) {
	cfg := NewOracleSQLConfig()
	SetupOracleSQLDatabase(t, cfg)
	SetupOracleSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create Oracle DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownOracleDatabase(t, cfg)
	}()

	s := sqltests.NewHistoryExecutionTimerSuite(t, store)
	suite.Run(t, s)
}

func TestOracleHistoryExecutionRequestCancelSuite(t *testing.T) {
	cfg := NewOracleSQLConfig()
	SetupOracleSQLDatabase(t, cfg)
	SetupOracleSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create Oracle DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownOracleDatabase(t, cfg)
	}()

	s := sqltests.NewHistoryExecutionRequestCancelSuite(t, store)
	suite.Run(t, s)
}

func TestOracleHistoryExecutionSignalSuite(t *testing.T) {
	cfg := NewOracleSQLConfig()
	SetupOracleSQLDatabase(t, cfg)
	SetupOracleSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create Oracle DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownOracleDatabase(t, cfg)
	}()

	s := sqltests.NewHistoryExecutionSignalSuite(t, store)
	suite.Run(t, s)
}

func TestOracleHistoryExecutionSignalRequestSuite(t *testing.T) {
	cfg := NewOracleSQLConfig()
	SetupOracleSQLDatabase(t, cfg)
	SetupOracleSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create Oracle DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownOracleDatabase(t, cfg)
	}()

	s := sqltests.NewHistoryExecutionSignalRequestSuite(t, store)
	suite.Run(t, s)
}

func TestOracleVisibilitySuite(t *testing.T) {
	cfg := NewOracleSQLConfig()
	SetupOracleSQLDatabase(t, cfg)
	SetupOracleSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindVisibility, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create Oracle DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownOracleDatabase(t, cfg)
	}()

	s := sqltests.NewVisibilitySuite(t, store)
	suite.Run(t, s)
}

func TestOracleClosedConnectionError(t *testing.T) {
	testData, tearDown := setUpOracleTest(t)
	defer tearDown()

	s := newConnectionSuite(t, testData.Factory)
	suite.Run(t, s)
}

func TestOracleNexusEndpointPersistence(t *testing.T) {
	testData, tearDown := setUpOracleTest(t)
	defer tearDown()

	tableVersion := atomic.Int64{}
	store, err := testData.Factory.NewNexusEndpointStore()
	if err != nil {
		t.Fatalf("unable to create Oracle NexusEndpointStore: %v", err)
	}

	t.Run("Generic", func(t *testing.T) {
		RunNexusEndpointTestSuite(t, store, &tableVersion)
	})
}
