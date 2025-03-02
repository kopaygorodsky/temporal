package tests

import (
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/persistence/serialization"
	"testing"
)

type OracleSQLSuite struct {
	suite.Suite
	pluginName string
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
