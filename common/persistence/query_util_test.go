// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package persistence

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/log"
)

type (
	queryUtilSuite struct {
		suite.Suite
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		logger log.Logger
	}
)

func TestQueryUtilSuite(t *testing.T) {
	s := new(queryUtilSuite)
	suite.Run(t, s)
}

func (s *queryUtilSuite) SetupTest() {
	s.logger = log.NewTestLogger()
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
}

func (s *queryUtilSuite) TestLoadAndSplitQueryFromReaders() {
	input := `
		CREATE TABLE test (
			id BIGINT not null,
			col1 BIGINT, -- comment with unmatched parenthesis )
			col2 VARCHAR(255),
			PRIMARY KEY (id)
		);

		CREATE INDEX test_idx ON test (col1);

		--begin
		CREATE TRIGGER test_ai AFTER INSERT ON test
		BEGIN
			SELECT *, 'string with unmatched chars ")' FROM test;
			--end
		END;

		-- trailing comment
	`
	statements, err := LoadAndSplitQueryFromReaders([]io.Reader{bytes.NewBufferString(input)})
	s.NoError(err)
	s.Equal(3, len(statements))
	s.Equal(
		`CREATE TABLE test (
			id BIGINT not null,
			col1 BIGINT,
			col2 VARCHAR(255),
			PRIMARY KEY (id)
		);`,
		statements[0],
	)
	s.Equal(`CREATE INDEX test_idx ON test (col1);`, statements[1])
	// comments are removed, but the inner content is not trimmed
	s.Equal(
		`CREATE TRIGGER test_ai AFTER INSERT ON test
		BEGIN
			SELECT *, 'string with unmatched chars ")' FROM test;
			
		END;`,
		statements[2],
	)

	input = "CREATE TABLE test (;"
	statements, err = LoadAndSplitQueryFromReaders([]io.Reader{bytes.NewBufferString(input)})
	s.Error(err, "error reading contents: unmatched left parenthesis")
	s.Nil(statements)

	input = "CREATE TABLE test ());"
	statements, err = LoadAndSplitQueryFromReaders([]io.Reader{bytes.NewBufferString(input)})
	s.Error(err, "error reading contents: unmatched right parenthesis")
	s.Nil(statements)

	input = "begin"
	statements, err = LoadAndSplitQueryFromReaders([]io.Reader{bytes.NewBufferString(input)})
	s.Error(err, "error reading contents: unmatched `BEGIN` keyword")
	s.Nil(statements)

	input = "end"
	statements, err = LoadAndSplitQueryFromReaders([]io.Reader{bytes.NewBufferString(input)})
	s.Error(err, "error reading contents: unmatched `END` keyword")
	s.Nil(statements)

	input = "select ' from test;"
	statements, err = LoadAndSplitQueryFromReaders([]io.Reader{bytes.NewBufferString(input)})
	s.Error(err, "error reading contents: unmatched quotes")
	s.Nil(statements)
}

func (s *queryUtilSuite) TestHasWordAt() {
	s.True(hasWordAt("BEGIN", "BEGIN", 0))
	s.True(hasWordAt(" BEGIN ", "BEGIN", 1))
	s.True(hasWordAt(")BEGIN;", "BEGIN", 1))
	s.False(hasWordAt("BEGIN", "BEGIN", 1))
	s.False(hasWordAt("sBEGIN", "BEGIN", 1))
	s.False(hasWordAt("BEGINs", "BEGIN", 0))
	s.False(hasWordAt("7BEGIN", "BEGIN", 1))
	s.False(hasWordAt("BEGIN7", "BEGIN", 0))
}

func (s *queryUtilSuite) TestWithStupidOracleQuery() {
	query := `CREATE TABLE executions_visibility (
    namespace_id                       CHAR(64)        NOT NULL,
    run_id                             CHAR(64)        NOT NULL,
    version_num                        NUMBER(19)      DEFAULT 0 NOT NULL,
    start_time                         TIMESTAMP(6)    NOT NULL,
    execution_time                     TIMESTAMP(6)    NOT NULL,
    workflow_id                        VARCHAR2(255)   NOT NULL,
    workflow_type_name                 VARCHAR2(255)   NOT NULL,
    status                             NUMBER(10)      NOT NULL,
    close_time                         TIMESTAMP(6)    NULL,
    history_length                     NUMBER(19)      NULL,
    history_size_bytes                 NUMBER(19)      NULL,
    execution_duration                 NUMBER(19)      NULL,
    state_transition_count             NUMBER(19)      NULL,
    memo                               BLOB            NULL,
    encoding                           VARCHAR2(64)    NOT NULL,
    task_queue                         VARCHAR2(255)   DEFAULT '' NOT NULL,
    search_attributes                  CLOB            NULL,
    parent_workflow_id                 VARCHAR2(255)   NULL,
    parent_run_id                      VARCHAR2(255)   NULL,
    root_workflow_id                   VARCHAR2(255)   DEFAULT '' NOT NULL,
    root_run_id                        VARCHAR2(255)   DEFAULT '' NOT NULL,
    BatcherUser                        VARCHAR2(255)   GENERATED ALWAYS AS (
                                                          JSON_VALUE(search_attributes, '$.BatcherUser' 
                                                          RETURNING VARCHAR2(255))
                                                       ) VIRTUAL,
    TemporalScheduledStartTime         TIMESTAMP(6)    GENERATED ALWAYS AS (
                                                          TO_TIMESTAMP_TZ(
                                                             JSON_VALUE(search_attributes, '$.TemporalScheduledStartTime' 
                                                             RETURNING VARCHAR2(255)), 
                                                             'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'
                                                          )
                                                       ) VIRTUAL,
    TemporalScheduledById              VARCHAR2(255)   GENERATED ALWAYS AS (
                                                          JSON_VALUE(search_attributes, '$.TemporalScheduledById' 
                                                          RETURNING VARCHAR2(255))
                                                       ) VIRTUAL,
    TemporalSchedulePaused             NUMBER(1)       GENERATED ALWAYS AS (
                                                          CASE JSON_VALUE(search_attributes, '$.TemporalSchedulePaused' 
                                                               RETURNING VARCHAR2(5))
                                                               WHEN 'true' THEN 1 
                                                               WHEN 'false' THEN 0 
                                                               ELSE NULL 
                                                          END
                                                       ) VIRTUAL,
    TemporalNamespaceDivision          VARCHAR2(255)   GENERATED ALWAYS AS (
                                                          JSON_VALUE(search_attributes, '$.TemporalNamespaceDivision' 
                                                          RETURNING VARCHAR2(255))
                                                       ) VIRTUAL,
    TemporalWorkerDeploymentVersion    VARCHAR2(255)   GENERATED ALWAYS AS (
                                                          JSON_VALUE(search_attributes, '$.TemporalWorkerDeploymentVersion' 
                                                          RETURNING VARCHAR2(255))
                                                       ) VIRTUAL,
    TemporalWorkflowVersioningBehavior VARCHAR2(255)   GENERATED ALWAYS AS (
                                                          JSON_VALUE(search_attributes, '$.TemporalWorkflowVersioningBehavior' 
                                                          RETURNING VARCHAR2(255))
                                                       ) VIRTUAL,
    TemporalWorkerDeployment           VARCHAR2(255)   GENERATED ALWAYS AS (
                                                          JSON_VALUE(search_attributes, '$.TemporalWorkerDeployment' 
                                                          RETURNING VARCHAR2(255))
                                                       ) VIRTUAL,
    PRIMARY KEY (namespace_id, run_id));
`

	statements, err := LoadAndSplitQueryFromReaders([]io.Reader{bytes.NewBufferString(query)})
	s.NoError(err)
	s.Len(statements, 1)
}

func (s *queryUtilSuite) TestDumpOracleFucntion() {
	query := `CREATE OR REPLACE FUNCTION get_close_time_or_max(close_time IN TIMESTAMP) RETURN TIMESTAMP IS BEGIN RETURN COALESCE(close_time, TIMESTAMP '9999-12-31 23:59:59'); END;`
	statements, err := LoadAndSplitQueryFromReaders([]io.Reader{bytes.NewBufferString(query)})
	s.NoError(err)
	s.Len(statements, 1)
}
