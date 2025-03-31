package oracle

import (
	"context"
	"database/sql"
	go_ora "github.com/sijms/go-ora/v2"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/oracle/session"
	"go.temporal.io/server/common/primitives"
	"time"
)

const (
	executionsColumns = `shard_id, namespace_id, workflow_id, run_id, next_event_id, last_write_version, data, data_encoding, state, state_encoding, db_record_version`

	createExecutionQuery = `INSERT INTO executions(` + executionsColumns + `)
 VALUES(:shard_id, :namespace_id, :workflow_id, :run_id, :next_event_id, :last_write_version, :data, :data_encoding, :state, :state_encoding, :db_record_version)`

	updateExecutionQuery = `UPDATE executions SET
 db_record_version = :db_record_version, next_event_id = :next_event_id, last_write_version = :last_write_version, 
 data = :data, data_encoding = :data_encoding, state = :state, state_encoding = :state_encoding
 WHERE shard_id = :shard_id AND namespace_id = :namespace_id AND workflow_id = :workflow_id AND run_id = :run_id`

	getExecutionQuery = `SELECT ` + executionsColumns + ` FROM executions
 WHERE shard_id = :shard_id AND namespace_id = :namespace_id AND workflow_id = :workflow_id AND run_id = :run_id`

	deleteExecutionQuery = `DELETE FROM executions 
 WHERE shard_id = :shard_id AND namespace_id = :namespace_id AND workflow_id = :workflow_id AND run_id = :run_id`

	lockExecutionQueryBase = `SELECT db_record_version, next_event_id FROM executions 
 WHERE shard_id = :shard_id AND namespace_id = :namespace_id AND workflow_id = :workflow_id AND run_id = :run_id`

	writeLockExecutionQuery = lockExecutionQueryBase + ` FOR UPDATE`
	readLockExecutionQuery  = lockExecutionQueryBase + ` FOR UPDATE NOWAIT` // Oracle uses FOR UPDATE NOWAIT instead of LOCK IN SHARE MODE

	createCurrentExecutionQuery = `INSERT INTO current_executions
(shard_id, namespace_id, workflow_id, run_id, create_request_id, state, status, start_time, last_write_version, data, data_encoding) VALUES
(:shard_id, :namespace_id, :workflow_id, :run_id, :create_request_id, :state, :status, :start_time, :last_write_version, :data, :data_encoding)`

	deleteCurrentExecutionQuery = `DELETE FROM current_executions 
WHERE shard_id = :shard_id AND namespace_id = :namespace_id AND workflow_id = :workflow_id AND run_id = :run_id`

	getCurrentExecutionQuery = `SELECT
shard_id, namespace_id, workflow_id, run_id, create_request_id, state, status, start_time, last_write_version, data, data_encoding
FROM current_executions WHERE shard_id = :shard_id AND namespace_id = :namespace_id AND workflow_id = :workflow_id`

	lockCurrentExecutionJoinExecutionsQuery = `SELECT
ce.shard_id, ce.namespace_id, ce.workflow_id, ce.run_id, ce.create_request_id, ce.state, ce.status, ce.start_time, e.last_write_version, ce.data, ce.data_encoding
FROM current_executions ce
INNER JOIN executions e ON e.shard_id = ce.shard_id AND e.namespace_id = ce.namespace_id AND e.workflow_id = ce.workflow_id AND e.run_id = ce.run_id
WHERE ce.shard_id = :shard_id AND ce.namespace_id = :namespace_id AND ce.workflow_id = :workflow_id
FOR UPDATE`

	lockCurrentExecutionQuery = getCurrentExecutionQuery + ` FOR UPDATE`

	updateCurrentExecutionsQuery = `UPDATE current_executions SET
run_id = :run_id,
create_request_id = :create_request_id,
state = :state,
status = :status,
start_time = :start_time,
last_write_version = :last_write_version,
data = :data,
data_encoding = :data_encoding
WHERE
shard_id = :shard_id AND
namespace_id = :namespace_id AND
workflow_id = :workflow_id`

	// History immediate tasks
	createHistoryImmediateTasksQuery = `INSERT INTO history_immediate_tasks(shard_id, category_id, task_id, data, data_encoding) 
 VALUES(:shard_id, :category_id, :task_id, :data, :data_encoding)`

	getHistoryImmediateTasksQuery = `SELECT task_id, data, data_encoding 
 FROM history_immediate_tasks 
 WHERE shard_id = :shard_id AND category_id = :category_id AND task_id >= :min_task_id AND task_id < :max_task_id 
 ORDER BY task_id
 FETCH FIRST :page_size ROWS ONLY`

	deleteHistoryImmediateTaskQuery       = `DELETE FROM history_immediate_tasks WHERE shard_id = :shard_id AND category_id = :category_id AND task_id = :task_id`
	rangeDeleteHistoryImmediateTasksQuery = `DELETE FROM history_immediate_tasks WHERE shard_id = :shard_id AND category_id = :category_id AND task_id >= :min_task_id AND task_id < :max_task_id`

	// History scheduled tasks
	createHistoryScheduledTasksQuery = `INSERT INTO history_scheduled_tasks (shard_id, category_id, visibility_timestamp, task_id, data, data_encoding)
  VALUES (:shard_id, :category_id, :visibility_timestamp, :task_id, :data, :data_encoding)`

	getHistoryScheduledTasksQuery = `SELECT visibility_timestamp, task_id, data, data_encoding FROM history_scheduled_tasks 
  WHERE shard_id = :shard_id 
  AND category_id = :category_id 
  AND ((visibility_timestamp >= :min_visibility_ts AND task_id >= :min_task_id) OR visibility_timestamp > :min_visibility_ts) 
  AND visibility_timestamp < :max_visibility_ts
  ORDER BY visibility_timestamp, task_id
  FETCH FIRST :page_size ROWS ONLY`

	deleteHistoryScheduledTaskQuery       = `DELETE FROM history_scheduled_tasks WHERE shard_id = :shard_id AND category_id = :category_id AND visibility_timestamp = :visibility_ts AND task_id = :task_id`
	rangeDeleteHistoryScheduledTasksQuery = `DELETE FROM history_scheduled_tasks WHERE shard_id = :shard_id AND category_id = :category_id AND visibility_timestamp >= :min_visibility_ts AND visibility_timestamp < :max_visibility_ts`

	// Transfer tasks
	createTransferTasksQuery = `INSERT INTO transfer_tasks(shard_id, task_id, data, data_encoding) 
 VALUES(:shard_id, :task_id, :data, :data_encoding)`

	getTransferTasksQuery = `SELECT task_id, data, data_encoding 
 FROM transfer_tasks 
 WHERE shard_id = :shard_id AND task_id >= :min_task_id AND task_id < :max_task_id 
 ORDER BY task_id
 FETCH FIRST :page_size ROWS ONLY`

	deleteTransferTaskQuery      = `DELETE FROM transfer_tasks WHERE shard_id = :shard_id AND task_id = :task_id`
	rangeDeleteTransferTaskQuery = `DELETE FROM transfer_tasks WHERE shard_id = :shard_id AND task_id >= :min_task_id AND task_id < :max_task_id`

	// Timer tasks
	createTimerTasksQuery = `INSERT INTO timer_tasks(shard_id, visibility_timestamp, task_id, data, data_encoding) VALUES(:shard_id, :visibility_timestamp, :task_id, :data, :data_encoding)`

	getTimerTasksQuery = `SELECT visibility_timestamp, task_id, data, data_encoding FROM timer_tasks 
  WHERE shard_id = :shard_id 
  AND ((visibility_timestamp >= :min_visibility_ts AND task_id >= :min_task_id) OR visibility_timestamp > :min_visibility_ts) 
  AND visibility_timestamp < :max_visibility_ts
  ORDER BY visibility_timestamp, task_id
  FETCH FIRST :page_size ROWS ONLY`

	deleteTimerTaskQuery      = `DELETE FROM timer_tasks WHERE shard_id = :shard_id AND visibility_timestamp = :visibility_ts AND task_id = :task_id`
	rangeDeleteTimerTaskQuery = `DELETE FROM timer_tasks WHERE shard_id = :shard_id AND visibility_timestamp >= :min_visibility_ts AND visibility_timestamp < :max_visibility_ts`

	// Replication tasks
	createReplicationTasksQuery = `INSERT INTO replication_tasks (shard_id, task_id, data, data_encoding) 
  VALUES(:shard_id, :task_id, :data, :data_encoding)`

	getReplicationTasksQuery = `SELECT task_id, data, data_encoding FROM replication_tasks 
  WHERE shard_id = :shard_id AND task_id >= :min_task_id AND task_id < :max_task_id 
  ORDER BY task_id
  FETCH FIRST :page_size ROWS ONLY`

	deleteReplicationTaskQuery      = `DELETE FROM replication_tasks WHERE shard_id = :shard_id AND task_id = :task_id`
	rangeDeleteReplicationTaskQuery = `DELETE FROM replication_tasks WHERE shard_id = :shard_id AND task_id >= :min_task_id AND task_id < :max_task_id`

	getReplicationTasksDLQQuery = `SELECT task_id, data, data_encoding FROM replication_tasks_dlq 
  WHERE source_cluster_name = :source_cluster_name
  AND shard_id = :shard_id
  AND task_id >= :min_task_id
  AND task_id < :max_task_id
  ORDER BY task_id
  FETCH FIRST :page_size ROWS ONLY`

	// Visibility tasks
	createVisibilityTasksQuery = `INSERT INTO visibility_tasks(shard_id, task_id, data, data_encoding) 
 VALUES(:shard_id, :task_id, :data, :data_encoding)`

	getVisibilityTasksQuery = `SELECT task_id, data, data_encoding 
 FROM visibility_tasks 
 WHERE shard_id = :shard_id AND task_id >= :min_task_id AND task_id < :max_task_id 
 ORDER BY task_id
 FETCH FIRST :page_size ROWS ONLY`

	deleteVisibilityTaskQuery      = `DELETE FROM visibility_tasks WHERE shard_id = :shard_id AND task_id = :task_id`
	rangeDeleteVisibilityTaskQuery = `DELETE FROM visibility_tasks WHERE shard_id = :shard_id AND task_id >= :min_task_id AND task_id < :max_task_id`

	// Buffered events
	bufferedEventsColumns     = `shard_id, namespace_id, workflow_id, run_id, data, data_encoding`
	createBufferedEventsQuery = `INSERT INTO buffered_events(` + bufferedEventsColumns + `)
VALUES (:shard_id, :namespace_id, :workflow_id, :run_id, :data, :data_encoding)`

	deleteBufferedEventsQuery = `DELETE FROM buffered_events 
WHERE shard_id = :shard_id AND namespace_id = :namespace_id AND workflow_id = :workflow_id AND run_id = :run_id`

	getBufferedEventsQuery = `SELECT data, data_encoding FROM buffered_events 
WHERE shard_id = :shard_id AND namespace_id = :namespace_id AND workflow_id = :workflow_id AND run_id = :run_id`

	// Replication DLQ
	insertReplicationTaskDLQQuery = `
INSERT INTO replication_tasks_dlq 
            (source_cluster_name, 
             shard_id, 
             task_id, 
             data, 
             data_encoding) 
VALUES     (:source_cluster_name, 
            :shard_id, 
            :task_id, 
            :data, 
            :data_encoding)
`
	deleteReplicationTaskFromDLQQuery = `
	DELETE FROM replication_tasks_dlq 
		WHERE source_cluster_name = :source_cluster_name 
		AND shard_id = :shard_id 
		AND task_id = :task_id`

	rangeDeleteReplicationTaskFromDLQQuery = `
	DELETE FROM replication_tasks_dlq 
		WHERE source_cluster_name = :source_cluster_name 
		AND shard_id = :shard_id 
		AND task_id >= :min_task_id
		AND task_id < :max_task_id`
)

type localExecutionsRow struct {
	ShardID          int32  `db:"shard_id"`
	NamespaceID      []byte `db:"namespace_id"`
	WorkflowID       string `db:"workflow_id"`
	RunID            []byte `db:"run_id"`
	NextEventID      int64  `db:"next_event_id"`
	LastWriteVersion int64  `db:"last_write_version"`
	Data             []byte `db:"data"`
	DataEncoding     string `db:"data_encoding"`
	State            []byte `db:"state"`
	StateEncoding    string `db:"state_encoding"`
	DBRecordVersion  int64  `db:"db_record_version"`
}

func newLocalExecutionsRow(r *sqlplugin.ExecutionsRow) localExecutionsRow {
	return localExecutionsRow{
		ShardID:          r.ShardID,
		NamespaceID:      r.NamespaceID,
		WorkflowID:       r.WorkflowID,
		RunID:            r.RunID,
		NextEventID:      r.NextEventID,
		LastWriteVersion: r.LastWriteVersion,
		Data:             r.Data,
		DataEncoding:     r.DataEncoding,
		State:            r.State,
		StateEncoding:    r.StateEncoding,
		DBRecordVersion:  r.DBRecordVersion,
	}
}

// InsertIntoExecutions inserts a row into executions table
func (mdb *db) InsertIntoExecutions(
	ctx context.Context,
	row *sqlplugin.ExecutionsRow,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		createExecutionQuery,
		newLocalExecutionsRow(row),
	)
}

// UpdateExecutions updates a single row in executions table
func (mdb *db) UpdateExecutions(
	ctx context.Context,
	row *sqlplugin.ExecutionsRow,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		updateExecutionQuery,
		newLocalExecutionsRow(row),
	)
}

// SelectFromExecutions reads a single row from executions table
func (mdb *db) SelectFromExecutions(
	ctx context.Context,
	filter sqlplugin.ExecutionsFilter,
) (*sqlplugin.ExecutionsRow, error) {
	var row sqlplugin.ExecutionsRow
	err := mdb.NamedGetContext(ctx,
		&row,
		getExecutionQuery,
		map[string]interface{}{
			"shard_id":     filter.ShardID,
			"namespace_id": filter.NamespaceID.Downcast(),
			"workflow_id":  filter.WorkflowID,
			"run_id":       filter.RunID.Downcast(),
		},
	)
	if err != nil {
		return nil, err
	}
	return &row, nil
}

// DeleteFromExecutions deletes a single row from executions table
func (mdb *db) DeleteFromExecutions(
	ctx context.Context,
	filter sqlplugin.ExecutionsFilter,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		deleteExecutionQuery,
		map[string]interface{}{
			"shard_id":     filter.ShardID,
			"namespace_id": filter.NamespaceID.Downcast(),
			"workflow_id":  filter.WorkflowID,
			"run_id":       filter.RunID.Downcast(),
		},
	)
}

// ReadLockExecutions acquires a read lock on a single row in executions table
func (mdb *db) ReadLockExecutions(
	ctx context.Context,
	filter sqlplugin.ExecutionsFilter,
) (int64, int64, error) {
	var executionVersion sqlplugin.ExecutionVersion
	err := mdb.NamedGetContext(ctx,
		&executionVersion,
		readLockExecutionQuery,
		map[string]interface{}{
			"shard_id":     filter.ShardID,
			"namespace_id": filter.NamespaceID.Downcast(),
			"workflow_id":  filter.WorkflowID,
			"run_id":       filter.RunID.Downcast(),
		},
	)
	return executionVersion.DBRecordVersion, executionVersion.NextEventID, err
}

// WriteLockExecutions acquires a write lock on a single row in executions table
func (mdb *db) WriteLockExecutions(
	ctx context.Context,
	filter sqlplugin.ExecutionsFilter,
) (int64, int64, error) {
	var executionVersion sqlplugin.ExecutionVersion
	err := mdb.NamedGetContext(ctx,
		&executionVersion,
		writeLockExecutionQuery,
		map[string]interface{}{
			"shard_id":     filter.ShardID,
			"namespace_id": filter.NamespaceID.Downcast(),
			"workflow_id":  filter.WorkflowID,
			"run_id":       filter.RunID.Downcast(),
		},
	)
	return executionVersion.DBRecordVersion, executionVersion.NextEventID, err
}

type execCurrentExecutionsRow struct {
	ShardID          int32                           `db:"shard_id"`
	NamespaceID      []byte                          `db:"namespace_id"`
	WorkflowID       string                          `db:"workflow_id"`
	RunID            []byte                          `db:"run_id"`
	CreateRequestID  string                          `db:"create_request_id"`
	StartTime        *go_ora.TimeStamp               `db:"start_time"`
	LastWriteVersion int64                           `db:"last_write_version"`
	State            enumsspb.WorkflowExecutionState `db:"state"`
	Status           enumspb.WorkflowExecutionStatus `db:"status"`
	Data             []byte                          `db:"data"`
	DataEncoding     string                          `db:"data_encoding"`
}

func newExecCurrentExecutionsRow(row *sqlplugin.CurrentExecutionsRow) execCurrentExecutionsRow {
	return execCurrentExecutionsRow{
		ShardID:          row.ShardID,
		NamespaceID:      row.NamespaceID,
		WorkflowID:       row.WorkflowID,
		RunID:            row.RunID,
		CreateRequestID:  row.CreateRequestID,
		StartTime:        session.GetOraTimeStampPtr(row.StartTime),
		LastWriteVersion: row.LastWriteVersion,
		State:            row.State,
		Status:           row.Status,
		Data:             row.Data,
		DataEncoding:     row.DataEncoding,
	}
}

type queryCurrentExecutionsRow struct {
	ShardID          int32
	NamespaceID      primitives.UUID
	WorkflowID       string
	RunID            primitives.UUID
	CreateRequestID  string
	StartTime        *go_ora.TimeStamp
	LastWriteVersion int64
	State            enumsspb.WorkflowExecutionState
	Status           enumspb.WorkflowExecutionStatus
	Data             []byte
	DataEncoding     string
}

func (r queryCurrentExecutionsRow) toExternalType() sqlplugin.CurrentExecutionsRow {
	return sqlplugin.CurrentExecutionsRow{
		ShardID:          r.ShardID,
		NamespaceID:      r.NamespaceID,
		WorkflowID:       r.WorkflowID,
		RunID:            r.RunID,
		CreateRequestID:  r.CreateRequestID,
		StartTime:        session.GetTimeStampPtrFromOra(r.StartTime),
		LastWriteVersion: r.LastWriteVersion,
		State:            r.State,
		Status:           r.Status,
		Data:             r.Data,
		DataEncoding:     r.DataEncoding,
	}
}

// InsertIntoCurrentExecutions inserts a single row into current_executions table
func (mdb *db) InsertIntoCurrentExecutions(
	ctx context.Context,
	row *sqlplugin.CurrentExecutionsRow,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		createCurrentExecutionQuery,
		newExecCurrentExecutionsRow(row),
	)
}

// UpdateCurrentExecutions updates a single row in current_executions table
func (mdb *db) UpdateCurrentExecutions(
	ctx context.Context,
	row *sqlplugin.CurrentExecutionsRow,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		updateCurrentExecutionsQuery,
		newExecCurrentExecutionsRow(row),
	)
}

// SelectFromCurrentExecutions reads one or more rows from current_executions table
func (mdb *db) SelectFromCurrentExecutions(
	ctx context.Context,
	filter sqlplugin.CurrentExecutionsFilter,
) (*sqlplugin.CurrentExecutionsRow, error) {
	var row queryCurrentExecutionsRow

	err := mdb.NamedGetContext(ctx,
		&row,
		getCurrentExecutionQuery,
		map[string]interface{}{
			"shard_id":     filter.ShardID,
			"namespace_id": filter.NamespaceID.Downcast(),
			"workflow_id":  filter.WorkflowID,
		},
	)
	if err != nil {
		return nil, err
	}

	res := row.toExternalType()

	return &res, nil
}

// DeleteFromCurrentExecutions deletes a single row in current_executions table
func (mdb *db) DeleteFromCurrentExecutions(
	ctx context.Context,
	filter sqlplugin.CurrentExecutionsFilter,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		deleteCurrentExecutionQuery,
		map[string]interface{}{
			"shard_id":     filter.ShardID,
			"namespace_id": filter.NamespaceID.Downcast(),
			"workflow_id":  filter.WorkflowID,
			"run_id":       filter.RunID.Downcast(),
		},
	)
}

// LockCurrentExecutions acquires a write lock on a single row in current_executions table
func (mdb *db) LockCurrentExecutions(
	ctx context.Context,
	filter sqlplugin.CurrentExecutionsFilter,
) (*sqlplugin.CurrentExecutionsRow, error) {
	var row queryCurrentExecutionsRow

	err := mdb.NamedGetContext(ctx,
		&row,
		lockCurrentExecutionQuery,
		map[string]interface{}{
			"shard_id":     filter.ShardID,
			"namespace_id": filter.NamespaceID.Downcast(),
			"workflow_id":  filter.WorkflowID,
		},
	)
	if err != nil {
		return nil, err
	}

	res := row.toExternalType()

	return &res, nil
}

// LockCurrentExecutionsJoinExecutions joins a row in current_executions with executions table and acquires a
// write lock on the result
func (mdb *db) LockCurrentExecutionsJoinExecutions(
	ctx context.Context,
	filter sqlplugin.CurrentExecutionsFilter,
) ([]sqlplugin.CurrentExecutionsRow, error) {
	var rows []sqlplugin.CurrentExecutionsRow
	err := mdb.NamedSelectContext(ctx,
		&rows,
		lockCurrentExecutionJoinExecutionsQuery,
		map[string]interface{}{
			"shard_id":     filter.ShardID,
			"namespace_id": filter.NamespaceID.Downcast(),
			"workflow_id":  filter.WorkflowID,
		},
	)
	if err != nil {
		return nil, err
	}

	//res := make([]sqlplugin.CurrentExecutionsRow, len(rows))
	//
	//for i := range rows {
	//	res[i] = rows[i].toExternalType()
	//}

	return rows, nil
}

type localHistoryImmediateTasksRow struct {
	ShardID      int32  `db:"shard_id"`
	CategoryID   int32  `db:"category_id"`
	TaskID       int64  `db:"task_id"`
	Data         []byte `db:"data"`
	DataEncoding string `db:"data_encoding"`
}

func newLocalHistoryImmediateTasksRow(row sqlplugin.HistoryImmediateTasksRow) localHistoryImmediateTasksRow {
	return localHistoryImmediateTasksRow{
		ShardID:      row.ShardID,
		CategoryID:   row.CategoryID,
		TaskID:       row.TaskID,
		Data:         row.Data,
		DataEncoding: row.DataEncoding,
	}
}

// InsertIntoHistoryImmediateTasks inserts one or more rows into history_immediate_tasks table
func (mdb *db) InsertIntoHistoryImmediateTasks(
	ctx context.Context,
	rows []sqlplugin.HistoryImmediateTasksRow,
) (sql.Result, error) {
	inserts := make([]localHistoryImmediateTasksRow, len(rows))

	for i, row := range rows {
		inserts[i] = newLocalHistoryImmediateTasksRow(row)
	}

	return mdb.NamedExecContext(ctx,
		createHistoryImmediateTasksQuery,
		inserts,
	)
}

// RangeSelectFromHistoryImmediateTasks reads one or more rows from history_immediate_tasks table
func (mdb *db) RangeSelectFromHistoryImmediateTasks(
	ctx context.Context,
	filter sqlplugin.HistoryImmediateTasksRangeFilter,
) ([]sqlplugin.HistoryImmediateTasksRow, error) {
	var rows []sqlplugin.HistoryImmediateTasksRow
	if err := mdb.NamedSelectContext(ctx,
		&rows,
		getHistoryImmediateTasksQuery,
		map[string]interface{}{
			"shard_id":    filter.ShardID,
			"category_id": filter.CategoryID,
			"min_task_id": filter.InclusiveMinTaskID,
			"max_task_id": filter.ExclusiveMaxTaskID,
			"page_size":   filter.PageSize,
		},
	); err != nil {
		return nil, err
	}
	return rows, nil
}

// DeleteFromHistoryImmediateTasks deletes one or more rows from history_immediate_tasks table
func (mdb *db) DeleteFromHistoryImmediateTasks(
	ctx context.Context,
	filter sqlplugin.HistoryImmediateTasksFilter,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		deleteHistoryImmediateTaskQuery,
		map[string]interface{}{
			"shard_id":    filter.ShardID,
			"category_id": filter.CategoryID,
			"task_id":     filter.TaskID,
		},
	)
}

// RangeDeleteFromHistoryImmediateTasks deletes one or more rows from history_immediate_tasks table
func (mdb *db) RangeDeleteFromHistoryImmediateTasks(
	ctx context.Context,
	filter sqlplugin.HistoryImmediateTasksRangeFilter,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		rangeDeleteHistoryImmediateTasksQuery,
		map[string]interface{}{
			"shard_id":    filter.ShardID,
			"category_id": filter.CategoryID,
			"min_task_id": filter.InclusiveMinTaskID,
			"max_task_id": filter.ExclusiveMaxTaskID,
		},
	)
}

// InsertIntoHistoryScheduledTasks inserts one or more rows into history_scheduled_tasks table
func (mdb *db) InsertIntoHistoryScheduledTasks(
	ctx context.Context,
	rows []sqlplugin.HistoryScheduledTasksRow,
) (sql.Result, error) {
	type localHistoryScheduledTasksRow struct {
		ShardID             int32            `db:"shard_id"`
		CategoryID          int32            `db:"category_id"`
		VisibilityTimestamp go_ora.TimeStamp `db:"visibility_timestamp"`
		TaskID              int64            `db:"task_id"`
		Data                []byte           `db:"data"`
		DataEncoding        string           `db:"data_encoding"`
	}

	inserts := make([]localHistoryScheduledTasksRow, len(rows))
	for i, row := range rows {
		inserts[i] = localHistoryScheduledTasksRow{
			ShardID:             row.ShardID,
			CategoryID:          row.CategoryID,
			VisibilityTimestamp: go_ora.TimeStamp(row.VisibilityTimestamp),
			TaskID:              row.TaskID,
			Data:                row.Data,
			DataEncoding:        row.DataEncoding,
		}
	}

	return mdb.NamedExecContext(ctx,
		createHistoryScheduledTasksQuery,
		inserts,
	)
}

// RangeSelectFromHistoryScheduledTasks reads one or more rows from history_scheduled_tasks table
func (mdb *db) RangeSelectFromHistoryScheduledTasks(
	ctx context.Context,
	filter sqlplugin.HistoryScheduledTasksRangeFilter,
) ([]sqlplugin.HistoryScheduledTasksRow, error) {
	type localHistoryScheduledTasksRow struct {
		ShardID             int32
		CategoryID          int32
		VisibilityTimestamp go_ora.TimeStamp
		TaskID              int64
		Data                []byte
		DataEncoding        string
	}
	var rows []localHistoryScheduledTasksRow

	if err := mdb.NamedSelectContext(ctx,
		&rows,
		getHistoryScheduledTasksQuery,
		map[string]interface{}{
			"shard_id":          filter.ShardID,
			"category_id":       filter.CategoryID,
			"min_visibility_ts": session.NewTimeStamp(filter.InclusiveMinVisibilityTimestamp).AsParam(),
			"min_task_id":       filter.InclusiveMinTaskID,
			"max_visibility_ts": session.NewTimeStamp(filter.ExclusiveMaxVisibilityTimestamp).AsParam(),
			"page_size":         filter.PageSize,
		},
	); err != nil {
		return nil, err
	}

	res := make([]sqlplugin.HistoryScheduledTasksRow, len(rows))
	for i, row := range rows {
		res[i] = sqlplugin.HistoryScheduledTasksRow{
			ShardID:             row.ShardID,
			CategoryID:          row.CategoryID,
			VisibilityTimestamp: time.Time(row.VisibilityTimestamp),
			TaskID:              row.TaskID,
			Data:                row.Data,
			DataEncoding:        row.DataEncoding,
		}
	}

	return res, nil
}

// DeleteFromHistoryScheduledTasks deletes one or more rows from history_scheduled_tasks table
func (mdb *db) DeleteFromHistoryScheduledTasks(
	ctx context.Context,
	filter sqlplugin.HistoryScheduledTasksFilter,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		deleteHistoryScheduledTaskQuery,
		map[string]interface{}{
			"shard_id":      filter.ShardID,
			"category_id":   filter.CategoryID,
			"visibility_ts": session.NewTimeStamp(filter.VisibilityTimestamp).AsParam(),
			"task_id":       filter.TaskID,
		},
	)
}

// RangeDeleteFromHistoryScheduledTasks deletes one or more rows from history_scheduled_tasks table
func (mdb *db) RangeDeleteFromHistoryScheduledTasks(
	ctx context.Context,
	filter sqlplugin.HistoryScheduledTasksRangeFilter,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		rangeDeleteHistoryScheduledTasksQuery,
		map[string]interface{}{
			"shard_id":          filter.ShardID,
			"category_id":       filter.CategoryID,
			"min_visibility_ts": session.NewTimeStamp(filter.InclusiveMinVisibilityTimestamp).AsParam(),
			"max_visibility_ts": session.NewTimeStamp(filter.ExclusiveMaxVisibilityTimestamp).AsParam(),
		},
	)
}

type localTransferTasksRow struct {
	ShardID      int32  `db:"shard_id"`
	TaskID       int64  `db:"task_id"`
	Data         []byte `db:"data"`
	DataEncoding string `db:"data_encoding"`
}

func newLocalTransferTasksRow(row sqlplugin.TransferTasksRow) localTransferTasksRow {
	return localTransferTasksRow{
		ShardID:      row.ShardID,
		TaskID:       row.TaskID,
		Data:         row.Data,
		DataEncoding: row.DataEncoding,
	}
}

// InsertIntoTransferTasks inserts one or more rows into transfer_tasks table
func (mdb *db) InsertIntoTransferTasks(
	ctx context.Context,
	rows []sqlplugin.TransferTasksRow,
) (sql.Result, error) {
	inserts := make([]localTransferTasksRow, len(rows))
	for i, row := range rows {
		inserts[i] = newLocalTransferTasksRow(row)
	}
	return mdb.NamedExecContext(ctx,
		createTransferTasksQuery,
		inserts,
	)
}

// RangeSelectFromTransferTasks reads one or more rows from transfer_tasks table
func (mdb *db) RangeSelectFromTransferTasks(
	ctx context.Context,
	filter sqlplugin.TransferTasksRangeFilter,
) ([]sqlplugin.TransferTasksRow, error) {
	var rows []sqlplugin.TransferTasksRow
	if err := mdb.NamedSelectContext(ctx,
		&rows,
		getTransferTasksQuery,
		map[string]interface{}{
			"shard_id":    filter.ShardID,
			"min_task_id": filter.InclusiveMinTaskID,
			"max_task_id": filter.ExclusiveMaxTaskID,
			"page_size":   filter.PageSize,
		},
	); err != nil {
		return nil, err
	}
	return rows, nil
}

// DeleteFromTransferTasks deletes one or more rows from transfer_tasks table
func (mdb *db) DeleteFromTransferTasks(
	ctx context.Context,
	filter sqlplugin.TransferTasksFilter,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		deleteTransferTaskQuery,
		map[string]interface{}{
			"shard_id": filter.ShardID,
			"task_id":  filter.TaskID,
		},
	)
}

// RangeDeleteFromTransferTasks deletes one or more rows from transfer_tasks table
func (mdb *db) RangeDeleteFromTransferTasks(
	ctx context.Context,
	filter sqlplugin.TransferTasksRangeFilter,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		rangeDeleteTransferTaskQuery,
		map[string]interface{}{
			"shard_id":    filter.ShardID,
			"min_task_id": filter.InclusiveMinTaskID,
			"max_task_id": filter.ExclusiveMaxTaskID,
		},
	)
}

// InsertIntoTimerTasks inserts one or more rows into timer_tasks table
func (mdb *db) InsertIntoTimerTasks(
	ctx context.Context,
	rows []sqlplugin.TimerTasksRow,
) (sql.Result, error) {
	type insertionLocalTimerTasksRow struct {
		ShardID             int32            `db:"shard_id"`
		VisibilityTimestamp go_ora.TimeStamp `db:"visibility_timestamp"`
		TaskID              int64            `db:"task_id"`
		Data                []byte           `db:"data"`
		DataEncoding        string           `db:"data_encoding"`
	}

	inserts := make([]insertionLocalTimerTasksRow, len(rows))
	for i, row := range rows {
		inserts[i] = insertionLocalTimerTasksRow{
			ShardID:             row.ShardID,
			VisibilityTimestamp: go_ora.TimeStamp(row.VisibilityTimestamp),
			TaskID:              row.TaskID,
			Data:                row.Data,
			DataEncoding:        row.DataEncoding,
		}
	}
	return mdb.NamedExecContext(ctx,
		createTimerTasksQuery,
		inserts,
	)
}

// RangeSelectFromTimerTasks reads one or more rows from timer_tasks table
func (mdb *db) RangeSelectFromTimerTasks(
	ctx context.Context,
	filter sqlplugin.TimerTasksRangeFilter,
) ([]sqlplugin.TimerTasksRow, error) {
	type localTimerTasksRow struct {
		ShardID             int32
		VisibilityTimestamp go_ora.TimeStamp
		TaskID              int64
		Data                []byte
		DataEncoding        string
	}

	var rows []localTimerTasksRow

	if err := mdb.NamedSelectContext(ctx,
		&rows,
		getTimerTasksQuery,
		map[string]interface{}{
			"shard_id":          filter.ShardID,
			"min_visibility_ts": session.NewTimeStamp(filter.InclusiveMinVisibilityTimestamp).AsParam(),
			"min_task_id":       filter.InclusiveMinTaskID,
			"max_visibility_ts": session.NewTimeStamp(filter.ExclusiveMaxVisibilityTimestamp).AsParam(),
			"page_size":         filter.PageSize,
		},
	); err != nil {
		return nil, err
	}

	res := make([]sqlplugin.TimerTasksRow, len(rows))

	for i := range rows {
		res[i] = sqlplugin.TimerTasksRow{
			ShardID:             rows[i].ShardID,
			VisibilityTimestamp: time.Time(rows[i].VisibilityTimestamp),
			TaskID:              rows[i].TaskID,
			Data:                rows[i].Data,
			DataEncoding:        rows[i].DataEncoding,
		}
	}
	return res, nil
}

// DeleteFromTimerTasks deletes one or more rows from timer_tasks table
func (mdb *db) DeleteFromTimerTasks(
	ctx context.Context,
	filter sqlplugin.TimerTasksFilter,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		deleteTimerTaskQuery,
		map[string]interface{}{
			"shard_id":      filter.ShardID,
			"visibility_ts": session.NewTimeStamp(filter.VisibilityTimestamp).AsParam(),
			"task_id":       filter.TaskID,
		},
	)
}

// RangeDeleteFromTimerTasks deletes one or more rows from timer_tasks table
func (mdb *db) RangeDeleteFromTimerTasks(
	ctx context.Context,
	filter sqlplugin.TimerTasksRangeFilter,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		rangeDeleteTimerTaskQuery,
		map[string]interface{}{
			"shard_id":          filter.ShardID,
			"min_visibility_ts": session.NewTimeStamp(filter.InclusiveMinVisibilityTimestamp).AsParam(),
			"max_visibility_ts": session.NewTimeStamp(filter.ExclusiveMaxVisibilityTimestamp).AsParam(),
		},
	)
}

type localBufferedEventsRow struct {
	ShardID      int32  `db:"shard_id"`
	NamespaceID  []byte `db:"namespace_id"`
	WorkflowID   string `db:"workflow_id"`
	RunID        []byte `db:"run_id"`
	Data         []byte `db:"data"`
	DataEncoding string `db:"data_encoding"`
}

func newLocalBufferedEventsRow(row sqlplugin.BufferedEventsRow) localBufferedEventsRow {
	return localBufferedEventsRow{
		ShardID:      row.ShardID,
		NamespaceID:  row.NamespaceID.Downcast(),
		WorkflowID:   row.WorkflowID,
		RunID:        row.RunID.Downcast(),
		Data:         row.Data,
		DataEncoding: row.DataEncoding,
	}
}

// InsertIntoBufferedEvents inserts one or more rows into buffered_events table
func (mdb *db) InsertIntoBufferedEvents(
	ctx context.Context,
	rows []sqlplugin.BufferedEventsRow,
) (sql.Result, error) {
	inserts := make([]localBufferedEventsRow, len(rows))
	for i, row := range rows {
		inserts[i] = newLocalBufferedEventsRow(row)
	}
	return mdb.NamedExecContext(ctx,
		createBufferedEventsQuery,
		inserts,
	)
}

// SelectFromBufferedEvents reads one or more rows from buffered_events table
func (mdb *db) SelectFromBufferedEvents(
	ctx context.Context,
	filter sqlplugin.BufferedEventsFilter,
) ([]sqlplugin.BufferedEventsRow, error) {
	var rows []sqlplugin.BufferedEventsRow
	if err := mdb.NamedSelectContext(ctx,
		&rows,
		getBufferedEventsQuery,
		map[string]interface{}{
			"shard_id":     filter.ShardID,
			"namespace_id": filter.NamespaceID.Downcast(),
			"workflow_id":  filter.WorkflowID,
			"run_id":       filter.RunID.Downcast(),
		},
	); err != nil {
		return nil, err
	}
	// @todo ??????? wtf
	for i := 0; i < len(rows); i++ {
		rows[i].NamespaceID = filter.NamespaceID
		rows[i].WorkflowID = filter.WorkflowID
		rows[i].RunID = filter.RunID
		rows[i].ShardID = filter.ShardID
	}
	return rows, nil
}

// DeleteFromBufferedEvents deletes one or more rows from buffered_events table
func (mdb *db) DeleteFromBufferedEvents(
	ctx context.Context,
	filter sqlplugin.BufferedEventsFilter,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		deleteBufferedEventsQuery,
		map[string]interface{}{
			"shard_id":     filter.ShardID,
			"namespace_id": filter.NamespaceID.Downcast(),
			"workflow_id":  filter.WorkflowID,
			"run_id":       filter.RunID.Downcast(),
		},
	)
}

type localReplicationTasksRow struct {
	ShardID      int32  `db:"shard_id"`
	TaskID       int64  `db:"task_id"`
	Data         []byte `db:"data"`
	DataEncoding string `db:"data_encoding"`
}

func newLocalReplicationTasksRow(row sqlplugin.ReplicationTasksRow) localReplicationTasksRow {
	return localReplicationTasksRow{
		ShardID:      row.ShardID,
		TaskID:       row.TaskID,
		Data:         row.Data,
		DataEncoding: row.DataEncoding,
	}
}

// InsertIntoReplicationTasks inserts one or more rows into replication_tasks table
func (mdb *db) InsertIntoReplicationTasks(
	ctx context.Context,
	rows []sqlplugin.ReplicationTasksRow,
) (sql.Result, error) {
	inserts := make([]localReplicationTasksRow, len(rows))
	for i, row := range rows {
		inserts[i] = newLocalReplicationTasksRow(row)
	}
	return mdb.NamedExecContext(ctx,
		createReplicationTasksQuery,
		inserts,
	)
}

// RangeSelectFromReplicationTasks reads one or more rows from replication_tasks table
func (mdb *db) RangeSelectFromReplicationTasks(
	ctx context.Context,
	filter sqlplugin.ReplicationTasksRangeFilter,
) ([]sqlplugin.ReplicationTasksRow, error) {
	var rows []sqlplugin.ReplicationTasksRow
	err := mdb.NamedSelectContext(ctx,
		&rows,
		getReplicationTasksQuery,
		map[string]interface{}{
			"shard_id":    filter.ShardID,
			"min_task_id": filter.InclusiveMinTaskID,
			"max_task_id": filter.ExclusiveMaxTaskID,
			"page_size":   filter.PageSize,
		},
	)
	return rows, err
}

// DeleteFromReplicationTasks deletes one row from replication_tasks table
func (mdb *db) DeleteFromReplicationTasks(
	ctx context.Context,
	filter sqlplugin.ReplicationTasksFilter,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		deleteReplicationTaskQuery,
		map[string]interface{}{
			"shard_id": filter.ShardID,
			"task_id":  filter.TaskID,
		},
	)
}

// RangeDeleteFromReplicationTasks deletes multi rows from replication_tasks table
func (mdb *db) RangeDeleteFromReplicationTasks(
	ctx context.Context,
	filter sqlplugin.ReplicationTasksRangeFilter,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		rangeDeleteReplicationTaskQuery,
		map[string]interface{}{
			"shard_id":    filter.ShardID,
			"min_task_id": filter.InclusiveMinTaskID,
			"max_task_id": filter.ExclusiveMaxTaskID,
		},
	)
}

type localReplicationDLQTasksRow struct {
	SourceClusterName string `db:"source_cluster_name"`
	ShardID           int32  `db:"shard_id"`
	TaskID            int64  `db:"task_id"`
	Data              []byte `db:"data"`
	DataEncoding      string `db:"data_encoding"`
}

func newLocalReplicationDLQTasksRow(row sqlplugin.ReplicationDLQTasksRow) localReplicationDLQTasksRow {
	data := row.Data
	if len(row.Data) == 0 {
		data = []byte{0}
	}
	return localReplicationDLQTasksRow{
		SourceClusterName: row.SourceClusterName,
		ShardID:           row.ShardID,
		TaskID:            row.TaskID,
		Data:              data,
		DataEncoding:      row.DataEncoding,
	}
}

// InsertIntoReplicationDLQTasks inserts one or more rows into replication_tasks_dlq table
func (mdb *db) InsertIntoReplicationDLQTasks(
	ctx context.Context,
	rows []sqlplugin.ReplicationDLQTasksRow,
) (sql.Result, error) {
	inserts := make([]localReplicationDLQTasksRow, len(rows))
	for i, row := range rows {
		inserts[i] = newLocalReplicationDLQTasksRow(row)
	}
	return mdb.NamedExecContext(ctx,
		insertReplicationTaskDLQQuery,
		inserts,
	)
}

// RangeSelectFromReplicationDLQTasks reads one or more rows from replication_tasks_dlq table
func (mdb *db) RangeSelectFromReplicationDLQTasks(
	ctx context.Context,
	filter sqlplugin.ReplicationDLQTasksRangeFilter,
) ([]sqlplugin.ReplicationDLQTasksRow, error) {
	var rows []sqlplugin.ReplicationDLQTasksRow
	err := mdb.NamedSelectContext(ctx,
		&rows,
		getReplicationTasksDLQQuery,
		map[string]interface{}{
			"source_cluster_name": filter.SourceClusterName,
			"shard_id":            filter.ShardID,
			"min_task_id":         filter.InclusiveMinTaskID,
			"max_task_id":         filter.ExclusiveMaxTaskID,
			"page_size":           filter.PageSize,
		},
	)
	return rows, err
}

// DeleteFromReplicationDLQTasks deletes one row from replication_tasks_dlq table
func (mdb *db) DeleteFromReplicationDLQTasks(
	ctx context.Context,
	filter sqlplugin.ReplicationDLQTasksFilter,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		deleteReplicationTaskFromDLQQuery,
		map[string]interface{}{
			"source_cluster_name": filter.SourceClusterName,
			"shard_id":            filter.ShardID,
			"task_id":             filter.TaskID,
		},
	)
}

// RangeDeleteFromReplicationDLQTasks deletes one or more rows from replication_tasks_dlq table
func (mdb *db) RangeDeleteFromReplicationDLQTasks(
	ctx context.Context,
	filter sqlplugin.ReplicationDLQTasksRangeFilter,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		rangeDeleteReplicationTaskFromDLQQuery,
		map[string]interface{}{
			"source_cluster_name": filter.SourceClusterName,
			"shard_id":            filter.ShardID,
			"min_task_id":         filter.InclusiveMinTaskID,
			"max_task_id":         filter.ExclusiveMaxTaskID,
		},
	)
}

type localVisibilityTasksRow struct {
	ShardID      int32  `db:"shard_id"`
	TaskID       int64  `db:"task_id"`
	Data         []byte `db:"data"`
	DataEncoding string `db:"data_encoding"`
}

func newLocalVisibilityTasksRow(row sqlplugin.VisibilityTasksRow) localVisibilityTasksRow {
	return localVisibilityTasksRow{
		ShardID:      row.ShardID,
		TaskID:       row.TaskID,
		Data:         row.Data,
		DataEncoding: row.DataEncoding,
	}
}

// InsertIntoVisibilityTasks inserts one or more rows into visibility_tasks table
func (mdb *db) InsertIntoVisibilityTasks(
	ctx context.Context,
	rows []sqlplugin.VisibilityTasksRow,
) (sql.Result, error) {
	inserts := make([]localVisibilityTasksRow, len(rows))
	for i, row := range rows {
		inserts[i] = newLocalVisibilityTasksRow(row)
	}
	return mdb.NamedExecContext(ctx,
		createVisibilityTasksQuery,
		inserts,
	)
}

// RangeSelectFromVisibilityTasks reads one or more rows from visibility_tasks table
func (mdb *db) RangeSelectFromVisibilityTasks(
	ctx context.Context,
	filter sqlplugin.VisibilityTasksRangeFilter,
) ([]sqlplugin.VisibilityTasksRow, error) {
	var rows []sqlplugin.VisibilityTasksRow
	if err := mdb.NamedSelectContext(ctx,
		&rows,
		getVisibilityTasksQuery,
		map[string]interface{}{
			"shard_id":    filter.ShardID,
			"min_task_id": filter.InclusiveMinTaskID,
			"max_task_id": filter.ExclusiveMaxTaskID,
			"page_size":   filter.PageSize,
		},
	); err != nil {
		return nil, err
	}
	return rows, nil
}

// DeleteFromVisibilityTasks deletes one or more rows from visibility_tasks table
func (mdb *db) DeleteFromVisibilityTasks(
	ctx context.Context,
	filter sqlplugin.VisibilityTasksFilter,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		deleteVisibilityTaskQuery,
		map[string]interface{}{
			"shard_id": filter.ShardID,
			"task_id":  filter.TaskID,
		},
	)
}

// RangeDeleteFromVisibilityTasks deletes one or more rows from visibility_tasks table
func (mdb *db) RangeDeleteFromVisibilityTasks(
	ctx context.Context,
	filter sqlplugin.VisibilityTasksRangeFilter,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		rangeDeleteVisibilityTaskQuery,
		map[string]interface{}{
			"shard_id":    filter.ShardID,
			"min_task_id": filter.InclusiveMinTaskID,
			"max_task_id": filter.ExclusiveMaxTaskID,
		},
	)
}
