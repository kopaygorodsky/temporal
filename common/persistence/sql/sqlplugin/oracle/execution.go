package oracle

import (
	"context"
	"database/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
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
	createTimerTasksQuery = `INSERT INTO timer_tasks (shard_id, visibility_timestamp, task_id, data, data_encoding)
  VALUES (:shard_id, :visibility_timestamp, :task_id, :data, :data_encoding)`

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

// InsertIntoExecutions inserts a row into executions table
func (mdb *db) InsertIntoExecutions(
	ctx context.Context,
	row *sqlplugin.ExecutionsRow,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		createExecutionQuery,
		row,
	)
}

// UpdateExecutions updates a single row in executions table
func (mdb *db) UpdateExecutions(
	ctx context.Context,
	row *sqlplugin.ExecutionsRow,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		updateExecutionQuery,
		row,
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
			"namespace_id": filter.NamespaceID,
			"workflow_id":  filter.WorkflowID,
			"run_id":       filter.RunID,
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
			"namespace_id": filter.NamespaceID,
			"workflow_id":  filter.WorkflowID,
			"run_id":       filter.RunID,
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
			"namespace_id": filter.NamespaceID,
			"workflow_id":  filter.WorkflowID,
			"run_id":       filter.RunID,
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
			"namespace_id": filter.NamespaceID,
			"workflow_id":  filter.WorkflowID,
			"run_id":       filter.RunID,
		},
	)
	return executionVersion.DBRecordVersion, executionVersion.NextEventID, err
}

// InsertIntoCurrentExecutions inserts a single row into current_executions table
func (mdb *db) InsertIntoCurrentExecutions(
	ctx context.Context,
	row *sqlplugin.CurrentExecutionsRow,
) (sql.Result, error) {
	// Convert the start time to Oracle timestamp
	if row.StartTime != nil {
		ts := mdb.converter.ToMySQLDateTime(*row.StartTime)
		row.StartTime = &ts
	}

	return mdb.NamedExecContext(ctx,
		createCurrentExecutionQuery,
		row,
	)
}

// UpdateCurrentExecutions updates a single row in current_executions table
func (mdb *db) UpdateCurrentExecutions(
	ctx context.Context,
	row *sqlplugin.CurrentExecutionsRow,
) (sql.Result, error) {
	// Convert the start time to Oracle timestamp
	if row.StartTime != nil {
		ts := mdb.converter.ToMySQLDateTime(*row.StartTime)
		row.StartTime = &ts
	}

	return mdb.NamedExecContext(ctx,
		updateCurrentExecutionsQuery,
		row,
	)
}

// SelectFromCurrentExecutions reads one or more rows from current_executions table
func (mdb *db) SelectFromCurrentExecutions(
	ctx context.Context,
	filter sqlplugin.CurrentExecutionsFilter,
) (*sqlplugin.CurrentExecutionsRow, error) {
	var row sqlplugin.CurrentExecutionsRow
	err := mdb.NamedGetContext(ctx,
		&row,
		getCurrentExecutionQuery,
		map[string]interface{}{
			"shard_id":     filter.ShardID,
			"namespace_id": filter.NamespaceID,
			"workflow_id":  filter.WorkflowID,
		},
	)
	if err != nil {
		return nil, err
	}

	// Convert timestamp back to original format
	if row.StartTime != nil {
		ts := mdb.converter.FromMySQLDateTime(*row.StartTime)
		row.StartTime = &ts
	}

	return &row, nil
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
			"namespace_id": filter.NamespaceID,
			"workflow_id":  filter.WorkflowID,
			"run_id":       filter.RunID,
		},
	)
}

// LockCurrentExecutions acquires a write lock on a single row in current_executions table
func (mdb *db) LockCurrentExecutions(
	ctx context.Context,
	filter sqlplugin.CurrentExecutionsFilter,
) (*sqlplugin.CurrentExecutionsRow, error) {
	var row sqlplugin.CurrentExecutionsRow
	err := mdb.NamedGetContext(ctx,
		&row,
		lockCurrentExecutionQuery,
		map[string]interface{}{
			"shard_id":     filter.ShardID,
			"namespace_id": filter.NamespaceID,
			"workflow_id":  filter.WorkflowID,
		},
	)
	if err != nil {
		return nil, err
	}

	// Convert timestamp back to original format
	if row.StartTime != nil {
		ts := mdb.converter.FromMySQLDateTime(*row.StartTime)
		row.StartTime = &ts
	}

	return &row, nil
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
			"namespace_id": filter.NamespaceID,
			"workflow_id":  filter.WorkflowID,
		},
	)
	if err != nil {
		return nil, err
	}

	// Convert timestamps back to original format
	for i := range rows {
		if rows[i].StartTime != nil {
			ts := mdb.converter.FromMySQLDateTime(*rows[i].StartTime)
			rows[i].StartTime = &ts
		}
	}

	return rows, nil
}

// InsertIntoHistoryImmediateTasks inserts one or more rows into history_immediate_tasks table
func (mdb *db) InsertIntoHistoryImmediateTasks(
	ctx context.Context,
	rows []sqlplugin.HistoryImmediateTasksRow,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		createHistoryImmediateTasksQuery,
		rows,
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
	for i := range rows {
		rows[i].VisibilityTimestamp = mdb.converter.ToMySQLDateTime(rows[i].VisibilityTimestamp)
	}
	return mdb.NamedExecContext(ctx,
		createHistoryScheduledTasksQuery,
		rows,
	)
}

// RangeSelectFromHistoryScheduledTasks reads one or more rows from history_scheduled_tasks table
func (mdb *db) RangeSelectFromHistoryScheduledTasks(
	ctx context.Context,
	filter sqlplugin.HistoryScheduledTasksRangeFilter,
) ([]sqlplugin.HistoryScheduledTasksRow, error) {
	var rows []sqlplugin.HistoryScheduledTasksRow
	minVisibilityTS := mdb.converter.ToMySQLDateTime(filter.InclusiveMinVisibilityTimestamp)
	maxVisibilityTS := mdb.converter.ToMySQLDateTime(filter.ExclusiveMaxVisibilityTimestamp)

	if err := mdb.NamedSelectContext(ctx,
		&rows,
		getHistoryScheduledTasksQuery,
		map[string]interface{}{
			"shard_id":          filter.ShardID,
			"category_id":       filter.CategoryID,
			"min_visibility_ts": minVisibilityTS,
			"min_task_id":       filter.InclusiveMinTaskID,
			"max_visibility_ts": maxVisibilityTS,
			"page_size":         filter.PageSize,
		},
	); err != nil {
		return nil, err
	}

	for i := range rows {
		rows[i].VisibilityTimestamp = mdb.converter.FromMySQLDateTime(rows[i].VisibilityTimestamp)
	}
	return rows, nil
}

// DeleteFromHistoryScheduledTasks deletes one or more rows from history_scheduled_tasks table
func (mdb *db) DeleteFromHistoryScheduledTasks(
	ctx context.Context,
	filter sqlplugin.HistoryScheduledTasksFilter,
) (sql.Result, error) {
	visibilityTS := mdb.converter.ToMySQLDateTime(filter.VisibilityTimestamp)
	return mdb.NamedExecContext(ctx,
		deleteHistoryScheduledTaskQuery,
		map[string]interface{}{
			"shard_id":      filter.ShardID,
			"category_id":   filter.CategoryID,
			"visibility_ts": visibilityTS,
			"task_id":       filter.TaskID,
		},
	)
}

// RangeDeleteFromHistoryScheduledTasks deletes one or more rows from history_scheduled_tasks table
func (mdb *db) RangeDeleteFromHistoryScheduledTasks(
	ctx context.Context,
	filter sqlplugin.HistoryScheduledTasksRangeFilter,
) (sql.Result, error) {
	minVisibilityTS := mdb.converter.ToMySQLDateTime(filter.InclusiveMinVisibilityTimestamp)
	maxVisibilityTS := mdb.converter.ToMySQLDateTime(filter.ExclusiveMaxVisibilityTimestamp)

	return mdb.NamedExecContext(ctx,
		rangeDeleteHistoryScheduledTasksQuery,
		map[string]interface{}{
			"shard_id":          filter.ShardID,
			"category_id":       filter.CategoryID,
			"min_visibility_ts": minVisibilityTS,
			"max_visibility_ts": maxVisibilityTS,
		},
	)
}

// InsertIntoTransferTasks inserts one or more rows into transfer_tasks table
func (mdb *db) InsertIntoTransferTasks(
	ctx context.Context,
	rows []sqlplugin.TransferTasksRow,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		createTransferTasksQuery,
		rows,
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
	for i := range rows {
		rows[i].VisibilityTimestamp = mdb.converter.ToMySQLDateTime(rows[i].VisibilityTimestamp)
	}
	return mdb.NamedExecContext(ctx,
		createTimerTasksQuery,
		rows,
	)
}

// RangeSelectFromTimerTasks reads one or more rows from timer_tasks table
func (mdb *db) RangeSelectFromTimerTasks(
	ctx context.Context,
	filter sqlplugin.TimerTasksRangeFilter,
) ([]sqlplugin.TimerTasksRow, error) {
	var rows []sqlplugin.TimerTasksRow
	minVisibilityTS := mdb.converter.ToMySQLDateTime(filter.InclusiveMinVisibilityTimestamp)
	maxVisibilityTS := mdb.converter.ToMySQLDateTime(filter.ExclusiveMaxVisibilityTimestamp)

	if err := mdb.NamedSelectContext(ctx,
		&rows,
		getTimerTasksQuery,
		map[string]interface{}{
			"shard_id":          filter.ShardID,
			"min_visibility_ts": minVisibilityTS,
			"min_task_id":       filter.InclusiveMinTaskID,
			"max_visibility_ts": maxVisibilityTS,
			"page_size":         filter.PageSize,
		},
	); err != nil {
		return nil, err
	}

	for i := range rows {
		rows[i].VisibilityTimestamp = mdb.converter.FromMySQLDateTime(rows[i].VisibilityTimestamp)
	}
	return rows, nil
}

// DeleteFromTimerTasks deletes one or more rows from timer_tasks table
func (mdb *db) DeleteFromTimerTasks(
	ctx context.Context,
	filter sqlplugin.TimerTasksFilter,
) (sql.Result, error) {
	visibilityTS := mdb.converter.ToMySQLDateTime(filter.VisibilityTimestamp)
	return mdb.NamedExecContext(ctx,
		deleteTimerTaskQuery,
		map[string]interface{}{
			"shard_id":      filter.ShardID,
			"visibility_ts": visibilityTS,
			"task_id":       filter.TaskID,
		},
	)
}

// RangeDeleteFromTimerTasks deletes one or more rows from timer_tasks table
func (mdb *db) RangeDeleteFromTimerTasks(
	ctx context.Context,
	filter sqlplugin.TimerTasksRangeFilter,
) (sql.Result, error) {
	minVisibilityTS := mdb.converter.ToMySQLDateTime(filter.InclusiveMinVisibilityTimestamp)
	maxVisibilityTS := mdb.converter.ToMySQLDateTime(filter.ExclusiveMaxVisibilityTimestamp)

	return mdb.NamedExecContext(ctx,
		rangeDeleteTimerTaskQuery,
		map[string]interface{}{
			"shard_id":          filter.ShardID,
			"min_visibility_ts": minVisibilityTS,
			"max_visibility_ts": maxVisibilityTS,
		},
	)
}

// InsertIntoBufferedEvents inserts one or more rows into buffered_events table
func (mdb *db) InsertIntoBufferedEvents(
	ctx context.Context,
	rows []sqlplugin.BufferedEventsRow,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		createBufferedEventsQuery,
		rows,
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
			"namespace_id": filter.NamespaceID,
			"workflow_id":  filter.WorkflowID,
			"run_id":       filter.RunID,
		},
	); err != nil {
		return nil, err
	}
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
			"namespace_id": filter.NamespaceID,
			"workflow_id":  filter.WorkflowID,
			"run_id":       filter.RunID,
		},
	)
}

// InsertIntoReplicationTasks inserts one or more rows into replication_tasks table
func (mdb *db) InsertIntoReplicationTasks(
	ctx context.Context,
	rows []sqlplugin.ReplicationTasksRow,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		createReplicationTasksQuery,
		rows,
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

// InsertIntoReplicationDLQTasks inserts one or more rows into replication_tasks_dlq table
func (mdb *db) InsertIntoReplicationDLQTasks(
	ctx context.Context,
	rows []sqlplugin.ReplicationDLQTasksRow,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		insertReplicationTaskDLQQuery,
		rows,
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

// InsertIntoVisibilityTasks inserts one or more rows into visibility_tasks table
func (mdb *db) InsertIntoVisibilityTasks(
	ctx context.Context,
	rows []sqlplugin.VisibilityTasksRow,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		createVisibilityTasksQuery,
		rows,
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
