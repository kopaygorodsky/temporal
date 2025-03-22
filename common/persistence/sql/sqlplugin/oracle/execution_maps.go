package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	deleteMapQryTemplate = `DELETE FROM %v
WHERE
shard_id = :shard_id AND
namespace_id = :namespace_id AND
workflow_id = :workflow_id AND
run_id = :run_id`

	// Oracle MERGE statement for upsert operation (replaces MySQL's ON DUPLICATE KEY UPDATE)
	setKeyInMapQryTemplate = `MERGE INTO %[1]v t
USING (
    SELECT 
        :shard_id as shard_id, 
        :namespace_id as namespace_id, 
        :workflow_id as workflow_id, 
        :run_id as run_id, 
        :%[5]v as %[5]v, 
        %[3]v 
    FROM dual
) s
ON (
    t.shard_id = s.shard_id AND 
    t.namespace_id = s.namespace_id AND 
    t.workflow_id = s.workflow_id AND 
    t.run_id = s.run_id AND 
    t.%[5]v = s.%[5]v
)
WHEN MATCHED THEN
    UPDATE SET %[4]v
WHEN NOT MATCHED THEN
    INSERT (shard_id, namespace_id, workflow_id, run_id, %[5]v, %[2]v)
    VALUES (s.shard_id, s.namespace_id, s.workflow_id, s.run_id, s.%[5]v, %[6]v)`

	// Oracle's IN clause needs special handling for SQL parameters
	deleteKeyInMapQryTemplate = `DELETE FROM %[1]v
WHERE
shard_id = :shard_id AND
namespace_id = :namespace_id AND
workflow_id = :workflow_id AND
run_id = :run_id AND
%[2]v IN (%[3]v)`

	// Get map query template
	getMapQryTemplate = `SELECT %[2]v, %[3]v FROM %[1]v
WHERE
shard_id = :shard_id AND
namespace_id = :namespace_id AND
workflow_id = :workflow_id AND
run_id = :run_id`
)

func stringMap(a []string, f func(string) string) []string {
	b := make([]string, len(a))
	for i, v := range a {
		b[i] = f(v)
	}
	return b
}

func makeDeleteMapQry(tableName string) string {
	return fmt.Sprintf(deleteMapQryTemplate, tableName)
}

func makeSetKeyInMapQry(tableName string, nonPrimaryKeyColumns []string, mapKeyName string) string {
	// Create column assignments for UPDATE clause (column1 = s.column1, column2 = s.column2)
	updateAssignments := strings.Join(stringMap(nonPrimaryKeyColumns, func(x string) string {
		return fmt.Sprintf("t.%s = s.%s", x, x)
	}), ", ")

	// Create source column references for VALUES clause (without trailing comma)
	sourceColumns := strings.Join(stringMap(nonPrimaryKeyColumns, func(x string) string {
		return fmt.Sprintf("s.%s", x)
	}), ", ")

	// Create named placeholders for SELECT in USING clause
	namedPlaceholders := strings.Join(stringMap(nonPrimaryKeyColumns, func(x string) string {
		return fmt.Sprintf(":%s as %s", x, x)
	}), ", ")

	return fmt.Sprintf(setKeyInMapQryTemplate,
		tableName,
		strings.Join(nonPrimaryKeyColumns, ", "),
		namedPlaceholders,
		updateAssignments,
		mapKeyName,
		sourceColumns)
}

func makeDeleteKeyInMapQry(tableName string, mapKeyName string) string {
	// This is a placeholder - the actual placeholders will be generated at runtime
	// based on the number of elements in the IN clause
	return fmt.Sprintf(deleteKeyInMapQryTemplate,
		tableName,
		mapKeyName,
		"%s")
}

func makeGetMapQryTemplate(tableName string, nonPrimaryKeyColumns []string, mapKeyName string) string {
	return fmt.Sprintf(getMapQryTemplate,
		tableName,
		mapKeyName,
		strings.Join(nonPrimaryKeyColumns, ", "))
}

var (
	// Omit shard_id, run_id, namespace_id, workflow_id, schedule_id since they're in the primary key
	activityInfoColumns = []string{
		"data",
		"data_encoding",
	}
	activityInfoTableName = "activity_info_maps"
	activityInfoKey       = "schedule_id"

	deleteActivityInfoMapQry = makeDeleteMapQry(activityInfoTableName)
	//@todo can't be used with multiple inserts in Oracle, so had to switch to another approach
	setKeyInActivityInfoMapQry    = makeSetKeyInMapQry(activityInfoTableName, activityInfoColumns, activityInfoKey)
	deleteKeyInActivityInfoMapQry = makeDeleteKeyInMapQry(activityInfoTableName, activityInfoKey)
	getActivityInfoMapQry         = makeGetMapQryTemplate(activityInfoTableName, activityInfoColumns, activityInfoKey)
)

func (mdb *db) ReplaceIntoActivityInfoMaps(
	ctx context.Context,
	rows []sqlplugin.ActivityInfoMapsRow,
) (sql.Result, error) {
	return replaceIntoInfoMaps(
		ctx,
		mdb,
		rows,
		"activity_info_maps",
		"schedule_id",
		func(row sqlplugin.ActivityInfoMapsRow) int32 { return row.ShardID },
		func(row sqlplugin.ActivityInfoMapsRow) []byte { return row.NamespaceID.Downcast() },
		func(row sqlplugin.ActivityInfoMapsRow) string { return row.WorkflowID },
		func(row sqlplugin.ActivityInfoMapsRow) []byte { return row.RunID.Downcast() },
		func(row sqlplugin.ActivityInfoMapsRow) interface{} { return row.ScheduleID },
		func(row sqlplugin.ActivityInfoMapsRow) []byte { return row.Data },
		func(row sqlplugin.ActivityInfoMapsRow) string { return row.DataEncoding },
	)
}

// SelectAllFromActivityInfoMaps reads all rows from activity_info_maps table
func (mdb *db) SelectAllFromActivityInfoMaps(
	ctx context.Context,
	filter sqlplugin.ActivityInfoMapsAllFilter,
) ([]sqlplugin.ActivityInfoMapsRow, error) {
	var rows []sqlplugin.ActivityInfoMapsRow
	if err := mdb.NamedSelectContext(ctx,
		&rows,
		getActivityInfoMapQry,
		map[string]interface{}{
			"shard_id":     filter.ShardID,
			"namespace_id": filter.NamespaceID.Downcast(),
			"workflow_id":  filter.WorkflowID,
			"run_id":       filter.RunID.Downcast(),
		}); err != nil {
		return nil, err
	}
	for i := 0; i < len(rows); i++ {
		rows[i].ShardID = filter.ShardID
		rows[i].NamespaceID = filter.NamespaceID
		rows[i].WorkflowID = filter.WorkflowID
		rows[i].RunID = filter.RunID
	}
	return rows, nil
}

// DeleteFromActivityInfoMaps deletes one or more rows from activity_info_maps table
func (mdb *db) DeleteFromActivityInfoMaps(
	ctx context.Context,
	filter sqlplugin.ActivityInfoMapsFilter,
) (sql.Result, error) {
	// For Oracle, we need to handle the IN clause differently
	// Generate placeholders for the IN clause
	inParams := make([]string, len(filter.ScheduleIDs))
	params := make(map[string]interface{})

	params["shard_id"] = filter.ShardID
	params["namespace_id"] = filter.NamespaceID.Downcast()
	params["workflow_id"] = filter.WorkflowID
	params["run_id"] = filter.RunID.Downcast()

	for i, id := range filter.ScheduleIDs {
		paramName := fmt.Sprintf("id_%d", i)
		inParams[i] = ":" + paramName
		params[paramName] = id
	}

	query := fmt.Sprintf(
		deleteKeyInActivityInfoMapQry,
		strings.Join(inParams, ", "),
	)

	return mdb.NamedExecContext(ctx, query, params)
}

// DeleteAllFromActivityInfoMaps deletes all rows from activity_info_maps table
func (mdb *db) DeleteAllFromActivityInfoMaps(
	ctx context.Context,
	filter sqlplugin.ActivityInfoMapsAllFilter,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		deleteActivityInfoMapQry,
		map[string]interface{}{
			"shard_id":     filter.ShardID,
			"namespace_id": filter.NamespaceID.Downcast(),
			"workflow_id":  filter.WorkflowID,
			"run_id":       filter.RunID.Downcast(),
		},
	)
}

var (
	timerInfoColumns = []string{
		"data",
		"data_encoding",
	}
	timerInfoTableName = "timer_info_maps"
	timerInfoKey       = "timer_id"

	deleteTimerInfoMapSQLQuery      = makeDeleteMapQry(timerInfoTableName)
	setKeyInTimerInfoMapSQLQuery    = makeSetKeyInMapQry(timerInfoTableName, timerInfoColumns, timerInfoKey)
	deleteKeyInTimerInfoMapSQLQuery = makeDeleteKeyInMapQry(timerInfoTableName, timerInfoKey)
	getTimerInfoMapSQLQuery         = makeGetMapQryTemplate(timerInfoTableName, timerInfoColumns, timerInfoKey)
)

// ReplaceIntoTimerInfoMaps replaces one or more rows in timer_info_maps table
func (mdb *db) ReplaceIntoTimerInfoMaps(
	ctx context.Context,
	rows []sqlplugin.TimerInfoMapsRow,
) (sql.Result, error) {
	return replaceIntoInfoMaps(
		ctx,
		mdb,
		rows,
		"timer_info_maps",
		"timer_id",
		func(row sqlplugin.TimerInfoMapsRow) int32 { return row.ShardID },
		func(row sqlplugin.TimerInfoMapsRow) []byte { return row.NamespaceID.Downcast() },
		func(row sqlplugin.TimerInfoMapsRow) string { return row.WorkflowID },
		func(row sqlplugin.TimerInfoMapsRow) []byte { return row.RunID.Downcast() },
		func(row sqlplugin.TimerInfoMapsRow) interface{} { return row.TimerID },
		func(row sqlplugin.TimerInfoMapsRow) []byte { return row.Data },
		func(row sqlplugin.TimerInfoMapsRow) string { return row.DataEncoding },
	)
}

// SelectAllFromTimerInfoMaps reads all rows from timer_info_maps table
func (mdb *db) SelectAllFromTimerInfoMaps(
	ctx context.Context,
	filter sqlplugin.TimerInfoMapsAllFilter,
) ([]sqlplugin.TimerInfoMapsRow, error) {
	var rows []sqlplugin.TimerInfoMapsRow
	if err := mdb.NamedSelectContext(ctx,
		&rows,
		getTimerInfoMapSQLQuery,
		map[string]interface{}{
			"shard_id":     filter.ShardID,
			"namespace_id": filter.NamespaceID.Downcast(),
			"workflow_id":  filter.WorkflowID,
			"run_id":       filter.RunID.Downcast(),
		}); err != nil {
		return nil, err
	}
	for i := 0; i < len(rows); i++ {
		rows[i].ShardID = filter.ShardID
		rows[i].NamespaceID = filter.NamespaceID
		rows[i].WorkflowID = filter.WorkflowID
		rows[i].RunID = filter.RunID
	}
	return rows, nil
}

// DeleteFromTimerInfoMaps deletes one or more rows from timer_info_maps table
func (mdb *db) DeleteFromTimerInfoMaps(
	ctx context.Context,
	filter sqlplugin.TimerInfoMapsFilter,
) (sql.Result, error) {
	// For Oracle, we need to handle the IN clause differently
	// Generate placeholders for the IN clause
	inParams := make([]string, len(filter.TimerIDs))
	params := make(map[string]interface{})

	params["shard_id"] = filter.ShardID
	params["namespace_id"] = filter.NamespaceID.Downcast()
	params["workflow_id"] = filter.WorkflowID
	params["run_id"] = filter.RunID.Downcast()

	for i, id := range filter.TimerIDs {
		paramName := fmt.Sprintf("id_%d", i)
		inParams[i] = ":" + paramName
		params[paramName] = id
	}

	query := fmt.Sprintf(
		deleteKeyInTimerInfoMapSQLQuery,
		strings.Join(inParams, ", "),
	)

	return mdb.NamedExecContext(ctx, query, params)
}

// DeleteAllFromTimerInfoMaps deletes all rows from timer_info_maps table
func (mdb *db) DeleteAllFromTimerInfoMaps(
	ctx context.Context,
	filter sqlplugin.TimerInfoMapsAllFilter,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		deleteTimerInfoMapSQLQuery,
		map[string]interface{}{
			"shard_id":     filter.ShardID,
			"namespace_id": filter.NamespaceID.Downcast(),
			"workflow_id":  filter.WorkflowID,
			"run_id":       filter.RunID.Downcast(),
		},
	)
}

var (
	childExecutionInfoColumns = []string{
		"data",
		"data_encoding",
	}
	childExecutionInfoTableName = "child_execution_info_maps"
	childExecutionInfoKey       = "initiated_id"

	deleteChildExecutionInfoMapQry      = makeDeleteMapQry(childExecutionInfoTableName)
	setKeyInChildExecutionInfoMapQry    = makeSetKeyInMapQry(childExecutionInfoTableName, childExecutionInfoColumns, childExecutionInfoKey)
	deleteKeyInChildExecutionInfoMapQry = makeDeleteKeyInMapQry(childExecutionInfoTableName, childExecutionInfoKey)
	getChildExecutionInfoMapQry         = makeGetMapQryTemplate(childExecutionInfoTableName, childExecutionInfoColumns, childExecutionInfoKey)
)

func (mdb *db) ReplaceIntoChildExecutionInfoMaps(
	ctx context.Context,
	rows []sqlplugin.ChildExecutionInfoMapsRow,
) (sql.Result, error) {
	return replaceIntoInfoMaps(
		ctx,
		mdb,
		rows,
		"child_execution_info_maps",
		"initiated_id",
		func(row sqlplugin.ChildExecutionInfoMapsRow) int32 { return row.ShardID },
		func(row sqlplugin.ChildExecutionInfoMapsRow) []byte { return row.NamespaceID.Downcast() },
		func(row sqlplugin.ChildExecutionInfoMapsRow) string { return row.WorkflowID },
		func(row sqlplugin.ChildExecutionInfoMapsRow) []byte { return row.RunID.Downcast() },
		func(row sqlplugin.ChildExecutionInfoMapsRow) interface{} { return row.InitiatedID },
		func(row sqlplugin.ChildExecutionInfoMapsRow) []byte { return row.Data },
		func(row sqlplugin.ChildExecutionInfoMapsRow) string { return row.DataEncoding },
	)
}

// SelectAllFromChildExecutionInfoMaps reads all rows from child_execution_info_maps table
func (mdb *db) SelectAllFromChildExecutionInfoMaps(
	ctx context.Context,
	filter sqlplugin.ChildExecutionInfoMapsAllFilter,
) ([]sqlplugin.ChildExecutionInfoMapsRow, error) {
	var rows []sqlplugin.ChildExecutionInfoMapsRow
	if err := mdb.NamedSelectContext(ctx,
		&rows,
		getChildExecutionInfoMapQry,
		map[string]interface{}{
			"shard_id":     filter.ShardID,
			"namespace_id": filter.NamespaceID.Downcast(),
			"workflow_id":  filter.WorkflowID,
			"run_id":       filter.RunID.Downcast(),
		}); err != nil {
		return nil, err
	}
	for i := 0; i < len(rows); i++ {
		rows[i].ShardID = filter.ShardID
		rows[i].NamespaceID = filter.NamespaceID
		rows[i].WorkflowID = filter.WorkflowID
		rows[i].RunID = filter.RunID
	}
	return rows, nil
}

// DeleteFromChildExecutionInfoMaps deletes one or more rows from child_execution_info_maps table
func (mdb *db) DeleteFromChildExecutionInfoMaps(
	ctx context.Context,
	filter sqlplugin.ChildExecutionInfoMapsFilter,
) (sql.Result, error) {
	// For Oracle, we need to handle the IN clause differently
	// Generate placeholders for the IN clause
	inParams := make([]string, len(filter.InitiatedIDs))
	params := make(map[string]interface{})

	params["shard_id"] = filter.ShardID
	params["namespace_id"] = filter.NamespaceID.Downcast()
	params["workflow_id"] = filter.WorkflowID
	params["run_id"] = filter.RunID.Downcast()

	for i, id := range filter.InitiatedIDs {
		paramName := fmt.Sprintf("id_%d", i)
		inParams[i] = ":" + paramName
		params[paramName] = id
	}

	query := fmt.Sprintf(
		deleteKeyInChildExecutionInfoMapQry,
		strings.Join(inParams, ", "),
	)

	return mdb.NamedExecContext(ctx, query, params)
}

// DeleteAllFromChildExecutionInfoMaps deletes all rows from child_execution_info_maps table
func (mdb *db) DeleteAllFromChildExecutionInfoMaps(
	ctx context.Context,
	filter sqlplugin.ChildExecutionInfoMapsAllFilter,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		deleteChildExecutionInfoMapQry,
		map[string]interface{}{
			"shard_id":     filter.ShardID,
			"namespace_id": filter.NamespaceID.Downcast(),
			"workflow_id":  filter.WorkflowID,
			"run_id":       filter.RunID.Downcast(),
		},
	)
}

var (
	requestCancelInfoColumns = []string{
		"data",
		"data_encoding",
	}
	requestCancelInfoTableName = "request_cancel_info_maps"
	requestCancelInfoKey       = "initiated_id"

	deleteRequestCancelInfoMapQry      = makeDeleteMapQry(requestCancelInfoTableName)
	setKeyInRequestCancelInfoMapQry    = makeSetKeyInMapQry(requestCancelInfoTableName, requestCancelInfoColumns, requestCancelInfoKey)
	deleteKeyInRequestCancelInfoMapQry = makeDeleteKeyInMapQry(requestCancelInfoTableName, requestCancelInfoKey)
	getRequestCancelInfoMapQry         = makeGetMapQryTemplate(requestCancelInfoTableName, requestCancelInfoColumns, requestCancelInfoKey)
)

// ReplaceIntoRequestCancelInfoMaps replaces one or more rows in request_cancel_info_maps table
func (mdb *db) ReplaceIntoRequestCancelInfoMaps(
	ctx context.Context,
	rows []sqlplugin.RequestCancelInfoMapsRow,
) (sql.Result, error) {
	return replaceIntoInfoMaps(
		ctx,
		mdb,
		rows,
		"request_cancel_info_maps",
		"initiated_id",
		func(row sqlplugin.RequestCancelInfoMapsRow) int32 { return row.ShardID },
		func(row sqlplugin.RequestCancelInfoMapsRow) []byte { return row.NamespaceID.Downcast() },
		func(row sqlplugin.RequestCancelInfoMapsRow) string { return row.WorkflowID },
		func(row sqlplugin.RequestCancelInfoMapsRow) []byte { return row.RunID.Downcast() },
		func(row sqlplugin.RequestCancelInfoMapsRow) interface{} { return row.InitiatedID },
		func(row sqlplugin.RequestCancelInfoMapsRow) []byte { return row.Data },
		func(row sqlplugin.RequestCancelInfoMapsRow) string { return row.DataEncoding },
	)
}

// SelectAllFromRequestCancelInfoMaps reads all rows from request_cancel_info_maps table
func (mdb *db) SelectAllFromRequestCancelInfoMaps(
	ctx context.Context,
	filter sqlplugin.RequestCancelInfoMapsAllFilter,
) ([]sqlplugin.RequestCancelInfoMapsRow, error) {
	var rows []sqlplugin.RequestCancelInfoMapsRow
	if err := mdb.NamedSelectContext(ctx,
		&rows,
		getRequestCancelInfoMapQry,
		map[string]interface{}{
			"shard_id":     filter.ShardID,
			"namespace_id": filter.NamespaceID.Downcast(),
			"workflow_id":  filter.WorkflowID,
			"run_id":       filter.RunID.Downcast(),
		}); err != nil {
		return nil, err
	}
	for i := 0; i < len(rows); i++ {
		rows[i].ShardID = filter.ShardID
		rows[i].NamespaceID = filter.NamespaceID
		rows[i].WorkflowID = filter.WorkflowID
		rows[i].RunID = filter.RunID
	}
	return rows, nil
}

// DeleteFromRequestCancelInfoMaps deletes one or more rows from request_cancel_info_maps table
func (mdb *db) DeleteFromRequestCancelInfoMaps(
	ctx context.Context,
	filter sqlplugin.RequestCancelInfoMapsFilter,
) (sql.Result, error) {
	// For Oracle, we need to handle the IN clause differently
	// Generate placeholders for the IN clause
	inParams := make([]string, len(filter.InitiatedIDs))
	params := make(map[string]interface{})

	params["shard_id"] = filter.ShardID
	params["namespace_id"] = filter.NamespaceID.Downcast()
	params["workflow_id"] = filter.WorkflowID
	params["run_id"] = filter.RunID.Downcast()

	for i, id := range filter.InitiatedIDs {
		paramName := fmt.Sprintf("id_%d", i)
		inParams[i] = ":" + paramName
		params[paramName] = id
	}

	query := fmt.Sprintf(
		deleteKeyInRequestCancelInfoMapQry,
		strings.Join(inParams, ", "),
	)

	return mdb.NamedExecContext(ctx, query, params)
}

// DeleteAllFromRequestCancelInfoMaps deletes all rows from request_cancel_info_maps table
func (mdb *db) DeleteAllFromRequestCancelInfoMaps(
	ctx context.Context,
	filter sqlplugin.RequestCancelInfoMapsAllFilter,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		deleteRequestCancelInfoMapQry,
		map[string]interface{}{
			"shard_id":     filter.ShardID,
			"namespace_id": filter.NamespaceID.Downcast(),
			"workflow_id":  filter.WorkflowID,
			"run_id":       filter.RunID.Downcast(),
		},
	)
}

var (
	signalInfoColumns = []string{
		"data",
		"data_encoding",
	}
	signalInfoTableName = "signal_info_maps"
	signalInfoKey       = "initiated_id"

	deleteSignalInfoMapQry      = makeDeleteMapQry(signalInfoTableName)
	setKeyInSignalInfoMapQry    = makeSetKeyInMapQry(signalInfoTableName, signalInfoColumns, signalInfoKey)
	deleteKeyInSignalInfoMapQry = makeDeleteKeyInMapQry(signalInfoTableName, signalInfoKey)
	getSignalInfoMapQry         = makeGetMapQryTemplate(signalInfoTableName, signalInfoColumns, signalInfoKey)
)

// ReplaceIntoSignalInfoMaps replaces one or more rows in signal_info_maps table
func (mdb *db) ReplaceIntoSignalInfoMaps(
	ctx context.Context,
	rows []sqlplugin.SignalInfoMapsRow,
) (sql.Result, error) {
	return replaceIntoInfoMaps(
		ctx,
		mdb,
		rows,
		"signal_info_maps",
		"initiated_id",
		func(row sqlplugin.SignalInfoMapsRow) int32 { return row.ShardID },
		func(row sqlplugin.SignalInfoMapsRow) []byte { return row.NamespaceID.Downcast() },
		func(row sqlplugin.SignalInfoMapsRow) string { return row.WorkflowID },
		func(row sqlplugin.SignalInfoMapsRow) []byte { return row.RunID.Downcast() },
		func(row sqlplugin.SignalInfoMapsRow) interface{} { return row.InitiatedID },
		func(row sqlplugin.SignalInfoMapsRow) []byte { return row.Data },
		func(row sqlplugin.SignalInfoMapsRow) string { return row.DataEncoding },
	)
}

// SelectAllFromSignalInfoMaps reads all rows from signal_info_maps table
func (mdb *db) SelectAllFromSignalInfoMaps(
	ctx context.Context,
	filter sqlplugin.SignalInfoMapsAllFilter,
) ([]sqlplugin.SignalInfoMapsRow, error) {
	var rows []sqlplugin.SignalInfoMapsRow
	if err := mdb.NamedSelectContext(ctx,
		&rows,
		getSignalInfoMapQry,
		map[string]interface{}{
			"shard_id":     filter.ShardID,
			"namespace_id": filter.NamespaceID.Downcast(),
			"workflow_id":  filter.WorkflowID,
			"run_id":       filter.RunID.Downcast(),
		}); err != nil {
		return nil, err
	}
	for i := 0; i < len(rows); i++ {
		rows[i].ShardID = filter.ShardID
		rows[i].NamespaceID = filter.NamespaceID
		rows[i].WorkflowID = filter.WorkflowID
		rows[i].RunID = filter.RunID
	}
	return rows, nil
}

// DeleteFromSignalInfoMaps deletes one or more rows from signal_info_maps table
func (mdb *db) DeleteFromSignalInfoMaps(
	ctx context.Context,
	filter sqlplugin.SignalInfoMapsFilter,
) (sql.Result, error) {
	// For Oracle, we need to handle the IN clause differently
	// Generate placeholders for the IN clause
	inParams := make([]string, len(filter.InitiatedIDs))
	params := make(map[string]interface{})

	params["shard_id"] = filter.ShardID
	params["namespace_id"] = filter.NamespaceID.Downcast()
	params["workflow_id"] = filter.WorkflowID
	params["run_id"] = filter.RunID.Downcast()

	for i, id := range filter.InitiatedIDs {
		paramName := fmt.Sprintf("id_%d", i)
		inParams[i] = ":" + paramName
		params[paramName] = id
	}

	query := fmt.Sprintf(
		deleteKeyInSignalInfoMapQry,
		strings.Join(inParams, ", "),
	)

	return mdb.NamedExecContext(ctx, query, params)
}

// DeleteAllFromSignalInfoMaps deletes all rows from signal_info_maps table
func (mdb *db) DeleteAllFromSignalInfoMaps(
	ctx context.Context,
	filter sqlplugin.SignalInfoMapsAllFilter,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		deleteSignalInfoMapQry,
		map[string]interface{}{
			"shard_id":     filter.ShardID,
			"namespace_id": filter.NamespaceID.Downcast(),
			"workflow_id":  filter.WorkflowID,
			"run_id":       filter.RunID.Downcast(),
		},
	)
}

const (
	deleteAllSignalsRequestedSetQry = `DELETE FROM signals_requested_sets
WHERE
shard_id = :shard_id AND
namespace_id = :namespace_id AND
workflow_id = :workflow_id AND
run_id = :run_id`

	// Oracle MERGE statement for signals_requested_sets
	createSignalsRequestedSetQry = `MERGE INTO signals_requested_sets t
USING (
    SELECT 
        :shard_id as shard_id, 
        :namespace_id as namespace_id, 
        :workflow_id as workflow_id, 
        :run_id as run_id, 
        :signal_id as signal_id 
    FROM dual
) s
ON (
    t.shard_id = s.shard_id AND 
    t.namespace_id = s.namespace_id AND 
    t.workflow_id = s.workflow_id AND 
    t.run_id = s.run_id
)
WHEN MATCHED THEN
    UPDATE SET t.signal_id = s.signal_id
WHEN NOT MATCHED THEN
    INSERT (shard_id, namespace_id, workflow_id, run_id, signal_id)
    VALUES (s.shard_id, s.namespace_id, s.workflow_id, s.run_id, s.signal_id)`

	// Oracle's IN clause needs parameters for each value
	deleteSignalsRequestedSetQry = `DELETE FROM signals_requested_sets
WHERE 
shard_id = :shard_id AND
namespace_id = :namespace_id AND
workflow_id = :workflow_id AND
run_id = :run_id AND
signal_id IN (%s)`

	getSignalsRequestedSetQry = `SELECT signal_id FROM signals_requested_sets WHERE
shard_id = :shard_id AND
namespace_id = :namespace_id AND
workflow_id = :workflow_id AND
run_id = :run_id`
)

//ReplaceIntoSignalsRequestedSets implements MySQL-like ON DUPLICATE KEY behavior for Oracle
func (mdb *db) ReplaceIntoSignalsRequestedSets(
	ctx context.Context,
	rows []sqlplugin.SignalsRequestedSetsRow,
) (sql.Result, error) {
	if len(rows) == 0 {
		return nil, nil
	}

	// Begin transaction
	tx, err := mdb.BeginWithFullTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	insertQuery := `
		INSERT INTO signals_requested_sets
		(shard_id, namespace_id, workflow_id, run_id, signal_id)
		VALUES (:shard_id, :namespace_id, :workflow_id, :run_id, :signal_id)
	`

	// Process each row individually to handle duplicates
	successCount := 0
	for _, row := range rows {
		params := map[string]interface{}{
			"shard_id":     row.ShardID,
			"namespace_id": row.NamespaceID.Downcast(),
			"workflow_id":  row.WorkflowID,
			"run_id":       row.RunID.Downcast(),
			"signal_id":    row.SignalID,
		}

		_, err := tx.NamedExecContext(ctx, insertQuery, params)
		if err != nil {
			if tx.IsDupEntryError(err) {
				continue
			}
			// Any other error should be returned
			return nil, err
		}
		successCount++
	}

	// Commit transaction
	if err = tx.Commit(); err != nil {
		return nil, err
	}

	// Return a dummy result
	return &queryResult{
		lastInsertedID: int64(successCount),
		rowsAffected:   int64(successCount),
	}, nil
}

// SelectAllFromSignalsRequestedSets reads all rows from signals_requested_sets table
func (mdb *db) SelectAllFromSignalsRequestedSets(
	ctx context.Context,
	filter sqlplugin.SignalsRequestedSetsAllFilter,
) ([]sqlplugin.SignalsRequestedSetsRow, error) {
	var rows []sqlplugin.SignalsRequestedSetsRow
	if err := mdb.NamedSelectContext(ctx,
		&rows,
		getSignalsRequestedSetQry,
		map[string]interface{}{
			"shard_id":     filter.ShardID,
			"namespace_id": filter.NamespaceID.Downcast(),
			"workflow_id":  filter.WorkflowID,
			"run_id":       filter.RunID.Downcast(),
		}); err != nil {
		return nil, err
	}
	for i := 0; i < len(rows); i++ {
		rows[i].ShardID = filter.ShardID
		rows[i].NamespaceID = filter.NamespaceID
		rows[i].WorkflowID = filter.WorkflowID
		rows[i].RunID = filter.RunID
	}
	return rows, nil
}

// DeleteFromSignalsRequestedSets deletes one or more rows from signals_requested_sets table
func (mdb *db) DeleteFromSignalsRequestedSets(
	ctx context.Context,
	filter sqlplugin.SignalsRequestedSetsFilter,
) (sql.Result, error) {
	// For Oracle, we need to handle the IN clause differently
	// Generate placeholders for the IN clause
	inParams := make([]string, len(filter.SignalIDs))
	params := make(map[string]interface{})

	params["shard_id"] = filter.ShardID
	params["namespace_id"] = filter.NamespaceID.Downcast()
	params["workflow_id"] = filter.WorkflowID
	params["run_id"] = filter.RunID.Downcast()

	for i, id := range filter.SignalIDs {
		paramName := fmt.Sprintf("id_%d", i)
		inParams[i] = ":" + paramName
		params[paramName] = id
	}

	query := fmt.Sprintf(
		deleteSignalsRequestedSetQry,
		strings.Join(inParams, ", "),
	)

	return mdb.NamedExecContext(ctx, query, params)
}

// DeleteAllFromSignalsRequestedSets deletes all rows from signals_requested_sets table: bugged
func (mdb *db) DeleteAllFromSignalsRequestedSets(
	ctx context.Context,
	filter sqlplugin.SignalsRequestedSetsAllFilter,
) (sql.Result, error) {
	sqlRes, err := mdb.NamedExecContext(ctx,
		deleteAllSignalsRequestedSetQry,
		map[string]interface{}{
			"shard_id":     filter.ShardID,
			"namespace_id": filter.NamespaceID.Downcast(),
			"workflow_id":  filter.WorkflowID,
			"run_id":       filter.RunID.Downcast(),
		},
	)
	if err != nil {
		return nil, fmt.Errorf("delete Signal Request Set with the following params: %w", err)
	}

	return sqlRes, nil
}

// replaceIntoInfoMaps is a generic function to replace rows in any info maps table
// @todo
// Note on implementation: We previously used Oracle's MERGE statement, but switched
// to this implementation with separate batch UPDATE/INSERT operations for better
// performance. The reasons for this change include:
//
// 1. Performance: Oracle's MERGE statement operates row-by-row, which causes
//    multiple round trips when processing batches. This implementation reduces
//    database round trips by processing INSERTs and UPDATEs in two bulk operations.
//
// 2. Execution plan optimization: Oracle can better optimize separate UPDATE and
//    INSERT operations than it can for MERGE statements with complex conditions.
//
// 3. Deadlock reduction: MERGE statements can be more prone to deadlocks in
//    high-concurrency scenarios due to their locking behavior.
//
// 4. Compatibility with go-ora's batch operations: This approach leverages the
//    batch operation capabilities of the go-ora driver, which aren't available
//    with the MERGE statement in the same way.
//
// The trade-off is slightly more complex code, but the performance benefits are
// substantial, especially for larger batch sizes (10+ rows).
func replaceIntoInfoMaps[T any](
	ctx context.Context,
	db *db,
	rows []T,
	tableName string,
	keyColName string,
	getShardID func(T) int32,
	getNamespaceID func(T) []byte,
	getWorkflowID func(T) string,
	getRunID func(T) []byte,
	getMapKey func(T) interface{},
	getData func(T) []byte,
	getDataEncoding func(T) string,
) (sql.Result, error) {
	if len(rows) == 0 {
		return nil, nil
	}

	// Get common values from the first row
	firstRow := rows[0]
	shardID := getShardID(firstRow)
	namespaceID := getNamespaceID(firstRow)
	workflowID := getWorkflowID(firstRow)
	runID := getRunID(firstRow)

	// Collect all keys for the IN clause
	keys := make([]interface{}, len(rows))
	for i, row := range rows {
		keys[i] = getMapKey(row)
	}

	// Create a map for quick lookup of rows by key
	rowsByKey := make(map[interface{}]T, len(rows))
	for _, row := range rows {
		rowsByKey[getMapKey(row)] = row
	}

	// Determine key type
	isStringKey := false
	if len(keys) > 0 {
		_, isStringKey = keys[0].(string)
	}

	// Fetch existing rows to determine what needs to be inserted vs updated
	inClause := make([]string, len(keys))
	params := make(map[string]interface{})
	params["shard_id"] = shardID
	params["namespace_id"] = namespaceID
	params["workflow_id"] = workflowID
	params["run_id"] = runID

	for i, key := range keys {
		paramName := fmt.Sprintf("key_%d", i)
		inClause[i] = ":" + paramName
		params[paramName] = key
	}

	query := fmt.Sprintf(`
        SELECT %s 
        FROM %s
        WHERE shard_id = :shard_id 
        AND namespace_id = :namespace_id 
        AND workflow_id = :workflow_id 
        AND run_id = :run_id 
        AND %s IN (%s)
    `, keyColName, tableName, keyColName, strings.Join(inClause, ", "))

	// Query for existing keys
	var existingKeys []interface{}
	if isStringKey {
		var stringKeys []string
		if err := db.NamedSelectContext(ctx, &stringKeys, query, params); err != nil {
			return nil, err
		}
		existingKeys = make([]interface{}, len(stringKeys))
		for i, key := range stringKeys {
			existingKeys[i] = key
		}
	} else {
		var int64Keys []int64
		if err := db.NamedSelectContext(ctx, &int64Keys, query, params); err != nil {
			return nil, err
		}
		existingKeys = make([]interface{}, len(int64Keys))
		for i, key := range int64Keys {
			existingKeys[i] = key
		}
	}

	// Create sets for existing and new keys
	existingKeySet := make(map[interface{}]struct{}, len(existingKeys))
	for _, key := range existingKeys {
		existingKeySet[key] = struct{}{}
	}

	// Prepare rows for update and insert
	var updates []T
	var inserts []T

	for _, row := range rows {
		if _, exists := existingKeySet[getMapKey(row)]; exists {
			updates = append(updates, row)
		} else {
			inserts = append(inserts, row)
		}
	}

	// Begin transaction
	tx, err := db.BeginWithFullTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Process updates if needed
	if len(updates) > 0 {
		updateQuery := fmt.Sprintf(`
            UPDATE %s
            SET data = :data, data_encoding = :data_encoding
            WHERE shard_id = :shard_id 
            AND namespace_id = :namespace_id 
            AND workflow_id = :workflow_id 
            AND run_id = :run_id 
            AND %s = :%s
        `, tableName, keyColName, keyColName)

		// For batch operations with go-ora, create separate slices for each column
		shardIDs := make([]int32, len(updates))
		namespaceIDs := make([][]byte, len(updates))
		workflowIDs := make([]string, len(updates))
		runIDs := make([][]byte, len(updates))
		mapKeys := make([]interface{}, len(updates))
		datas := make([][]byte, len(updates))
		dataEncodings := make([]string, len(updates))

		for i, row := range updates {
			shardIDs[i] = getShardID(row)
			namespaceIDs[i] = getNamespaceID(row)
			workflowIDs[i] = getWorkflowID(row)
			runIDs[i] = getRunID(row)
			mapKeys[i] = getMapKey(row)
			datas[i] = getData(row)
			dataEncodings[i] = getDataEncoding(row)
		}

		// Create batch parameter map
		batchUpdateParams := map[string]interface{}{
			"shard_id":      shardIDs,
			"namespace_id":  namespaceIDs,
			"workflow_id":   workflowIDs,
			"run_id":        runIDs,
			keyColName:      mapKeys,
			"data":          datas,
			"data_encoding": dataEncodings,
		}

		if _, err = tx.NamedExecContext(ctx, updateQuery, batchUpdateParams); err != nil {
			return nil, err
		}
	}

	// Process inserts if needed
	if len(inserts) > 0 {
		insertQuery := fmt.Sprintf(`
            INSERT INTO %s
            (shard_id, namespace_id, workflow_id, run_id, %s, data, data_encoding)
            VALUES (:shard_id, :namespace_id, :workflow_id, :run_id, :%s, :data, :data_encoding)
        `, tableName, keyColName, keyColName)

		// For batch operations with go-ora, create separate slices for each column
		shardIDs := make([]int32, len(inserts))
		namespaceIDs := make([][]byte, len(inserts))
		workflowIDs := make([]string, len(inserts))
		runIDs := make([][]byte, len(inserts))
		mapKeys := make([]interface{}, len(inserts))
		datas := make([][]byte, len(inserts))
		dataEncodings := make([]string, len(inserts))

		for i, row := range inserts {
			shardIDs[i] = getShardID(row)
			namespaceIDs[i] = getNamespaceID(row)
			workflowIDs[i] = getWorkflowID(row)
			runIDs[i] = getRunID(row)
			mapKeys[i] = getMapKey(row)
			datas[i] = getData(row)
			dataEncodings[i] = getDataEncoding(row)
		}

		// Create batch parameter map
		batchInsertParams := map[string]interface{}{
			"shard_id":      shardIDs,
			"namespace_id":  namespaceIDs,
			"workflow_id":   workflowIDs,
			"run_id":        runIDs,
			keyColName:      mapKeys,
			"data":          datas,
			"data_encoding": dataEncodings,
		}

		if _, err = tx.NamedExecContext(ctx, insertQuery, batchInsertParams); err != nil {
			return nil, err
		}
	}

	// Commit transaction
	if err = tx.Commit(); err != nil {
		return nil, err
	}

	return &queryResult{
		lastInsertedID: int64(len(inserts)),
		rowsAffected:   int64(len(updates) + len(inserts)),
	}, nil
}

type queryResult struct {
	lastInsertedID int64
	rowsAffected   int64
}

func (rs *queryResult) LastInsertId() (int64, error) {
	return rs.lastInsertedID, nil
}

func (rs *queryResult) RowsAffected() (int64, error) {
	return rs.rowsAffected, nil
}
