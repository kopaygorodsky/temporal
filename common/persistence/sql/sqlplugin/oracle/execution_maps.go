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

	deleteActivityInfoMapQry      = makeDeleteMapQry(activityInfoTableName)
	setKeyInActivityInfoMapQry    = makeSetKeyInMapQry(activityInfoTableName, activityInfoColumns, activityInfoKey)
	deleteKeyInActivityInfoMapQry = makeDeleteKeyInMapQry(activityInfoTableName, activityInfoKey)
	getActivityInfoMapQry         = makeGetMapQryTemplate(activityInfoTableName, activityInfoColumns, activityInfoKey)
)

// ReplaceIntoActivityInfoMaps replaces one or more rows in activity_info_maps table
func (mdb *db) ReplaceIntoActivityInfoMaps(
	ctx context.Context,
	rows []sqlplugin.ActivityInfoMapsRow,
) (sql.Result, error) {
	type localActivityInfoMapsRow struct {
		ShardID      int32  `db:"shard_id"`
		NamespaceID  []byte `db:"namespace_id"`
		WorkflowID   string `db:"workflow_id"`
		RunID        []byte `db:"run_id"`
		ScheduleID   int64  `db:"schedule_id"`
		Data         []byte `db:"data"`
		DataEncoding string `db:"data_encoding"`
	}
	insertions := make([]localActivityInfoMapsRow, len(rows))
	for i, row := range rows {
		insertions[i] = localActivityInfoMapsRow{
			ShardID:      row.ShardID,
			NamespaceID:  row.NamespaceID.Downcast(),
			WorkflowID:   row.WorkflowID,
			RunID:        row.RunID.Downcast(),
			ScheduleID:   row.ScheduleID,
			Data:         row.Data,
			DataEncoding: row.DataEncoding,
		}
	}

	return mdb.NamedExecContext(ctx,
		setKeyInActivityInfoMapQry,
		insertions,
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
	type localTimerInfoMapsRow struct {
		ShardID      int32  `db:"shard_id"`
		NamespaceID  []byte `db:"namespace_id"`
		WorkflowID   string `db:"workflow_id"`
		RunID        []byte `db:"run_id"`
		TimerID      string `db:"timer_id"`
		Data         []byte `db:"data"`
		DataEncoding string `db:"data_encoding"`
	}

	insertions := make([]localTimerInfoMapsRow, len(rows))

	for i, row := range rows {
		insertions[i] = localTimerInfoMapsRow{
			ShardID:      row.ShardID,
			NamespaceID:  row.NamespaceID.Downcast(),
			WorkflowID:   row.WorkflowID,
			RunID:        row.RunID.Downcast(),
			TimerID:      row.TimerID,
			Data:         row.Data,
			DataEncoding: row.DataEncoding,
		}
	}
	return mdb.NamedExecContext(ctx,
		setKeyInTimerInfoMapSQLQuery,
		insertions,
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

// ReplaceIntoChildExecutionInfoMaps replaces one or more rows in child_execution_info_maps table
func (mdb *db) ReplaceIntoChildExecutionInfoMaps(
	ctx context.Context,
	rows []sqlplugin.ChildExecutionInfoMapsRow,
) (sql.Result, error) {
	type localChildExecutionInfoMapsRow struct {
		ShardID      int32  `db:"shard_id"`
		NamespaceID  []byte `db:"namespace_id"`
		WorkflowID   string `db:"workflow_id"`
		RunID        []byte `db:"run_id"`
		InitiatedID  int64  `db:"initiated_id"`
		Data         []byte `db:"data"`
		DataEncoding string `db:"data_encoding"`
	}

	insertions := make([]localChildExecutionInfoMapsRow, len(rows))

	for i, row := range rows {
		insertions[i] = localChildExecutionInfoMapsRow{
			ShardID:      row.ShardID,
			NamespaceID:  row.NamespaceID.Downcast(),
			WorkflowID:   row.WorkflowID,
			RunID:        row.RunID.Downcast(),
			InitiatedID:  row.InitiatedID,
			Data:         row.Data,
			DataEncoding: row.DataEncoding,
		}
	}
	return mdb.NamedExecContext(ctx,
		setKeyInChildExecutionInfoMapQry,
		insertions,
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
	type localRequestCancelInfoMapsRow struct {
		ShardID      int32  `db:"shard_id"`
		NamespaceID  []byte `db:"namespace_id"`
		WorkflowID   string `db:"workflow_id"`
		RunID        []byte `db:"run_id"`
		InitiatedID  int64  `db:"initiated_id"`
		Data         []byte `db:"data"`
		DataEncoding string `db:"data_encoding"`
	}

	insertions := make([]localRequestCancelInfoMapsRow, len(rows))
	for i, row := range rows {
		insertions[i] = localRequestCancelInfoMapsRow{
			ShardID:      row.ShardID,
			NamespaceID:  row.NamespaceID.Downcast(),
			WorkflowID:   row.WorkflowID,
			RunID:        row.RunID.Downcast(),
			InitiatedID:  row.InitiatedID,
			Data:         row.Data,
			DataEncoding: row.DataEncoding,
		}
	}

	return mdb.NamedExecContext(ctx,
		setKeyInRequestCancelInfoMapQry,
		insertions,
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
	type localSignalInfoMapsRow struct {
		ShardID      int32  `db:"shard_id"`
		NamespaceID  []byte `db:"namespace_id"`
		WorkflowID   string `db:"workflow_id"`
		RunID        []byte `db:"run_id"`
		InitiatedID  int64  `db:"initiated_id"`
		Data         []byte `db:"data"`
		DataEncoding string `db:"data_encoding"`
	}

	insertions := make([]localSignalInfoMapsRow, len(rows))
	for i, row := range rows {
		insertions[i] = localSignalInfoMapsRow{
			ShardID:      row.ShardID,
			NamespaceID:  row.NamespaceID.Downcast(),
			WorkflowID:   row.WorkflowID,
			RunID:        row.RunID.Downcast(),
			InitiatedID:  row.InitiatedID,
			Data:         row.Data,
			DataEncoding: row.DataEncoding,
		}
	}

	return mdb.NamedExecContext(ctx,
		setKeyInSignalInfoMapQry,
		insertions,
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

// ReplaceIntoSignalsRequestedSets inserts one or more rows into signals_requested_sets table
func (mdb *db) ReplaceIntoSignalsRequestedSets(
	ctx context.Context,
	rows []sqlplugin.SignalsRequestedSetsRow,
) (sql.Result, error) {
	type localSignalsRequestedSetsRow struct {
		ShardID     int32  `db:"shard_id"`
		NamespaceID []byte `db:"namespace_id"`
		WorkflowID  string `db:"workflow_id"`
		RunID       []byte `db:"run_id"`
		SignalID    string `db:"signal_id"`
	}
	insertions := make([]localSignalsRequestedSetsRow, len(rows))
	for i, row := range rows {
		insertions[i] = localSignalsRequestedSetsRow{
			ShardID:     row.ShardID,
			NamespaceID: row.NamespaceID.Downcast(),
			WorkflowID:  row.WorkflowID,
			RunID:       row.RunID.Downcast(),
			SignalID:    row.SignalID,
		}
	}
	return mdb.NamedExecContext(ctx,
		createSignalsRequestedSetQry,
		insertions,
	)
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

// DeleteAllFromSignalsRequestedSets deletes all rows from signals_requested_sets table
func (mdb *db) DeleteAllFromSignalsRequestedSets(
	ctx context.Context,
	filter sqlplugin.SignalsRequestedSetsAllFilter,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		deleteAllSignalsRequestedSetQry,
		map[string]interface{}{
			"shard_id":     filter.ShardID,
			"namespace_id": filter.NamespaceID.Downcast(),
			"workflow_id":  filter.WorkflowID,
			"run_id":       filter.RunID.Downcast(),
		},
	)
}
