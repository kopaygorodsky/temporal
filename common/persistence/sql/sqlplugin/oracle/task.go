package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	// Task Queues
	taskQueueCreatePart = `INTO task_queues(range_hash, task_queue_id, range_id, data, data_encoding) 
		VALUES (:range_hash, :task_queue_id, :range_id, :data, :data_encoding)`

	// Oracle's INSERT statement
	createTaskQueueQry = `INSERT ` + taskQueueCreatePart

	// Oracle's UPDATE statement
	updateTaskQueueQry = `UPDATE task_queues SET
range_id = :range_id,
data = :data,
data_encoding = :data_encoding
WHERE
range_hash = :range_hash AND
task_queue_id = :task_queue_id`

	// Queries for task_queues table
	listTaskQueueRowSelect = `SELECT range_hash, task_queue_id, range_id, data, data_encoding from task_queues `

	// Oracle uses FETCH FIRST instead of LIMIT
	listTaskQueueWithHashRangeQry = listTaskQueueRowSelect +
		`WHERE range_hash >= :range_hash_min AND range_hash <= :range_hash_max AND task_queue_id > :task_queue_id_min 
		ORDER BY task_queue_id ASC FETCH FIRST :page_size ROWS ONLY`

	listTaskQueueQry = listTaskQueueRowSelect +
		`WHERE range_hash = :range_hash AND task_queue_id > :task_queue_id_min 
		ORDER BY task_queue_id ASC FETCH FIRST :page_size ROWS ONLY`

	getTaskQueueQry = listTaskQueueRowSelect +
		`WHERE range_hash = :range_hash AND task_queue_id = :task_queue_id`

	deleteTaskQueueQry = `DELETE FROM task_queues WHERE range_hash = :range_hash AND task_queue_id = :task_queue_id AND range_id = :range_id`

	lockTaskQueueQry = `SELECT range_id FROM task_queues 
		WHERE range_hash = :range_hash AND task_queue_id = :task_queue_id FOR UPDATE`

	// Tasks queries
	getTaskMinMaxQry = `SELECT task_id, data, data_encoding 
		FROM tasks 
		WHERE range_hash = :range_hash AND task_queue_id = :task_queue_id AND task_id >= :min_task_id AND task_id < :max_task_id 
		ORDER BY task_id FETCH FIRST :page_size ROWS ONLY`

	getTaskMinQry = `SELECT task_id, data, data_encoding 
		FROM tasks 
		WHERE range_hash = :range_hash AND task_queue_id = :task_queue_id AND task_id >= :min_task_id 
		ORDER BY task_id FETCH FIRST :page_size ROWS ONLY`

	createTaskQry = `INSERT INTO 
		tasks(range_hash, task_queue_id, task_id, data, data_encoding) 
		VALUES(:range_hash, :task_queue_id, :task_id, :data, :data_encoding)`

	deleteTaskQry = `DELETE FROM tasks 
		WHERE range_hash = :range_hash AND task_queue_id = :task_queue_id AND task_id = :task_id`

	// Oracle uses ROWNUM <= instead of LIMIT for DELETE with ORDER BY
	rangeDeleteTaskQry = `DELETE FROM tasks 
		WHERE range_hash = :range_hash AND task_queue_id = :task_queue_id AND task_id < :task_id AND 
		ROWID IN (
			SELECT ROWID FROM (
				SELECT ROWID FROM tasks 
				WHERE range_hash = :range_hash AND task_queue_id = :task_queue_id AND task_id < :task_id 
				ORDER BY task_queue_id, task_id
			) WHERE ROWNUM <= :row_limit
		)`

	// Task queue user data queries
	getTaskQueueUserDataQry = `SELECT data, data_encoding, version FROM task_queue_user_data 
		WHERE namespace_id = :namespace_id AND task_queue_name = :task_queue_name`

	updateTaskQueueUserDataQry = `UPDATE task_queue_user_data SET 
		data = :data, 
		data_encoding = :data_encoding, 
		version = :new_version 
		WHERE namespace_id = :namespace_id 
		AND task_queue_name = :task_queue_name 
		AND version = :version`

	insertTaskQueueUserDataQry = `INSERT INTO task_queue_user_data
		(namespace_id, task_queue_name, data, data_encoding, version) 
		VALUES (:namespace_id, :task_queue_name, :data, :data_encoding, 1)`

	listTaskQueueUserDataQry = `SELECT task_queue_name, data, data_encoding, version 
		FROM task_queue_user_data 
		WHERE namespace_id = :namespace_id AND task_queue_name > :task_queue_name_min 
		FETCH FIRST :page_size ROWS ONLY`

	// Oracle uses VALUES in INSERT statements differently for multiple rows
	addBuildIdToTaskQueueMappingQryBase = `INSERT INTO build_id_to_task_queue (namespace_id, build_id, task_queue_name) 
		VALUES (:namespace_id, :build_id, :task_queue_name)`

	removeBuildIdToTaskQueueMappingQryBase = `DELETE FROM build_id_to_task_queue 
		WHERE namespace_id = :namespace_id AND task_queue_name = :task_queue_name AND build_id IN (`

	listTaskQueuesByBuildIdQry = `SELECT task_queue_name FROM build_id_to_task_queue 
		WHERE namespace_id = :namespace_id AND build_id = :build_id`

	countTaskQueuesByBuildIdQry = `SELECT COUNT(*) FROM build_id_to_task_queue 
		WHERE namespace_id = :namespace_id AND build_id = :build_id`
)

type localTasksRow struct {
	RangeHash    uint32 `db:"range_hash"`
	TaskQueueID  []byte `db:"task_queue_id"`
	TaskID       int64  `db:"task_id"`
	Data         []byte `db:"data"`
	DataEncoding string `db:"data_encoding"`
}

// InsertIntoTasks inserts one or more rows into tasks table
func (mdb *db) InsertIntoTasks(
	ctx context.Context,
	rows []sqlplugin.TasksRow,
) (sql.Result, error) {
	insertRows := make([]localTasksRow, len(rows))
	for i := range rows {
		insertRows[i] = localTasksRow{
			RangeHash:    rows[i].RangeHash,
			TaskQueueID:  rows[i].TaskQueueID,
			TaskID:       rows[i].TaskID,
			Data:         rows[i].Data,
			DataEncoding: rows[i].DataEncoding,
		}
	}
	return mdb.ExecContext(ctx,
		createTaskQry,
		insertRows,
	)
}

// SelectFromTasks reads one or more rows from tasks table
func (mdb *db) SelectFromTasks(
	ctx context.Context,
	filter sqlplugin.TasksFilter,
) ([]sqlplugin.TasksRow, error) {
	var err error
	var rows []sqlplugin.TasksRow
	params := map[string]interface{}{
		"range_hash":    filter.RangeHash,
		"task_queue_id": filter.TaskQueueID,
		"min_task_id":   *filter.InclusiveMinTaskID,
		"page_size":     *filter.PageSize,
	}

	if filter.ExclusiveMaxTaskID != nil {
		params["max_task_id"] = *filter.ExclusiveMaxTaskID
		err = mdb.NamedSelectContext(ctx,
			&rows,
			getTaskMinMaxQry,
			params,
		)
	} else {
		err = mdb.NamedSelectContext(ctx,
			&rows,
			getTaskMinQry,
			params,
		)
	}
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// DeleteFromTasks deletes one or more rows from tasks table
func (mdb *db) DeleteFromTasks(
	ctx context.Context,
	filter sqlplugin.TasksFilter,
) (sql.Result, error) {
	if filter.ExclusiveMaxTaskID == nil {
		return nil, serviceerror.NewInternal("missing ExclusiveMaxTaskID parameter")
	}
	if filter.Limit == nil || *filter.Limit == 0 {
		return nil, serviceerror.NewInternal("missing limit parameter")
	}

	return mdb.NamedExecContext(ctx,
		rangeDeleteTaskQry,
		map[string]interface{}{
			"range_hash":    filter.RangeHash,
			"task_queue_id": filter.TaskQueueID,
			"task_id":       *filter.ExclusiveMaxTaskID,
			"row_limit":     *filter.Limit,
		},
	)
}

type localTaskQueuesRow struct {
	RangeHash    uint32 `db:"range_hash"`
	TaskQueueID  []byte `db:"task_queue_id"`
	RangeID      int64  `db:"range_id"`
	Data         []byte `db:"data"`
	DataEncoding string `db:"data_encoding"`
}

// InsertIntoTaskQueues inserts one or more rows into task_queues table
func (mdb *db) InsertIntoTaskQueues(
	ctx context.Context,
	row *sqlplugin.TaskQueuesRow,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		createTaskQueueQry,
		localTaskQueuesRow{
			RangeHash:    row.RangeHash,
			TaskQueueID:  row.TaskQueueID,
			RangeID:      row.RangeID,
			Data:         row.Data,
			DataEncoding: row.DataEncoding,
		},
	)
}

// UpdateTaskQueues updates a row in task_queues table
func (mdb *db) UpdateTaskQueues(
	ctx context.Context,
	row *sqlplugin.TaskQueuesRow,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		updateTaskQueueQry,
		localTaskQueuesRow{
			RangeHash:    row.RangeHash,
			TaskQueueID:  row.TaskQueueID,
			RangeID:      row.RangeID,
			Data:         row.Data,
			DataEncoding: row.DataEncoding,
		},
	)
}

// SelectFromTaskQueues reads one or more rows from task_queues table
func (mdb *db) SelectFromTaskQueues(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilter,
) ([]sqlplugin.TaskQueuesRow, error) {
	switch {
	case filter.TaskQueueID != nil:
		if filter.RangeHashLessThanEqualTo != 0 || filter.RangeHashGreaterThanEqualTo != 0 {
			return nil, serviceerror.NewInternal("range of hashes not supported for specific selection")
		}
		return mdb.selectFromTaskQueues(ctx, filter)
	case filter.RangeHashLessThanEqualTo != 0 && filter.PageSize != nil:
		if filter.RangeHashLessThanEqualTo < filter.RangeHashGreaterThanEqualTo {
			return nil, serviceerror.NewInternal("range of hashes bound is invalid")
		}
		return mdb.rangeSelectFromTaskQueues(ctx, filter)
	case filter.TaskQueueIDGreaterThan != nil && filter.PageSize != nil:
		return mdb.rangeSelectFromTaskQueues(ctx, filter)
	default:
		return nil, serviceerror.NewInternal("invalid set of query filter params")
	}
}

func (mdb *db) selectFromTaskQueues(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilter,
) ([]sqlplugin.TaskQueuesRow, error) {
	var err error
	var row sqlplugin.TaskQueuesRow
	err = mdb.NamedGetContext(ctx,
		&row,
		getTaskQueueQry,
		map[string]interface{}{
			"range_hash":    filter.RangeHash,
			"task_queue_id": filter.TaskQueueID,
		},
	)
	if err != nil {
		return nil, err
	}
	return []sqlplugin.TaskQueuesRow{row}, nil
}

func (mdb *db) rangeSelectFromTaskQueues(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilter,
) ([]sqlplugin.TaskQueuesRow, error) {
	var err error
	var rows []sqlplugin.TaskQueuesRow

	if filter.RangeHashLessThanEqualTo != 0 {
		err = mdb.NamedSelectContext(ctx,
			&rows,
			listTaskQueueWithHashRangeQry,
			map[string]interface{}{
				"range_hash_min":    filter.RangeHashGreaterThanEqualTo,
				"range_hash_max":    filter.RangeHashLessThanEqualTo,
				"task_queue_id_min": filter.TaskQueueIDGreaterThan,
				"page_size":         *filter.PageSize,
			},
		)
	} else {
		err = mdb.NamedSelectContext(ctx,
			&rows,
			listTaskQueueQry,
			map[string]interface{}{
				"range_hash":        filter.RangeHash,
				"task_queue_id_min": filter.TaskQueueIDGreaterThan,
				"page_size":         *filter.PageSize,
			},
		)
	}
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// DeleteFromTaskQueues deletes a row from task_queues table
func (mdb *db) DeleteFromTaskQueues(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilter,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		deleteTaskQueueQry,
		map[string]interface{}{
			"range_hash":    filter.RangeHash,
			"task_queue_id": filter.TaskQueueID,
			"range_id":      *filter.RangeID,
		},
	)
}

// LockTaskQueues locks a row in task_queues table
func (mdb *db) LockTaskQueues(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilter,
) (int64, error) {
	var rangeID int64
	err := mdb.NamedGetContext(ctx,
		&rangeID,
		lockTaskQueueQry,
		map[string]interface{}{
			"range_hash":    filter.RangeHash,
			"task_queue_id": filter.TaskQueueID,
		},
	)
	return rangeID, err
}

func (mdb *db) GetTaskQueueUserData(
	ctx context.Context,
	request *sqlplugin.GetTaskQueueUserDataRequest,
) (*sqlplugin.VersionedBlob, error) {
	var row sqlplugin.VersionedBlob
	err := mdb.NamedGetContext(ctx,
		&row,
		getTaskQueueUserDataQry,
		map[string]interface{}{
			"namespace_id":    request.NamespaceID,
			"task_queue_name": request.TaskQueueName,
		},
	)
	return &row, err
}

func (mdb *db) UpdateTaskQueueUserData(
	ctx context.Context,
	request *sqlplugin.UpdateTaskQueueDataRequest,
) error {
	if request.Version == 0 {
		_, err := mdb.NamedExecContext(
			ctx,
			insertTaskQueueUserDataQry,
			map[string]interface{}{
				"namespace_id":    request.NamespaceID,
				"task_queue_name": request.TaskQueueName,
				"data":            request.Data,
				"data_encoding":   request.DataEncoding,
			},
		)
		return err
	}

	result, err := mdb.NamedExecContext(
		ctx,
		updateTaskQueueUserDataQry,
		map[string]interface{}{
			"data":            request.Data,
			"data_encoding":   request.DataEncoding,
			"new_version":     request.Version + 1,
			"namespace_id":    request.NamespaceID,
			"task_queue_name": request.TaskQueueName,
			"version":         request.Version,
		},
	)
	if err != nil {
		return err
	}

	numRows, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if numRows != 1 {
		return &persistence.ConditionFailedError{Msg: "Expected exactly one row to be updated"}
	}
	return nil
}

func (mdb *db) AddToBuildIdToTaskQueueMapping(
	ctx context.Context,
	request sqlplugin.AddToBuildIdToTaskQueueMapping,
) error {
	if len(request.BuildIds) == 0 {
		return nil
	}

	// Oracle doesn't have a direct way to insert multiple rows like MySQL
	// We need to use a different approach - use UNION ALL or execute multiple queries

	// For simplicity here, we'll execute multiple insert statements
	for _, buildId := range request.BuildIds {
		_, err := mdb.NamedExecContext(
			ctx,
			addBuildIdToTaskQueueMappingQryBase,
			map[string]interface{}{
				"namespace_id":    request.NamespaceID,
				"build_id":        buildId,
				"task_queue_name": request.TaskQueueName,
			},
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (mdb *db) RemoveFromBuildIdToTaskQueueMapping(
	ctx context.Context,
	request sqlplugin.RemoveFromBuildIdToTaskQueueMapping,
) error {
	if len(request.BuildIds) == 0 {
		return nil
	}

	// Generate the IN clause placeholders and parameters
	inParams := make([]string, len(request.BuildIds))
	params := map[string]interface{}{
		"namespace_id":    request.NamespaceID,
		"task_queue_name": request.TaskQueueName,
	}

	for i, buildId := range request.BuildIds {
		paramName := fmt.Sprintf("build_id_%d", i)
		inParams[i] = ":" + paramName
		params[paramName] = buildId
	}

	query := removeBuildIdToTaskQueueMappingQryBase + strings.Join(inParams, ", ") + ")"

	_, err := mdb.NamedExecContext(ctx, query, params)
	return err
}

func (mdb *db) ListTaskQueueUserDataEntries(
	ctx context.Context,
	request *sqlplugin.ListTaskQueueUserDataEntriesRequest,
) ([]sqlplugin.TaskQueueUserDataEntry, error) {
	var rows []sqlplugin.TaskQueueUserDataEntry
	err := mdb.NamedSelectContext(
		ctx,
		&rows,
		listTaskQueueUserDataQry,
		map[string]interface{}{
			"namespace_id":        request.NamespaceID,
			"task_queue_name_min": request.LastTaskQueueName,
			"page_size":           request.Limit,
		},
	)
	return rows, err
}

func (mdb *db) GetTaskQueuesByBuildId(
	ctx context.Context,
	request *sqlplugin.GetTaskQueuesByBuildIdRequest,
) ([]string, error) {
	var rows []struct {
		TaskQueueName string
	}

	err := mdb.NamedSelectContext(
		ctx,
		&rows,
		listTaskQueuesByBuildIdQry,
		map[string]interface{}{
			"namespace_id": request.NamespaceID,
			"build_id":     request.BuildID,
		},
	)

	taskQueues := make([]string, len(rows))
	for i, row := range rows {
		taskQueues[i] = row.TaskQueueName
	}
	return taskQueues, err
}

func (mdb *db) CountTaskQueuesByBuildId(
	ctx context.Context,
	request *sqlplugin.CountTaskQueuesByBuildIdRequest,
) (int, error) {
	var count int
	err := mdb.NamedGetContext(
		ctx,
		&count,
		countTaskQueuesByBuildIdQry,
		map[string]interface{}{
			"namespace_id": request.NamespaceID,
			"build_id":     request.BuildID,
		},
	)
	return count, err
}
