package oracle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	go_ora "github.com/sijms/go-ora/v2"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/oracle/session"
	"strings"
	"time"
)

const (
	// Explicit INSERT statement for workflow execution
	templateInsertWorkflowExecution = `
		INSERT INTO executions_visibility (
			namespace_id, 
			run_id, 
			workflow_type_name,
			workflow_id,
			start_time,
			execution_time,
			status,
			close_time,
			history_length,
			history_size_bytes,
			execution_duration,
			state_transition_count,
			memo,
			encoding,
			task_queue,
			search_attributes,
			parent_workflow_id,
			parent_run_id,
			root_workflow_id,
			root_run_id,
			version_num
		) VALUES (
			:namespace_id, 
			:run_id, 
			:workflow_type_name,
			:workflow_id,
			:start_time,
			:execution_time,
			:status,
			:close_time,
			:history_length,
			:history_size_bytes,
			:execution_duration,
			:state_transition_count,
			:memo,
			:encoding,
			:task_queue,
			:search_attributes,
			:parent_workflow_id,
			:parent_run_id,
			:root_workflow_id,
			:root_run_id,
			:version_num
		)`

	templateUpdateWorkflowExecution = `
		UPDATE executions_visibility
		SET 
			workflow_type_name = :workflow_type_name,
			workflow_id = :workflow_id,
			start_time = :start_time,
			execution_time = :execution_time,
			status = :status,
			close_time = :close_time,
			history_length = :history_length,
			history_size_bytes = :history_size_bytes,
			execution_duration = :execution_duration,
			state_transition_count = :state_transition_count,
			memo = :memo,
			encoding = :encoding,
			task_queue = :task_queue,
			search_attributes = :search_attributes,
			parent_workflow_id = :parent_workflow_id,
			parent_run_id = :parent_run_id,
			root_workflow_id = :root_workflow_id,
			root_run_id = :root_run_id,
			version_num = :version_num
		WHERE 
			namespace_id = :namespace_id 
			AND run_id = :run_id
			AND version_num < :version_num`

	// Explicit MERGE statement for workflow execution upsert
	templateUpsertWorkflowExecution = `
		MERGE INTO executions_visibility t
		USING (
			SELECT 
				:namespace_id as namespace_id,
				:run_id as run_id,
				:workflow_type_name as workflow_type_name,
				:workflow_id as workflow_id,
				:start_time as start_time,
				:execution_time as execution_time,
				:status as status,
				:close_time as close_time,
				:history_length as history_length,
				:history_size_bytes as history_size_bytes,
				:execution_duration as execution_duration,
				:state_transition_count as state_transition_count,
				:memo as memo,
				:encoding as encoding,
				:task_queue as task_queue,
				:search_attributes as search_attributes,
				:parent_workflow_id as parent_workflow_id,
				:parent_run_id as parent_run_id,
				:root_workflow_id as root_workflow_id,
				:root_run_id as root_run_id,
				:version_num as version_num
			FROM dual
		) s
		ON (t.namespace_id = s.namespace_id AND t.run_id = s.run_id)
		WHEN MATCHED THEN
			UPDATE SET 
				t.workflow_type_name = CASE WHEN t.version_num < s.version_num THEN s.workflow_type_name ELSE t.workflow_type_name END,
				t.workflow_id = CASE WHEN t.version_num < s.version_num THEN s.workflow_id ELSE t.workflow_id END,
				t.start_time = CASE WHEN t.version_num < s.version_num THEN s.start_time ELSE t.start_time END,
				t.execution_time = CASE WHEN t.version_num < s.version_num THEN s.execution_time ELSE t.execution_time END,
				t.status = CASE WHEN t.version_num < s.version_num THEN s.status ELSE t.status END,
				t.close_time = CASE WHEN t.version_num < s.version_num THEN s.close_time ELSE t.close_time END,
				t.history_length = CASE WHEN t.version_num < s.version_num THEN s.history_length ELSE t.history_length END,
				t.history_size_bytes = CASE WHEN t.version_num < s.version_num THEN s.history_size_bytes ELSE t.history_size_bytes END,
				t.execution_duration = CASE WHEN t.version_num < s.version_num THEN s.execution_duration ELSE t.execution_duration END,
				t.state_transition_count = CASE WHEN t.version_num < s.version_num THEN s.state_transition_count ELSE t.state_transition_count END,
				t.memo = CASE WHEN t.version_num < s.version_num THEN s.memo ELSE t.memo END,
				t.encoding = CASE WHEN t.version_num < s.version_num THEN s.encoding ELSE t.encoding END,
				t.task_queue = CASE WHEN t.version_num < s.version_num THEN s.task_queue ELSE t.task_queue END,
				t.search_attributes = CASE WHEN t.version_num < s.version_num THEN s.search_attributes ELSE t.search_attributes END,
				t.parent_workflow_id = CASE WHEN t.version_num < s.version_num THEN s.parent_workflow_id ELSE t.parent_workflow_id END,
				t.parent_run_id = CASE WHEN t.version_num < s.version_num THEN s.parent_run_id ELSE t.parent_run_id END,
				t.root_workflow_id = CASE WHEN t.version_num < s.version_num THEN s.root_workflow_id ELSE t.root_workflow_id END,
				t.root_run_id = CASE WHEN t.version_num < s.version_num THEN s.root_run_id ELSE t.root_run_id END,
				t.version_num = CASE WHEN t.version_num < s.version_num THEN s.version_num ELSE t.version_num END
		WHEN NOT MATCHED THEN
			INSERT (
				namespace_id, 
				run_id, 
				workflow_type_name,
				workflow_id,
				start_time,
				execution_time,
				status,
				close_time,
				history_length,
				history_size_bytes,
				execution_duration,
				state_transition_count,
				memo,
				encoding,
				task_queue,
				search_attributes,
				parent_workflow_id,
				parent_run_id,
				root_workflow_id,
				root_run_id,
				version_num
			) VALUES (
				s.namespace_id, 
				s.run_id, 
				s.workflow_type_name,
				s.workflow_id,
				s.start_time,
				s.execution_time,
				s.status,
				s.close_time,
				s.history_length,
				s.history_size_bytes,
				s.execution_duration,
				s.state_transition_count,
				s.memo,
				s.encoding,
				s.task_queue,
				s.search_attributes,
				s.parent_workflow_id,
				s.parent_run_id,
				s.root_workflow_id,
				s.root_run_id,
				s.version_num
			)`

	// Explicit INSERT SQL for custom search attributes
	templateInsertCustomSearchAttributes = `
		INSERT INTO custom_search_attributes (
			namespace_id, 
			run_id, 
			search_attributes,
			version_num
		) VALUES (
			:namespace_id, 
			:run_id, 
			:search_attributes,
			:version_num
		)`

	templateUpdateCustomSearchAttributes = `
	UPDATE custom_search_attributes
		SET 
			search_attributes = :search_attributes,
			version_num = :version_num
		WHERE 
			namespace_id = :namespace_id 
			AND run_id = :run_id
			AND version_num < :version_num`

	// Explicit UPSERT SQL for custom search attributes
	templateUpsertCustomSearchAttributes = `
		MERGE INTO custom_search_attributes t
		USING (
			SELECT 
				:namespace_id as namespace_id,
				:run_id as run_id,
				:search_attributes as search_attributes,
				:version_num as version_num
			FROM dual
		) s
		ON (t.namespace_id = s.namespace_id AND t.run_id = s.run_id)
		WHEN MATCHED THEN
			UPDATE SET 
				search_attributes = CASE WHEN t.version_num < s.version_num THEN s.search_attributes ELSE t.search_attributes END,
				version_num = CASE WHEN t.version_num < s.version_num THEN s.version_num ELSE t.version_num END
		WHEN NOT MATCHED THEN
			INSERT (namespace_id, run_id, search_attributes, version_num)
			VALUES (s.namespace_id, s.run_id, s.search_attributes, s.version_num)`

	// Explicit GET SQL for workflow execution
	templateGetWorkflowExecution = `
		SELECT 
			namespace_id,
			run_id,
			workflow_type_name,
			workflow_id,
			start_time,
			execution_time,
			status,
			close_time,
			history_length,
			history_size_bytes,
			execution_duration,
			state_transition_count,
			memo,
			encoding,
			task_queue,
			search_attributes,
			parent_workflow_id,
			parent_run_id,
			root_workflow_id,
			root_run_id,
			version_num
		FROM executions_visibility
		WHERE namespace_id = :namespace_id AND run_id = :run_id`

	// Explicit DELETE SQL for workflow execution
	templateDeleteWorkflowExecution = `
		DELETE FROM executions_visibility
		WHERE namespace_id = :namespace_id AND run_id = :run_id`

	// Explicit DELETE SQL for custom search attributes
	templateDeleteCustomSearchAttributes = `
		DELETE FROM custom_search_attributes
		WHERE namespace_id = :namespace_id AND run_id = :run_id`
)

// execLocalVisibilityRow represents a row in the visibility table
type execLocalVisibilityRow struct {
	NamespaceID          string                               `db:"namespace_id"`
	RunID                string                               `db:"run_id"`
	WorkflowTypeName     string                               `db:"workflow_type_name"`
	WorkflowID           string                               `db:"workflow_id"`
	StartTime            go_ora.TimeStamp                     `db:"start_time"`
	ExecutionTime        go_ora.TimeStamp                     `db:"execution_time"`
	Status               int32                                `db:"status"`
	CloseTime            *go_ora.TimeStamp                    `db:"close_time"`
	HistoryLength        *int64                               `db:"history_length"`
	HistorySizeBytes     *int64                               `db:"history_size_bytes"`
	ExecutionDuration    *time.Duration                       `db:"execution_duration"`
	StateTransitionCount *int64                               `db:"state_transition_count"`
	Memo                 []byte                               `db:"memo"`
	Encoding             string                               `db:"encoding"`
	TaskQueue            string                               `db:"task_queue"`
	SearchAttributes     sqlplugin.VisibilitySearchAttributes `db:"search_attributes"`
	ParentWorkflowID     *string                              `db:"parent_workflow_id"`
	ParentRunID          *string                              `db:"parent_run_id"`
	RootWorkflowID       string                               `db:"root_workflow_id"`
	RootRunID            string                               `db:"root_run_id"`
	VersionNum           int64                                `db:"version_num"` // Changed from Version to VersionNum for consistency
}

func newExecLocalVisibilityRow(row *sqlplugin.VisibilityRow) *execLocalVisibilityRow {
	var attrs sqlplugin.VisibilitySearchAttributes
	if row.SearchAttributes != nil {
		attrs = *row.SearchAttributes
	}
	return &execLocalVisibilityRow{
		NamespaceID:          padID(row.NamespaceID),
		RunID:                padID(row.RunID),
		WorkflowTypeName:     row.WorkflowTypeName,
		WorkflowID:           row.WorkflowID,
		StartTime:            go_ora.TimeStamp(row.StartTime),
		ExecutionTime:        go_ora.TimeStamp(row.ExecutionTime),
		Status:               row.Status,
		CloseTime:            session.GetOraTimeStampPtr(row.CloseTime),
		HistoryLength:        row.HistoryLength,
		HistorySizeBytes:     row.HistorySizeBytes,
		ExecutionDuration:    row.ExecutionDuration,
		StateTransitionCount: row.StateTransitionCount,
		Memo:                 row.Memo,
		Encoding:             row.Encoding,
		TaskQueue:            row.TaskQueue,
		SearchAttributes:     attrs,
		ParentWorkflowID:     row.ParentWorkflowID,
		ParentRunID:          row.ParentRunID,
		RootWorkflowID:       row.RootWorkflowID,
		RootRunID:            row.RootRunID,
		VersionNum:           row.Version, // Updated field name
	}
}

type queryLocalVisibilityRow struct {
	NamespaceID          string
	RunID                string
	WorkflowTypeName     string
	WorkflowID           string
	StartTime            go_ora.TimeStamp
	ExecutionTime        go_ora.TimeStamp
	Status               int32
	CloseTime            *go_ora.TimeStamp
	HistoryLength        *int64
	HistorySizeBytes     *int64
	ExecutionDuration    *time.Duration
	StateTransitionCount *int64
	Memo                 []byte
	Encoding             string
	TaskQueue            sql.NullString
	SearchAttributes     *sqlplugin.VisibilitySearchAttributes
	ParentWorkflowID     *string
	ParentRunID          *string
	RootWorkflowID       sql.NullString
	RootRunID            sql.NullString
	VersionNum           int64
}

func (row queryLocalVisibilityRow) toExternalType() sqlplugin.VisibilityRow {
	taskQueue := ""
	if row.TaskQueue.Valid {
		taskQueue = row.TaskQueue.String
	}
	rootWorkflowID := ""
	if row.RootWorkflowID.Valid {
		rootWorkflowID = row.RootWorkflowID.String
	}

	rootRunID := ""
	if row.RootRunID.Valid {
		rootRunID = row.RootRunID.String
	}

	return sqlplugin.VisibilityRow{
		NamespaceID:          row.NamespaceID,
		RunID:                row.RunID,
		WorkflowTypeName:     row.WorkflowTypeName,
		WorkflowID:           row.WorkflowID,
		StartTime:            time.Time(row.StartTime),
		ExecutionTime:        time.Time(row.ExecutionTime),
		Status:               row.Status,
		CloseTime:            session.GetTimeStampPtrFromOra(row.CloseTime),
		HistoryLength:        row.HistoryLength,
		HistorySizeBytes:     row.HistorySizeBytes,
		ExecutionDuration:    row.ExecutionDuration,
		StateTransitionCount: row.StateTransitionCount,
		Memo:                 row.Memo,
		Encoding:             row.Encoding,
		TaskQueue:            taskQueue,
		SearchAttributes:     row.SearchAttributes,
		ParentWorkflowID:     row.ParentWorkflowID,
		ParentRunID:          row.ParentRunID,
		RootWorkflowID:       rootWorkflowID,
		RootRunID:            rootRunID,
		Version:              row.VersionNum,
	}
}

// InsertIntoVisibility inserts a row into visibility table. If a row already exists,
// it's left as such and no update will be made
func (mdb *db) InsertIntoVisibility(
	ctx context.Context,
	row *sqlplugin.VisibilityRow,
) (result sql.Result, retError error) {
	defer func() {
		retError = mdb.handle.ConvertError(retError)
	}()
	db, err := mdb.handle.DB()
	if err != nil {
		return nil, err
	}

	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		err := tx.Rollback()
		// If the error is sql.ErrTxDone, it means the transaction already closed, so ignore error.
		if err != nil && !errors.Is(err, sql.ErrTxDone) {
			// Transaction rollback error should never happen, unless db connection was lost.
			retError = fmt.Errorf("transaction rollback failed: %w", retError)
		}
	}()
	result, err = tx.NamedExecContext(ctx, templateInsertWorkflowExecution, newExecLocalVisibilityRow(row))
	if err != nil {
		if !mdb.IsDupEntryError(err) {
			return nil, fmt.Errorf("unable to insert workflow execution: %w", err)
		}
	}

	_, err = tx.NamedExecContext(ctx, templateInsertCustomSearchAttributes, newExecLocalVisibilityRow(row))
	if err != nil {
		if !mdb.IsDupEntryError(err) {
			return nil, fmt.Errorf("unable to insert custom search attributes: %w", err)
		}
	}
	err = tx.Commit()
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ReplaceIntoVisibility replaces an existing row if it exists or creates a new row in visibility table
func (mdb *db) ReplaceIntoVisibility(
	ctx context.Context,
	row *sqlplugin.VisibilityRow,
) (result sql.Result, retError error) {
	defer func() {
		retError = mdb.handle.ConvertError(retError)
	}()

	db, err := mdb.handle.DB()
	if err != nil {
		return nil, err
	}
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		err := tx.Rollback()
		// If the error is sql.ErrTxDone, it means the transaction already closed, so ignore error.
		if err != nil && !errors.Is(err, sql.ErrTxDone) {
			// Transaction rollback error should never happen, unless db connection was lost.
			retError = fmt.Errorf("transaction rollback failed: %w", retError)
		}
	}()

	queryRes := &queryResult{
		lastInsertedID: 0,
		rowsAffected:   0,
	}

	result, err = tx.NamedExecContext(ctx, templateInsertWorkflowExecution, newExecLocalVisibilityRow(row))
	if err != nil {
		if !mdb.IsDupEntryError(err) {
			return nil, fmt.Errorf("unable to upsert (insert) workflow execution: %w", err)
		}

		if _, err := tx.NamedExecContext(ctx, templateUpdateWorkflowExecution, newExecLocalVisibilityRow(row)); err != nil {
			return nil, fmt.Errorf("unable to upsert (update) workflow execution: %w", err)
		}
	}

	queryRes.rowsAffected++

	result, err = tx.NamedExecContext(ctx, templateInsertCustomSearchAttributes, newExecLocalVisibilityRow(row))
	if err != nil {
		if !mdb.IsDupEntryError(err) {
			return nil, fmt.Errorf("unable to upsert (insert) custom search attributes: %w", err)
		}

		if _, err := tx.NamedExecContext(ctx, templateUpdateCustomSearchAttributes, newExecLocalVisibilityRow(row)); err != nil {
			return nil, fmt.Errorf("unable to upsert (update) custom search attributes: %w", err)
		}
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}
	return queryRes, nil
}

// DeleteFromVisibility deletes a row from visibility table if it exists
func (mdb *db) DeleteFromVisibility(
	ctx context.Context,
	filter sqlplugin.VisibilityDeleteFilter,
) (result sql.Result, retError error) {
	defer func() {
		retError = mdb.handle.ConvertError(retError)
	}()
	db, err := mdb.handle.DB()
	if err != nil {
		return nil, err
	}

	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		err := tx.Rollback()
		// If the error is sql.ErrTxDone, it means the transaction already closed, so ignore error.
		if err != nil && !errors.Is(err, sql.ErrTxDone) {
			// Transaction rollback error should never happen, unless db connection was lost.
			retError = fmt.Errorf("transaction rollback failed: %w", retError)
		}
	}()
	_, err = tx.NamedExecContext(ctx, templateDeleteCustomSearchAttributes, map[string]interface{}{
		"namespace_id": padID(filter.NamespaceID),
		"run_id":       padID(filter.RunID),
	})
	if err != nil {
		return nil, fmt.Errorf("unable to delete custom search attributes: %w", err)
	}
	result, err = tx.NamedExecContext(ctx, templateDeleteWorkflowExecution, map[string]interface{}{
		"namespace_id": padID(filter.NamespaceID),
		"run_id":       padID(filter.RunID),
	})
	if err != nil {
		return nil, fmt.Errorf("unable to delete workflow execution: %w", err)
	}
	err = tx.Commit()
	if err != nil {
		return nil, err
	}
	return result, nil
}

// SelectFromVisibility reads one or more rows from visibility table
// I was trying to keep it consistent with other plugins, but it's a mess. All these mayhem will be cleaned in the new extension.
func (mdb *db) SelectFromVisibility(
	ctx context.Context,
	filter sqlplugin.VisibilitySelectFilter,
) ([]sqlplugin.VisibilityRow, error) {
	if len(filter.Query) == 0 {
		// backward compatibility for existing tests
		err := GenerateOracleSelectQuery(&filter)
		if err != nil {
			return nil, err
		}
	}

	// When modifying filter parameters
	for i, arg := range filter.QueryArgs {
		switch argR := arg.(type) {
		case time.Time:
			ts := go_ora.TimeStamp(argR)
			filter.QueryArgs[i] = ts
		case *time.Time:
			ts := go_ora.TimeStamp(*argR)
			filter.QueryArgs[i] = &ts
		case string:
			if argR == "" {
				filter.QueryArgs[i] = nil
			}
		}
	}

	var rows []queryLocalVisibilityRow
	err := mdb.SelectContext(ctx, &rows, filter.Query, filter.QueryArgs...)
	if err != nil {
		return nil, err
	}

	res := make([]sqlplugin.VisibilityRow, len(rows))
	for i := range rows {
		externalRow := rows[i].toExternalType()
		if err := mdb.processRowFromDB(&externalRow); err != nil {
			return nil, err
		}
		res[i] = externalRow
	}
	return res, nil
}

// GetFromVisibility reads one row from visibility table
func (mdb *db) GetFromVisibility(
	ctx context.Context,
	filter sqlplugin.VisibilityGetFilter,
) (*sqlplugin.VisibilityRow, error) {
	var row queryLocalVisibilityRow
	stmt, err := mdb.PrepareNamedContext(ctx, templateGetWorkflowExecution)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	err = stmt.GetContext(ctx, &row, filter)
	if err != nil {
		return nil, err
	}

	externalRow := row.toExternalType()
	if err := mdb.processRowFromDB(&externalRow); err != nil {
		return nil, err
	}
	return &externalRow, nil
}

// CountFromVisibility counts rows based on the filter
func (mdb *db) CountFromVisibility(
	ctx context.Context,
	filter sqlplugin.VisibilitySelectFilter,
) (int64, error) {
	var count int64
	err := mdb.GetContext(ctx, &count, filter.Query, filter.QueryArgs...)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// CountGroupByFromVisibility performs a count with group by
func (mdb *db) CountGroupByFromVisibility(
	ctx context.Context,
	filter sqlplugin.VisibilitySelectFilter,
) (_ []sqlplugin.VisibilityCountRow, retError error) {
	defer func() {
		retError = mdb.handle.ConvertError(retError)
	}()
	db, err := mdb.handle.DB()
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, filter.Query, filter.QueryArgs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return sqlplugin.ParseCountGroupByRows(rows, filter.GroupBy)
}

// processRowFromDB converts row data from the database format to application format
func (mdb *db) processRowFromDB(row *sqlplugin.VisibilityRow) error {
	if row == nil {
		return nil
	}

	if row.SearchAttributes != nil {
		for saName, saValue := range *row.SearchAttributes {
			switch typedSaValue := saValue.(type) {
			case []interface{}:
				// the only valid type is slice of strings
				strSlice := make([]string, len(typedSaValue))
				for i, item := range typedSaValue {
					switch v := item.(type) {
					case string:
						strSlice[i] = v
					default:
						return fmt.Errorf("%w: %T (expected string)", sqlplugin.ErrInvalidKeywordListDataType, v)
					}
				}
				(*row.SearchAttributes)[saName] = strSlice
			default:
				// no-op
			}
		}
	}
	return nil
}

//it was used for padding CHAR(64) type, but we did switch to VARCHAR2 to remove stupid behaviour of padding.
func padID(id string) string {
	return id
}

func GenerateOracleSelectQuery(filter *sqlplugin.VisibilitySelectFilter) error {
	// Start building the query
	whereClauses := []string{"namespace_id = :1"}
	queryArgs := []interface{}{filter.NamespaceID}
	paramIndex := 1

	// Determine page size (with safe default)
	pageSize := 10
	if filter.PageSize != nil {
		pageSize = *filter.PageSize
	}

	// Add WorkflowID condition if present
	if filter.WorkflowID != nil {
		paramIndex++
		whereClauses = append(whereClauses, fmt.Sprintf("workflow_id = :%d", paramIndex))
		queryArgs = append(queryArgs, *filter.WorkflowID)
	}

	// Add WorkflowTypeName condition if present
	if filter.WorkflowTypeName != nil {
		paramIndex++
		whereClauses = append(whereClauses, fmt.Sprintf("workflow_type_name = :%d", paramIndex))
		queryArgs = append(queryArgs, *filter.WorkflowTypeName)
	}

	// Determine time attribute based on status
	timeAttr := "start_time"
	if filter.Status != int32(1) { // RUNNING = 1
		timeAttr = "close_time"
	}

	// Add status condition
	if filter.Status == int32(0) { // UNSPECIFIED = 0
		paramIndex++
		whereClauses = append(whereClauses, fmt.Sprintf("status != :%d", paramIndex))
		queryArgs = append(queryArgs, int32(1)) // NOT RUNNING
	} else {
		paramIndex++
		whereClauses = append(whereClauses, fmt.Sprintf("status = :%d", paramIndex))
		queryArgs = append(queryArgs, filter.Status)
	}

	// For pagination, always add MinTime and MaxTime with defaults
	// Add MinTime - use very old date (1700-01-01) if nil
	paramIndex++
	minTimeParam := paramIndex
	whereClauses = append(whereClauses, fmt.Sprintf("NVL(%s, TO_TIMESTAMP('9999-12-31', 'YYYY-MM-DD')) >= :%d", timeAttr, minTimeParam))

	var minTimeOracle go_ora.TimeStamp
	if filter.MinTime != nil {
		minTimeOracle = go_ora.TimeStamp(*filter.MinTime)
	} else {
		// Create a TimeStamp for 1700-01-01
		oldTime, _ := time.Parse("2006-01-02", "1700-01-01")
		minTimeOracle = go_ora.TimeStamp(oldTime)
	}
	queryArgs = append(queryArgs, minTimeOracle)

	// Add MaxTime - use far future date (9999-12-31) if nil
	paramIndex++
	maxTimeParam := paramIndex
	whereClauses = append(whereClauses, fmt.Sprintf("NVL(%s, TO_TIMESTAMP('9999-12-31', 'YYYY-MM-DD')) <= :%d", timeAttr, maxTimeParam))

	var maxTimeOracle go_ora.TimeStamp
	if filter.MaxTime != nil {
		maxTimeOracle = go_ora.TimeStamp(*filter.MaxTime)
	} else {
		// Create a TimeStamp for 9999-12-31
		farFutureTime, _ := time.Parse("2006-01-02", "9999-12-31")
		maxTimeOracle = go_ora.TimeStamp(farFutureTime)
	}
	queryArgs = append(queryArgs, maxTimeOracle)

	// Add RunID pagination when RunID is provided
	if filter.RunID != nil && *filter.RunID != "" {
		paramIndex++
		maxTimeAgainParam := paramIndex
		queryArgs = append(queryArgs, maxTimeOracle) // Reuse maxTime

		paramIndex++
		runIDParam := paramIndex
		queryArgs = append(queryArgs, *filter.RunID)

		// IMPORTANT FIX: Correct order in condition - timestamps with timestamps, UUIDs with UUIDs
		paginationClause := fmt.Sprintf(`(
            (NVL(%s, TO_TIMESTAMP('9999-12-31', 'YYYY-MM-DD')) = :%d AND run_id > :%d) 
            OR NVL(%s, TO_TIMESTAMP('9999-12-31', 'YYYY-MM-DD')) < :%d
        )`, timeAttr, maxTimeAgainParam, runIDParam, timeAttr, maxTimeAgainParam)

		whereClauses = append(whereClauses, paginationClause)
	}

	// Build the complete query
	filter.Query = fmt.Sprintf(
		`SELECT * FROM (
            SELECT namespace_id, run_id, workflow_type_name, workflow_id, start_time, execution_time, 
                status, close_time, history_length, history_size_bytes, execution_duration, 
                state_transition_count, memo, encoding, task_queue, search_attributes, 
                parent_workflow_id, parent_run_id, root_workflow_id, root_run_id, version_num 
            FROM executions_visibility
            WHERE %s
            ORDER BY NVL(%s, TO_TIMESTAMP('9999-12-31', 'YYYY-MM-DD')) DESC, run_id
        ) WHERE ROWNUM <= %d`,
		strings.Join(whereClauses, " AND "),
		timeAttr,
		pageSize,
	)

	filter.QueryArgs = queryArgs
	return nil
}
