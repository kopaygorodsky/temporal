package oracle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	go_ora "github.com/sijms/go-ora/v2"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/oracle/session"
	"strings"
	"time"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	templateInsertCustomSearchAttributes = `
		MERGE INTO custom_search_attributes t
		USING (
			SELECT 
				:namespace_id as namespace_id,
				:run_id as run_id,
				:search_attributes as search_attributes
			FROM dual
		) s
		ON (t.namespace_id = s.namespace_id AND t.run_id = s.run_id)
		WHEN MATCHED THEN
			UPDATE SET search_attributes = s.search_attributes
		WHEN NOT MATCHED THEN
			INSERT (namespace_id, run_id, search_attributes)
			VALUES (s.namespace_id, s.run_id, s.search_attributes)`

	templateUpsertCustomSearchAttributes = `
		MERGE INTO custom_search_attributes t
		USING (
			SELECT 
				:namespace_id as namespace_id,
				:run_id as run_id,
				:search_attributes as search_attributes,
				:_version as _version
			FROM dual
		) s
		ON (t.namespace_id = s.namespace_id AND t.run_id = s.run_id)
		WHEN MATCHED THEN
			UPDATE SET 
				search_attributes = CASE WHEN t._version < s._version THEN s.search_attributes ELSE t.search_attributes END,
				_version = CASE WHEN t._version < s._version THEN s._version ELSE t._version END
		WHEN NOT MATCHED THEN
			INSERT (namespace_id, run_id, search_attributes, _version)
			VALUES (s.namespace_id, s.run_id, s.search_attributes, s._version)`

	templateDeleteWorkflowExecution_v8 = `
		DELETE FROM executions_visibility
		WHERE namespace_id = :namespace_id AND run_id = :run_id`

	templateDeleteCustomSearchAttributes = `
		DELETE FROM custom_search_attributes
		WHERE namespace_id = :namespace_id AND run_id = :run_id`
)

var (
	templateInsertWorkflowExecution = fmt.Sprintf(
		`INSERT INTO executions_visibility (%s)
		VALUES (%s)`,
		strings.Join(sqlplugin.DbFields, ", "),
		sqlplugin.BuildNamedPlaceholder(sqlplugin.DbFields...),
	)
	// Oracle MERGE statement for upsert
	templateUpsertWorkflowExecution = fmt.Sprintf(
		`MERGE INTO executions_visibility t
		USING (
			SELECT %s FROM dual
		) s
		ON (t.namespace_id = s.namespace_id AND t.run_id = s.run_id)
		WHEN MATCHED THEN
			UPDATE SET %s
		WHEN NOT MATCHED THEN
			INSERT (%s)
			VALUES (%s)`,
		buildOracleDualSource(sqlplugin.DbFields),
		buildOracleUpdateSet(sqlplugin.DbFields),
		strings.Join(sqlplugin.DbFields, ", "),
		strings.Join(prefixFields("s.", sqlplugin.DbFields), ", "),
	)
	templateGetWorkflowExecution_v8 = fmt.Sprintf(
		`SELECT %s FROM executions_visibility
		WHERE namespace_id = :namespace_id AND run_id = :run_id`,
		strings.Join(sqlplugin.DbFields, ", "),
	)
)

// Helper function to create the source part of MERGE statement for Oracle
func buildOracleDualSource(fields []string) string {
	sourceParts := make([]string, len(fields))
	for i, field := range fields {
		sourceParts[i] = fmt.Sprintf(":%s as %s", field, field)
	}
	return strings.Join(sourceParts, ", ")
}

// Helper function to create the UPDATE SET part of MERGE statement for Oracle with versioning
func buildOracleUpdateSet(fields []string) string {
	versionIdx := -1
	for i, field := range fields {
		if field == sqlplugin.VersionColumnName {
			versionIdx = i
			break
		}
	}

	if versionIdx < 0 {
		// If version column isn't found, just do a regular update
		updateParts := make([]string, len(fields))
		for i, field := range fields {
			updateParts[i] = fmt.Sprintf("t.%s = s.%s", field, field)
		}
		return strings.Join(updateParts, ", ")
	}

	// With version column, we need to update only if new version is greater
	updateParts := make([]string, len(fields))
	for i, field := range fields {
		if field == sqlplugin.VersionColumnName {
			updateParts[i] = fmt.Sprintf("t.%s = CASE WHEN t.%s < s.%s THEN s.%s ELSE t.%s END",
				field, sqlplugin.VersionColumnName, sqlplugin.VersionColumnName, field, field)
		} else {
			updateParts[i] = fmt.Sprintf("t.%s = CASE WHEN t.%s < s.%s THEN s.%s ELSE t.%s END",
				field, sqlplugin.VersionColumnName, sqlplugin.VersionColumnName, field, field)
		}
	}
	return strings.Join(updateParts, ", ")
}

// Helper function to prefix fields (s.field1, s.field2, ...)
func prefixFields(prefix string, fields []string) []string {
	result := make([]string, len(fields))
	for i, field := range fields {
		result[i] = prefix + field
	}
	return result
}

type execLocalVisibilityRow struct {
	NamespaceID          string                 `db:"namespace_id"`
	RunID                string                 `db:"run_id"`
	WorkflowTypeName     string                 `db:"workflow_type_name"`
	WorkflowID           string                 `db:"workflow_id"`
	StartTime            go_ora.TimeStamp       `db:"start_time"`
	ExecutionTime        go_ora.TimeStamp       `db:"execution_time"`
	Status               int32                  `db:"status"`
	CloseTime            *go_ora.TimeStamp      `db:"close_time"`
	HistoryLength        *int64                 `db:"history_length"`
	HistorySizeBytes     *int64                 `db:"history_size_bytes"`
	ExecutionDuration    *time.Duration         `db:"execution_duration"`
	StateTransitionCount *int64                 `db:"state_transition_count"`
	Memo                 []byte                 `db:"memo"`
	Encoding             string                 `db:"encoding"`
	TaskQueue            string                 `db:"task_queue"`
	SearchAttributes     map[string]interface{} `db:"search_attributes"`
	ParentWorkflowID     *string                `db:"parent_workflow_id"`
	ParentRunID          *string                `db:"parent_run_id"`
	RootWorkflowID       string                 `db:"root_workflow_id"`
	RootRunID            string                 `db:"root_run_id"`

	// Version must be at the end because the version column has to be the last column in the insert statement.
	// Otherwise we may do partial updates as the version changes halfway through.
	// This is because MySQL doesn't support row versioning in a way that prevents out-of-order updates.
	Version int64 `db:"_version"`
}

func newExecLocalVisibilityRow(row *sqlplugin.VisibilityRow) *execLocalVisibilityRow {

	return &execLocalVisibilityRow{
		NamespaceID:          row.NamespaceID,
		RunID:                row.RunID,
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
		SearchAttributes:     nil,
		ParentWorkflowID:     row.ParentWorkflowID,
		ParentRunID:          row.ParentRunID,
		RootWorkflowID:       row.RootWorkflowID,
		RootRunID:            row.RootRunID,
		Version:              row.Version,
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
	TaskQueue            string
	SearchAttributes     *sqlplugin.VisibilitySearchAttributes
	ParentWorkflowID     *string
	ParentRunID          *string
	RootWorkflowID       string
	RootRunID            string

	// Version must be at the end because the version column has to be the last column in the insert statement.
	// Otherwise we may do partial updates as the version changes halfway through.
	// This is because MySQL doesn't support row versioning in a way that prevents out-of-order updates.
	Version int64 `db:"_version"`
}

func (row queryLocalVisibilityRow) toExternalType() sqlplugin.VisibilityRow {
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
		TaskQueue:            row.TaskQueue,
		SearchAttributes:     row.SearchAttributes,
		ParentWorkflowID:     row.ParentWorkflowID,
		ParentRunID:          row.ParentRunID,
		RootWorkflowID:       row.RootWorkflowID,
		RootRunID:            row.RootRunID,
		Version:              row.Version,
	}
}

// VisibilitySelectFilter contains the column names within executions_visibility table that
// can be used to filter results through a WHERE clause
type localVisibilitySelectFilter struct {
	NamespaceID      string
	RunID            *string
	WorkflowID       *string
	WorkflowTypeName *string
	Status           int32
	MinTime          *go_ora.TimeStamp
	MaxTime          *go_ora.TimeStamp
	PageSize         *int

	Query     string
	QueryArgs []interface{}
	GroupBy   []string
}

func newLocalVisibilitySelectFilter(filter sqlplugin.VisibilitySelectFilter) *localVisibilitySelectFilter {
	return &localVisibilitySelectFilter{
		NamespaceID:      filter.NamespaceID,
		RunID:            filter.RunID,
		WorkflowID:       filter.WorkflowID,
		WorkflowTypeName: filter.WorkflowTypeName,
		Status:           filter.Status,
		MinTime:          session.GetOraTimeStampPtr(filter.MinTime),
		MaxTime:          session.GetOraTimeStampPtr(filter.MaxTime),
		PageSize:         filter.PageSize,
		Query:            filter.Query,
		QueryArgs:        filter.QueryArgs,
		GroupBy:          filter.GroupBy,
	}
}

// InsertIntoVisibility inserts a row into visibility table. If an row already exist,
// its left as such and no update will be made
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
		return nil, fmt.Errorf("unable to insert workflow execution: %w", err)
	}
	_, err = tx.NamedExecContext(ctx, templateInsertCustomSearchAttributes, newExecLocalVisibilityRow(row))
	if err != nil {
		return nil, fmt.Errorf("unable to insert custom search attributes: %w", err)
	}
	err = tx.Commit()
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ReplaceIntoVisibility replaces an existing row if it exist or creates a new row in visibility table
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
	result, err = tx.NamedExecContext(ctx, templateUpsertWorkflowExecution, newExecLocalVisibilityRow(row))
	if err != nil {
		return nil, fmt.Errorf("unable to upsert workflow execution: %w", err)
	}
	_, err = tx.NamedExecContext(ctx, templateUpsertCustomSearchAttributes, newExecLocalVisibilityRow(row))
	if err != nil {
		return nil, fmt.Errorf("unable to upsert custom search attributes: %w", err)
	}
	err = tx.Commit()
	if err != nil {
		return nil, err
	}
	return result, nil
}

// DeleteFromVisibility deletes a row from visibility table if it exist
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
	_, err = mdb.ExecContext(ctx, templateDeleteCustomSearchAttributes, map[string]interface{}{
		"namespace_id": filter.NamespaceID,
		"run_id":       filter.RunID,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to delete custom search attributes: %w", err)
	}
	result, err = mdb.ExecContext(ctx, templateDeleteWorkflowExecution_v8, map[string]interface{}{
		"namespace_id": filter.NamespaceID,
		"run_id":       filter.RunID,
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
func (mdb *db) SelectFromVisibility(
	ctx context.Context,
	filter sqlplugin.VisibilitySelectFilter,
) ([]sqlplugin.VisibilityRow, error) {
	if len(filter.Query) == 0 {
		// backward compatibility for existing tests
		err := sqlplugin.GenerateSelectQuery(&filter, func(t time.Time) time.Time {
			return time.Now()
		})
		if err != nil {
			return nil, err
		}

		// Oracle uses FETCH FIRST instead of LIMIT
		filter.Query = strings.Replace(filter.Query, "LIMIT ?", "FETCH FIRST ? ROWS ONLY", 1)
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
	stmt, err := mdb.PrepareNamedContext(ctx, templateGetWorkflowExecution_v8)
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
