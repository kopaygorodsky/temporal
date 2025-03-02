package oracle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

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

// InsertIntoVisibility inserts a row into visibility table. If an row already exist,
// its left as such and no update will be made
func (mdb *db) InsertIntoVisibility(
	ctx context.Context,
	row *sqlplugin.VisibilityRow,
) (result sql.Result, retError error) {
	finalRow := mdb.prepareRowForDB(row)
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
	result, err = tx.NamedExecContext(ctx, templateInsertWorkflowExecution, finalRow)
	if err != nil {
		return nil, fmt.Errorf("unable to insert workflow execution: %w", err)
	}
	_, err = tx.NamedExecContext(ctx, templateInsertCustomSearchAttributes, finalRow)
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
	finalRow := mdb.prepareRowForDB(row)
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
	result, err = tx.NamedExecContext(ctx, templateUpsertWorkflowExecution, finalRow)
	if err != nil {
		return nil, fmt.Errorf("unable to upsert workflow execution: %w", err)
	}
	_, err = tx.NamedExecContext(ctx, templateUpsertCustomSearchAttributes, finalRow)
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
	_, err = mdb.NamedExecContext(ctx, templateDeleteCustomSearchAttributes, filter)
	if err != nil {
		return nil, fmt.Errorf("unable to delete custom search attributes: %w", err)
	}
	result, err = mdb.NamedExecContext(ctx, templateDeleteWorkflowExecution_v8, filter)
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
		err := sqlplugin.GenerateSelectQuery(&filter, mdb.converter.ToOracleTimestamp)
		if err != nil {
			return nil, err
		}

		// Oracle uses FETCH FIRST instead of LIMIT
		filter.Query = strings.Replace(filter.Query, "LIMIT ?", "FETCH FIRST ? ROWS ONLY", 1)
	}

	var rows []sqlplugin.VisibilityRow
	err := mdb.SelectContext(ctx, &rows, filter.Query, filter.QueryArgs...)
	if err != nil {
		return nil, err
	}
	for i := range rows {
		err = mdb.processRowFromDB(&rows[i])
		if err != nil {
			return nil, err
		}
	}
	return rows, nil
}

// GetFromVisibility reads one row from visibility table
func (mdb *db) GetFromVisibility(
	ctx context.Context,
	filter sqlplugin.VisibilityGetFilter,
) (*sqlplugin.VisibilityRow, error) {
	var row sqlplugin.VisibilityRow
	stmt, err := mdb.PrepareNamedContext(ctx, templateGetWorkflowExecution_v8)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	err = stmt.GetContext(ctx, &row, filter)
	if err != nil {
		return nil, err
	}
	err = mdb.processRowFromDB(&row)
	if err != nil {
		return nil, err
	}
	return &row, nil
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

// prepareRowForDB converts time fields to the database format
func (mdb *db) prepareRowForDB(row *sqlplugin.VisibilityRow) *sqlplugin.VisibilityRow {
	if row == nil {
		return nil
	}
	finalRow := *row
	finalRow.StartTime = mdb.converter.ToOracleTimestamp(finalRow.StartTime)
	finalRow.ExecutionTime = mdb.converter.ToOracleTimestamp(finalRow.ExecutionTime)
	if finalRow.CloseTime != nil {
		*finalRow.CloseTime = mdb.converter.ToOracleTimestamp(*finalRow.CloseTime)
	}
	return &finalRow
}

// processRowFromDB converts row data from the database format to application format
func (mdb *db) processRowFromDB(row *sqlplugin.VisibilityRow) error {
	if row == nil {
		return nil
	}
	row.StartTime = mdb.converter.FromOracleTimestamp(row.StartTime)
	row.ExecutionTime = mdb.converter.FromOracleTimestamp(row.ExecutionTime)
	if row.CloseTime != nil {
		closeTime := mdb.converter.FromOracleTimestamp(*row.CloseTime)
		row.CloseTime = &closeTime
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
