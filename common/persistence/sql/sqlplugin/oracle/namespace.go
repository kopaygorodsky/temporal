package oracle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	createNamespaceQuery = `INSERT INTO namespaces (partition_id, id, name, is_global, data, data_encoding, notification_version) VALUES(:partition_id, :id, :name, :is_global, :data, :data_encoding, :notification_version)`

	updateNamespaceQuery = `UPDATE namespaces SET name = :name, data = :data, data_encoding = :data_encoding, is_global = :is_global, notification_version = :notification_version WHERE partition_id=54321 AND id = :id`

	getNamespacePart = `SELECT id, name, is_global, data, data_encoding, notification_version FROM namespaces`

	getNamespaceByIDQuery   = getNamespacePart + ` WHERE partition_id=:partition_id AND id = :id`
	getNamespaceByNameQuery = getNamespacePart + ` WHERE partition_id=:partition_id AND name = :name`

	listNamespacesQuery      = getNamespacePart + ` WHERE partition_id=:partition_id ORDER BY id FETCH FIRST :page_size ROWS ONLY`
	listNamespacesRangeQuery = getNamespacePart + ` WHERE partition_id=:partition_id AND id > :greater_than_id ORDER BY id FETCH FIRST :page_size ROWS ONLY`

	deleteNamespaceByIDQuery   = `DELETE FROM namespaces WHERE partition_id=:partition_id AND id = :id`
	deleteNamespaceByNameQuery = `DELETE FROM namespaces WHERE partition_id=:partition_id AND name = :name`

	getNamespaceMetadataQuery    = `SELECT notification_version FROM namespace_metadata WHERE partition_id = 54321`
	lockNamespaceMetadataQuery   = `SELECT notification_version FROM namespace_metadata WHERE partition_id = 54321 FOR UPDATE`
	updateNamespaceMetadataQuery = `UPDATE namespace_metadata SET notification_version = :new_version WHERE notification_version = :current_version AND partition_id = 54321`
)

const (
	partitionID = 54321
)

var errMissingArgs = errors.New("missing one or more args for API")

// InsertIntoNamespace inserts a single row into namespaces table
func (mdb *db) InsertIntoNamespace(
	ctx context.Context,
	row *sqlplugin.NamespaceRow,
) (sql.Result, error) {
	params := map[string]interface{}{
		"partition_id":         partitionID,
		"id":                   row.ID.Downcast(),
		"name":                 row.Name,
		"is_global":            row.IsGlobal,
		"data":                 row.Data,
		"data_encoding":        row.DataEncoding,
		"notification_version": row.NotificationVersion,
	}
	return mdb.NamedExecContext(ctx, createNamespaceQuery, params)
}

// UpdateNamespace updates a single row in namespaces table
func (mdb *db) UpdateNamespace(
	ctx context.Context,
	row *sqlplugin.NamespaceRow,
) (sql.Result, error) {
	params := map[string]interface{}{
		"name":                 row.Name,
		"data":                 row.Data,
		"data_encoding":        row.DataEncoding,
		"is_global":            row.IsGlobal,
		"notification_version": row.NotificationVersion,
		"id":                   row.ID.Downcast(),
	}
	return mdb.NamedExecContext(ctx, updateNamespaceQuery, params)
}

// SelectFromNamespace reads one or more rows from namespaces table
func (mdb *db) SelectFromNamespace(
	ctx context.Context,
	filter sqlplugin.NamespaceFilter,
) ([]sqlplugin.NamespaceRow, error) {
	switch {
	case filter.ID != nil || filter.Name != nil:
		if filter.ID != nil && filter.Name != nil {
			return nil, serviceerror.NewInternal("only ID or name filter can be specified for selection")
		}
		return mdb.selectFromNamespace(ctx, filter)
	case filter.PageSize != nil && *filter.PageSize > 0:
		return mdb.selectAllFromNamespace(ctx, filter)
	default:
		return nil, errMissingArgs
	}
}

func (mdb *db) selectFromNamespace(
	ctx context.Context,
	filter sqlplugin.NamespaceFilter,
) ([]sqlplugin.NamespaceRow, error) {
	var row sqlplugin.NamespaceRow
	var query string
	params := map[string]interface{}{
		"partition_id": partitionID,
	}

	switch {
	case filter.ID != nil:
		query = getNamespaceByIDQuery
		params["id"] = filter.ID.Downcast()
	case filter.Name != nil:
		query = getNamespaceByNameQuery
		params["name"] = *filter.Name
	}

	if err := mdb.NamedGetContext(ctx, &row, query, params); err != nil {
		return nil, err
	}

	return []sqlplugin.NamespaceRow{row}, nil
}

func (mdb *db) selectAllFromNamespace(
	ctx context.Context,
	filter sqlplugin.NamespaceFilter,
) ([]sqlplugin.NamespaceRow, error) {
	var rows []sqlplugin.NamespaceRow
	var query string
	params := map[string]interface{}{
		"partition_id": partitionID,
		"page_size":    *filter.PageSize,
	}

	switch {
	case filter.GreaterThanID != nil:
		query = listNamespacesRangeQuery
		params["greater_than_id"] = filter.GreaterThanID.Downcast()
	default:
		query = listNamespacesQuery
	}

	if err := mdb.NamedSelectContext(ctx, &rows, query, params); err != nil {
		return nil, fmt.Errorf("failed to fetch all namespaces rows: %v", err)
	}

	return rows, nil
}

// DeleteFromNamespace deletes a single row in namespaces table
func (mdb *db) DeleteFromNamespace(
	ctx context.Context,
	filter sqlplugin.NamespaceFilter,
) (sql.Result, error) {
	var result sql.Result
	var err error

	switch {
	case filter.ID != nil:
		params := map[string]interface{}{
			"partition_id": partitionID,
			"id":           filter.ID.Downcast(),
		}
		result, err = mdb.NamedExecContext(ctx, deleteNamespaceByIDQuery, params)

	default:
		params := map[string]interface{}{
			"partition_id": partitionID,
			"name":         *filter.Name,
		}
		result, err = mdb.NamedExecContext(ctx, deleteNamespaceByNameQuery, params)
	}

	return result, err
}

// LockNamespaceMetadata acquires a write lock on a single row in namespace_metadata table
func (mdb *db) LockNamespaceMetadata(
	ctx context.Context,
) (*sqlplugin.NamespaceMetadataRow, error) {
	var row sqlplugin.NamespaceMetadataRow
	err := mdb.GetContext(ctx,
		&row.NotificationVersion,
		lockNamespaceMetadataQuery,
	)
	if err != nil {
		return nil, err
	}
	return &row, nil
}

// SelectFromNamespaceMetadata reads a single row in namespace_metadata table
func (mdb *db) SelectFromNamespaceMetadata(
	ctx context.Context,
) (*sqlplugin.NamespaceMetadataRow, error) {
	var row sqlplugin.NamespaceMetadataRow
	err := mdb.GetContext(ctx,
		&row.NotificationVersion,
		getNamespaceMetadataQuery,
	)
	return &row, err
}

// UpdateNamespaceMetadata updates a single row in namespace_metadata table
func (mdb *db) UpdateNamespaceMetadata(
	ctx context.Context,
	row *sqlplugin.NamespaceMetadataRow,
) (sql.Result, error) {
	params := map[string]interface{}{
		"new_version":     row.NotificationVersion + 1,
		"current_version": row.NotificationVersion,
	}
	return mdb.NamedExecContext(ctx, updateNamespaceMetadataQuery, params)
}
