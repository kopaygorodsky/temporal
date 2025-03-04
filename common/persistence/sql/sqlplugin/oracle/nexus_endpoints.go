package oracle

import (
	"context"
	"database/sql"
	"errors"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	createEndpointsTableVersionQry    = `INSERT INTO nexus_endpoints_partition_status(version) VALUES(1)`
	incrementEndpointsTableVersionQry = `UPDATE nexus_endpoints_partition_status SET version = :new_version WHERE version = :current_version`
	getEndpointsTableVersionQry       = `SELECT version FROM nexus_endpoints_partition_status`

	createEndpointQry  = `INSERT INTO nexus_endpoints(id, data, data_encoding, version) VALUES (:id, :data, :data_encoding, 1)`
	updateEndpointQry  = `UPDATE nexus_endpoints SET data = :data, data_encoding = :data_encoding, version = :new_version WHERE id = :id AND version = :current_version`
	deleteEndpointQry  = `DELETE FROM nexus_endpoints WHERE id = :id`
	getEndpointByIdQry = `SELECT id, data, data_encoding, version FROM nexus_endpoints WHERE id = :id`
	getEndpointsQry    = `SELECT id, data, data_encoding, version FROM nexus_endpoints WHERE id > :last_id ORDER BY id FETCH FIRST :limit_rows ROWS ONLY`
)

func (mdb *db) InitializeNexusEndpointsTableVersion(ctx context.Context) (sql.Result, error) {
	return mdb.ExecContext(ctx, createEndpointsTableVersionQry)
}

func (mdb *db) IncrementNexusEndpointsTableVersion(
	ctx context.Context,
	lastKnownTableVersion int64,
) (sql.Result, error) {
	params := map[string]interface{}{
		"new_version":     lastKnownTableVersion + 1,
		"current_version": lastKnownTableVersion,
	}
	return mdb.NamedExecContext(ctx, incrementEndpointsTableVersionQry, params)
}

func (mdb *db) GetNexusEndpointsTableVersion(ctx context.Context) (int64, error) {
	var version int64
	err := mdb.GetContext(ctx, &version, getEndpointsTableVersionQry)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return version, err
}

func (mdb *db) InsertIntoNexusEndpoints(
	ctx context.Context,
	row *sqlplugin.NexusEndpointsRow,
) (sql.Result, error) {
	params := map[string]interface{}{
		"id":            row.ID,
		"data":          row.Data,
		"data_encoding": row.DataEncoding,
	}
	return mdb.NamedExecContext(ctx, createEndpointQry, params)
}

func (mdb *db) UpdateNexusEndpoint(
	ctx context.Context,
	row *sqlplugin.NexusEndpointsRow,
) (sql.Result, error) {
	params := map[string]interface{}{
		"data":            row.Data,
		"data_encoding":   row.DataEncoding,
		"new_version":     row.Version + 1,
		"id":              row.ID,
		"current_version": row.Version,
	}
	return mdb.NamedExecContext(ctx, updateEndpointQry, params)
}

func (mdb *db) DeleteFromNexusEndpoints(
	ctx context.Context,
	id []byte,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx, deleteEndpointQry, map[string]interface{}{"id": id})
}

func (mdb *db) GetNexusEndpointByID(
	ctx context.Context,
	id []byte,
) (*sqlplugin.NexusEndpointsRow, error) {
	var row sqlplugin.NexusEndpointsRow
	err := mdb.NamedGetContext(ctx, &row, getEndpointByIdQry, map[string]interface{}{"id": id})
	return &row, err
}

func (mdb *db) ListNexusEndpoints(
	ctx context.Context,
	request *sqlplugin.ListNexusEndpointsRequest,
) ([]sqlplugin.NexusEndpointsRow, error) {
	var rows []sqlplugin.NexusEndpointsRow

	lastID := request.LastID
	// use a default value for last_id when it's empty
	if len(lastID) == 0 {
		lastID = []byte{0}
	}

	params := map[string]interface{}{
		"last_id":    lastID,
		"limit_rows": request.Limit,
	}

	if err := mdb.NamedSelectContext(ctx, &rows, getEndpointsQry, params); err != nil {
		return nil, err
	}
	return rows, nil
}
