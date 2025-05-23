package oracle

import (
	"context"
	"database/sql"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	//shards is somehow reserved keyword and oracle conflicts with this name so we have to specify either table owner like TEMPORAL.shards or an alias "shards" for table shards
	createShardQry = `INSERT INTO shards shards(shard_id, range_id, data, data_encoding) VALUES (:shard_id, :range_id, :data, :data_encoding)`

	getShardQry = `SELECT shard_id, range_id, data, data_encoding FROM shards WHERE shard_id = :shard_id`

	updateShardQry = `UPDATE shards SET range_id = :range_id, data = :data, data_encoding = :data_encoding WHERE shard_id = :shard_id`

	lockShardQry     = `SELECT range_id FROM shards WHERE shard_id = :shard_id FOR UPDATE`
	readLockShardQry = `SELECT range_id FROM shards WHERE shard_id = :shard_id FOR UPDATE NOWAIT` // Oracle uses FOR UPDATE NOWAIT instead of LOCK IN SHARE MODE
)

// InsertIntoShards inserts one or more rows into shards table
func (mdb *db) InsertIntoShards(
	ctx context.Context,
	row *sqlplugin.ShardsRow,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		createShardQry,
		map[string]interface{}{
			"shard_id":      row.ShardID,
			"range_id":      row.RangeID,
			"data":          row.Data,
			"data_encoding": row.DataEncoding,
		},
	)
}

// UpdateShards updates one or more rows into shards table
func (mdb *db) UpdateShards(
	ctx context.Context,
	row *sqlplugin.ShardsRow,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		updateShardQry,
		map[string]interface{}{
			"range_id":      row.RangeID,
			"data":          row.Data,
			"data_encoding": row.DataEncoding,
			"shard_id":      row.ShardID,
		},
	)
}

// SelectFromShards reads one or more rows from shards table
func (mdb *db) SelectFromShards(
	ctx context.Context,
	filter sqlplugin.ShardsFilter,
) (*sqlplugin.ShardsRow, error) {
	var row sqlplugin.ShardsRow
	err := mdb.NamedGetContext(ctx,
		&row,
		getShardQry,
		map[string]interface{}{
			"shard_id": filter.ShardID,
		},
	)
	if err != nil {
		return nil, err
	}
	return &row, err
}

// ReadLockShards acquires a read lock on a single row in shards table
func (mdb *db) ReadLockShards(
	ctx context.Context,
	filter sqlplugin.ShardsFilter,
) (int64, error) {
	var rangeID int64
	err := mdb.NamedGetContext(ctx,
		&rangeID,
		readLockShardQry,
		map[string]interface{}{
			"shard_id": filter.ShardID,
		},
	)
	return rangeID, err
}

// WriteLockShards acquires a write lock on a single row in shards table
func (mdb *db) WriteLockShards(
	ctx context.Context,
	filter sqlplugin.ShardsFilter,
) (int64, error) {
	var rangeID int64
	err := mdb.NamedGetContext(ctx,
		&rangeID,
		lockShardQry,
		map[string]interface{}{
			"shard_id": filter.ShardID,
		},
	)
	return rangeID, err
}
