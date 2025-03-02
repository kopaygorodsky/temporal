package oracle

import (
	"context"
	"database/sql"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	// below are templates for history_node table
	addHistoryNodesQuery = `MERGE INTO history_node
		USING DUAL ON (shard_id = :shard_id AND tree_id = :tree_id AND branch_id = :branch_id AND node_id = :node_id AND txn_id = :txn_id)
		WHEN MATCHED THEN 
			UPDATE SET prev_txn_id = :prev_txn_id, data = :data, data_encoding = :data_encoding
		WHEN NOT MATCHED THEN
			INSERT (shard_id, tree_id, branch_id, node_id, prev_txn_id, txn_id, data, data_encoding)
			VALUES (:shard_id, :tree_id, :branch_id, :node_id, :prev_txn_id, :txn_id, :data, :data_encoding)`

	// Oracle version of getHistoryNodesQuery using FETCH FIRST instead of LIMIT
	getHistoryNodesQuery = `SELECT node_id, prev_txn_id, txn_id, data, data_encoding FROM history_node
		WHERE shard_id = :shard_id AND tree_id = :tree_id AND branch_id = :branch_id AND 
		((node_id = :min_node_id AND txn_id > :min_txn_id) OR node_id > :min_node_id) AND node_id < :max_node_id
		ORDER BY shard_id, tree_id, branch_id, node_id, txn_id
		FETCH FIRST :page_size ROWS ONLY`

	// Oracle version of getHistoryNodesReverseQuery using FETCH FIRST instead of LIMIT
	getHistoryNodesReverseQuery = `SELECT node_id, prev_txn_id, txn_id, data, data_encoding FROM history_node
		WHERE shard_id = :shard_id AND tree_id = :tree_id AND branch_id = :branch_id AND 
		node_id >= :min_node_id AND ((node_id = :max_node_id AND txn_id < :max_txn_id) OR node_id < :max_node_id)
		ORDER BY shard_id, tree_id, branch_id DESC, node_id DESC, txn_id DESC
		FETCH FIRST :page_size ROWS ONLY`

	// Oracle version of getHistoryNodeMetadataQuery using FETCH FIRST instead of LIMIT
	getHistoryNodeMetadataQuery = `SELECT node_id, prev_txn_id, txn_id FROM history_node
		WHERE shard_id = :shard_id AND tree_id = :tree_id AND branch_id = :branch_id AND 
		((node_id = :min_node_id AND txn_id > :min_txn_id) OR node_id > :min_node_id) AND node_id < :max_node_id
		ORDER BY shard_id, tree_id, branch_id, node_id, txn_id
		FETCH FIRST :page_size ROWS ONLY`

	deleteHistoryNodeQuery = `DELETE FROM history_node 
		WHERE shard_id = :shard_id AND tree_id = :tree_id AND branch_id = :branch_id AND node_id = :node_id AND txn_id = :txn_id`

	deleteHistoryNodesQuery = `DELETE FROM history_node 
		WHERE shard_id = :shard_id AND tree_id = :tree_id AND branch_id = :branch_id AND node_id >= :min_node_id`

	// below are templates for history_tree table
	addHistoryTreeQuery = `MERGE INTO history_tree
		USING DUAL ON (shard_id = :shard_id AND tree_id = :tree_id AND branch_id = :branch_id)
		WHEN MATCHED THEN
			UPDATE SET data = :data, data_encoding = :data_encoding
		WHEN NOT MATCHED THEN
			INSERT (shard_id, tree_id, branch_id, data, data_encoding)
			VALUES (:shard_id, :tree_id, :branch_id, :data, :data_encoding)`

	getHistoryTreeQuery = `SELECT branch_id, data, data_encoding FROM history_tree 
		WHERE shard_id = :shard_id AND tree_id = :tree_id`

	// Oracle version of pagination query using FETCH FIRST
	paginateBranchesQuery = `SELECT shard_id, tree_id, branch_id, data, data_encoding
		FROM (
			SELECT shard_id, tree_id, branch_id, data, data_encoding 
			FROM history_tree
			WHERE (shard_id = :shard_id AND ((tree_id = :tree_id AND branch_id > :branch_id) OR tree_id > :tree_id)) 
				OR shard_id > :shard_id
			ORDER BY shard_id, tree_id, branch_id
		)
		FETCH FIRST :page_size ROWS ONLY`

	deleteHistoryTreeQuery = `DELETE FROM history_tree 
		WHERE shard_id = :shard_id AND tree_id = :tree_id AND branch_id = :branch_id`
)

// For history_node table:

// InsertIntoHistoryNode inserts a row into history_node table
func (mdb *db) InsertIntoHistoryNode(
	ctx context.Context,
	row *sqlplugin.HistoryNodeRow,
) (sql.Result, error) {
	// NOTE: txn_id is *= -1 within DB
	row.TxnID = -row.TxnID
	return mdb.NamedExecContext(ctx,
		addHistoryNodesQuery,
		map[string]interface{}{
			"shard_id":      row.ShardID,
			"tree_id":       row.TreeID,
			"branch_id":     row.BranchID,
			"node_id":       row.NodeID,
			"prev_txn_id":   row.PrevTxnID,
			"txn_id":        row.TxnID,
			"data":          row.Data,
			"data_encoding": row.DataEncoding,
		})
}

// DeleteFromHistoryNode delete a row from history_node table
func (mdb *db) DeleteFromHistoryNode(
	ctx context.Context,
	row *sqlplugin.HistoryNodeRow,
) (sql.Result, error) {
	// NOTE: txn_id is *= -1 within DB
	row.TxnID = -row.TxnID
	return mdb.NamedExecContext(ctx,
		deleteHistoryNodeQuery,
		map[string]interface{}{
			"shard_id":  row.ShardID,
			"tree_id":   row.TreeID,
			"branch_id": row.BranchID,
			"node_id":   row.NodeID,
			"txn_id":    row.TxnID,
		})
}

// RangeSelectFromHistoryNode reads one or more rows from history_node table
func (mdb *db) RangeSelectFromHistoryNode(
	ctx context.Context,
	filter sqlplugin.HistoryNodeSelectFilter,
) ([]sqlplugin.HistoryNodeRow, error) {
	var query string
	if filter.MetadataOnly {
		query = getHistoryNodeMetadataQuery
	} else if filter.ReverseOrder {
		query = getHistoryNodesReverseQuery
	} else {
		query = getHistoryNodesQuery
	}

	var params map[string]interface{}
	if filter.ReverseOrder {
		params = map[string]interface{}{
			"shard_id":    filter.ShardID,
			"tree_id":     filter.TreeID,
			"branch_id":   filter.BranchID,
			"min_node_id": filter.MinNodeID,
			"max_node_id": filter.MaxNodeID,
			"max_txn_id":  -filter.MaxTxnID, // Note the negation
			"page_size":   filter.PageSize,
		}
	} else {
		params = map[string]interface{}{
			"shard_id":    filter.ShardID,
			"tree_id":     filter.TreeID,
			"branch_id":   filter.BranchID,
			"min_node_id": filter.MinNodeID,
			"min_txn_id":  -filter.MinTxnID, // Note the negation
			"max_node_id": filter.MaxNodeID,
			"page_size":   filter.PageSize,
		}
	}

	var rows []sqlplugin.HistoryNodeRow
	if err := mdb.NamedSelectContext(ctx, &rows, query, params); err != nil {
		return nil, err
	}

	// NOTE: since we let txn_id multiple by -1 when inserting, we have to revert it back here
	for index := range rows {
		rows[index].TxnID = -rows[index].TxnID
	}
	return rows, nil
}

// RangeDeleteFromHistoryNode deletes one or more rows from history_node table
func (mdb *db) RangeDeleteFromHistoryNode(
	ctx context.Context,
	filter sqlplugin.HistoryNodeDeleteFilter,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		deleteHistoryNodesQuery,
		map[string]interface{}{
			"shard_id":    filter.ShardID,
			"tree_id":     filter.TreeID,
			"branch_id":   filter.BranchID,
			"min_node_id": filter.MinNodeID,
		})
}

// InsertIntoHistoryTree inserts a row into history_tree table
func (mdb *db) InsertIntoHistoryTree(
	ctx context.Context,
	row *sqlplugin.HistoryTreeRow,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		addHistoryTreeQuery,
		map[string]interface{}{
			"shard_id":      row.ShardID,
			"tree_id":       row.TreeID,
			"branch_id":     row.BranchID,
			"data":          row.Data,
			"data_encoding": row.DataEncoding,
		})
}

// SelectFromHistoryTree reads one or more rows from history_tree table
func (mdb *db) SelectFromHistoryTree(
	ctx context.Context,
	filter sqlplugin.HistoryTreeSelectFilter,
) ([]sqlplugin.HistoryTreeRow, error) {
	var rows []sqlplugin.HistoryTreeRow
	err := mdb.NamedSelectContext(ctx,
		&rows,
		getHistoryTreeQuery,
		map[string]interface{}{
			"shard_id": filter.ShardID,
			"tree_id":  filter.TreeID,
		})
	return rows, err
}

// PaginateBranchesFromHistoryTree reads up to page.Limit rows from the history_tree table
func (mdb *db) PaginateBranchesFromHistoryTree(
	ctx context.Context,
	page sqlplugin.HistoryTreeBranchPage,
) ([]sqlplugin.HistoryTreeRow, error) {
	var rows []sqlplugin.HistoryTreeRow
	err := mdb.NamedSelectContext(ctx,
		&rows,
		paginateBranchesQuery,
		map[string]interface{}{
			"shard_id":  page.ShardID,
			"tree_id":   page.TreeID,
			"branch_id": page.BranchID,
			"page_size": page.Limit,
		})
	return rows, err
}

// DeleteFromHistoryTree deletes one or more rows from history_tree table
func (mdb *db) DeleteFromHistoryTree(
	ctx context.Context,
	filter sqlplugin.HistoryTreeDeleteFilter,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		deleteHistoryTreeQuery,
		map[string]interface{}{
			"shard_id":  filter.ShardID,
			"tree_id":   filter.TreeID,
			"branch_id": filter.BranchID,
		})
}
