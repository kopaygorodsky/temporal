package oracle

import (
	"context"
	"database/sql"
	"strings"

	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	templateEnqueueMessageQuery      = `INSERT INTO queue (queue_type, message_id, message_payload, message_encoding) VALUES(:queue_type, :message_id, :message_payload, :message_encoding)`
	templateGetMessageQuery          = `SELECT message_id, message_payload, message_encoding FROM queue WHERE queue_type = :queue_type AND message_id = :message_id`
	templateGetMessagesQuery         = `SELECT message_id, message_payload, message_encoding FROM queue WHERE queue_type = :queue_type AND message_id > :min_message_id AND message_id <= :max_message_id ORDER BY message_id ASC FETCH FIRST :page_size ROWS ONLY`
	templateDeleteMessageQuery       = `DELETE FROM queue WHERE queue_type = :queue_type AND message_id = :message_id`
	templateRangeDeleteMessagesQuery = `DELETE FROM queue WHERE queue_type = :queue_type AND message_id > :min_message_id AND message_id <= :max_message_id`

	//@todo verify usage of MAX() in mysql
	templateGetLastMessageIDQuery = `SELECT message_id FROM queue
WHERE message_id = (
    SELECT message_id FROM queue
    WHERE queue_type = :queue_type
    ORDER BY message_id DESC
    FETCH FIRST 1 ROW ONLY
)
FOR UPDATE`

	templateCreateQueueMetadataQuery = `INSERT INTO queue_metadata COLUMNS (queue_type, data, data_encoding, version) VALUES(:queue_type, :data, :data_encoding, :version)`

	templateUpdateQueueMetadataQuery = `UPDATE queue_metadata 
        SET data = :data, data_encoding = :data_encoding, version = :version + 1 
        WHERE queue_type = :queue_type AND version = :version`

	templateGetQueueMetadataQuery  = `SELECT data, data_encoding, version from queue_metadata WHERE queue_type = :queue_type`
	templateLockQueueMetadataQuery = templateGetQueueMetadataQuery + " FOR UPDATE"
)

// InsertIntoMessages inserts a new row into queue table
func (mdb *db) InsertIntoMessages(
	ctx context.Context,
	rows []sqlplugin.QueueMessageRow,
) (sql.Result, error) {
	type localQueueMessageRow struct {
		QueueType       int32  `db:"queue_type"`
		MessageID       int64  `db:"message_id"`
		MessagePayload  []byte `db:"message_payload"`
		MessageEncoding string `db:"message_encoding"`
	}
	args := make([]localQueueMessageRow, len(rows))
	for i, row := range rows {
		args[i] = localQueueMessageRow{
			QueueType:       int32(row.QueueType),
			MessageID:       row.MessageID,
			MessagePayload:  row.MessagePayload,
			MessageEncoding: row.MessageEncoding,
		}
	}
	return mdb.ExecContext(ctx,
		templateEnqueueMessageQuery,
		args,
	)
}

func (mdb *db) SelectFromMessages(
	ctx context.Context,
	filter sqlplugin.QueueMessagesFilter,
) ([]sqlplugin.QueueMessageRow, error) {
	var rows []sqlplugin.QueueMessageRow
	err := mdb.NamedSelectContext(ctx,
		&rows,
		templateGetMessageQuery,
		map[string]interface{}{
			"queue_type": int32(filter.QueueType),
			"message_id": filter.MessageID,
		},
	)
	return rows, err
}

func (mdb *db) RangeSelectFromMessages(
	ctx context.Context,
	filter sqlplugin.QueueMessagesRangeFilter,
) ([]sqlplugin.QueueMessageRow, error) {
	var rows []sqlplugin.QueueMessageRow
	err := mdb.NamedSelectContext(ctx,
		&rows,
		templateGetMessagesQuery,
		map[string]interface{}{
			"queue_type":     int32(filter.QueueType),
			"min_message_id": filter.MinMessageID,
			"max_message_id": filter.MaxMessageID,
			"page_size":      filter.PageSize,
		},
	)
	return rows, err
}

// DeleteFromMessages deletes message with a messageID from the queue
func (mdb *db) DeleteFromMessages(
	ctx context.Context,
	filter sqlplugin.QueueMessagesFilter,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		templateDeleteMessageQuery,
		map[string]interface{}{
			"queue_type": int32(filter.QueueType),
			"message_id": filter.MessageID,
		},
	)
}

// RangeDeleteFromMessages deletes messages before messageID from the queue
func (mdb *db) RangeDeleteFromMessages(
	ctx context.Context,
	filter sqlplugin.QueueMessagesRangeFilter,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		templateRangeDeleteMessagesQuery,
		map[string]interface{}{
			"queue_type":     int32(filter.QueueType),
			"min_message_id": filter.MinMessageID,
			"max_message_id": filter.MaxMessageID,
		},
	)
}

// GetLastEnqueuedMessageIDForUpdate returns the last enqueued message ID
func (mdb *db) GetLastEnqueuedMessageIDForUpdate(
	ctx context.Context,
	queueType persistence.QueueType,
) (int64, error) {
	var lastMessageID *int64
	err := mdb.NamedGetContext(ctx,
		&lastMessageID,
		templateGetLastMessageIDQuery,
		map[string]interface{}{
			"queue_type": int32(queueType),
		},
	)
	if lastMessageID == nil {
		// The layer of code above us expects ErrNoRows when the queue is empty. MAX() yields
		// null when the queue is empty, so we need to turn that into the correct error.
		return 0, sql.ErrNoRows
	} else {
		return *lastMessageID, err
	}
}

func (mdb *db) InsertIntoQueueMetadata(
	ctx context.Context,
	row *sqlplugin.QueueMetadataRow,
) (sql.Result, error) {
	query := templateCreateQueueMetadataQuery
	args := map[string]interface{}{
		"queue_type":    int32(row.QueueType),
		"version":       row.Version,
		"data":          row.Data,
		"data_encoding": row.DataEncoding,
	}
	//  in the table 'data' can't be null. in mysql empty value is ok, so I decided to do the same here
	if len(row.Data) == 0 {
		query = strings.Replace(query, ":data", "EMPTY_BLOB()", 1)
		delete(args, "data")
	}

	return mdb.NamedExecContext(ctx,
		query,
		args,
	)
}

func (mdb *db) UpdateQueueMetadata(
	ctx context.Context,
	row *sqlplugin.QueueMetadataRow,
) (sql.Result, error) {
	type localQueueMetadataRow struct {
		QueueType    int32  `db:"queue_type"`
		Data         []byte `db:"data"`
		DataEncoding string `db:"data_encoding"`
		Version      int64  `db:"version"`
	}

	return mdb.ExecContext(ctx,
		templateUpdateQueueMetadataQuery,
		localQueueMetadataRow{
			QueueType:    int32(row.QueueType),
			Data:         row.Data,
			DataEncoding: row.DataEncoding,
			Version:      row.Version,
		},
	)
}

func (mdb *db) SelectFromQueueMetadata(
	ctx context.Context,
	filter sqlplugin.QueueMetadataFilter,
) (*sqlplugin.QueueMetadataRow, error) {
	var row sqlplugin.QueueMetadataRow
	err := mdb.NamedGetContext(ctx,
		&row,
		templateGetQueueMetadataQuery,
		map[string]interface{}{
			"queue_type": int32(filter.QueueType),
		},
	)
	if err != nil {
		return nil, err
	}
	return &row, nil
}

func (mdb *db) LockQueueMetadata(
	ctx context.Context,
	filter sqlplugin.QueueMetadataFilter,
) (*sqlplugin.QueueMetadataRow, error) {
	var row sqlplugin.QueueMetadataRow
	err := mdb.NamedGetContext(ctx,
		&row,
		templateLockQueueMetadataQuery,
		map[string]interface{}{
			"queue_type": int32(filter.QueueType),
		},
	)
	if err != nil {
		return nil, err
	}
	return &row, nil
}
