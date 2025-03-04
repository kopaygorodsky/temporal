package oracle

import (
	"context"
	"database/sql"
	"go.temporal.io/server/common/persistence"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	templateEnqueueMessageQueryV2 = `INSERT INTO queue_messages (queue_type, queue_name, queue_partition, message_id, message_payload, message_encoding) VALUES(:queue_type, :queue_name, :queue_partition, :message_id, :message_payload, :message_encoding)`

	templateGetMessagesQueryV2 = `SELECT message_id, message_payload, message_encoding 
		FROM queue_messages 
		WHERE queue_type = :queue_type AND queue_name = :queue_name AND queue_partition = :queue_partition AND message_id >= :min_message_id 
		ORDER BY message_id ASC 
		FETCH FIRST :page_size ROWS ONLY`

	templateRangeDeleteMessagesQueryV2 = `DELETE FROM queue_messages 
		WHERE queue_type = :queue_type AND queue_name = :queue_name AND queue_partition = :queue_partition 
		AND message_id >= :min_message_id AND message_id <= :max_message_id`

	templateGetMaxMessageIDQueryV2 = `SELECT MAX(message_id) FROM queue_messages WHERE queue_type = :queue_type AND queue_name = :queue_name AND queue_partition = :queue_partition`

	templateGetLastMessageIDQueryV2 = `SELECT message_id FROM queue_messages 
                 WHERE message_id = :message_id
                 AND queue_type = :queue_type AND queue_name = :queue_name AND queue_partition = :queue_partition
                 FOR UPDATE`

	templateCreateQueueMetadataQueryV2 = `INSERT INTO queues (queue_type, queue_name, metadata_payload, metadata_encoding) 
		VALUES(:queue_type, :queue_name, :metadata_payload, :metadata_encoding)`

	templateUpdateQueueMetadataQueryV2 = `UPDATE queues 
		SET metadata_payload = :metadata_payload, metadata_encoding = :metadata_encoding 
		WHERE queue_type = :queue_type AND queue_name = :queue_name`

	templateGetQueueMetadataQueryV2 = `SELECT metadata_payload, metadata_encoding from queues 
		WHERE queue_type = :queue_type AND queue_name = :queue_name`

	templateGetQueueMetadataQueryV2ForUpdate = `SELECT metadata_payload, metadata_encoding from queues 
		WHERE queue_type = :queue_type AND queue_name = :queue_name FOR UPDATE`

	templateGetNameFromQueueMetadataV2 = `SELECT queue_type, queue_name, metadata_payload, metadata_encoding
		FROM (
			SELECT queue_type, queue_name, metadata_payload, metadata_encoding, ROWNUM rnum
			FROM queues
			WHERE queue_type = :queue_type
			AND ROWNUM <= :page_offset + :page_size
		)
		WHERE rnum > :page_offset`
)

type localQueueV2MetadataRow struct {
	QueueType        persistence.QueueV2Type `db:"queue_type"`
	QueueName        string                  `db:"queue_name"`
	MetadataPayload  []byte                  `db:"metadata_payload"`
	MetadataEncoding string                  `db:"metadata_encoding"`
}

func (mdb *db) InsertIntoQueueV2Metadata(ctx context.Context, row *sqlplugin.QueueV2MetadataRow) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		templateCreateQueueMetadataQueryV2,
		localQueueV2MetadataRow{
			QueueType:        row.QueueType,
			QueueName:        row.QueueName,
			MetadataPayload:  row.MetadataPayload,
			MetadataEncoding: row.MetadataEncoding,
		},
	)
}

func (mdb *db) UpdateQueueV2Metadata(ctx context.Context, row *sqlplugin.QueueV2MetadataRow) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		templateUpdateQueueMetadataQueryV2,
		localQueueV2MetadataRow{
			QueueType:        row.QueueType,
			QueueName:        row.QueueName,
			MetadataPayload:  row.MetadataPayload,
			MetadataEncoding: row.MetadataEncoding,
		},
	)
}

func (mdb *db) SelectFromQueueV2Metadata(ctx context.Context, filter sqlplugin.QueueV2MetadataFilter) (*sqlplugin.QueueV2MetadataRow, error) {
	var row sqlplugin.QueueV2MetadataRow
	err := mdb.NamedGetContext(ctx,
		&row,
		templateGetQueueMetadataQueryV2,
		map[string]interface{}{
			"queue_type": filter.QueueType,
			"queue_name": filter.QueueName,
		},
	)
	if err != nil {
		return nil, err
	}
	return &row, nil
}

func (mdb *db) SelectFromQueueV2MetadataForUpdate(ctx context.Context, filter sqlplugin.QueueV2MetadataFilter) (*sqlplugin.QueueV2MetadataRow, error) {
	var row sqlplugin.QueueV2MetadataRow
	err := mdb.NamedGetContext(ctx,
		&row,
		templateGetQueueMetadataQueryV2ForUpdate,
		map[string]interface{}{
			"queue_type": filter.QueueType,
			"queue_name": filter.QueueName,
		},
	)
	if err != nil {
		return nil, err
	}
	return &row, nil
}

func (mdb *db) SelectNameFromQueueV2Metadata(ctx context.Context, filter sqlplugin.QueueV2MetadataTypeFilter) ([]sqlplugin.QueueV2MetadataRow, error) {
	var rows []sqlplugin.QueueV2MetadataRow
	err := mdb.NamedSelectContext(ctx,
		&rows,
		templateGetNameFromQueueMetadataV2,
		map[string]interface{}{
			"queue_type":  filter.QueueType,
			"page_size":   filter.PageSize,
			"page_offset": filter.PageOffset,
		},
	)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (mdb *db) InsertIntoQueueV2Messages(ctx context.Context, row []sqlplugin.QueueV2MessageRow) (sql.Result, error) {
	type localQueueV2MessageRow struct {
		QueueType       persistence.QueueV2Type `db:"queue_type"`
		QueueName       string                  `db:"queue_name"`
		QueuePartition  int64                   `db:"queue_partition"`
		MessageID       int64                   `db:"message_id"`
		MessagePayload  []byte                  `db:"message_payload"`
		MessageEncoding string                  `db:"message_encoding"`
	}
	data := make([]localQueueV2MessageRow, len(row))
	for i := range row {
		data[i] = localQueueV2MessageRow{
			QueueType:       row[i].QueueType,
			QueueName:       row[i].QueueName,
			QueuePartition:  row[i].QueuePartition,
			MessageID:       row[i].MessageID,
			MessagePayload:  row[i].MessagePayload,
			MessageEncoding: row[i].MessageEncoding,
		}
	}
	return mdb.NamedExecContext(ctx,
		templateEnqueueMessageQueryV2,
		data,
	)
}

func (mdb *db) RangeSelectFromQueueV2Messages(ctx context.Context, filter sqlplugin.QueueV2MessagesFilter) ([]sqlplugin.QueueV2MessageRow, error) {
	var rows []sqlplugin.QueueV2MessageRow
	err := mdb.NamedSelectContext(ctx,
		&rows,
		templateGetMessagesQueryV2,
		map[string]interface{}{
			"queue_type":      filter.QueueType,
			"queue_name":      filter.QueueName,
			"queue_partition": filter.Partition,
			"min_message_id":  filter.MinMessageID,
			"page_size":       filter.PageSize,
		},
	)
	return rows, err
}

func (mdb *db) RangeDeleteFromQueueV2Messages(ctx context.Context, filter sqlplugin.QueueV2MessagesFilter) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		templateRangeDeleteMessagesQueryV2,
		map[string]interface{}{
			"queue_type":      filter.QueueType,
			"queue_name":      filter.QueueName,
			"queue_partition": filter.Partition,
			"min_message_id":  filter.MinMessageID,
			"max_message_id":  filter.MaxMessageID,
		},
	)
}

func (mdb *db) GetLastEnqueuedMessageIDForUpdateV2(ctx context.Context, filter sqlplugin.QueueV2Filter) (int64, error) {
	var maxID *int64

	if err := mdb.NamedGetContext(ctx,
		&maxID,
		templateGetMaxMessageIDQueryV2,
		map[string]interface{}{
			"queue_type":      filter.QueueType,
			"queue_name":      filter.QueueName,
			"queue_partition": filter.Partition,
		},
	); err != nil {
		return 0, err
	}

	if maxID == nil {
		return 0, sql.ErrNoRows
	}

	var lockedMessageID *int64

	if err := mdb.NamedGetContext(ctx,
		&lockedMessageID,
		templateGetLastMessageIDQueryV2,
		map[string]interface{}{
			"message_id":      *maxID,
			"queue_type":      filter.QueueType,
			"queue_name":      filter.QueueName,
			"queue_partition": filter.Partition,
		},
	); err != nil {
		return 0, err
	}

	if lockedMessageID == nil {
		// This should rarely happen (if the row was deleted between queries)
		return 0, sql.ErrNoRows
	}

	return *lockedMessageID, nil
}
