package oracle

import (
	"context"
	"database/sql"

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

	templateGetLastMessageIDQueryV2 = `SELECT MAX(message_id) FROM queue_messages 
		WHERE queue_type = :queue_type AND queue_name = :queue_name AND queue_partition = :queue_partition 
		AND message_id >= (
			SELECT message_id FROM queue_messages 
			WHERE queue_type = :queue_type AND queue_name = :queue_name AND queue_partition = :queue_partition 
			ORDER BY message_id DESC 
			FETCH FIRST 1 ROW ONLY
		) FOR UPDATE`

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

func (mdb *db) InsertIntoQueueV2Metadata(ctx context.Context, row *sqlplugin.QueueV2MetadataRow) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		templateCreateQueueMetadataQueryV2,
		row,
	)
}

func (mdb *db) UpdateQueueV2Metadata(ctx context.Context, row *sqlplugin.QueueV2MetadataRow) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		templateUpdateQueueMetadataQueryV2,
		row,
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
	return mdb.NamedExecContext(ctx,
		templateEnqueueMessageQueryV2,
		row,
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
	var lastMessageID *int64
	err := mdb.NamedGetContext(ctx,
		&lastMessageID,
		templateGetLastMessageIDQueryV2,
		map[string]interface{}{
			"queue_type":      filter.QueueType,
			"queue_name":      filter.QueueName,
			"queue_partition": filter.Partition,
		},
	)
	if lastMessageID == nil {
		// The layer of code above us expects ErrNoRows when the queue is empty. MAX() yields
		// null when the queue is empty, so we need to turn that into the correct error.
		return 0, sql.ErrNoRows
	}
	return *lastMessageID, err
}
