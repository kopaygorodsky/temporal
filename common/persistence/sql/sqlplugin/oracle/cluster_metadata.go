package oracle

import (
	"context"
	"database/sql"
	go_ora "github.com/sijms/go-ora/v2"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/oracle/session"
	"strings"
)

const constMetadataPartition = 0
const constMembershipPartition = 0

const (
	// ****** CLUSTER_METADATA_INFO TABLE ******
	insertClusterMetadataQry = `INSERT INTO cluster_metadata_info 
		(metadata_partition, cluster_name, data, data_encoding, version) 
		VALUES (:metadata_partition, :cluster_name, :data, :data_encoding, :version)`

	updateClusterMetadataQry = `UPDATE cluster_metadata_info 
		SET data = :data, data_encoding = :data_encoding, version = :version 
		WHERE metadata_partition = :metadata_partition AND cluster_name = :cluster_name`

	getClusterMetadataBase         = `SELECT data, data_encoding, version FROM cluster_metadata_info `
	listClusterMetadataQry         = getClusterMetadataBase + `WHERE metadata_partition = :metadata_partition ORDER BY cluster_name`
	listClusterMetadataRangeQry    = getClusterMetadataBase + `WHERE metadata_partition = :metadata_partition AND cluster_name > :cluster_name ORDER BY cluster_name`
	getClusterMetadataQry          = getClusterMetadataBase + `WHERE metadata_partition = :metadata_partition AND cluster_name = :cluster_name`
	writeLockGetClusterMetadataQry = getClusterMetadataQry + ` FOR UPDATE`

	deleteClusterMetadataQry = `DELETE FROM cluster_metadata_info WHERE metadata_partition = :metadata_partition AND cluster_name = :cluster_name`

	// ****** CLUSTER_MEMBERSHIP TABLE ******
	// Oracle uses MERGE statement instead of MySQL's INSERT ... ON DUPLICATE KEY UPDATE
	templateUpsertActiveClusterMembership = `MERGE INTO cluster_membership m
		USING (SELECT :membership_partition as membership_partition, 
			:host_id as host_id, 
			:rpc_address as rpc_address, 
			:rpc_port as rpc_port, 
			:role as role, 
			:session_start as session_start, 
			:last_heartbeat as last_heartbeat, 
			:record_expiry as record_expiry 
		FROM dual) s
		ON (m.membership_partition = s.membership_partition AND m.host_id = s.host_id)
		WHEN MATCHED THEN
			UPDATE SET
				session_start = s.session_start,
				last_heartbeat = s.last_heartbeat,
				record_expiry = s.record_expiry
		WHEN NOT MATCHED THEN
			INSERT (membership_partition, host_id, rpc_address, rpc_port, role, session_start, last_heartbeat, record_expiry)
			VALUES (s.membership_partition, s.host_id, s.rpc_address, s.rpc_port, s.role, s.session_start, s.last_heartbeat, s.record_expiry)`

	templatePruneStaleClusterMembership = `DELETE FROM
		cluster_membership 
		WHERE membership_partition = :membership_partition AND record_expiry < :record_expiry`

	templateGetClusterMembership = `SELECT host_id, rpc_address, rpc_port, role, session_start, last_heartbeat, record_expiry FROM
		cluster_membership WHERE membership_partition = :membership_partition`

	// ClusterMembership WHERE Suffixes
	templateWithRoleSuffix           = ` AND role = :role`
	templateWithHeartbeatSinceSuffix = ` AND last_heartbeat > :last_heartbeat`
	templateWithRecordExpirySuffix   = ` AND record_expiry > :record_expiry`
	templateWithRPCAddressSuffix     = ` AND rpc_address = :rpc_address`
	templateWithHostIDSuffix         = ` AND host_id = :host_id`
	templateWithHostIDGreaterSuffix  = ` AND host_id > :host_id_greater`
	templateWithSessionStartSuffix   = ` AND session_start >= :session_start`

	// Generic SELECT Suffixes - Oracle uses FETCH FIRST n ROWS ONLY instead of LIMIT
	templateWithOrderBySessionStartSuffix = ` ORDER BY membership_partition ASC, host_id ASC`
	templateWithLimitSuffix               = ` FETCH FIRST :limit ROWS ONLY`
)

func (mdb *db) SaveClusterMetadata(
	ctx context.Context,
	row *sqlplugin.ClusterMetadataRow,
) (sql.Result, error) {
	if row.Version == 0 {
		return mdb.NamedExecContext(ctx,
			insertClusterMetadataQry,
			map[string]interface{}{
				"metadata_partition": constMetadataPartition,
				"cluster_name":       row.ClusterName,
				"data":               row.Data,
				"data_encoding":      row.DataEncoding,
				"version":            1,
			})
	}
	return mdb.NamedExecContext(ctx,
		updateClusterMetadataQry,
		map[string]interface{}{
			"data":               row.Data,
			"data_encoding":      row.DataEncoding,
			"version":            row.Version + 1,
			"metadata_partition": constMetadataPartition,
			"cluster_name":       row.ClusterName,
		})
}

func (mdb *db) ListClusterMetadata(
	ctx context.Context,
	filter *sqlplugin.ClusterMetadataFilter,
) ([]sqlplugin.ClusterMetadataRow, error) {
	var rows []sqlplugin.ClusterMetadataRow
	var err error

	// Oracle doesn't support LIMIT directly - we'll use FETCH FIRST n ROWS ONLY
	var pageSize int
	if filter.PageSize != nil {
		pageSize = *filter.PageSize
	} else {
		pageSize = 100 // reasonable default
	}

	if len(filter.ClusterName) != 0 {
		err = mdb.NamedSelectContext(ctx,
			&rows,
			listClusterMetadataRangeQry,
			map[string]interface{}{
				"metadata_partition": constMetadataPartition,
				"cluster_name":       filter.ClusterName,
			})
	} else {
		err = mdb.NamedSelectContext(ctx,
			&rows,
			listClusterMetadataQry+" FETCH FIRST :page_size ROWS ONLY",
			map[string]interface{}{
				"metadata_partition": constMetadataPartition,
				"page_size":          pageSize,
			})
	}

	return rows, err
}

func (mdb *db) GetClusterMetadata(
	ctx context.Context,
	filter *sqlplugin.ClusterMetadataFilter,
) (*sqlplugin.ClusterMetadataRow, error) {
	var row sqlplugin.ClusterMetadataRow
	err := mdb.NamedGetContext(ctx,
		&row,
		getClusterMetadataQry,
		map[string]interface{}{
			"metadata_partition": constMetadataPartition,
			"cluster_name":       filter.ClusterName,
		})
	if err != nil {
		return nil, err
	}
	return &row, nil
}

func (mdb *db) DeleteClusterMetadata(
	ctx context.Context,
	filter *sqlplugin.ClusterMetadataFilter,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		deleteClusterMetadataQry,
		map[string]interface{}{
			"metadata_partition": constMetadataPartition,
			"cluster_name":       filter.ClusterName,
		})
}

func (mdb *db) WriteLockGetClusterMetadata(
	ctx context.Context,
	filter *sqlplugin.ClusterMetadataFilter,
) (*sqlplugin.ClusterMetadataRow, error) {
	var row sqlplugin.ClusterMetadataRow
	err := mdb.NamedGetContext(ctx,
		&row,
		writeLockGetClusterMetadataQry,
		map[string]interface{}{
			"metadata_partition": constMetadataPartition,
			"cluster_name":       filter.ClusterName,
		})
	if err != nil {
		return nil, err
	}
	return &row, nil
}

func (mdb *db) UpsertClusterMembership(
	ctx context.Context,
	row *sqlplugin.ClusterMembershipRow,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		templateUpsertActiveClusterMembership,
		map[string]interface{}{
			"membership_partition": constMembershipPartition,
			"host_id":              row.HostID,
			"rpc_address":          row.RPCAddress,
			"rpc_port":             row.RPCPort,
			"role":                 row.Role,
			"session_start":        row.SessionStart,
			"last_heartbeat":       row.LastHeartbeat,
			"record_expiry":        row.RecordExpiry,
		})
}

func (mdb *db) GetClusterMembers(
	ctx context.Context,
	filter *sqlplugin.ClusterMembershipFilter,
) ([]sqlplugin.ClusterMembershipRow, error) {
	var queryString strings.Builder
	params := map[string]interface{}{
		"membership_partition": constMembershipPartition,
	}

	queryString.WriteString(templateGetClusterMembership)

	if filter.HostIDEquals != nil {
		queryString.WriteString(templateWithHostIDSuffix)
		params["host_id"] = filter.HostIDEquals
	}

	if filter.RPCAddressEquals != "" {
		queryString.WriteString(templateWithRPCAddressSuffix)
		params["rpc_address"] = filter.RPCAddressEquals
	}

	if filter.RoleEquals != p.All {
		queryString.WriteString(templateWithRoleSuffix)
		params["role"] = filter.RoleEquals
	}

	if !filter.LastHeartbeatAfter.IsZero() {
		queryString.WriteString(templateWithHeartbeatSinceSuffix)
		params["last_heartbeat"] = session.NewTimeStamp(filter.LastHeartbeatAfter)
	}

	if !filter.RecordExpiryAfter.IsZero() {
		queryString.WriteString(templateWithRecordExpirySuffix)
		params["record_expiry"] = session.NewTimeStamp(filter.RecordExpiryAfter)
	}

	if !filter.SessionStartedAfter.IsZero() {
		queryString.WriteString(templateWithSessionStartSuffix)
		params["session_start"] = session.NewTimeStamp(filter.SessionStartedAfter)
	}

	if filter.HostIDGreaterThan != nil {
		queryString.WriteString(templateWithHostIDGreaterSuffix)
		params["host_id_greater"] = filter.HostIDGreaterThan
	}

	queryString.WriteString(templateWithOrderBySessionStartSuffix)

	if filter.MaxRecordCount > 0 {
		queryString.WriteString(templateWithLimitSuffix)
		params["limit"] = filter.MaxRecordCount
	}

	compiledQryString := queryString.String()

	type ClusterMembershipRow struct {
		Role           p.ServiceType
		HostID         []byte
		RPCAddress     string
		RPCPort        uint16
		SessionStart   session.TimeStamp
		LastHeartbeat  session.TimeStamp
		RecordExpiry   session.TimeStamp
		InsertionOrder uint64
	}

	var rows []ClusterMembershipRow
	if err := mdb.NamedSelectContext(ctx,
		&rows,
		compiledQryString,
		params); err != nil {
		return nil, err
	}

	res := make([]sqlplugin.ClusterMembershipRow, len(rows))
	for i, row := range rows {
		res[i] = sqlplugin.ClusterMembershipRow{
			Role:           row.Role,
			HostID:         row.HostID,
			RPCAddress:     row.RPCAddress,
			RPCPort:        row.RPCPort,
			SessionStart:   row.SessionStart.ToTime(),
			LastHeartbeat:  row.LastHeartbeat.ToTime(),
			RecordExpiry:   row.RecordExpiry.ToTime(),
			InsertionOrder: row.InsertionOrder,
		}
	}

	return res, nil
}

func (mdb *db) PruneClusterMembership(
	ctx context.Context,
	filter *sqlplugin.PruneClusterMembershipFilter,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		templatePruneStaleClusterMembership,
		map[string]interface{}{
			"membership_partition": constMembershipPartition,
			"record_expiry":        go_ora.TimeStamp(filter.PruneRecordsBefore),
		})
}

func (mdb *db) NamedSelectContext(
	ctx context.Context,
	dest interface{},
	query string,
	arg interface{},
) error {
	stmt, err := mdb.conn().PrepareNamedContext(ctx, query)
	if err != nil {
		return err
	}
	defer stmt.Close()

	return stmt.SelectContext(ctx, dest, arg)
}

func (mdb *db) NamedGetContext(
	ctx context.Context,
	dest interface{},
	query string,
	arg interface{},
) error {
	stmt, err := mdb.conn().PrepareNamedContext(ctx, query)
	if err != nil {
		return err
	}
	defer stmt.Close()

	return stmt.GetContext(ctx, dest, arg)
}
