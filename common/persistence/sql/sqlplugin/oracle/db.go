package oracle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/jmoiron/sqlx"
	oracleNetwork "github.com/sijms/go-ora/v2/network"
	"go.temporal.io/server/common/persistence/schema"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	oracleShema "go.temporal.io/server/schema/oracle"
	"strings"
)

// Oracle error codes
const (
	// ErrDupEntryCode MySQL Error 1062 indicates a duplicate primary key i.e. the row already exists,
	// so we don't do the insert and return a ConditionalUpdate error.
	ErrDupEntryCode = 1062

	// Cannot execute statement in a READ ONLY transaction.
	readOnlyTransactionCode = 1792
	// Too many connections open
	tooManyConnectionsCode = 1040
	// Running in read-only mode
	readOnlyModeCode = 1836
)

var _ sqlplugin.AdminDB = (*db)(nil)
var _ sqlplugin.DB = (*db)(nil)
var _ sqlplugin.Tx = (*db)(nil)

// db represents a logical connection to mysql database
type db struct {
	dbKind    sqlplugin.DbKind
	dbName    string
	handle    *sqlplugin.DatabaseHandle
	tx        *sqlx.Tx
	converter DataConverter
}

// newDB returns an instance of DB, which is a logical
// connection to the underlying mysql database
func newDB(
	dbKind sqlplugin.DbKind,
	dbName string,
	handle *sqlplugin.DatabaseHandle,
	tx *sqlx.Tx,
) *db {
	mdb := &db{
		dbKind: dbKind,
		dbName: dbName,
		handle: handle,
		tx:     tx,
	}
	return mdb
}

func (mdb *db) conn() sqlplugin.Conn {
	if mdb.tx != nil {
		return mdb.tx
	}
	return mdb.handle.Conn()
}

func (mdb *db) IsDupEntryError(err error) bool {
	if err == nil {
		return false
	}

	// Oracle duplicate entry error code is ORA-00001
	const OracleDupEntryCode = 1 // ORA-00001 unique constraint violated

	oracleErr := &oracleNetwork.OracleError{}
	if !errors.As(err, &oracleErr) {
		return false
	}

	if oracleErr.ErrCode == OracleDupEntryCode {
		return true
	}

	// Also check for the error text, as a fallback
	return strings.Contains(err.Error(), "ORA-00001") ||
		strings.Contains(err.Error(), "unique constraint")
}

// PluginName returns the name of the mysql plugin
func (mdb *db) PluginName() string {
	return PluginName
}

// DbName returns the name of the database
func (mdb *db) DbName() string {
	return mdb.dbName
}

// BeginTx starts a new transaction and returns a reference to the Tx object
func (mdb *db) BeginTx(ctx context.Context) (sqlplugin.Tx, error) {
	db, err := mdb.handle.DB()
	if err != nil {
		return nil, err
	}
	xtx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, mdb.handle.ConvertError(err)
	}
	return newDB(mdb.dbKind, mdb.dbName, mdb.handle, xtx), nil
}

// BeginWithFullTxx is a stupid method that I'm gonna get rid of once moved to implmenetation own stores and not a sql plugin.
// reason: sqlplugin.Tx DOES NOT HAVE EXEC method, only TableCRUD operations.
// I understand the intention but there are usecases for custom stuff as not everything is supported various SQL databases.
func (mdb *db) BeginWithFullTxx(ctx context.Context, opt *sql.TxOptions) (*db, error) {
	db, err := mdb.handle.DB()
	if err != nil {
		return nil, err
	}
	xtx, err := db.BeginTxx(ctx, opt)
	if err != nil {
		return nil, mdb.handle.ConvertError(err)
	}
	return newDB(mdb.dbKind, mdb.dbName, mdb.handle, xtx), nil
}

// Commit commits a previously started transaction
func (mdb *db) Commit() error {
	return mdb.tx.Commit()
}

// Rollback triggers rollback of a previously started transaction
func (mdb *db) Rollback() error {
	return mdb.tx.Rollback()
}

// Close closes the connection to the mysql db
func (mdb *db) Close() error {
	mdb.handle.Close()
	return nil
}

// ExpectedVersion returns expected version.
func (mdb *db) ExpectedVersion() string {
	switch mdb.dbKind {
	case sqlplugin.DbKindMain:
		return oracleShema.Version
	case sqlplugin.DbKindVisibility:
		return oracleShema.VisibilityVersion
	default:
		panic(fmt.Sprintf("unknown db kind %v", mdb.dbKind))
	}
}

// VerifyVersion verify schema version is up to date
func (mdb *db) VerifyVersion() error {
	expectedVersion := mdb.ExpectedVersion()
	return schema.VerifyCompatibleVersion(mdb, mdb.dbName, expectedVersion)
}

func (mdb *db) Rebind(query string) string {
	return mdb.conn().Rebind(query)
}

func (mdb *db) ExecContext(ctx context.Context, stmt string, args ...interface{}) (sql.Result, error) {
	res, err := mdb.conn().ExecContext(ctx, stmt, args...)
	return res, mdb.handle.ConvertError(err)
}

func (mdb *db) GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	err := mdb.conn().GetContext(ctx, dest, query, args...)
	return mdb.handle.ConvertError(err)
}

func (mdb *db) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	err := mdb.conn().SelectContext(ctx, dest, query, args...)
	return mdb.handle.ConvertError(err)
}

func (mdb *db) NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error) {
	res, err := mdb.conn().NamedExecContext(ctx, query, arg)
	return res, mdb.handle.ConvertError(err)
}

func (mdb *db) PrepareNamedContext(ctx context.Context, query string) (*sqlx.NamedStmt, error) {
	stmt, err := mdb.conn().PrepareNamedContext(ctx, query)
	return stmt, mdb.handle.ConvertError(err)
}
