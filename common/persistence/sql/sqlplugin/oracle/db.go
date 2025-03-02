package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"go.temporal.io/server/common/persistence/schema"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	oracleShema "go.temporal.io/server/schema/oracle"
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
	return false
	sqlErr, ok := err.(*mysql.MySQLError)
	return ok && sqlErr.Number == ErrDupEntryCode
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

func (mdb *db) NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error) {
	res, err := mdb.conn().NamedExecContext(ctx, query, arg)
	return res, mdb.handle.ConvertError(err)
}

func (d *db) GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	err := d.conn().GetContext(ctx, dest, query, args...)
	return d.handle.ConvertError(err)
}

func (d *db) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	err := d.conn().SelectContext(ctx, dest, query, args...)
	return d.handle.ConvertError(err)
}

func (d *db) PrepareNamedContext(ctx context.Context, query string) (*sqlx.NamedStmt, error) {
	stmt, err := d.conn().PrepareNamedContext(ctx, query)
	return stmt, d.handle.ConvertError(err)
}
