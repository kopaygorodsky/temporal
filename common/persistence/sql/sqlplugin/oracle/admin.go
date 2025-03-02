package oracle

import (
	"fmt"
	"time"
)

const (
	// Using Oracle's syntax for schema version table queries
	readSchemaVersionQuery = `SELECT curr_version FROM schema_version WHERE version_partition=0 AND db_name=:db_name`

	writeSchemaVersionQuery = `MERGE INTO schema_version
		USING dual ON (version_partition=0 AND db_name=:db_name)
		WHEN MATCHED THEN
			UPDATE SET creation_time=:creation_time, curr_version=:curr_version, min_compatible_version=:min_compatible_version
		WHEN NOT MATCHED THEN
			INSERT (version_partition, db_name, creation_time, curr_version, min_compatible_version)
			VALUES (0, :db_name, :creation_time, :curr_version, :min_compatible_version)`

	writeSchemaUpdateHistoryQuery = `INSERT INTO schema_update_history
		(version_partition, year, month, update_time, old_version, new_version, manifest_md5, description)
		VALUES(0, :year, :month, :update_time, :old_version, :new_version, :manifest_md5, :description)`

	createSchemaVersionTableQuery = `CREATE TABLE schema_version(
		version_partition NUMBER(10) NOT NULL,
		db_name VARCHAR2(255) NOT NULL,
		creation_time TIMESTAMP,
		curr_version VARCHAR2(64),
		min_compatible_version VARCHAR2(64),
		PRIMARY KEY (version_partition, db_name))`

	createSchemaUpdateHistoryTableQuery = `CREATE TABLE schema_update_history(
		version_partition NUMBER(10) NOT NULL,
		year NUMBER(10) NOT NULL,
		month NUMBER(10) NOT NULL,
		update_time TIMESTAMP NOT NULL,
		description VARCHAR2(255),
		manifest_md5 VARCHAR2(64),
		new_version VARCHAR2(64),
		old_version VARCHAR2(64),
		PRIMARY KEY (version_partition, year, month, update_time))`

	// Oracle-specific query to check database existence
	checkDatabaseQuery = "SELECT 1 FROM v$database WHERE name = :db_name"

	// List tables for a specific schema (user) in Oracle
	listTablesQuery = "SELECT table_name FROM all_tables WHERE owner = :owner"

	// Drop table in Oracle
	dropTableQuery = "DROP TABLE %v"
)

// CreateSchemaVersionTables sets up the schema version tables
func (mdb *db) CreateSchemaVersionTables() error {
	if err := mdb.Exec(createSchemaVersionTableQuery); err != nil {
		return err
	}
	return mdb.Exec(createSchemaUpdateHistoryTableQuery)
}

// ReadSchemaVersion returns the current schema version for the keyspace
func (mdb *db) ReadSchemaVersion(database string) (string, error) {
	var version string
	db, err := mdb.handle.DB()
	if err != nil {
		return "", err
	}
	stmt, err := db.Prepare(readSchemaVersionQuery)
	if err != nil {
		return "", mdb.handle.ConvertError(err)
	}
	defer stmt.Close()

	err = stmt.QueryRow(database).Scan(&version)
	return version, mdb.handle.ConvertError(err)
}

// UpdateSchemaVersion updates the schema version for the keyspace
func (mdb *db) UpdateSchemaVersion(database string, newVersion string, minCompatibleVersion string) error {
	params := map[string]interface{}{
		"db_name":                database,
		"creation_time":          time.Now().UTC(),
		"curr_version":           newVersion,
		"min_compatible_version": minCompatibleVersion,
	}
	return mdb.NamedExec(writeSchemaVersionQuery, params)
}

// WriteSchemaUpdateLog adds an entry to the schema update history table
func (mdb *db) WriteSchemaUpdateLog(oldVersion string, newVersion string, manifestMD5 string, desc string) error {
	now := time.Now().UTC()
	params := map[string]interface{}{
		"year":         now.Year(),
		"month":        int(now.Month()),
		"update_time":  now,
		"old_version":  oldVersion,
		"new_version":  newVersion,
		"manifest_md5": manifestMD5,
		"description":  desc,
	}
	return mdb.NamedExec(writeSchemaUpdateHistoryQuery, params)
}

// Exec executes a sql statement
func (mdb *db) Exec(stmt string, args ...interface{}) error {
	db, err := mdb.handle.DB()
	if err != nil {
		return err
	}
	_, err = db.Exec(stmt, args...)
	return mdb.handle.ConvertError(err)
}

// NamedExec executes a named sql statement
func (mdb *db) NamedExec(stmt string, args map[string]interface{}) error {
	db, err := mdb.handle.DB()
	if err != nil {
		return err
	}
	_, err = db.NamedExec(stmt, args)
	return mdb.handle.ConvertError(err)
}

// ListTables returns a list of tables in this database
func (mdb *db) ListTables(database string) ([]string, error) {
	var tables []string
	db, err := mdb.handle.DB()
	if err != nil {
		return nil, err
	}

	// In Oracle, we list tables for the current schema (user)
	err = db.Select(&tables, "SELECT table_name FROM user_tables")
	return tables, mdb.handle.ConvertError(err)
}

// DropTable drops a given table from the database
func (mdb *db) DropTable(name string) error {
	return mdb.Exec(fmt.Sprintf(dropTableQuery, name))
}

// DropAllTables drops all tables from this database
func (mdb *db) DropAllTables(database string) error {
	tables, err := mdb.ListTables(database)
	if err != nil {
		return err
	}
	for _, tab := range tables {
		if err := mdb.DropTable(tab); err != nil {
			return err
		}
	}

	return mdb.DropAllSequences()
}

func (mdb *db) ListSequences() ([]string, error) {
	var sequences []string
	db, err := mdb.handle.DB()
	if err != nil {
		return nil, err
	}

	// Query to list all sequences owned by the current user
	err = db.Select(&sequences, "SELECT sequence_name FROM user_sequences")
	return sequences, mdb.handle.ConvertError(err)
}

func (mdb *db) DropSequence(name string) error {
	return mdb.Exec(fmt.Sprintf("DROP SEQUENCE %v", name))
}

// DropAllSequences drops all sequences from this database
func (mdb *db) DropAllSequences() error {
	sequences, err := mdb.ListSequences()
	if err != nil {
		return err
	}
	for _, seq := range sequences {
		if err := mdb.DropSequence(seq); err != nil {
			return err
		}
	}
	return nil
}

// CreateDatabase creates a database if it doesn't exist
// In Oracle, this is typically a DBA operation and requires specific privileges
func (mdb *db) CreateDatabase(name string) error {
	// Implement a placeholder as this is typically a DBA operation in Oracle
	// You might want to log a warning or handle this according to your application needs
	return nil
}

// DropDatabase drops a database
// In Oracle, this is typically a DBA operation and requires specific privileges
func (mdb *db) DropDatabase(name string) error {
	// Implement a placeholder as this is typically a DBA operation in Oracle
	// You might want to log a warning or handle this according to your application needs
	return nil
}
