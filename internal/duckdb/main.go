package duckdb

import (
	"context"
	"fmt"
	"github.com/WhisperN/Go-Flight-Server/internal/config"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/sirupsen/logrus"
	"io"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

var CONFIG = config.LoadConfig(true)

type SQLRunner struct {
	ctx  context.Context
	conn adbc.Connection
	db   adbc.Database
}

// NewSQLRunner
/* Instantiates new SQL Database
 * @param ctx context.Context
 * @return *SQLRunner
 */
func NewSQLRunner(ctx context.Context) (*SQLRunner, error) {
	var drv drivermgr.Driver
	db, err := drv.NewDatabase(map[string]string{
		"driver":     CONFIG.DuckDB.Driver,
		"entrypoint": CONFIG.DuckDB.Entrypoint,
		"path":       CONFIG.DuckDB.Path,
	})
	if err != nil {
		logrus.Fatal("failed to create new in-memory DuckDB database: ", err)
		return nil, nil
	}
	conn, err := db.Open(ctx)
	if err != nil {
		logrus.Error("failed to open connection to new in-memory DuckDB database: ", err)
		return nil, nil
	}
	return &SQLRunner{ctx: ctx, conn: conn, db: db}, err
}

// ImportRecord
/*
 * Record importer
 * @param sr io.Reader: Native object in Go for Buffer that data gets written to
 * @param tableName string: The table name we want to write the sr to.
 * Returns void
 */
func (r *SQLRunner) ImportRecord(sr io.Reader) error {
	rdr, err := ipc.NewReader(sr)
	if err != nil {
		logrus.Error("failed to create IPC reader: ", err)
		return err
	}
	defer rdr.Release()
	stmt, err := r.conn.NewStatement()
	if err != nil {
		logrus.Error("failed to create new statement: ", err)
		return err
	}
	if err := stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeCreate); err != nil {
		logrus.Error("failed to set ingest mode: ", err)
		return err
	}
	if err := stmt.SetOption(adbc.OptionKeyIngestTargetTable, CONFIG.DuckDB.TableName); err != nil {
		logrus.Error("failed to set ingest target table: ", err)
		return err
	}
	if err := stmt.BindStream(r.ctx, rdr); err != nil {
		logrus.Error("failed to bind stream: ", err)
		return err
	}
	if _, err := stmt.ExecuteUpdate(r.ctx); err != nil {
		logrus.Error("failed to execute update: ", err)
		return err
	}
	return stmt.Close()
}

// RunSQL
/* Runs an SQL command on top of a database
 * @param sql string: takes an SQL string
 * returns array of type arrow.Record
 */
func (r *SQLRunner) RunSQL(sql string) ([]arrow.Record, error) {
	stmt, err := r.conn.NewStatement()
	if err != nil {
		logrus.Error("failed to create new statement: ", err)
		return nil, err
	}
	defer func(stmt adbc.Statement) {
		err := stmt.Close()
		if err != nil {
			logrus.Error("failed to close statement: ", err)
		}
	}(stmt)

	if err := stmt.SetSqlQuery(sql); err != nil {
		logrus.Error("failed to set SQL query: ", err)
		return nil, err
	}
	out, n, err := stmt.ExecuteQuery(r.ctx)
	if err != nil {
		logrus.Error("failed to execute query: ", err)
		return nil, err
	}
	defer out.Release()

	result := make([]arrow.Record, 0, n)
	for out.Next() {
		rec := out.Record()
		rec.Retain() // .Next() will release the record, so we need to retain it
		result = append(result, rec)
	}
	if out.Err() != nil {
		logrus.Error("failed to execute query: ", out.Err())
		return nil, out.Err()
	}
	return result, nil
}

// PopulateDB
// Populates the DB with a parquet (column format) file
//
//goland:noinspection ALL
func (r *SQLRunner) PopulateDB() error {
	_, err := r.RunSQL(
		fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM read_parquet(%q)",
			CONFIG.DuckDB.TableName,
			CONFIG.DuckDB.TableSource))
	if err != nil {
		logrus.Error("Unexpected error at runtime: ", err)
		return err
	}
	return nil
}

// GetSchema
/* retrieves the current schema of the Dataset in use
 * by only selecting the first column
 * @param table string
 * @return *arrow.Schema
 */
//goland:noinspection SqlNoDataSourceInspection
func (r *SQLRunner) GetSchema(table string) (*arrow.Schema, error) {
	if table == "" {
		logrus.Error("table name is required")
		return nil, nil
	}
	sql := fmt.Sprintf("SELECT * FROM %s LIMIT 1", table)

	stmt, err := r.conn.NewStatement()
	if err != nil {
		logrus.Error("failed to create new statement: ", err)
		return nil, err
	}
	defer func(stmt adbc.Statement) {
		err := stmt.Close()
		if err != nil {
			logrus.Error("failed to close statement: ", err)
		}
	}(stmt)

	if err := stmt.SetSqlQuery(sql); err != nil {
		logrus.Error("failed to set SQL query: ", err)
		return nil, err
	}
	out, _, err := stmt.ExecuteQuery(r.ctx)
	if err != nil {
		logrus.Error("failed to execute query: ", err)
		return nil, err
	}
	defer out.Release()

	schema := out.Schema()
	return schema, nil
}

// Close
/* For winning the beauty contest...
 * "Your boy is always crispy clean"
 * ~Money Boy
 */
func (r *SQLRunner) Close() {
	err := r.conn.Close()
	if err != nil {
		logrus.Error("failed to close connection: ", err)
	}
	err = r.db.Close()
}
