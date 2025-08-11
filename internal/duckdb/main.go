package duckdb

import (
	"bytes"
	"context"
	"fmt"
	"github.com/WhisperN/Go-Flight-Server/internal/config"
	"github.com/sirupsen/logrus"
	"io"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

var CONFIG = config.LoadConfig(true)

type SQLRunner struct {
	ctx  context.Context
	conn adbc.Connection
	db   adbc.Database
}

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

/*
 * Record serializer
 * Returns buffer io.Reader and error
 */
func serializeRecord(record arrow.Record) (io.Reader, error) {
	// Allocates new dynamic memory
	buf := new(bytes.Buffer)
	wr := ipc.NewWriter(buf, ipc.WithSchema(record.Schema()))
	if err := wr.Write(record); err != nil {
		logrus.Error("failed to write record: ", err)
		return nil, nil
	}
	if err := wr.Close(); err != nil {
		logrus.Error("failed to close writer: ", err)
		return nil, nil
	}
	return buf, nil
}

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

/*
 * Runs an SQL command on top of a database
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

/*
 * Runs SQL on a Table of the database
 * @param record arrow.Record: Something that is currently in the Database
 * @param sql string: The SQL query we want to run on the entry
 */
func (r *SQLRunner) RunSQLOnRecord(record arrow.Record, sql string) ([]arrow.Record, error) {
	serializedRecord, err := serializeRecord(record)
	if err != nil {
		logrus.Error("failed to serialize record: ", err)
		return nil, err
	}
	if err := r.ImportRecord(serializedRecord); err != nil {
		logrus.Error("failed to import record: ", err)
		return nil, err
	}
	result, err := r.RunSQL(sql)
	if err != nil {
		logrus.Error("failed to run SQL: ", err)
		return nil, err
	}

	return result, nil
}

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

/*
 * For winning the beauty contest...
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

// GetSchema
/* retrieves the current schema of the Dataset in use
 * by only selecting the first column
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
