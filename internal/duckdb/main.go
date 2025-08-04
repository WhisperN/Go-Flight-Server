package duckdb

import (
	"bytes"
	"context"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"io"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

const tableName string = "sPlot"

type DuckDBSQLRunner struct {
	ctx  context.Context
	conn adbc.Connection
	db   adbc.Database
}

func NewDuckDBSQLRunner(ctx context.Context) (*DuckDBSQLRunner, error) {
	var drv drivermgr.Driver
	db, err := drv.NewDatabase(map[string]string{
		"driver":     "duckdb",
		"entrypoint": "duckdb_adbc_init",
		"path":       ":memory:",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create new in-memory DuckDB database: %w", err)
	}
	conn, err := db.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection to new in-memory DuckDB database: %w", err)
	}
	return &DuckDBSQLRunner{ctx: ctx, conn: conn, db: db}, err
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
		return nil, fmt.Errorf("failed to write record: %w", err)
	}
	if err := wr.Close(); err != nil {
		return nil, fmt.Errorf("failed to close writer: %w", err)
	}
	return buf, nil
}

/*
 * Record importer
 * @param sr io.Reader: Native object in Go for Buffer that data gets written to
 * @param tableName string: The table name we want to write the sr to.
 * Returns void
 */
func (r *DuckDBSQLRunner) ImportRecord(sr io.Reader) error {
	rdr, err := ipc.NewReader(sr)
	if err != nil {
		return fmt.Errorf("failed to create IPC reader: %w", err)
	}
	defer rdr.Release()
	stmt, err := r.conn.NewStatement()
	if err != nil {
		return fmt.Errorf("failed to create new statement: %w", err)
	}
	if err := stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeCreate); err != nil {
		return fmt.Errorf("failed to set ingest mode: %w", err)
	}
	if err := stmt.SetOption(adbc.OptionKeyIngestTargetTable, tableName); err != nil {
		return fmt.Errorf("failed to set ingest target table: %w", err)
	}
	if err := stmt.BindStream(r.ctx, rdr); err != nil {
		return fmt.Errorf("failed to bind stream: %w", err)
	}
	if _, err := stmt.ExecuteUpdate(r.ctx); err != nil {
		return fmt.Errorf("failed to execute update: %w", err)
	}
	return stmt.Close()
}

/*
 * Runs an SQL command on top of a database
 * @param sql string: takes an SQL string
 * returns array of type arrow.Record
 */
func (r *DuckDBSQLRunner) RunSQL(sql string) ([]arrow.Record, error) {
	stmt, err := r.conn.NewStatement()
	if err != nil {
		return nil, fmt.Errorf("failed to create new statement: %w", err)
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(sql); err != nil {
		return nil, fmt.Errorf("failed to set SQL query: %w", err)
	}
	out, n, err := stmt.ExecuteQuery(r.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer out.Release()

	result := make([]arrow.Record, 0, n)
	for out.Next() {
		rec := out.Record()
		rec.Retain() // .Next() will release the record, so we need to retain it
		result = append(result, rec)
	}
	if out.Err() != nil {
		return nil, out.Err()
	}
	return result, nil
}

/*
 * Runs SQL on a Table of the database
 * @param record arrow.Record: Something that is currently in the Database
 * @param sql string: The SQL query we want to run on the entry
 */
func (r *DuckDBSQLRunner) RunSQLOnRecord(record arrow.Record, sql string) ([]arrow.Record, error) {
	serializedRecord, err := serializeRecord(record)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize record: %w", err)
	}
	if err := r.ImportRecord(serializedRecord); err != nil {
		return nil, fmt.Errorf("failed to import record: %w", err)
	}
	result, err := r.RunSQL(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to run SQL: %w", err)
	}

	// DANGER ZONE: Sniff snoff...
	/*
		if _, err := r.runSQL("DROP TABLE temp_table"); err != nil {
			return nil, fmt.Errorf("failed to drop temp table after running query: %w", err)
		}*/
	return result, nil
}

func (db *DuckDBSQLRunner) PopulateDBwithsPlot() error {
	_, err := db.RunSQL("CREATE TABLE sPlot AS SELECT * FROM read_parquet('third_party/dataset/sPlot_CWM_CWV.parquet')")
	if err != nil {
		panic(err)
	}
	return nil
}

/*
 * For winning the beauty contest...
 * "Your boy is always crispy clean"
 * ~Money Boy
 */
func (r *DuckDBSQLRunner) Close() {
	r.conn.Close()
	r.db.Close()
}

func (r *DuckDBSQLRunner) GetSchema(table string) (*arrow.Schema, error) {
	var sql = ("SELECT * FROM " + table + " LIMIT 1")

	stmt, err := r.conn.NewStatement()
	if err != nil {
		return nil, fmt.Errorf("failed to create new statement: %w", err)
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(sql); err != nil {
		return nil, fmt.Errorf("failed to set SQL query: %w", err)
	}
	out, _, err := stmt.ExecuteQuery(r.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer out.Release()

	schema := out.Schema()
	return schema, nil
}

func SchemaToBytes(schema *arrow.Schema) []byte {
	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema), ipc.WithAllocator(memory.NewGoAllocator()))
	defer writer.Close()
	return buf.Bytes()
}

func main() {
	/*ctx := context.Background()
	var db *DuckDBSQLRunner
	db, err := NewDuckDBSQLRunner(ctx)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	fmt.Println("DuckDBSQLRunner started")
	*/
	/*
	 * If you want to import something from a .parquet file then run it in an SQL Query
	 * SELECT * FROM read_parquet('input.parquet');
	 * find other examples here: https://duckdb.org/docs/stable/guides/file_formats/parquet_import
	 */

	// let's try this...
	/*
		result, err := db.runSQL("SELECT * FROM read_parquet('third_party/dataset/sPlot_CWM_CWV.parquet')")
		if err != nil {
			panic(err)
		}

		// Works like a charme
		fmt.Println("Executed sql Query")
		for _, record := range result {
			fmt.Println(record)
			record.Release()
		}
	*/
}
