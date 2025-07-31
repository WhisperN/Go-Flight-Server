// sources:
// 1. https://github.com/PacktPublishing/In-Memory-Analytics-with-Apache-Arrow-Second-Edition/blob/main/chapter8/go/main.go

package duckdb

import (
	"context"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/parquet/file"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
)

func stdInterface() adbc.Database {
	var duckDriver drivermgr.Driver
	db, err := duckDriver.NewDatabase(map[string]string{
		"driver":     "duckdb",
		"entrypoint": "duckdb_adbc_init",
		"path":       ":memory:",
	})
	if err != nil {
		panic(err)
	}
	defer db.Close()

	return db
}

func populateDuckDB(db adbc.Database) {
	ctx := context.Background()

	rdr, err := file.OpenParquetFile("../../data/sPlot_CWM_CWV.parquet", false)
	if err != nil {
		panic(err)
	}
	defer rdr.Close()

	pqrdr, err := pqarrow.NewFileReader(rdr,
		pqarrow.ArrowReadProperties{
			Parallel:  true,
			BatchSize: 102400},
		memory.DefaultAllocator)
	if err != nil {
		panic(err)
	}

	// Filter out arrow.NULL columns
	/* I think we can avoid this if we filter beforehand?
	 * Currently it's just a failsafe statement */
	colIndices := make([]int, 0)
	for _, f := range pqrdr.Manifest.Fields {
		if f.Field.Type.ID() != arrow.NULL {
			colIndices = append(colIndices, f.ColIndex)
		}
	}

	recrdr, err := pqrdr.GetRecordReader(ctx, colIndices, nil)
	if err != nil {
		panic(err)
	}
	defer recrdr.Release()
	/* End failsafe */
	/*
		cnxn, err := db.Open(ctx)
		if err != nil {
			panic(err)
		}

		stmt, err := cnxn.NewStatement()
		if err != nil {
			panic(err)
		}
		defer stmt.Close()

		stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeReplace)
		stmt.SetOption(adbc.OptionKeyIngestTargetTable, "")

		if err := stmt.BindStream(ctx, recrdr); err != nil {
			panic(err)
		}

		n, err := stmt.ExecuteUpdate(ctx)
		if err != nil {
			panic(err)
		}

		fmt.Println(n)*/
}

func sqlInterface(db adbc.Database) {
	ctx := context.Background()
	cnxn, err := db.Open(ctx)
	if err != nil {
		panic(err)
	}
	defer cnxn.Close()

	sc, err := cnxn.GetTableSchema(ctx, nil, nil, "foo")
	if err != nil {
		panic(err)
	}
	fmt.Println(sc)

	stmt, err := cnxn.NewStatement()
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	stmt.SetSqlQuery("SHOW TABLES")
	rdr, n, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		panic(err)
	}
	defer rdr.Release()

	fmt.Println(n)

	for rdr.Next() {
		fmt.Println(rdr.Record())
	}
}

func main() {
	var database = stdInterface()
	// populateDuckDB(database)
	sqlInterface(database)
}
