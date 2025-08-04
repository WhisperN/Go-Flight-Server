// Server
// The main application of the server
// If you are unfamiliar with gRPC, Arrow IPC and ADBC please
// visit their documentation
// gRPC: https://grpc.io/docs/languages/go/quickstart/
// Apache Arrow Flight: https://arrow.apache.org/docs/format/Flight.html
// IPC: https://arrow.apache.org/docs/python/ipc.html
// ADBC: https://arrow.apache.org/docs/format/ADBC.html
// Also if you have specific questions please read my own documentation for reference
package main

import (
	"context"
	"github.com/sirupsen/logrus"
	"time"

	OPTIONALS "github.com/WhisperN/Go-Flight-Server/components/Optionals"
	"github.com/WhisperN/Go-Flight-Server/internal/duckdb"
	"github.com/WhisperN/Go-Flight-Server/server"
	"github.com/common-nighthawk/go-figure"
	"github.com/fatih/color"
)

func superSexyStart(ctx *context.Context) {
	// Beautiful start title
	banner := figure.NewFigure("Go-Flight-Server", "", true)
	banner.Print()

	// logger
	log := logrus.New().WithTime(time.Now())

	// coloring
	color.Green(":: Starting application")
	color.Yellow(":: Version 1.0.0 alpha")

	// Initializing DuckDB
	var db *duckdb.DuckDBSQLRunner
	db, err := duckdb.NewDuckDBSQLRunner(*ctx)
	if err != nil {
		panic(err)
	}
	log.Info("DuckDB: DuckDBSQLRunner is started.")
	defer db.Close()

	db.PopulateDBwithsPlot()
	log.Info("DuckDB: Database is populated with data.")
	color.Cyan(":: DuckDB: Finished")

	// INITIALIZING THE SERVER
	var srv *server.Server
	srv, err = server.NewServer(&OPTIONALS.ADDRESS{
		IP:   OPTIONALS.String("127.0.0.1"),
		PORT: OPTIONALS.String("8080"),
	}, db)
	if err != nil {
		panic(err)
	}
	log.Info("Server: New server instantiated")
	err = srv.Serve()
	log.Info("Server: Serving...")
	if err != nil {
		panic(err)
	}
	defer func(srv *server.Server) {
		err := srv.Shutdown()
		if err != nil {

		}
	}(srv)
	color.Cyan(":: Server: Finished")
	color.Green(":: Application running")

	color.White("Press Ctrl+C to quit.")
	select {}
}

func main() {
	// SET THE CONTEXT
	ctx := context.Background()

	//superFastStart(&ctx)
	superSexyStart(&ctx)
}
