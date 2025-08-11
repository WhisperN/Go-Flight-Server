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
	OPTIONALS "github.com/WhisperN/Go-Flight-Server/internal/components/Optionals"
	"github.com/WhisperN/Go-Flight-Server/internal/config"
	"github.com/sirupsen/logrus"
	"time"

	"github.com/WhisperN/Go-Flight-Server/internal/duckdb"
	"github.com/WhisperN/Go-Flight-Server/server"
	"github.com/common-nighthawk/go-figure"
	"github.com/fatih/color"
)

var CONFIG = config.LoadConfig(true)

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
	var db *duckdb.SQLRunner
	db, err := duckdb.NewSQLRunner(*ctx)
	if err != nil {
		panic(err)
	}
	log.Info("DuckDB: DuckDBSQLRunner is started.")
	defer db.Close()

	err = db.PopulateDB()
	if err != nil {
		log.Fatal(err)
	}
	log.Info("DuckDB: Database is populated with data.")
	color.Cyan(":: DuckDB: Finished")

	// INITIALIZING THE SERVER
	var srv *server.Server
	srv, err = server.NewServer(&OPTIONALS.ADDRESS{
		IP:   OPTIONALS.String(CONFIG.Server.Address),
		PORT: OPTIONALS.String(CONFIG.Server.Port),
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
