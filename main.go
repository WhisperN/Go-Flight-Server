// server
// https://arrow.apache.org/docs/format/Flight.html
// See Notion notes...
package main

import (
	"context"
	"fmt"
	"github.com/WhisperN/Go-Flight-Server/internal/duckdb"
	"github.com/WhisperN/Go-Flight-Server/client"
	"github.com/apache/arrow/go/v18/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"github.com/WhisperN/Go-Flight-Server/server"
)

func main() {
	ctx := context.Background()
	var db *duckdb.DuckDBSQLRunner
	db, err := duckdb.NewDuckDBSQLRunner(ctx)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	db.PopulateDBwithsPlot()

	var srv *server

	client, err := flight.NewClientWithMiddleware(srv.Addr().String(), nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err) // handle the error
	}
	defer client.Close()

	infoStream, err := client.ListFlights(context.TODO(),
		&flight.Criteria{Expression: []byte("2009")})
	if err != nil {
		panic(err) // handle the error
	}

	for {
		info, err := infoStream.Recv()
		if err != nil {
			if err == io.EOF { // we hit the end of the stream
				break
			}
			panic(err) // we got an error!
		}
		fmt.Println(info.GetFlightDescriptor().GetPath())
	}
}
