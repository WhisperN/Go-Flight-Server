// server
// https://arrow.apache.org/docs/format/Flight.html
// See Notion notes...
package main

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"

	OPTIONALS "github.com/WhisperN/Go-Flight-Server/components/Optionals"
	"github.com/WhisperN/Go-Flight-Server/internal/duckdb"
	"github.com/WhisperN/Go-Flight-Server/server"
	//"github.com/WhisperN/go-Flight-Server/client"
	flight2 "github.com/apache/arrow-go/v18/arrow/flight"
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
	fmt.Println("DuckDBSQLRunner is ready")

	defer db.Close()

	var srv *server.Server
	// Because we work with optionals this would also work
	// server.NewServer(&OPTIONALS.ADDRESS{IP: OPTIONALS.String("127.0.0.1")}, db)
	fmt.Println("instantiating server")
	srv, err = server.NewServer(&OPTIONALS.ADDRESS{
		IP:   OPTIONALS.String("127.0.0.1"),
		PORT: OPTIONALS.String("8080"),
	}, db)
	if err != nil {
		panic(err)
	}
	fmt.Println("Serving...")
	err = srv.Serve()
	if err != nil {
		panic(err)
	}
	fmt.Println("Server is ready")
	defer srv.Shutdown()

	client, err := flight2.NewClientWithMiddleware("127.0.0.1:8080", nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}

	infoStream, err := client.ListFlights(context.TODO(),
		&flight2.Criteria{Expression: []byte("2009")})
	if err != nil {
		panic(err)
	}

	for {
		info, err := infoStream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
		}
		fmt.Println(info.GetFlightDescriptor().GetPath())
	}
}
