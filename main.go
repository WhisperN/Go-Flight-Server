package src

import (
	"Go-Flight-Server/internal/duckdb"
	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type server struct {
	flight.BaseFlightServer
	s3Client *s3.Client
	bucket   string
}

func NewServer() *server {
	return &server{
		s3Client: s3.New(s3.Options{Region: "eu-west"}),
		bucket:   "sPlot-iDiv",
	}
}

func (s *server) ListActions(c *flight.Client, fs *flight.FlightService_ListFlightsServer) error {
	return nil
}

func (s *server) DoGet(fs flight.FlightService_DoGetServer) error {
	// Get the data form DuckDB
	// ?Transform the data from DuckDB to parquet?
	// Send back to client
	return nil
}
