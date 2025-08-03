package server

import (
	"github.com/WhisperN/Go-Flight-Server/internal/duckdb"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

/*
 * Conventions:
 * - The server returns errors instead of a response if the query fails.
 * - We define HTTP/2 over Http with Uniform Resource Identifiers.
 *   Could look like so: POST /flight.FlightService/DoGet HTTP/2
 */

type server struct {
	flight.BaseFlightServer
	s3Client *s3.Client
	bucket   string
	db       *duckdb.DuckDBSQLRunner
}

func NewServer() *server {
	return &server{
		s3Client: s3.New(s3.Options{Region: "eu-west"}),
		bucket:   "sPlot-iDiv",
	}
}

/*
 *
 */
func (s *server) ListActions(c *flight.Client, fs *flight.FlightService_ListFlightsServer) error {
	return nil
}

/*
 * This is for the server to tell you where the data is located.
 * Endpoints contains a list of locations where this data is located.
 * The Ticked is binary data that the server needs to request a data.
 * If the server wishes to indicate that the data is on the local server
 * and not a different location, then it can return an empty list of locations.
 * The client can then reuse the existing connection to the original
 * server to fetch data. Otherwise, the client must connect to one of the
 * indicated locations.
 * @param FlightDescriptor:
 * returns {endpoints: [FlightEndpoint{ticket: Ticket}]}
 */
func (s *server) GetFlightInfo() error {
	return nil
}

/*
 *
 */
func (s *server) DoGet(ticket *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	// Get the schema of our DB
	schema, err := s.db.GetSchema("sPlot")
	// Get the data form DuckDB
	data, err := s.db.RunSQL("SELECT * FROM sPlot")
	if err != nil {
		panic(err)
	}
	
	writer := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
	if err != nil {
		panic(err)
	}
	defer writer.Close()

	// Send back to client
	for _, rec := range data {
		if err := writer.Write(rec); err != nil {
			panic(err)
		}
	}

	return nil
}

/*
 *
 */
func (s *server) DoPut(fs flight.FlightService_DoPutServer) error {
	return nil
}

/*
 * PollInfo
 * - flight_descriptor
 * - info
 * - progress element of [0.0, 1.0]
 * - timestamp
 * @param FlightDescriptor
 * returns PollInfo{descriptor: FlightDescriptor, ...}
 */
func (s *server) PollFlightInfo() error {
	return nil
}

func (s *server) CancelFlightInfo() error {
	return nil
}

func main() {

}
