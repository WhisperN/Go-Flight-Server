// Author: WhisperN
// Developed at University of Zürich

package server

import (
	"context"
	"errors"
	OPTIONALS "github.com/WhisperN/Go-Flight-Server/components/Optionals"
	"github.com/WhisperN/Go-Flight-Server/internal/duckdb"
	flight2 "github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

/*
 * Conventions:
 * - The server returns errors instead of a response if the query fails.
 * - We define HTTP/2 over Http with Uniform Resource Identifiers.
 *   Could look like so: POST /flight.FlightService/DoGet HTTP/2
 */

type Server struct {
	flight2.BaseFlightServer
	server *flight2.Server
	db     *duckdb.DuckDBSQLRunner
}

// ------------------------------------------------------------------------------------------
// Mandatory funtions
func (s *Server) Handshake(handshakeServer flight2.FlightService_HandshakeServer) error {
	//TODO implement me
	panic("implement me")
}

func (s *Server) ListFlights(criteria *flight2.Criteria, flightsServer flight2.FlightService_ListFlightsServer) error {
	//TODO implement me
	panic("implement me")
}

func (s *Server) GetSchema(ctx context.Context, descriptor *flight2.FlightDescriptor) (*flight2.SchemaResult, error) {
	//TODO implement me
	panic("implement me")
}

func (s *Server) DoExchange(exchangeServer flight2.FlightService_DoExchangeServer) error {
	//TODO implement me
	panic("implement me")
}

func (s *Server) DoAction(action *flight2.Action, actionServer flight2.FlightService_DoActionServer) error {
	//TODO implement me
	panic("implement me")
}

func (s *Server) mustEmbedUnimplementedFlightServiceServer() {
	//TODO implement me
	panic("implement me")
}

// ------------------------------------------------------------------------------------------
// My implemented functions

// NewServer
/* @param region string: Timezone something like: "eu-west-1"
 * @param bucket_name string: define the dataset that we want to connect to ("sPlot-iDiv")
 * returns -> &Server
 */
func NewServer(address *OPTIONALS.ADDRESS, db *duckdb.DuckDBSQLRunner) (*Server, error) {
	if db == nil {
		return nil, errors.New("db is nil: Please make sure to give a database Object")
	}
	// We use Middleware because of potential Authentication layers
	var srv = flight2.NewServerWithMiddleware(nil)
	if address != nil && address.Check() {
		srv.Init(*address.IP + ":" + *address.PORT)
	} else {
		srv.Init("0.0.0.0:8080")
	}

	srvLocalImpl := &Server{
		server: &srv,
		db:     db,
	}

	srv.RegisterFlightService(srvLocalImpl)
	return srvLocalImpl, nil
}

// ListActions method
/* @param c *flight.Client: The client that sent the request
 * @param fs *flight.FlightService_ListFlightsServer: Calling the request
 */
func (s *Server) ListActions(empty *flight2.Empty, actionsServer flight2.FlightService_ListActionsServer) error {
	return nil
}

// GetFlightInfo
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
func (s *Server) GetFlightInfo(context.Context, *flight2.FlightDescriptor) (*flight2.FlightInfo, error) {
	return nil, nil
}

// DoGet
/* @param ticket *flight.Ticket: The service the client requests in bytes
 * @param stream flight.FlightService_DoGetServer: Dong the call to the flight server
 */
func (s *Server) DoGet(ticket *flight2.Ticket, stream flight2.FlightService_DoGetServer) error {
	if ticket == nil {
		panic("nil flight ticket")
	}
	// Get the schema of our DB
	schema, err := s.db.GetSchema("sPlot")
	// Get the data form DuckDB
	data, err := s.db.RunSQL("SELECT * FROM sPlot")
	if err != nil {
		panic(err)
	}

	writer := flight2.NewRecordWriter(stream, ipc.WithSchema(schema))
	defer func(writer *flight2.Writer) {
		err := writer.Close()
		if err != nil {
			panic(err)
		}
	}(writer)

	// Send back to client
	for _, rec := range data {
		if err := writer.Write(rec); err != nil {
			panic(err)
		}
	}

	return nil
}

// DoPut method
/* @param fs flight.FlightService_DoPutServer
 */
func (s *Server) DoPut(fs flight2.FlightService_DoPutServer) error {
	if fs == nil {
		panic("nil flight server")
	}
	return nil
}

// PollFlightInfo method
/* - flight_descriptor
 * - info
 * - progress element of [0.0, 1.0]
 * - timestamp
 * @param FlightDescriptor
 * returns PollInfo{descriptor: FlightDescriptor, ...}
 */
func (s *Server) PollFlightInfo(ctx context.Context, desc *flight2.FlightDescriptor) (*flight2.PollInfo, error) {
	return nil, nil
}

func (s *Server) CancelFlightInfo() error {
	return nil
}

func (s *Server) Serve() error {
	srv := *s.server
	go srv.Serve()
	return nil
}

func (s *Server) Shutdown() error {
	srv := *s.server
	srv.Shutdown()
	return nil
}
