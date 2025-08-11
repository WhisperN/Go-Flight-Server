// Author: WhisperN
// Developed at University of Zürich

package server

import (
	"bytes"
	"context"
	"fmt"
	"github.com/WhisperN/Go-Flight-Server/internal/components/config"
	duckdb "github.com/WhisperN/Go-Flight-Server/internal/duckdb"
	flight2 "github.com/apache/arrow-go/v18/arrow/flight"
	ipc "github.com/apache/arrow-go/v18/arrow/ipc"
	memory "github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/sirupsen/logrus"
)

var CONFIG = config.LoadConfig(true)

/*
 * Conventions:
 * - The server returns errors instead of a response if the query fails.
 * - We define HTTP/2 over Http with Uniform Resource Identifiers.
 *   Could look like so: POST /flight.FlightService/DoGet HTTP/2
 */

type Server struct {
	flight2.BaseFlightServer
	server      *flight2.Server
	db          *duckdb.SQLRunner
	authHandler flight2.ServerAuthHandler
	ctx         context.Context
}

func newFlightTicket() *flight2.Ticket {
	return &flight2.Ticket{
		Ticket: []byte(CONFIG.DuckDB.TableName),
	}
}
func newFlightDescriptor() *flight2.FlightDescriptor {
	return &flight2.FlightDescriptor{
		Path: []string{CONFIG.DuckDB.TableName},
	}
}

func newFlightEndpoint() []*flight2.FlightEndpoint {
	out := []*flight2.FlightEndpoint{
		{
			Ticket: newFlightTicket(),
		},
	}
	return out
}

func newActionType() []*flight2.ActionType {
	supportedActions := []*flight2.ActionType{
		{
			Type:        "DoGet",
			Description: "Get the current dataset",
		},
	}
	return supportedActions
}

func (s *Server) NewFlightInfo() *flight2.FlightInfo {
	scm, err := s.GetSchema(s.ctx, newFlightDescriptor())
	if err != nil {
		logrus.Fatal(err)
	}
	out := &flight2.FlightInfo{
		Schema:           scm.Schema,
		FlightDescriptor: newFlightDescriptor(),
		Endpoint:         newFlightEndpoint(),
	}
	return out
}

// ------------------------------------------------------------------------------------------
// Mandatory funtions
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
/* Creates new Arrow Flight Server in Go
 * @param db *duckdb.SQLRunner: The instance of a duckdb
 * @return *Server
 */
func NewServer(db *duckdb.SQLRunner) (*Server, error) {
	if db == nil {
		logrus.Fatal("db is nil: Please make sure to give a database Object")
	}
	// We use Middleware because of potential Authentication layers
	var srvFlight = flight2.NewServerWithMiddleware(nil)
	err := srvFlight.Init(CONFIG.Server.Address + ":" + CONFIG.Server.Port)
	if err != nil {
		logrus.Fatal("Could not start server. Please provide a valid IP address or port")
	}

	srv := &Server{
		server: &srvFlight,
		db:     db,
		ctx:    context.Background(),
	}

	srvFlight.RegisterFlightService(srv)
	return srv, nil
}

// Handshake
// TODO: Implement at later stage
func (s *Server) Handshake(handshakeServer flight2.FlightService_HandshakeServer) error {
	req, err := handshakeServer.Recv()
	if err != nil {
		return err
	}
	if s.authHandler == nil {
		return nil
	}

	// return s.authHandler.Authenticate(&serverAuthConn{handshakeServer})

	logrus.Info("Received Handshake request: Payload=%s", string(req.Payload))

	return handshakeServer.Send(&flight2.HandshakeResponse{
		Payload: []byte("ok"),
	})
}

// ListActions
/* Lists available actions
 * @param empty *flight.Empty: An input of type Empty
 * @param actionServer *flight.FlightService_ListFlightsServer: Calling the request
 */
func (s *Server) ListActions(empty *flight2.Empty, actionsServer flight2.FlightService_ListActionsServer) error {
	supportedActions := newActionType()
	for _, action := range supportedActions {
		if err := actionsServer.Send(action); err != nil {
			logrus.Info("failed to send action %s: %w", action.Type, err)
		}
	}
	return nil
}

// ListFlights
/* Sends back a list of endpoints with corresponding tickets
 * @param criteria *flight2.Criteria
 * @param fs flight2.FlightService_ListFlightsServer
 */
func (s *Server) ListFlights(criteria *flight2.Criteria, fs flight2.FlightService_ListFlightsServer) error {
	logrus.Info("ListFlights: called")

	flightInfo := s.NewFlightInfo()

	if err := fs.Send(flightInfo); err != nil {
		logrus.Errorf("ListFlights: Failed to send flight info: %v", err)
	}
	logrus.Info("ListFlights: End of function")
	return nil
}

// GetFlightInfo
/* This is for the server to tell you where the data is located.
 * Endpoints contains a list of locations where this data is located.
 * The Ticked is binary data that the server needs to request a data.
 * If the server wishes to indicate that the data is on the local server
 * and not a different location, then it can return an empty list of locations.
 * The client can then reuse the existing connection to the original
 * server to fetch data. Otherwise, the client must connect to one of the
 * indicated locations.
 * @param fd *flight2.FlightDescriptor
 * @return *flight2.FlightInfo
 */
func (s *Server) GetFlightInfo(ctx context.Context, fd *flight2.FlightDescriptor) (*flight2.FlightInfo, error) {
	flightInfo := s.NewFlightInfo()

	return flightInfo, nil
}

// GetSchema
/* gets the schema of the database table
 * @param ctx context.Context
 * @param fd *flight2.FlightDescriptor
 * @return *flight2.SchemaResult
 */
func (s *Server) GetSchema(ctx context.Context, fd *flight2.FlightDescriptor) (*flight2.SchemaResult, error) {
	if fd == nil || len(fd.Path) == 0 {
		logrus.Errorf("missing descriptor or path")
		return nil, nil
	}
	tableName := fd.Path[0]
	schema, err := s.db.GetSchema(tableName)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	mem := memory.NewGoAllocator()

	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema), ipc.WithAllocator(mem))

	if err := writer.Close(); err != nil {
		logrus.Errorf("failed to close IPC writer: %w", err)
	}

	return &flight2.SchemaResult{
		Schema: buf.Bytes(),
	}, nil
}

// DoGet
/* Gets entry from a database with corresponding ticket (table name)
 * @param ticket *flight.Ticket: The service the client requests in bytes
 * @param stream flight.FlightService_DoGetServer: Dong the call to the flight server
 */
func (s *Server) DoGet(ticket *flight2.Ticket, stream flight2.FlightService_DoGetServer) error {
	if ticket == nil {
		logrus.Error("ticket is nil")
		return nil
	}
	logrus.Info("DoGet: called")
	// Get the schema of our DB
	schema, err := s.db.GetSchema(CONFIG.DuckDB.TableName)
	// Get the data form DuckDB
	data, err := s.db.RunSQL(fmt.Sprintf("SELECT * FROM %s", CONFIG.DuckDB.TableName))
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
	logrus.Info("DoGet: End of function")
	return nil
}

// DoPut method
/* If we want to upload some data...
 * @param fs flight.FlightService_DoPutServer
 */
func (s *Server) DoPut(fs flight2.FlightService_DoPutServer) error {
	if fs == nil {
		logrus.Info("nil flight server")
	}
	return nil
}

// PollFlightInfo method
/*
 * @param ctx context.Context
 * @param desc *flight2.FlightDescriptor
 * @return *flight2.PollInfo
 */
func (s *Server) PollFlightInfo(ctx context.Context, desc *flight2.FlightDescriptor) (*flight2.PollInfo, error) {
	return nil, nil
}

// Serve
// starts the server
func (s *Server) Serve() error {
	srv := *s.server
	go func() {
		err := srv.Serve()
		if err != nil {
			panic(err)
		}
	}()
	return nil
}

// Shutdown
// Stops the server and closes all records
func (s *Server) Shutdown() error {
	srv := *s.server
	srv.Shutdown()
	s.db.Close()
	return nil
}

// TODO:
// - CancelFlightInfoRequest
// - Handshake
