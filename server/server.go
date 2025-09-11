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

func newFlightTicket(path string) *flight2.Ticket {
	return &flight2.Ticket{
		Ticket: []byte(path),
	}
}
func newFlightDescriptor(path string) *flight2.FlightDescriptor {
	return &flight2.FlightDescriptor{
		Path: []string{path},
	}
}

func newFlightEndpoint(path string) []*flight2.FlightEndpoint {
	out := []*flight2.FlightEndpoint{
		{
			Ticket: newFlightTicket(path),
		},
	}
	return out
}

func newActionType() []*flight2.ActionType {
	supportedActions := []*flight2.ActionType{
		{
			Type:        "PING",
			Description: "Health-check the server",
		},
	}
	return supportedActions
}

func (s *Server) NewFlightInfo(path string) *flight2.FlightInfo {
	scm, err := s.GetSchema(s.ctx, newFlightDescriptor(path))
	if err != nil {
		logrus.Fatal(err)
	}
	out := &flight2.FlightInfo{
		Schema:           scm.Schema,
		FlightDescriptor: newFlightDescriptor(path),
		Endpoint:         newFlightEndpoint(path),
	}
	return out
}

func (s *Server) NewFlightInfos(paths []string) []*flight2.FlightInfo {
	var infos []*flight2.FlightInfo
	for _, path := range paths {
		flight := s.NewFlightInfo(path)
		if flight != nil {
			infos = append(infos, flight)
		}
	}
	return infos
}

// ------------------------------------------------------------------------------------------
// Mandatory funtions
func (s *Server) DoExchange(exchangeServer flight2.FlightService_DoExchangeServer) error {
	//TODO implement me
	panic("implement me")
}

func (s *Server) DoAction(action *flight2.Action, actionServer flight2.FlightService_DoActionServer) error {
	if action == nil {
		logrus.Error("DoAction: action is nil")
		return nil
	}

	logrus.Info("DoAction: called")

	switch action.Type {
	case "PING":
		return actionServer.Send(&flight2.Result{Body: []byte("pong")})
	default:
		logrus.Errorf("unsupported action type: %s", action.Type)
	}
	/*
		schema, err := s.db.Get3DPlotSchema()
		data, err := s.db.RunSQL("SELECT PlotObservationID, Biome, Longitude, Latitude, Species, Relative_cover FROM 3474_76_sPlotOpen_header JOIN 3474_76_sPlotOpen_DT ON PlotObservationID")
		if err != nil {
			panic(err)
		}
		writer := flight2.NewRecordWriter(actionServer, ipc.WithSchema(schema))*/
	return nil
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

	paths := []string{
		CONFIG.DuckDB.MockTable,
		CONFIG.DuckDB.LocationTable,
	}

	flightInfos := s.NewFlightInfos(paths)

	for _, info := range flightInfos {
		if err := fs.Send(info); err != nil {
			logrus.Errorf("ListFlights: Failed to send flight info: %v", err)
		}
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
	// Doesn't make sense to get info on multiple paths right?
	flightInfo := s.NewFlightInfo(fd.Path[0])

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
		logrus.Error("DoGet: ticket is nil")
		return nil
	}
	logrus.Info("DoGet: called")

	switch string(ticket.Ticket) {
	case CONFIG.DuckDB.MockTable:
		doGetService(s, CONFIG.DuckDB.MockTable, stream)
	case CONFIG.DuckDB.LocationTable:
		doGetService(s, CONFIG.DuckDB.LocationTable, stream)
	default:
		logrus.Warnf("DoGet: unknown ticket: %s", string(ticket.Ticket))
		return nil
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

// ------------------------------------------------------------------------------------------
// Service functions
func doGetService(s *Server, path string, stream flight2.FlightService_DoGetServer) {
	// Get the schema of our DB
	schema, err := s.db.GetSchema(path)
	// Get the data form DuckDB
	data, err := s.db.RunSQL(fmt.Sprintf("SELECT * FROM %s", path))
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
}
