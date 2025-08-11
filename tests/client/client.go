package client

import (
	"context"
	"fmt"
	"github.com/WhisperN/Go-Flight-Server/internal/config"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
)

var CONFIG = config.LoadConfig("../../config.yaml")

type Client struct {
}

func (c *Client) NewClient(server_address string, err error) {}
func main() {
	client, err := flight.NewClientWithMiddleware(fmt.Sprintf("%s:%s", CONFIG.Server.Address, CONFIG.Server.Port), nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logrus.Fatal(err)
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
		logrus.Info(info.GetFlightDescriptor().GetPath())
	}

	empty := flight.Empty{}

	listActions, err := client.ListActions(context.TODO(), &empty)
	if err != nil {
		panic(err)
	}

	for {
		list, err := listActions.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		logrus.Info(list.GetDescription())
	}
}
