package client

import (
	"context"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
)

type Client struct {
}

func (c *Client) NewClient(server_address string, err error) {}
func main() {
	client, err := flight.NewClientWithMiddleware("127.0.0.1:8080", nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
		fmt.Println(list.GetDescription())
	}
}
