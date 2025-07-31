package serve

import (
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
		s3Client: s3.New(s3.Options{Region: "us-east-2"}),
		bucket:   "ursa-labs-taxi-data",
	}
}
