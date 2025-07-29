package serve

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/apache/arrow/go/v17/arrow/arrio"
	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/wolfeidau/s3iofs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)
