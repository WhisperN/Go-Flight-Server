## Todo list (Apache Arrow)

1. Create a converter (Row to col format)
2. Create an API
3. Create a connection to the Database
4. Send Data from the DB in arrow IPC

### Folder structure
```
my-arrow-app/
│
├── cmd/                         # Einstiegspunkte (main.go etc.)
│   ├── serve/                   # z. B. main.go für FlightServer
│   └── cli/                     # falls du ein CLI-Tool hast
│
├── internal/                   # interner Anwendungs-Logikcode
│   ├── duckdb/                 # Verbindung, Queries, ADBC Wrapper
│   │   └── duckdb.go
│   ├── arrow/                  # RecordBatch-Handling, IPC, Schema
│   │   └── batch.go
│   └── flight/                 # Flight-Server (z. B. DoGet, DoPut)
│       └── server.go
│
├── data/                       # Optional: CSV, Parquet, DB-Files
│   ├── splot.csv
│   └── splot.duckdb
│
├── proto/                      # .proto-Dateien (falls gRPC oder Flight-Erweiterung)
│   └── flight.proto
│
├── Dockerfile                  # Optional
├── go.mod
├── README.md
└── tests/                      # Integrationstests, ggf. Benchmarks
    ├── duckdb_test.go
    └── arrow_test.go
```