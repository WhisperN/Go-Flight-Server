.DEFAULT_GOAL := build

.PHONY: fmt vet build clean
fmt:
	go fmt ./duckdb.go

vet: fmt
	go vet ./duckdb.go

build: vet
	go build

clean:
	go clean