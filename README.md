## Todo list (Apache Arrow)

1. Create an gRPC API

### Folder structure
... Will be adjusted in the future.

# Set-up
## Database
Download the corresponding libduckdb library from [here](https://github.com/duckdb/duckdb/releases)

## Backend Server
**IDEAS**
- include large Bazel if project get larger than anticipated.

# Proxy
```bash
envoy -c proxy/envoy.yaml
```
or run the run.sh script in the folder, if you want a super light-weight server.

# Run
````bash
 CGO_CXXFLAGS="-I$(pwd)/third_party" go run .
````