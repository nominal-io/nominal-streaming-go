# List available recipes
default:
    @just --list

# Build the library
build:
    go build -v ./...

# Run tests
test:
    go test -v -race

# Format code
fmt:
    go fmt ./...

# Clean build artifacts
clean:
    go clean -cache -testcache

# Install dependencies
deps:
    go mod download
    go mod tidy

# Install development tools
install:
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

# Generate Go code from proto files
gen-proto:
    protoc --go_out=. --go_opt=paths=source_relative proto/nominal_write.proto

# Run all checks
check: fmt test

# Run throughput benchmarks
benchmark:
    go test -bench=Throughput -benchmem -benchtime=5s -timeout=60s
