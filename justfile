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

# Run linter
lint:
    golangci-lint run ./...

# Clean build artifacts
clean:
    go clean -cache -testcache

# Install dependencies
deps:
    go mod download
    go mod tidy

# Run example
example:
    go run examples/basic/main.go

# Run all checks (format, lint, test)
check: fmt lint test

# Build and run example
run: build example
