# CsvQuery Makefile

.PHONY: all build test clean help build-all

# Binary name
BINARY=csvquery
GO_DIR=go
BIN_DIR=bin

# Native build
all: build

build:
	@echo "Building native binary..."
	@cd $(GO_DIR) && go build -ldflags="-s -w" -o ../$(BIN_DIR)/$(BINARY)
	@echo "✓ Native binary built in $(BIN_DIR)/$(BINARY)"

# Build for all platforms (cross-compile)
build-all:
	@./scripts/build.sh

# Run all tests
test: test-php test-go

test-php:
	@echo "Running PHP tests..."
	@vendor/bin/phpunit tests/

test-go:
	@echo "Running Go tests..."
	@cd $(GO_DIR) && go test -v ./internal/...

# Clean build artifacts
clean:
	@echo "Cleaning bin directory..."
	@rm -rf $(BIN_DIR)/*
	@echo "✓ Cleaned"

# Help command
help:
	@echo "CsvQuery Management Commands:"
	@echo "  make build      - Build native binary for current OS/Arch"
	@echo "  make build-all  - Build binaries for all supported platforms (Linux, macOS, Windows)"
	@echo "  make test       - Run both PHP and Go tests"
	@echo "  make test-php   - Run PHP tests"
	@echo "  make test-go    - Run Go tests"
	@echo "  make clean      - Remove all built binaries"
	@echo "  make help       - Show this help message"
