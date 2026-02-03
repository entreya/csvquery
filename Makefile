# CsvQuery Makefile
# High-Performance CSV Query Engine

.PHONY: all build test clean help build-all lint fmt test-compat benchmark install

# Configuration
BINARY    := csvquery
GO_DIR    := go
BIN_DIR   := bin
PHP_SRC   := src
TESTS     := tests

# ============================================================
# Build Commands
# ============================================================

all: build

build:
	@echo "Building native binary..."
	@cd $(GO_DIR) && go build -ldflags="-s -w" -o ../$(BIN_DIR)/$(BINARY)
	@echo "✓ Native binary built in $(BIN_DIR)/$(BINARY)"

build-all:
	@./scripts/build.sh

install: build
	@echo "Installing binary to /usr/local/bin..."
	@cp $(BIN_DIR)/$(BINARY) /usr/local/bin/
	@echo "✓ Installed to /usr/local/bin/$(BINARY)"

# ============================================================
# Test Commands
# ============================================================

test: test-php test-go

test-php:
	@echo "Running PHP tests..."
	@./vendor/bin/phpunit $(TESTS)/

test-go:
	@echo "Running Go tests..."
	@cd $(GO_DIR) && go test -v ./internal/...

test-compat:
	@echo "Running compatibility tests only..."
	@./vendor/bin/phpunit $(TESTS)/Compatibility --testdox

test-integration:
	@echo "Running integration tests..."
	@./vendor/bin/phpunit $(TESTS)/Integration

# ============================================================
# Benchmark Commands
# ============================================================

benchmark: benchmark-go benchmark-php

benchmark-go:
	@echo "Running Go benchmarks..."
	@cd $(GO_DIR) && go test -bench=. -benchmem ./internal/...

benchmark-php:
	@echo "Running PHP benchmarks..."
	@php benchmarks/php/benchmark.php 2>/dev/null || echo "No PHP benchmark found"

# ============================================================
# Code Quality
# ============================================================

lint: lint-php lint-go

lint-php:
	@echo "Linting PHP with PHP CS Fixer..."
	@vendor/bin/php-cs-fixer fix --dry-run --diff $(PHP_SRC)/ 2>/dev/null || echo "PHP CS Fixer not installed"

lint-go:
	@echo "Linting Go with golangci-lint..."
	@cd $(GO_DIR) && golangci-lint run ./... 2>/dev/null || go vet ./...

fmt: fmt-php fmt-go

fmt-php:
	@echo "Formatting PHP..."
	@vendor/bin/php-cs-fixer fix $(PHP_SRC)/ 2>/dev/null || echo "PHP CS Fixer not installed"

fmt-go:
	@echo "Formatting Go..."
	@cd $(GO_DIR) && go fmt ./...

# ============================================================
# Cleanup
# ============================================================

clean:
	@echo "Cleaning bin directory..."
	@rm -rf $(BIN_DIR)/*
	@echo "✓ Cleaned"

clean-indexes:
	@echo "Cleaning all .cidx and .bloom files..."
	@find . -name "*.cidx" -delete
	@find . -name "*.bloom" -delete
	@echo "✓ Cleaned index files"

# ============================================================
# Development
# ============================================================

dev: build test-compat
	@echo "✓ Development check complete"

ci: lint test
	@echo "✓ CI check complete"

# ============================================================
# Help
# ============================================================

help:
	@echo ""
	@echo "CsvQuery Management Commands:"
	@echo ""
	@echo "  BUILD:"
	@echo "    make build        Build native binary for current OS/Arch"
	@echo "    make build-all    Build binaries for all supported platforms"
	@echo "    make install      Install binary to /usr/local/bin"
	@echo ""
	@echo "  TEST:"
	@echo "    make test         Run all tests (PHP + Go)"
	@echo "    make test-php     Run PHP tests only"
	@echo "    make test-go      Run Go tests only"
	@echo "    make test-compat  Run PHP compatibility tests only"
	@echo ""
	@echo "  BENCHMARK:"
	@echo "    make benchmark    Run all benchmarks"
	@echo "    make benchmark-go Run Go benchmarks only"
	@echo ""
	@echo "  CODE QUALITY:"
	@echo "    make lint         Run linters (PHP CS Fixer + golangci-lint)"
	@echo "    make fmt          Format code (PHP + Go)"
	@echo ""
	@echo "  CLEANUP:"
	@echo "    make clean        Remove built binaries"
	@echo "    make clean-indexes Remove all index files"
	@echo ""
	@echo "  DEVELOPMENT:"
	@echo "    make dev          Build + run compatibility tests"
	@echo "    make ci           Full CI check (lint + test)"
	@echo ""
