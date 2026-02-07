# Contributing to CsvQuery

Thank you for considering contributing to CsvQuery! This document outlines the guidelines and process for contributing.

## Development Setup

### Requirements

- **PHP**: 8.1+
- **Go**: 1.21+
- **Composer**

### Getting Started

```bash
# Clone the repository
git clone https://github.com/entreya/csvquery.git
cd csvquery

# Install PHP dependencies
composer install

# Build Go binary
cd go
go build -o ../bin/csvquery
cd ..

# Run tests
./vendor/bin/phpunit tests/
cd go && go test -v ./internal/... && cd ..
```

## Code Style

### PHP

- Follow **PSR-12** coding standards
- Use `declare(strict_types=1);` in all files
- Use type hints for all parameters and return types
- Document public methods with PHPDoc

### Go

- Follow standard Go formatting (`gofmt`)
- Keep functions focused and small
- Use meaningful variable names
- Add comments to exported functions

## Pull Request Process

1. **Fork** the repository
2. Create a **feature branch** from `develop`:
   ```bash
   git checkout -b feature/your-feature-name
   ```
3. Make your changes
4. Add or update **tests** as needed
5. Update **documentation** if applicable
6. Ensure all tests pass:
   ```bash
   composer test
   cd go && go test ./internal/...
   ```
7. Commit with clear, descriptive messages:
   ```bash
   git commit -m "feat: add new aggregation function"
   ```
8. Push and open a **Pull Request** against `develop`

## Commit Message Convention

We follow [Conventional Commits](https://www.conventionalcommits.org/):

| Prefix | Description |
|--------|-------------|
| `feat:` | New feature |
| `fix:` | Bug fix |
| `docs:` | Documentation changes |
| `perf:` | Performance improvement |
| `refactor:` | Code refactoring |
| `test:` | Adding or updating tests |
| `chore:` | Maintenance tasks |

### Examples

```
feat: add DISTINCT support to query builder
fix: resolve bloom filter false positive rate issue
perf: optimize LZ4 decompression for large blocks
docs: update API reference for groupBy method
```

## Reporting Issues

When reporting bugs, please include:

1. **Environment**: OS, PHP version, Go version
2. **Steps to reproduce**
3. **Expected behavior**
4. **Actual behavior**
5. **Error messages** (if any)
6. **Sample CSV** (if applicable, anonymized)

## Feature Requests

For feature requests, please:

1. Check existing issues to avoid duplicates
2. Describe the **use case**
3. Propose a **solution** (if you have one)
4. Be open to discussion

## Architecture Guidelines

### PHP Component

- Keep `CsvQuery.php` as the main entry point
- `ActiveQuery.php` handles query building
- `GoBridge.php` is the sole interface to the Go binary
- Models (`Row`, `Cell`, `Column`) are for result representation

### Go Component

- Follow `internal/` package layout
- `indexer/` handles CSV scanning and index creation
- `query/` handles query execution
- `server/` handles UDS daemon
- `common/` has shared types (avoid circular imports)

### Communication Flow

```
PHP → GoBridge → SocketClient → UDS → Go Daemon
                     ↓
                Go CLI (fallback)
```

## API Stability Boundaries

> **CRITICAL**: CsvQuery maintains strict backward compatibility. Understanding these boundaries is essential.

### ❌ What CANNOT Be Changed (Public API)

| Component | Protected Elements |
|-----------|-------------------|
| **PHP Classes** | All public method signatures in `CsvQuery`, `ActiveQuery`, `GoBridge`, `SocketClient`, `Command`, `BloomFilter`, and Models |
| **Return Types** | All documented return types (arrays, objects, generators) |
| **Method Names** | All public method names |
| **Constructor Parameters** | Named parameters and their types |
| **Namespace** | `CsvQuery\*` - root namespace must remain |
| **CLI Commands** | `index`, `query`, `daemon`, `write`, `version` |
| **Index Format** | `.cidx`, `.bloom`, `_meta.json` file structures |

### ✅ What CAN Be Changed (Internal)

| Component | Changeable Elements |
|-----------|---------------------|
| **Private Methods** | Names, signatures, implementation |
| **Private Properties** | Names, types (if not reflected in public API) |
| **Internal Variables** | Local variable names |
| **Go Packages** | Internal package organization |
| **Performance** | Optimization of existing functionality |
| **Comments** | Documentation improvements |
| **Tests** | Test structure and organization |

### Before Modifying Public API

If you believe a public API change is necessary:

1. Open an **issue** to discuss the change
2. Provide **migration path** for existing users
3. Add **deprecation warnings** for at least one version
4. Update **all documentation** and **examples**
5. Ensure **compatibility tests** are updated


## Performance Considerations

When contributing performance-related changes:

1. **Benchmark** before and after
2. **Profile** for memory allocations
3. **Document** the improvement
4. Consider **backward compatibility**

### Go Benchmarks

```bash
cd go
go test -bench=. -benchmem -count=5 ./internal/query/
```

### PHP Profiling

```bash
# Using Xdebug
php -d xdebug.mode=profile examples/benchmark.php
```

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
