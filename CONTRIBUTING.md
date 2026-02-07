# Contributing to Entreya CsvQuery

Thank you for your interest in contributing to CsvQuery! We welcome contributions from developers on **all platforms** - Windows, Linux, and macOS.

## 🚀 Quick Start

### Prerequisites

| Platform | Requirements |
|----------|-------------|
| **All** | PHP 8.1+, Composer, Go 1.21+ |
| **Windows** | TDM-GCC or MSYS2 (for CGO support) |
| **Linux** | gcc, make (usually pre-installed) |
| **macOS** | Xcode Command Line Tools |

### Setup

```bash
# Clone the repository
git clone https://github.com/entreya/csvquery.git
cd csvquery

# Install PHP dependencies (also triggers Go build)
composer install

# Or build manually
composer build          # Build for current platform
composer build:all      # Build for all platforms
composer build:clean    # Clean binaries
```

## 🔧 Development Workflow

### Building the Go Binary

The library uses a Go binary for heavy CSV processing. The build script (`scripts/build.php`) automatically:
- Detects your OS and architecture
- Compiles the appropriate binary
- Places it in `bin/csvquery_<os>_<arch>`

```bash
# Manual build
php scripts/build.php

# Build all platform binaries
php scripts/build.php --all
```

### Running Tests

```bash
# Run PHP tests
composer test

# Run Go tests
cd src/go && go test -v ./internal/...
```

## 🪟 Windows Development Guide

### Installing Go

1. Download from [go.dev/dl](https://go.dev/dl/)
2. Run the installer
3. Verify: `go version`

### Installing GCC (Required for CGO)

CsvQuery uses CGO for some optimizations. Windows users need a GCC compiler:

**Option 1: TDM-GCC (Recommended)**
1. Download from [jmeubank.github.io/tdm-gcc](https://jmeubank.github.io/tdm-gcc/)
2. Run installer, select "MinGW-w64 based"
3. Add to PATH: `C:\TDM-GCC-64\bin`

**Option 2: MSYS2**
```powershell
# Install MSYS2 from msys2.org
# Then in MSYS2 terminal:
pacman -S mingw-w64-x86_64-gcc
```

### Troubleshooting Windows Builds

| Issue | Solution |
|-------|----------|
| `gcc: command not found` | Install TDM-GCC or add to PATH |
| `cgo: C compiler not found` | Set `CGO_ENABLED=0` for pure Go build |
| Binary not executable | Ensure `.exe` extension is present |
| Permission denied | Run terminal as Administrator |

## 🐧 Linux Development Guide

### Ubuntu/Debian
```bash
sudo apt update
sudo apt install golang-go gcc make
```

### Fedora/RHEL
```bash
sudo dnf install golang gcc make
```

### Arch Linux
```bash
sudo pacman -S go gcc make
```

## 🍎 macOS Development Guide

```bash
# Install Xcode Command Line Tools
xcode-select --install

# Install Go via Homebrew
brew install go
```

## 🐛 Reporting Issues

When reporting bugs, please include:

1. **Platform Info**
   ```bash
   php -v
   go version
   uname -a  # Linux/macOS
   systeminfo | findstr /B /C:"OS Name" /C:"OS Version"  # Windows
   ```

2. **Build Output**
   ```bash
   php scripts/build.php 2>&1
   ```

3. **Error Messages** - Full stack trace if available

4. **Steps to Reproduce** - Minimal code example

### Memory Leaks / Crashes

If you encounter memory-related issues:
1. Enable debug mode: `$bridge->debug = true;`
2. Note the exact query that caused the issue
3. Include memory usage before/after
4. Report architecture (arm64 vs amd64)

## 📝 Code Style

### PHP
- PSR-12 coding standard
- Strict types: `declare(strict_types=1);`
- Full docblocks for public methods

### Go
- Standard `gofmt` formatting
- Descriptive variable names
- Error handling over panics

## 🔀 Pull Request Process

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Make your changes with tests
4. Run tests: `composer test`
5. Commit: `git commit -m 'feat: add amazing feature'`
6. Push: `git push origin feature/amazing-feature`
7. Open a Pull Request

### Commit Message Format

```
type: description

[optional body]
```

Types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`

## 🙏 Thank You!

Every contribution helps make CsvQuery better. Whether it's:
- Bug reports
- Feature requests  
- Code contributions
- Documentation improvements
- Platform testing (especially Windows!)

We appreciate your help!

---

Questions? Open an issue or reach out to the maintainers.
