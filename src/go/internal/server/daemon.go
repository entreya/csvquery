// Package server provides daemon servers for the CsvQuery engine.
package server

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/entreya/csvquery/internal/common"
	"github.com/entreya/csvquery/internal/query"
)

// DaemonConfig holds configuration for the Unix socket daemon.
type DaemonConfig struct {
	Network        string // "unix" or "tcp"
	Address        string // Socket path or "host:port"
	CsvPath        string
	IndexDir       string
	MaxConcurrency int
	IdleTimeout    time.Duration
}

// UDSDaemon represents the Unix Domain Socket server.
type UDSDaemon struct {
	config   DaemonConfig
	listener net.Listener
	sem      chan struct{}
	shutdown chan struct{}
	wg       sync.WaitGroup

	// In-memory data (loaded on startup)
	csvData   []byte
	headers   []string
	headerMap map[string]int
	separator byte
}

// NewUDSDaemon creates a new Unix socket daemon.
func NewUDSDaemon(cfg DaemonConfig) *UDSDaemon {
	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = 50
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = 30 * time.Second
	}
	if cfg.Network == "" {
		cfg.Network = "unix"
	}
	if cfg.Address == "" {
		// Backwards compatibility for SocketPath if used
		if cfg.Network == "unix" {
			cfg.Address = os.Getenv("CSVQUERY_SOCKET")
			if cfg.Address == "" {
				cfg.Address = "/tmp/csvquery.sock"
			}
		} else {
			cfg.Address = "127.0.0.1:0" // Default to random port? Or 8080?
		}
	}

	return &UDSDaemon{
		config:   cfg,
		sem:      make(chan struct{}, cfg.MaxConcurrency),
		shutdown: make(chan struct{}),
	}
}

// Start initializes the daemon: loads CSV, builds indexes, starts listening.
func (d *UDSDaemon) Start() error {
	// 1. Remove stale socket file if exists (only for unix)
	if d.config.Network == "unix" {
		if _, err := os.Stat(d.config.Address); err == nil {
			if err := os.Remove(d.config.Address); err != nil {
				return fmt.Errorf("failed to remove stale socket: %w", err)
			}
		}
	}

	// 2. Load CSV into memory
	if d.config.CsvPath != "" {
		if err := d.loadCSV(); err != nil {
			return fmt.Errorf("failed to load CSV: %w", err)
		}
	}

	// 3. Create listener
	listener, err := net.Listen(d.config.Network, d.config.Address)
	if err != nil {
		return fmt.Errorf("failed to bind %s %s: %w", d.config.Network, d.config.Address, err)
	}
	d.listener = listener

	// 4. Setup signal handler for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigChan
		d.Shutdown()
	}()

	fmt.Printf("CsvQuery Daemon started on %s (%s)\n", d.config.Network, d.config.Address)
	if d.config.CsvPath != "" {
		fmt.Printf("  CSV: %s (%d rows, %d columns)\n", d.config.CsvPath, d.countRows(), len(d.headers))
	}

	// 5. Accept connections
	for {
		select {
		case <-d.shutdown:
			return nil
		default:
		}

		// Set accept deadline to allow periodic shutdown check
		// Only for UnixListener or TCPListener if supported?
		// net.Listener doesn't have SetDeadline.
		// We cast to specific types.
		if ul, ok := listener.(*net.UnixListener); ok {
			_ = ul.SetDeadline(time.Now().Add(1 * time.Second))
		} else if tl, ok := listener.(*net.TCPListener); ok {
			_ = tl.SetDeadline(time.Now().Add(1 * time.Second))
		}

		conn, err := listener.Accept()
		if err != nil {
			if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
				continue // Timeout, check shutdown
			}
			select {
			case <-d.shutdown:
				return nil
			default:
				fmt.Fprintf(os.Stderr, "Accept error: %v\n", err)
				continue
			}
		}

		// TCP KeepAlive
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			_ = tcpConn.SetKeepAlive(true)
			_ = tcpConn.SetKeepAlivePeriod(30 * time.Second)
		}

		d.wg.Add(1)
		go d.handleConnection(conn)
	}
}

// Shutdown gracefully stops the daemon.
func (d *UDSDaemon) Shutdown() {
	close(d.shutdown)
	if d.listener != nil {
		_ = d.listener.Close()
	}
	d.wg.Wait()

	// Cleanup socket file (only for unix)
	if d.config.Network == "unix" {
		_ = os.Remove(d.config.Address)
	}
	fmt.Println("Daemon shutdown complete")
}

// loadCSV loads the CSV file into memory and parses headers.
func (d *UDSDaemon) loadCSV() error {
	f, err := os.Open(d.config.CsvPath)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	data, err := common.MmapFile(f)
	if err != nil {
		return err
	}

	// Find separator (default comma)
	d.separator = ','

	// Parse headers
	nlIdx := bytes.IndexByte(data, '\n')
	if nlIdx == -1 {
		return fmt.Errorf("no newline found in CSV")
	}

	headerLine := string(data[:nlIdx])
	headerLine = strings.TrimSuffix(headerLine, "\r")

	d.headers = strings.Split(headerLine, string(d.separator))
	d.headerMap = make(map[string]int, len(d.headers))
	for i, h := range d.headers {
		d.headerMap[strings.ToLower(strings.TrimSpace(h))] = i
	}

	d.csvData = data
	return nil
}

// countRows returns the number of data rows (excluding header).
func (d *UDSDaemon) countRows() int {
	if d.csvData == nil {
		return 0
	}
	count := 0
	for _, b := range d.csvData {
		if b == '\n' {
			count++
		}
	}
	return count - 1 // Subtract header
}

// handleConnection processes a single client connection.
func (d *UDSDaemon) handleConnection(conn net.Conn) {
	defer d.wg.Done()
	defer func() { _ = conn.Close() }()

	// Acquire worker slot
	select {
	case d.sem <- struct{}{}:
		defer func() { <-d.sem }()
	case <-d.shutdown:
		return
	}

	reader := bufio.NewReader(conn)

	for {
		select {
		case <-d.shutdown:
			return
		default:
		}

		// Set idle timeout
		_ = conn.SetReadDeadline(time.Now().Add(d.config.IdleTimeout))

		line, err := reader.ReadBytes('\n')
		if err != nil {
			return // EOF or timeout
		}

		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}

		// Process request
		response := d.processRequest(line)

		// Write response
		_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		_, _ = conn.Write(response)
		_, _ = conn.Write([]byte("\n"))
	}
}

// Request represents incoming JSON request.
type DaemonRequest struct {
	Action  string            `json:"action"`
	Csv     string            `json:"csv,omitempty"`
	Where   map[string]string  `json:"where,omitempty"`
	Column  string            `json:"column,omitempty"`
	AggFunc string            `json:"aggFunc,omitempty"`
	Limit   int               `json:"limit,omitempty"`
	Offset  int               `json:"offset,omitempty"`
	GroupBy string            `json:"groupBy,omitempty"`
	Verbose bool              `json:"verbose,omitempty"`
	Explain bool              `json:"explain,omitempty"`
}

// processRequest handles a single JSON request.
func (d *UDSDaemon) processRequest(data []byte) []byte {
	var req DaemonRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return d.errorResponse("invalid JSON: " + err.Error())
	}

	switch req.Action {
	case "ping":
		return d.successResponse(map[string]interface{}{"pong": true})

	case "count":
		return d.handleCount(req)

	case "select":
		return d.handleSelect(req)

	case "query":
		return d.handleQuery(req)

	case "explain":
		// Explain queries are handled by the same handler as query
		req.Explain = true
		return d.handleQuery(req)

	case "groupby":
		return d.handleGroupBy(req)

	case "status":
		return d.handleStatus()

	default:
		return d.errorResponse("unknown action: " + req.Action)
	}
}

// handleCount returns count of matching rows.
func (d *UDSDaemon) handleCount(req DaemonRequest) []byte {
	csvPath := req.Csv
	if csvPath == "" {
		csvPath = d.config.CsvPath
	}

	// Use existing query engine
	cond, err := d.parseWhere(req.Where)
	if err != nil {
		return d.errorResponse(err.Error())
	}

	cfg := query.QueryConfig{
		CsvPath:   csvPath,
		IndexDir:  d.config.IndexDir,
		Where:     cond,
		CountOnly: true,
		Verbose:   req.Verbose,
	}

	var outBuf bytes.Buffer
	engine := query.NewQueryEngine(cfg)
	engine.Writer = &outBuf

	if err := engine.Run(); err != nil {
		return d.errorResponse(err.Error())
	}

	countStr := strings.TrimSpace(outBuf.String())
	var count int
	_, _ = fmt.Sscanf(countStr, "%d", &count)

	return d.successResponse(map[string]interface{}{"count": count})
}

// handleSelect returns matching rows.
func (d *UDSDaemon) handleSelect(req DaemonRequest) []byte {
	csvPath := req.Csv
	if csvPath == "" {
		csvPath = d.config.CsvPath
	}

	cond, err := d.parseWhere(req.Where)
	if err != nil {
		return d.errorResponse(err.Error())
	}

	cfg := query.QueryConfig{
		CsvPath:  csvPath,
		IndexDir: d.config.IndexDir,
		Where:    cond,
		Limit:    req.Limit,
		Offset:   req.Offset,
		Verbose:  req.Verbose,
	}

	var outBuf bytes.Buffer
	engine := query.NewQueryEngine(cfg)
	engine.Writer = &outBuf

	if err := engine.Run(); err != nil {
		return d.errorResponse(err.Error())
	}

	// Parse the output (newline-separated offset,line pairs)
	result := strings.TrimSpace(outBuf.String())
	lines := strings.Split(result, "\n")

	offsets := make([]map[string]interface{}, 0, len(lines))
	for _, line := range lines {
		if line == "" {
			continue
		}
		parts := strings.Split(line, ",")
		if len(parts) >= 2 {
			var offset, lineNum int
			_, _ = fmt.Sscanf(parts[0], "%d", &offset)
			_, _ = fmt.Sscanf(parts[1], "%d", &lineNum)
			offsets = append(offsets, map[string]interface{}{
				"offset": offset,
				"line":   lineNum,
			})
		}
	}

	return d.successResponse(map[string]interface{}{"rows": offsets})
}

// handleGroupBy returns grouped aggregation results.
func (d *UDSDaemon) handleGroupBy(req DaemonRequest) []byte {
	csvPath := req.Csv
	if csvPath == "" {
		csvPath = d.config.CsvPath
	}

	cond, err := d.parseWhere(req.Where)
	if err != nil {
		return d.errorResponse(err.Error())
	}

	groupCol := req.GroupBy
	if groupCol == "" {
		groupCol = req.Column
	}

	aggFunc := req.AggFunc
	if aggFunc == "" {
		aggFunc = "count"
	}

	cfg := query.QueryConfig{
		CsvPath:  csvPath,
		IndexDir: d.config.IndexDir,
		Where:    cond,
		GroupBy:  groupCol,
		AggFunc:  aggFunc,
		Verbose:  req.Verbose,
	}

	var outBuf bytes.Buffer
	engine := query.NewQueryEngine(cfg)
	engine.Writer = &outBuf

	if err := engine.Run(); err != nil {
		return d.errorResponse(err.Error())
	}

	// Parse JSON output from engine
	var groups map[string]interface{}
	if err := json.Unmarshal([]byte(strings.TrimSpace(outBuf.String())), &groups); err != nil {
		return d.errorResponse("failed to parse groupby result: " + err.Error())
	}

	return d.successResponse(map[string]interface{}{"groups": groups})
}

// handleQuery handles generic queries (agg, explain, or offsets).
func (d *UDSDaemon) handleQuery(req DaemonRequest) []byte {
	csvPath := req.Csv
	if csvPath == "" {
		csvPath = d.config.CsvPath
	}

	cond, err := d.parseWhere(req.Where)
	if err != nil {
		return d.errorResponse(err.Error())
	}

	cfg := query.QueryConfig{
		CsvPath:   csvPath,
		IndexDir:  d.config.IndexDir,
		Where:     cond,
		Limit:     req.Limit,
		Offset:    req.Offset,
		CountOnly: false,
		Explain:   req.Explain,
		GroupBy:   req.GroupBy,
		AggFunc:   req.AggFunc,
		Verbose:   req.Verbose,
	}

	var outBuf bytes.Buffer
	engine := query.NewQueryEngine(cfg)
	engine.Writer = &outBuf

	if err := engine.Run(); err != nil {
		return d.errorResponse(err.Error())
	}

	output := strings.TrimSpace(outBuf.String())

	// If it's JSON output (Agg or Explain), return as is (wrapped if needed)
	if strings.HasPrefix(output, "{") || strings.HasPrefix(output, "[") {
		// It's JSON from the engine.
		// We can return it directly as the "data" field or similar.
		var jsonData interface{}
		if err := json.Unmarshal([]byte(output), &jsonData); err == nil {
			return d.successResponse(map[string]interface{}{"data": jsonData})
		}
	}

	// Fallback to text (e.g. for simple selects, though select action is preferred)
	return d.successResponse(map[string]interface{}{"output": output})
}

// handleStatus returns daemon status.
func (d *UDSDaemon) handleStatus() []byte {
	return d.successResponse(map[string]interface{}{
		"status":   "running",
		"csv":      d.config.CsvPath,
		"indexDir": d.config.IndexDir,
		"rows":     d.countRows(),
		"columns":  len(d.headers),
		"network":  d.config.Network,
		"address":  d.config.Address,
	})
}

// parseWhere converts simple where map to query condition.
func (d *UDSDaemon) parseWhere(where map[string]string) (*query.Condition, error) {
	if len(where) == 0 {
		return nil, nil
	}

	// Convert to JSON for parsing
	whereJSON, err := json.Marshal(where)
	if err != nil {
		return nil, err
	}

	return query.ParseCondition(whereJSON)
}

// errorResponse creates an error JSON response.
func (d *UDSDaemon) errorResponse(msg string) []byte {
	b, _ := json.Marshal(map[string]interface{}{
		"error": msg,
	})
	return b
}

// successResponse creates a success JSON response.
func (d *UDSDaemon) successResponse(data map[string]interface{}) []byte {
	data["error"] = nil
	b, _ := json.Marshal(data)
	return b
}

// RunDaemon is the entry point called from main.go
func RunDaemon(network, address, csvPath, indexDir string, maxConcurrency int) error {
	cfg := DaemonConfig{
		Network:        network,
		Address:        address,
		CsvPath:        csvPath,
		IndexDir:       indexDir,
		MaxConcurrency: maxConcurrency,
	}

	// Use indexer to verify CSV path if provided
	if csvPath != "" {
		if _, err := os.Stat(csvPath); os.IsNotExist(err) {
			return fmt.Errorf("CSV file not found: %s", csvPath)
		}
	}

	daemon := NewUDSDaemon(cfg)
	return daemon.Start()
}
