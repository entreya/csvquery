package server

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/csvquery/csvquery/internal/query"
)

// ServerConfig holds configuration for the daemon
type ServerConfig struct {
	Port           int
	MaxConcurrency int
}

// Daemon represents the long-running search server
type Daemon struct {
	config ServerConfig
	sem    chan struct{} // Semaphore for concurrency limiting
}

// NewDaemon creates a new server instance
func NewDaemon(cfg ServerConfig) *Daemon {
	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = 50 // Default limit
	}
	return &Daemon{
		config: cfg,
		sem:    make(chan struct{}, cfg.MaxConcurrency),
	}
}

// Start begins listening on the TCP port
func (s *Daemon) Start() error {
	addr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to bind %s: %w", addr, err)
	}

	fmt.Printf("Search Daemon listening on %s (Concurrency: %d)\n", addr, s.config.MaxConcurrency)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Accept error: %v\n", err)
			continue
		}

		// Configure socket
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			_ = tcpConn.SetKeepAlive(true)
			_ = tcpConn.SetKeepAlivePeriod(30 * time.Second)
		}

		go s.handleConnection(conn)
	}
}

// handleConnection processes a single client connection
func (s *Daemon) handleConnection(conn net.Conn) {
	defer func() { _ = conn.Close() }()

	// 1. Acquire worker slot (non-blocking preferred? Or blocking queue?)
	// User Requirement: "If 51 requests come in, the 51st waits in a queue."
	// So blocking send to channel is correct.
	s.sem <- struct{}{}
	defer func() { <-s.sem }()

	// 2. Set Read Deadline (User Req: 500ms)
	// We set deadline for EACH read to ensure active clients.
	// Actually, if we want persistent connections, we set deadline before reading request,
	// and if it times out, we close.
	// But `pfsockopen` keeps connection open.
	// If IDLE, we shouldn't kill it immediately?
	// The requirement: "If PHP doesn't send the query fast enough, drop the connection".
	// This usually applies to the *initial* request read.
	// Let's use a 5-second idle timeout, but 500ms *read* timeout once data starts?
	// User said "Read Deadline (timeout) of 500ms".
	// Valid interpretation: Ensure queries are sent quickly.
	_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))

	scanner := bufio.NewScanner(conn)
	// We read line-by-line (JSON-RCP style)

	// Currently assuming one-shot per connection??
	// User said "Persistent Socket... keeps connection open... across multiple calls".
	// So we need a loop.

	for {
		// Reset deadline for next request (keepalive)
		// Wait, strict 500ms might be too aggressive for idle keepalive.
		// Usually we allow idle time.
		// "If PHP doesn't send the query fast enough" -> during active phase.
		// Let's set a distinct Idle Timeout vs Read Timeout.
		// For now, I'll stricly follow 500ms for read.
		// If `pfsockopen` is idle, it might timeout and reconnect.
		_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))

		if !scanner.Scan() {
			return // Checking EOF or error
		}

		reqLine := scanner.Bytes()
		if len(reqLine) == 0 {
			continue
		}

		// Process Request
		response := s.processRequest(reqLine)

		// Write Response
		_ = conn.SetWriteDeadline(time.Now().Add(500 * time.Millisecond))
		_, _ = conn.Write(response)
		_, _ = conn.Write([]byte("\n")) // Delimiter
	}
}

// Request structure
type Request struct {
	Command string          `json:"command"`
	Params  json.RawMessage `json:"params"`
}

type QueryParams struct {
	CsvPath  string          `json:"csv_path"`
	IndexDir string          `json:"index_dir"`
	Where    json.RawMessage `json:"where"`
	Limit    int             `json:"limit"`
	Offset   int             `json:"offset"`
}

func (s *Daemon) processRequest(data []byte) []byte {
	var req Request
	if err := json.Unmarshal(data, &req); err != nil {
		return errorResponse("Invalid JSON")
	}

	switch req.Command {
	case "query":
		return s.handleQuery(req.Params)
	case "ping":
		return successResponse("pong")
	default:
		return errorResponse("Unknown command")
	}
}

func (s *Daemon) handleQuery(paramsJSON json.RawMessage) []byte {
	var p QueryParams
	if err := json.Unmarshal(paramsJSON, &p); err != nil {
		return errorResponse("Invalid params")
	}

	// Parse Where
	cond, err := query.ParseCondition(p.Where) // Use exported ParseCondition
	if err != nil {
		return errorResponse(err.Error())
	}

	cfg := query.QueryConfig{
		CsvPath:  p.CsvPath,
		IndexDir: p.IndexDir,
		Where:    cond,
		Limit:    p.Limit,
		Offset:   p.Offset,
	}

	// Capture output in buffer
	var outBuf bytes.Buffer

	// Create Engine
	engine := query.NewQueryEngine(cfg)
	engine.Writer = &outBuf // Direct output to buffer

	// Execute
	if err := engine.Run(); err != nil {
		return errorResponse(err.Error())
	}

	scanRes := strings.TrimSpace(outBuf.String())
	return successResponse(scanRes)
}

func errorResponse(msg string) []byte {
	b, _ := json.Marshal(map[string]interface{}{
		"status": "error",
		"error":  msg,
	})
	return b
}

func successResponse(data interface{}) []byte {
	b, _ := json.Marshal(map[string]interface{}{
		"status": "ok",
		"data":   data,
	})
	return b
}
