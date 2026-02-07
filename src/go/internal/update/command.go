package update

import (
	"bufio"
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/entreya/csvquery/internal/query"
	"github.com/entreya/csvquery/internal/updatemgr"
)

// Config represents update configuration
type Config struct {
	CsvPath   string
	SetClause string // "COL=VAL,COL2=VAL2"
	WhereStr  string // "COL=VAL" or json filter
	IndexDir  string
}

// Execute logic handled by RunUpdate via main.go logic
// func Execute(cfg Config) error { ... }

// RunUpdate executes the update logic given a query engine and update map
func RunUpdate(q *query.QueryEngine, csvPath string, updates map[string]string) (int, error) {
	// Capture Output
	// We need to capture query output to get Offsets.
	// QueryEngine prints to q.Writer.
	var buf bytes.Buffer
	q.Writer = &buf

	// Run Query
	if err := q.Run(); err != nil {
		return 0, fmt.Errorf("query failed: %v", err)
	}

	// Load Updates Manager
	um, err := updatemgr.Load(csvPath)
	if err != nil {
		return 0, err
	}

	// Parse Results
	scanner := bufio.NewScanner(&buf)
	count := 0
	for scanner.Scan() {
		line := scanner.Text()

		// Output format: "Offset,LineNum"
		parts := strings.Split(line, ",")
		if len(parts) < 2 {
			continue
		}

		offsetStr := parts[0]
		offset, err := strconv.ParseInt(offsetStr, 10, 64)
		if err != nil {
			continue
		}

		// Apply Updates (Indexed by Offset now)
		for col, val := range updates {
			um.Set(offset, col, val)
		}
		count++
	}

	// Save
	if count > 0 {
		if err := um.Save(); err != nil {
			return count, fmt.Errorf("failed to save updates: %v", err)
		}
	}

	return count, nil
}
