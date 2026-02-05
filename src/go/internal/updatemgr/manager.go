package updatemgr

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// UpdateManager handles row-level overrides stored in a sidecar JSON file.
type UpdateManager struct {
	csvPath    string
	schemaPath string
	mu         sync.RWMutex
	// Overrides maps LineNumber -> Column -> Value
	Overrides map[string]map[string]string `json:"rows"`
	// Note: JSON keys are strings, so we use string for LineNumber key.
}

// Load creates a manager and loads existing updates if present.
func Load(csvPath string) (*UpdateManager, error) {
	absPath, err := filepath.Abs(csvPath)
	if err != nil {
		return nil, err
	}
	schemaPath := absPath + "_updates.json"

	um := &UpdateManager{
		csvPath:    absPath,
		schemaPath: schemaPath,
		Overrides:  make(map[string]map[string]string),
	}

	if _, err := os.Stat(schemaPath); err == nil {
		data, err := os.ReadFile(schemaPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read updates file: %v", err)
		}
		if len(data) > 0 {
			if err := json.Unmarshal(data, um); err != nil {
				return nil, fmt.Errorf("failed to parse updates file: %v", err)
			}
		}
	}

	return um, nil
}

// Save persists the updates to disk.
func (um *UpdateManager) Save() error {
	um.mu.RLock()
	defer um.mu.RUnlock()

	data, err := json.MarshalIndent(um, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(um.schemaPath, data, 0644)
}

// Set updates a value for a specific row via offset.
func (um *UpdateManager) Set(offset int64, column, value string) {
	um.mu.Lock()
	defer um.mu.Unlock()

	key := fmt.Sprintf("%d", offset)
	if _, ok := um.Overrides[key]; !ok {
		um.Overrides[key] = make(map[string]string)
	}
	um.Overrides[key][column] = value
}

// GetRow returns all overrides for a specific row offset, or nil if none exist.
func (um *UpdateManager) GetRow(offset int64) map[string]string {
	um.mu.RLock()
	defer um.mu.RUnlock()

	key := fmt.Sprintf("%d", offset)
	if row, ok := um.Overrides[key]; ok {
		// Return a copy to avoid race conditions if caller modifies it?
		// For read-only query engine, direct map access is risky if updates happen concurrently?
		// But in our model, updates happen via CLI (separate process).
		// If we are reading, we loaded snapshot at start.
		return row
	}
	return nil
}
