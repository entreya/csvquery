package schema

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
)

// Schema definition
type Schema struct {
	VirtualColumns map[string]string `json:"virtual_columns"` // Name -> Default Value
	path           string
	mu             sync.Mutex
}

// Load loads schema from metadata file.
func Load(csvPath string) (*Schema, error) {
	s := &Schema{
		VirtualColumns: make(map[string]string),
		path:           getHeaderPath(csvPath),
	}

	if _, err := os.Stat(s.path); os.IsNotExist(err) {
		return s, nil
	}

	data, err := ioutil.ReadFile(s.path)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(data, s); err != nil {
		return nil, err
	}

	// Ensure map is init
	if s.VirtualColumns == nil {
		s.VirtualColumns = make(map[string]string)
	}

	return s, nil
}

// Save saves schema to disk
func (s *Schema) Save() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}

	return ioutil.WriteFile(s.path, data, 0644)
}

// AddVirtualColumn registers a new virtual column
func (s *Schema) AddVirtualColumn(name, defaultValue string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.VirtualColumns[name] = defaultValue
}

// RemoveVirtualColumn removes a virtual column (used when materializing)
func (s *Schema) RemoveVirtualColumn(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.VirtualColumns, name)
}

func getHeaderPath(csvPath string) string {
	dir := filepath.Dir(csvPath)
	base := filepath.Base(csvPath)
	return filepath.Join(dir, base+"_schema.json") // Changed from meta to schema to avoid conflict with index meta
}
