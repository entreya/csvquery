package query

import (
	"encoding/json"
	"fmt"
	"strings"
)

// FilterOp defines comparison operators
type FilterOp string

const (
	OpEq        FilterOp = "="
	OpNeq       FilterOp = "!="
	OpGt        FilterOp = ">"
	OpLt        FilterOp = "<"
	OpGte       FilterOp = ">="
	OpLte       FilterOp = "<="
	OpLike      FilterOp = "LIKE"
	OpIsNull    FilterOp = "IS NULL"
	OpIsNotNull FilterOp = "IS NOT NULL"
	OpIn        FilterOp = "IN"
)

// Condition represents a single node in the filter tree
// It can be a leaf (Column op Value) or non-leaf (AND/OR with Children)
type Condition struct {
	Operator       FilterOp    `json:"operator"`
	Column         string      `json:"column,omitempty"`
	Value          interface{} `json:"value,omitempty"`
	Children       []Condition `json:"children,omitempty"`
	resolvedTarget string      // pre-computed string form of Value, set after parse
	resolvedColIdx int         // pre-resolved column index for fast evaluation (-1 if unresolved)
	lowerTarget    string      // pre-lowercased target for LIKE comparisons
}

// resolveTargets pre-computes valid string targets for faster evaluation
func (c *Condition) resolveTargets() {
	if c.Value != nil {
		c.resolvedTarget = fmt.Sprintf("%v", c.Value)
	}
	for i := range c.Children {
		c.Children[i].resolveTargets()
	}
}

// Evaluate checks if a row matches the condition
// row map is Column -> Value
func (c *Condition) Evaluate(row map[string]string) bool {
	switch c.Operator {
	case "AND":
		for _, child := range c.Children {
			if !child.Evaluate(row) {
				return false
			}
		}
		return true
	case "OR":
		for _, child := range c.Children {
			if child.Evaluate(row) {
				return true
			}
		}
		return false
	}

	// Leaf nodes
	val, exists := row[c.Column]

	switch c.Operator {
	case OpIsNull:
		return !exists || val == "" || val == "NULL"
	case OpIsNotNull:
		return exists && val != "" && val != "NULL"
	}

	if !exists {
		return false // Default fail if column missing for other ops
	}

	target := c.resolvedTarget

	switch c.Operator {
	case OpEq:
		return val == target
	case OpNeq:
		return val != target
	case OpGt:
		return val > target
	case OpLt:
		return val < target
	case OpGte:
		return val >= target
	case OpLte:
		return val <= target
	case OpLike:
		// Simple wildcard match
		// TODO: Regex or better globbing if needed
		return strings.Contains(strings.ToLower(val), strings.ToLower(target))
	}

	return false
}

// ResolveColumns pre-maps column names to integer indices for zero-allocation evaluation.
// Must be called once before using EvaluateFast.
func (c *Condition) ResolveColumns(headers map[string]int) {
	c.resolvedColIdx = -1 // default: unresolved
	if c.Column != "" {
		if idx, ok := headers[c.Column]; ok {
			c.resolvedColIdx = idx
		} else if idx, ok := headers[strings.ToLower(c.Column)]; ok {
			c.resolvedColIdx = idx
		}
	}
	// Pre-lowercase target for LIKE
	if c.Operator == OpLike {
		c.lowerTarget = strings.ToLower(c.resolvedTarget)
	}
	for i := range c.Children {
		c.Children[i].ResolveColumns(headers)
	}
}

// EvaluateFast checks if a row matches using pre-resolved column indices.
// Zero allocations per call — works directly on the []string cols slice.
func (c *Condition) EvaluateFast(cols []string) bool {
	switch c.Operator {
	case "AND":
		for i := range c.Children {
			if !c.Children[i].EvaluateFast(cols) {
				return false
			}
		}
		return true
	case "OR":
		for i := range c.Children {
			if c.Children[i].EvaluateFast(cols) {
				return true
			}
		}
		return false
	}

	// Leaf nodes
	idx := c.resolvedColIdx
	var val string
	exists := idx >= 0 && idx < len(cols)
	if exists {
		val = cols[idx]
	}

	switch c.Operator {
	case OpIsNull:
		return !exists || val == "" || val == "NULL"
	case OpIsNotNull:
		return exists && val != "" && val != "NULL"
	}

	if !exists {
		return false
	}

	target := c.resolvedTarget

	switch c.Operator {
	case OpEq:
		return val == target
	case OpNeq:
		return val != target
	case OpGt:
		return val > target
	case OpLt:
		return val < target
	case OpGte:
		return val >= target
	case OpLte:
		return val <= target
	case OpLike:
		return strings.Contains(strings.ToLower(val), c.lowerTarget)
	}

	return false
}

// ExtractBestIndexKey finds the best single equality condition for legacy single-column search
func (c *Condition) ExtractBestIndexKey() (string, string, bool) {
	conds := c.ExtractIndexConditions()
	for k, v := range conds {
		return k, v, true
	}
	return "", "", false
}

// ExtractIndexConditions finds all top-level equality conditions to use for composite indexing
func (c *Condition) ExtractIndexConditions() map[string]string {
	res := make(map[string]string)
	switch c.Operator {
	case "AND":
		for _, child := range c.Children {
			if child.Operator == OpEq {
				res[child.Column] = fmt.Sprintf("%v", child.Value)
			}
		}
	case OpEq:
		res[c.Column] = fmt.Sprintf("%v", c.Value)
	}
	return res
}

// ParseCondition parses the where JSON into a Condition tree
func ParseCondition(data []byte) (*Condition, error) {
	if len(data) == 0 || string(data) == "{}" || string(data) == "[]" {
		return nil, nil
	}
	// 1. Try simple map[string]interface{} (legacy, lenient)
	var simpleMap map[string]interface{}
	if err := json.Unmarshal(data, &simpleMap); err == nil && len(simpleMap) > 0 {
		// Check if it's actually the complex structure 'operator'
		_, hasOp := simpleMap["operator"]
		if !hasOp {
			// Convert simple map to logic: AND(Eq, Eq, ...)
			root := &Condition{
				Operator: "AND",
				Children: make([]Condition, 0, len(simpleMap)),
			}
			for col, val := range simpleMap {
				// Convert val to string
				valStr := fmt.Sprintf("%v", val)
				root.Children = append(root.Children, Condition{
					Operator: OpEq,
					Column:   strings.ToLower(col),
					Value:    valStr,
				})
			}
			root.resolveTargets()
			return root, nil
		}
	}

	// 2. Try complex struct
	var complexCond Condition
	if err := json.Unmarshal(data, &complexCond); err == nil {
		if complexCond.Operator != "" {
			complexCond.resolveTargets()
			return &complexCond, nil
		}
	}

	// Fallback or error
	return nil, fmt.Errorf("invalid where format")
}
