// Package predicate provides filter predicates for Paimon table scans.
//
// Predicates are used at two levels:
//  1. Stats-based pruning: applied against SimpleStats (min/max/null-count per file)
//     to skip entire data files during planning.
//  2. Row-level filtering: applied in the read path (pushed down to the Parquet reader).
package predicate

import (
	"fmt"

	"github.com/apache/paimon/paimon-go/internal/binaryrow"
	"github.com/apache/paimon/paimon-go/manifest"
	"github.com/apache/paimon/paimon-go/schema"
)

// Op is a predicate operator.
type Op int

const (
	OpEqual          Op = iota
	OpNotEqual
	OpLessThan
	OpLessOrEqual
	OpGreaterThan
	OpGreaterOrEqual
	OpIsNull
	OpIsNotNull
	OpIn
	OpAnd
	OpOr
	OpNot
)

// Predicate is a filter expression over a table's fields.
type Predicate struct {
	Op       Op
	// For leaf predicates: field index into the projected schema and the comparison value(s).
	FieldIdx int
	FieldName string
	// TypeTag as returned by schema.TypeTag (used for BinaryRow decoding).
	TypeTag  string
	Literals []interface{}
	// For AND / OR / NOT: child predicates.
	Children []*Predicate
}

// And combines predicates with AND.
func And(preds ...*Predicate) *Predicate {
	return &Predicate{Op: OpAnd, Children: preds}
}

// Or combines predicates with OR.
func Or(preds ...*Predicate) *Predicate {
	return &Predicate{Op: OpOr, Children: preds}
}

// Not negates a predicate.
func Not(p *Predicate) *Predicate {
	return &Predicate{Op: OpNot, Children: []*Predicate{p}}
}

// Builder constructs leaf predicates against a given schema.
type Builder struct {
	fields []schema.DataField
	// index map: field name → position in fields slice
	index  map[string]int
}

// NewBuilder creates a PredicateBuilder for the given fields.
func NewBuilder(fields []schema.DataField) *Builder {
	idx := make(map[string]int, len(fields))
	for i, f := range fields {
		idx[f.Name] = i
	}
	return &Builder{fields: fields, index: idx}
}

func (b *Builder) field(name string) (int, schema.DataField, error) {
	i, ok := b.index[name]
	if !ok {
		return 0, schema.DataField{}, fmt.Errorf("predicate: unknown field %q", name)
	}
	return i, b.fields[i], nil
}

func (b *Builder) leaf(op Op, name string, literals ...interface{}) (*Predicate, error) {
	i, f, err := b.field(name)
	if err != nil {
		return nil, err
	}
	return &Predicate{
		Op:        op,
		FieldIdx:  i,
		FieldName: name,
		TypeTag:   schema.TypeTag(f.Type),
		Literals:  literals,
	}, nil
}

// Equal builds an equality predicate.
func (b *Builder) Equal(name string, value interface{}) (*Predicate, error) {
	return b.leaf(OpEqual, name, value)
}

// NotEqual builds a not-equal predicate.
func (b *Builder) NotEqual(name string, value interface{}) (*Predicate, error) {
	return b.leaf(OpNotEqual, name, value)
}

// LessThan builds a < predicate.
func (b *Builder) LessThan(name string, value interface{}) (*Predicate, error) {
	return b.leaf(OpLessThan, name, value)
}

// LessOrEqual builds a <= predicate.
func (b *Builder) LessOrEqual(name string, value interface{}) (*Predicate, error) {
	return b.leaf(OpLessOrEqual, name, value)
}

// GreaterThan builds a > predicate.
func (b *Builder) GreaterThan(name string, value interface{}) (*Predicate, error) {
	return b.leaf(OpGreaterThan, name, value)
}

// GreaterOrEqual builds a >= predicate.
func (b *Builder) GreaterOrEqual(name string, value interface{}) (*Predicate, error) {
	return b.leaf(OpGreaterOrEqual, name, value)
}

// IsNull builds an IS NULL predicate.
func (b *Builder) IsNull(name string) (*Predicate, error) {
	return b.leaf(OpIsNull, name)
}

// IsNotNull builds an IS NOT NULL predicate.
func (b *Builder) IsNotNull(name string) (*Predicate, error) {
	return b.leaf(OpIsNotNull, name)
}

// In builds an IN predicate.
func (b *Builder) In(name string, values ...interface{}) (*Predicate, error) {
	return b.leaf(OpIn, name, values...)
}

// --- Stats-based file pruning ---

// TestByStats returns false if the predicate is guaranteed NOT to match any row
// in a file with the given stats. Returns true if the file may contain matching rows.
func TestByStats(p *Predicate, stats manifest.SimpleStats, rowCount int64) bool {
	if p == nil {
		return true
	}
	switch p.Op {
	case OpAnd:
		for _, c := range p.Children {
			if !TestByStats(c, stats, rowCount) {
				return false
			}
		}
		return true
	case OpOr:
		for _, c := range p.Children {
			if TestByStats(c, stats, rowCount) {
				return true
			}
		}
		return false
	case OpNot:
		// Conservative: can't negate stats-based checks safely; keep the file.
		return true
	}

	// Leaf predicate
	idx := p.FieldIdx

	// Null count check
	var nullCount *int64
	if stats.NullCounts != nil && idx < len(stats.NullCounts) {
		if nc, ok := stats.NullCounts[idx].(int64); ok {
			nullCount = &nc
		}
	}

	if p.Op == OpIsNull {
		return nullCount == nil || *nullCount > 0
	}
	if p.Op == OpIsNotNull {
		return nullCount == nil || rowCount == 0 || *nullCount < rowCount
	}

	// Get min/max from BinaryRow stats
	minVal := getStatField(stats.MinValues, idx, p.TypeTag)
	maxVal := getStatField(stats.MaxValues, idx, p.TypeTag)
	if minVal == nil || maxVal == nil {
		return true // no stats — keep file
	}
	if nullCount != nil && rowCount > 0 && *nullCount == rowCount {
		return true // all nulls — keep file conservatively
	}

	switch p.Op {
	case OpEqual:
		if len(p.Literals) == 0 {
			return true
		}
		cmpMin := compare(p.Literals[0], minVal)
		cmpMax := compare(p.Literals[0], maxVal)
		return cmpMin >= 0 && cmpMax <= 0
	case OpNotEqual:
		if len(p.Literals) == 0 {
			return true
		}
		// Only skip if all values equal the literal (min == max == literal)
		return !(compare(minVal, maxVal) == 0 && compare(p.Literals[0], minVal) == 0)
	case OpLessThan:
		if len(p.Literals) == 0 {
			return true
		}
		return compare(p.Literals[0], minVal) > 0
	case OpLessOrEqual:
		if len(p.Literals) == 0 {
			return true
		}
		return compare(p.Literals[0], minVal) >= 0
	case OpGreaterThan:
		if len(p.Literals) == 0 {
			return true
		}
		return compare(p.Literals[0], maxVal) < 0
	case OpGreaterOrEqual:
		if len(p.Literals) == 0 {
			return true
		}
		return compare(p.Literals[0], maxVal) <= 0
	case OpIn:
		for _, lit := range p.Literals {
			cmpMin := compare(lit, minVal)
			cmpMax := compare(lit, maxVal)
			if cmpMin >= 0 && cmpMax <= 0 {
				return true
			}
		}
		return false
	}
	return true
}

func getStatField(row *binaryrow.BinaryRow, idx int, typeTag string) interface{} {
	if row == nil || row.IsNull(idx) {
		return nil
	}
	v, err := binaryrow.GetField(row, idx, typeTag)
	if err != nil {
		return nil
	}
	return v
}

// compare performs a generic comparison between two values.
// Returns negative if a < b, 0 if a == b, positive if a > b.
func compare(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	switch av := a.(type) {
	case int8:
		bv, _ := toComparableInt(b)
		return compareInt64(int64(av), bv)
	case int16:
		bv, _ := toComparableInt(b)
		return compareInt64(int64(av), bv)
	case int32:
		bv, _ := toComparableInt(b)
		return compareInt64(int64(av), bv)
	case int64:
		bv, _ := toComparableInt(b)
		return compareInt64(av, bv)
	case int:
		bv, _ := toComparableInt(b)
		return compareInt64(int64(av), bv)
	case float32:
		bv, _ := toComparableFloat(b)
		if float64(av) < bv {
			return -1
		}
		if float64(av) > bv {
			return 1
		}
		return 0
	case float64:
		bv, _ := toComparableFloat(b)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case string:
		bv, _ := b.(string)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	}
	return 0
}

func compareInt64(a, b int64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func toComparableInt(v interface{}) (int64, bool) {
	switch x := v.(type) {
	case int:
		return int64(x), true
	case int8:
		return int64(x), true
	case int16:
		return int64(x), true
	case int32:
		return int64(x), true
	case int64:
		return x, true
	case float64:
		return int64(x), true
	}
	return 0, false
}

func toComparableFloat(v interface{}) (float64, bool) {
	switch x := v.(type) {
	case float32:
		return float64(x), true
	case float64:
		return x, true
	case int64:
		return float64(x), true
	}
	return 0, false
}

// WithProjection rebinds predicate leaf FieldIdx values to match a projected field slice.
// Fields not present in the projected set are removed (predicate becomes nil for that leaf).
func WithProjection(p *Predicate, projectedFields []schema.DataField) *Predicate {
	if p == nil {
		return nil
	}
	idx := make(map[string]int, len(projectedFields))
	for i, f := range projectedFields {
		idx[f.Name] = i
	}
	return rebind(p, idx)
}

func rebind(p *Predicate, idx map[string]int) *Predicate {
	if p == nil {
		return nil
	}
	switch p.Op {
	case OpAnd, OpOr, OpNot:
		children := make([]*Predicate, 0, len(p.Children))
		for _, c := range p.Children {
			rc := rebind(c, idx)
			if rc != nil {
				children = append(children, rc)
			}
		}
		if len(children) == 0 {
			return nil
		}
		return &Predicate{Op: p.Op, Children: children}
	default:
		newIdx, ok := idx[p.FieldName]
		if !ok {
			return nil // field not in projected schema — drop this leaf
		}
		clone := *p
		clone.FieldIdx = newIdx
		return &clone
	}
}
