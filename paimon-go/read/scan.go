// Package read provides the ReadBuilder, TableScan, Plan, and TableRead pipeline.
package read

import (
	"context"
	"fmt"

	"github.com/apache/paimon/paimon-go/manifest"
	"github.com/apache/paimon/paimon-go/predicate"
	"github.com/apache/paimon/paimon-go/schema"
	"github.com/apache/paimon/paimon-go/table"
)

// DataSplit represents a unit of work: a set of data files from one bucket/partition
// that should be read together.
type DataSplit struct {
	Partition *manifest.ManifestEntry // representative entry for partition info
	Bucket    int
	Files     []manifest.DataFileMeta
}

// Plan is the output of TableScan.Plan(): a list of splits ready to be read.
type Plan struct {
	Splits     []DataSplit
	SnapshotID int64
}

// ReadBuilder is the entry point for building a table scan + read pipeline.
type ReadBuilder struct {
	tbl            Table
	manifestReader ManifestReader
	filter         *predicate.Predicate
	projection     []string // nil = all columns
	limit          int64    // 0 = no limit
}

// NewReadBuilder creates a ReadBuilder for the given table.
func NewReadBuilder(tbl *table.FileStoreTable) *ReadBuilder {
	return &ReadBuilder{
		tbl:            tbl,
		manifestReader: newDefaultManifestReader(tbl.ManifestDir(), tbl.GetIO()),
	}
}

// newReadBuilderFromIface creates a ReadBuilder using the Table interface directly.
// Used internally and in tests.
func newReadBuilderFromIface(tbl Table, mr ManifestReader) *ReadBuilder {
	return &ReadBuilder{tbl: tbl, manifestReader: mr}
}

// WithFilter attaches a filter predicate.
func (rb *ReadBuilder) WithFilter(p *predicate.Predicate) *ReadBuilder {
	rb.filter = p
	return rb
}

// WithProjection limits the columns returned. Pass column names.
func (rb *ReadBuilder) WithProjection(cols []string) *ReadBuilder {
	rb.projection = cols
	return rb
}

// WithLimit sets a row limit hint (used for early exit, not guaranteed ordering).
func (rb *ReadBuilder) WithLimit(n int64) *ReadBuilder {
	rb.limit = n
	return rb
}

// NewPredicateBuilder returns a PredicateBuilder scoped to the (possibly projected) schema.
func (rb *ReadBuilder) NewPredicateBuilder() *predicate.Builder {
	return predicate.NewBuilder(rb.readFields())
}

// NewScan returns a TableScan configured with this builder's settings.
func (rb *ReadBuilder) NewScan() *TableScan {
	return &TableScan{rb: rb}
}

// NewRead returns a TableRead configured with this builder's settings.
func (rb *ReadBuilder) NewRead() *TableRead {
	return &TableRead{rb: rb}
}

// readFields returns the effective read schema (all fields or projected subset).
func (rb *ReadBuilder) readFields() []schema.DataField {
	s := rb.tbl.GetSchema()
	if len(rb.projection) == 0 {
		return s.Fields
	}
	var fields []schema.DataField
	for _, name := range rb.projection {
		if f, ok := s.FieldByName(name); ok {
			fields = append(fields, f)
		}
	}
	return fields
}

// --- TableScan ---

// TableScan resolves a snapshot into a Plan of DataSplits.
type TableScan struct {
	rb *ReadBuilder
}

// Plan resolves the latest snapshot, reads all manifest files, prunes by stats,
// and returns a Plan of DataSplits ready for reading.
func (ts *TableScan) Plan(ctx context.Context) (*Plan, error) {
	tbl := ts.rb.tbl
	mr := ts.rb.manifestReader
	s := tbl.GetSchema()

	snap, err := tbl.LatestSnapshot(ctx)
	if err != nil {
		return nil, fmt.Errorf("scan: latest snapshot: %w", err)
	}

	partFields := s.PartitionFields()
	valueFields := s.Fields

	// Read base + delta manifest lists.
	baseList, err := mr.ReadList(ctx, snap.BaseManifestList, partFields)
	if err != nil {
		return nil, fmt.Errorf("scan: base manifest list: %w", err)
	}
	deltaList, err := mr.ReadList(ctx, snap.DeltaManifestList, partFields)
	if err != nil {
		return nil, fmt.Errorf("scan: delta manifest list: %w", err)
	}

	allMeta := append(baseList, deltaList...)

	// Prune manifest files by partition stats.
	prunedMeta := ts.pruneManifestFiles(allMeta)

	// Read all manifest entries in parallel.
	entries, err := mr.ReadAllEntries(ctx, prunedMeta, partFields, valueFields)
	if err != nil {
		return nil, fmt.Errorf("scan: read manifest entries: %w", err)
	}

	// Prune individual files by value stats.
	entries = ts.pruneEntries(entries)

	// Build splits: group by bucket.
	splits := ts.buildSplits(entries)

	return &Plan{Splits: splits, SnapshotID: snap.ID}, nil
}

func (ts *TableScan) pruneManifestFiles(metas []manifest.ManifestFileMeta) []manifest.ManifestFileMeta {
	if ts.rb.filter == nil {
		return metas
	}
	result := metas[:0]
	for _, m := range metas {
		totalFiles := m.NumAddedFiles + m.NumDeletedFiles
		if predicate.TestByStats(ts.rb.filter, m.PartitionStats, totalFiles) {
			result = append(result, m)
		}
	}
	return result
}

func (ts *TableScan) pruneEntries(entries []manifest.ManifestEntry) []manifest.ManifestEntry {
	if ts.rb.filter == nil {
		return entries
	}
	result := entries[:0]
	for _, e := range entries {
		// Rebind predicate to value field indices for this file's stats.
		p := predicate.WithProjection(ts.rb.filter, ts.rb.tbl.GetSchema().Fields)
		if predicate.TestByStats(p, e.File.ValueStats, e.File.RowCount) {
			result = append(result, e)
		}
	}
	return result
}

func (ts *TableScan) buildSplits(entries []manifest.ManifestEntry) []DataSplit {
	// Group by bucket key: "partition_path/bucket-N"
	type key struct {
		partPath string
		bucket   int
	}
	groups := make(map[key][]manifest.ManifestEntry)
	for _, e := range entries {
		k := key{partPath: partitionKey(e), bucket: e.Bucket}
		groups[k] = append(groups[k], e)
	}

	splits := make([]DataSplit, 0, len(groups))
	for _, group := range groups {
		files := make([]manifest.DataFileMeta, 0, len(group))
		for _, e := range group {
			files = append(files, e.File)
		}
		splits = append(splits, DataSplit{
			Partition: &group[0],
			Bucket:    group[0].Bucket,
			Files:     files,
		})
	}
	return splits
}

// partitionKey returns a string key for grouping entries by partition.
// For unpartitioned tables all entries share the same (empty) key.
func partitionKey(e manifest.ManifestEntry) string {
	if e.Partition == nil {
		return ""
	}
	// Use a simple arity+bucket combo as a cheap key.
	return fmt.Sprintf("arity=%d", e.Partition.Arity())
}
