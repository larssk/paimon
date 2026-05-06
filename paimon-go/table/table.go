// Package table provides the FileStoreTable and its path factory.
package table

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/paimon/paimon-go/fileio"
	"github.com/apache/paimon/paimon-go/internal/binaryrow"
	"github.com/apache/paimon/paimon-go/internal/pathutil"
	"github.com/apache/paimon/paimon-go/schema"
	"github.com/apache/paimon/paimon-go/snapshot"
)

const defaultPartValue = "__DEFAULT_PARTITION__"

// PathFactory constructs paths within a Paimon table directory.
type PathFactory struct {
	root string
}

// NewPathFactory creates a PathFactory for the given table root path.
func NewPathFactory(root string) *PathFactory {
	return &PathFactory{root: root}
}

// Root returns the table root path.
func (p *PathFactory) Root() string { return p.root }

// SchemaDir returns the path to the schema/ directory.
func (p *PathFactory) SchemaDir() string { return pathutil.Join(p.root, "schema") }

// SnapshotDir returns the path to the snapshot/ directory.
func (p *PathFactory) SnapshotDir() string { return pathutil.Join(p.root, "snapshot") }

// ManifestDir returns the path to the manifest/ directory.
func (p *PathFactory) ManifestDir() string { return pathutil.Join(p.root, "manifest") }

// DataFilePath builds the absolute path for a data file.
//
// Layout: <root>/[partKey=val/.../]bucket-<N>/<fileName>
//
// The manifest stores only the bare filename; the full path must be reconstructed
// from the partition BinaryRow, partition field definitions, bucket number, and filename.
func (p *PathFactory) DataFilePath(
	partition *binaryrow.BinaryRow,
	partFields []schema.DataField,
	bucket int,
	fileName string,
) string {
	base := p.root
	// Append partition path segments.
	if partition != nil && len(partFields) > 0 {
		for i, f := range partFields {
			var val string
			if partition.IsNull(i) {
				val = defaultPartValue
			} else {
				v, err := binaryrow.GetField(partition, i, schema.TypeTag(f.Type))
				if err != nil || v == nil {
					val = defaultPartValue
				} else {
					val = fmt.Sprintf("%v", v)
				}
			}
			base = pathutil.Join(base, f.Name+"="+sanitizePartitionValue(val))
		}
	}
	// Append bucket directory.
	base = pathutil.Join(base, fmt.Sprintf("bucket-%d", bucket))
	// Append file name.
	return pathutil.Join(base, fileName)
}

// sanitizePartitionValue ensures the partition value is safe to use as a path segment.
func sanitizePartitionValue(v string) string {
	if strings.TrimSpace(v) == "" {
		return defaultPartValue
	}
	return v
}

// FileStoreTable is the central handle for a Paimon table.
type FileStoreTable struct {
	tableRoot string
	Schema    *schema.TableSchema
	Paths     *PathFactory
	IO        fileio.FileIO

	snapshotMgr *snapshot.Manager
	schemaMgr   *schema.Manager
}

// NewFileStoreTable opens a Paimon table at the given root path.
func NewFileStoreTable(ctx context.Context, tableRoot string, fio fileio.FileIO) (*FileStoreTable, error) {
	paths := NewPathFactory(tableRoot)
	schemaMgr := schema.NewManager(tableRoot, fio)
	snapshotMgr := snapshot.NewManager(tableRoot, fio)

	s, err := schemaMgr.Latest(ctx)
	if err != nil {
		return nil, err
	}
	return &FileStoreTable{
		tableRoot:   tableRoot,
		Schema:      s,
		Paths:       paths,
		IO:          fio,
		snapshotMgr: snapshotMgr,
		schemaMgr:   schemaMgr,
	}, nil
}

// LatestSnapshot returns the most recent snapshot.
func (t *FileStoreTable) LatestSnapshot(ctx context.Context) (*snapshot.Snapshot, error) {
	return t.snapshotMgr.Latest(ctx)
}

// SnapshotByID returns the snapshot for a specific ID (satisfies read.Table interface).
func (t *FileStoreTable) SnapshotByID(ctx context.Context, id int64) (*snapshot.Snapshot, error) {
	return t.snapshotMgr.Read(ctx, id)
}

// ListSnapshotIDs returns all available snapshot IDs in ascending order (satisfies read.Table interface).
func (t *FileStoreTable) ListSnapshotIDs(ctx context.Context) ([]int64, error) {
	return t.snapshotMgr.ListIDs(ctx)
}

// GetSchema returns the current table schema (satisfies read.Table interface).
func (t *FileStoreTable) GetSchema() *schema.TableSchema { return t.Schema }

// ManifestDir returns the manifest directory path (satisfies read.Table interface).
func (t *FileStoreTable) ManifestDir() string { return t.Paths.ManifestDir() }

// GetIO returns the FileIO (satisfies read.Table interface).
func (t *FileStoreTable) GetIO() fileio.FileIO { return t.IO }

// DataFilePath builds an absolute data file path (satisfies read.Table interface).
func (t *FileStoreTable) DataFilePath(partition *binaryrow.BinaryRow, partFields []schema.DataField, bucket int, fileName string) string {
	return t.Paths.DataFilePath(partition, partFields, bucket, fileName)
}

// SchemaForID returns the table schema for a specific schema ID.
func (t *FileStoreTable) SchemaForID(ctx context.Context, id int64) (*schema.TableSchema, error) {
	if id == t.Schema.ID {
		return t.Schema, nil
	}
	return t.schemaMgr.Read(ctx, id)
}
