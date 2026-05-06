package read

import (
	"context"

	"github.com/apache/paimon/paimon-go/fileio"
	"github.com/apache/paimon/paimon-go/internal/binaryrow"
	"github.com/apache/paimon/paimon-go/manifest"
	"github.com/apache/paimon/paimon-go/schema"
	"github.com/apache/paimon/paimon-go/snapshot"
)

// Table is the interface that ReadBuilder and TableScan depend on.
// *table.FileStoreTable satisfies this interface.
type Table interface {
	LatestSnapshot(ctx context.Context) (*snapshot.Snapshot, error)
	GetSchema() *schema.TableSchema
	ManifestDir() string
	GetIO() fileio.FileIO
	DataFilePath(partition *binaryrow.BinaryRow, partFields []schema.DataField, bucket int, fileName string) string
}

// ManifestReader reads manifest-list and manifest-entry Avro files.
// The real implementation wraps the free functions in the manifest package.
type ManifestReader interface {
	ReadList(ctx context.Context, filename string, partFields []schema.DataField) ([]manifest.ManifestFileMeta, error)
	ReadAllEntries(ctx context.Context, metas []manifest.ManifestFileMeta, partFields []schema.DataField, valueFields []schema.DataField) ([]manifest.ManifestEntry, error)
}

// defaultManifestReader is the real implementation that delegates to the manifest package.
type defaultManifestReader struct {
	dir string
	io  fileio.FileIO
}

func newDefaultManifestReader(dir string, io fileio.FileIO) ManifestReader {
	return &defaultManifestReader{dir: dir, io: io}
}

func (r *defaultManifestReader) ReadList(ctx context.Context, filename string, partFields []schema.DataField) ([]manifest.ManifestFileMeta, error) {
	return manifest.ReadManifestList(ctx, r.io, r.dir, filename, partFields)
}

func (r *defaultManifestReader) ReadAllEntries(ctx context.Context, metas []manifest.ManifestFileMeta, partFields []schema.DataField, valueFields []schema.DataField) ([]manifest.ManifestEntry, error) {
	return manifest.ReadAllEntries(ctx, r.io, r.dir, metas, partFields, valueFields)
}
