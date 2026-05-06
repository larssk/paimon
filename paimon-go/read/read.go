package read

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/apache/paimon/paimon-go/internal/binaryrow"
	"github.com/apache/paimon/paimon-go/manifest"
	"github.com/apache/paimon/paimon-go/schema"
)


// TableRead executes the read of splits and produces Arrow data.
type TableRead struct {
	rb *ReadBuilder
}

// ToArrowReader returns an array.RecordReader that streams record batches from all splits.
// The caller is responsible for calling Release() on each record and Release() on the reader.
func (tr *TableRead) ToArrowReader(ctx context.Context, splits []DataSplit) (array.RecordReader, error) {
	readFields := tr.rb.readFields()
	arrowSchema, err := schema.ToArrowSchema(readFields)
	if err != nil {
		return nil, fmt.Errorf("read: build arrow schema: %w", err)
	}

	return &splitRecordReader{
		ctx:         ctx,
		tr:          tr,
		splits:      splits,
		readFields:  readFields,
		arrowSchema: arrowSchema,
		splitIdx:    0,
		fileIdx:     0,
		alloc:       memory.NewGoAllocator(),
	}, nil
}

// ToArrow reads all splits and returns a single arrow.Table.
func (tr *TableRead) ToArrow(ctx context.Context, splits []DataSplit) (arrow.Table, error) {
	rr, err := tr.ToArrowReader(ctx, splits)
	if err != nil {
		return nil, err
	}
	defer rr.Release()

	var records []arrow.RecordBatch
	for rr.Next() {
		rec := rr.RecordBatch()
		rec.Retain()
		records = append(records, rec)
	}
	if err := rr.Err(); err != nil {
		return nil, err
	}
	if len(records) == 0 {
		s := rr.Schema()
		return array.NewTableFromRecords(s, nil), nil
	}
	defer func() {
		for _, r := range records {
			r.Release()
		}
	}()
	return array.NewTableFromRecords(rr.Schema(), records), nil
}

// splitRecordReader implements array.RecordReader over a list of DataSplits.
type splitRecordReader struct {
	ctx         context.Context
	tr          *TableRead
	splits      []DataSplit
	readFields  []schema.DataField
	arrowSchema *arrow.Schema
	alloc       memory.Allocator

	splitIdx int
	fileIdx  int

	current    arrow.RecordBatch
	currentErr error
	fileRdr    *pqarrow.FileReader
	rdrClose   io.Closer
	rowRdr     array.RecordReader
}

func (r *splitRecordReader) Schema() *arrow.Schema { return r.arrowSchema }

func (r *splitRecordReader) Retain()  {}
func (r *splitRecordReader) Release() {
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	r.closeCurrentFile()
}

func (r *splitRecordReader) RecordBatch() arrow.RecordBatch { return r.current }

// Record is deprecated but required by the interface; delegates to RecordBatch.
func (r *splitRecordReader) Record() arrow.RecordBatch { return r.current }
func (r *splitRecordReader) Err() error                { return r.currentErr }

func (r *splitRecordReader) Next() bool {
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}

	for {
		// Try to get the next batch from the current file reader.
		if r.rowRdr != nil {
			if r.rowRdr.Next() {
				rec := r.rowRdr.RecordBatch()
				// Project to read schema (handles missing columns as nulls).
				projected, err := r.projectRecord(rec)
				if err != nil {
					r.currentErr = err
					return false
				}
				r.current = projected
				return true
			}
			r.closeCurrentFile()
		}

		// Advance to the next file.
		if r.splitIdx >= len(r.splits) {
			return false
		}
		split := r.splits[r.splitIdx]
		if r.fileIdx >= len(split.Files) {
			r.splitIdx++
			r.fileIdx = 0
			continue
		}
		fm := split.Files[r.fileIdx]
		r.fileIdx++

		if err := r.openFile(split, fm); err != nil {
			r.currentErr = err
			return false
		}
	}
}

func (r *splitRecordReader) openFile(split DataSplit, fm manifest.DataFileMeta) error {
	tbl := r.tr.rb.tbl
	var filePath string
	if fm.ExternalPath != nil {
		filePath = *fm.ExternalPath
	} else {
		var partRow *binaryrow.BinaryRow
		if split.Partition != nil {
			partRow = split.Partition.Partition
		}
		filePath = tbl.DataFilePath(
			partRow,
			tbl.GetSchema().PartitionFields(),
			split.Bucket,
			fm.FileName,
		)
	}

	if !isParquet(filePath) {
		return fmt.Errorf("read: unsupported file format for %q (only parquet supported in v1)", filePath)
	}

	rc, err := tbl.GetIO().Open(r.ctx, filePath)
	if err != nil {
		return fmt.Errorf("read: open %s: %w", filePath, err)
	}

	// pqarrow requires a parquet.ReaderAtSeeker. Wrap the ReadCloser.
	ras, err := toReaderAtSeeker(rc)
	if err != nil {
		rc.Close()
		return fmt.Errorf("read: wrap reader for %s: %w", filePath, err)
	}
	r.rdrClose = rc

	pqReader, err := file.NewParquetReader(ras)
	if err != nil {
		rc.Close()
		return fmt.Errorf("read: open parquet %s: %w", filePath, err)
	}

	// Build column indices for projection.
	colIndices := r.parquetColumnIndices(pqReader)

	arrowReader, err := pqarrow.NewFileReader(pqReader, pqarrow.ArrowReadProperties{
		BatchSize: 65536,
	}, r.alloc)
	if err != nil {
		rc.Close()
		return fmt.Errorf("read: create arrow reader for %s: %w", filePath, err)
	}

	rowRdr, err := arrowReader.GetRecordReader(r.ctx, colIndices, nil)
	if err != nil {
		rc.Close()
		return fmt.Errorf("read: get record reader for %s: %w", filePath, err)
	}

	r.fileRdr = arrowReader
	r.rowRdr = rowRdr
	return nil
}

// parquetColumnIndices maps the read fields to Parquet column indices.
// Fields not present in the file are included as -1 (handled in projectRecord).
func (r *splitRecordReader) parquetColumnIndices(pqReader *file.Reader) []int {
	parquetSchema := pqReader.MetaData().Schema
	nameToCol := make(map[string]int, parquetSchema.NumColumns())
	for i := 0; i < parquetSchema.NumColumns(); i++ {
		nameToCol[parquetSchema.Column(i).Name()] = i
	}
	indices := make([]int, 0, len(r.readFields))
	for _, f := range r.readFields {
		if ci, ok := nameToCol[f.Name]; ok {
			indices = append(indices, ci)
		}
	}
	return indices
}

// projectRecord re-orders and/or pads a record to match the read schema exactly.
// Columns present in the file but not the read schema are dropped.
// Columns in the read schema but missing from the file are filled with null arrays.
// When a column's physical type is compatible but not identical (e.g. timestamp with
// vs without timezone), the column data is reused and the schema field is overridden
// to match what the file actually contains, avoiding Arrow type-mismatch panics.
func (r *splitRecordReader) projectRecord(rec arrow.RecordBatch) (arrow.RecordBatch, error) {
	if rec.Schema().Equal(r.arrowSchema) {
		rec.Retain()
		return rec, nil
	}

	fileSchema := rec.Schema()
	cols := make([]arrow.Array, len(r.readFields))
	// Build the effective schema: use the file's field type when it differs from
	// the read schema but is physically compatible (same base type + unit).
	effectiveFields := make([]arrow.Field, len(r.readFields))
	n := rec.NumRows()

	for i, f := range r.readFields {
		wantField, err := schema.ToArrowField(f)
		if err != nil {
			return nil, err
		}
		colIdx := fileSchema.FieldIndices(f.Name)
		if len(colIdx) > 0 {
			col := rec.Column(colIdx[0])
			col.Retain()
			cols[i] = col
			// Use the file's actual field type to avoid type-mismatch panics.
			fileField := fileSchema.Field(colIdx[0])
			if arrowTypesCompatible(fileField.Type, wantField.Type) {
				effectiveFields[i] = fileField
				effectiveFields[i].Name = f.Name // keep our name
			} else {
				effectiveFields[i] = wantField
			}
		} else {
			// Field missing from file: synthesize a null array.
			nullArr := array.MakeArrayOfNull(r.alloc, wantField.Type, int(n))
			cols[i] = nullArr
			effectiveFields[i] = wantField
		}
	}
	defer func() {
		for _, c := range cols {
			if c != nil {
				c.Release()
			}
		}
	}()

	effectiveSchema := arrow.NewSchema(effectiveFields, nil)
	result := array.NewRecord(effectiveSchema, cols, n)
	return result, nil
}

// arrowTypesCompatible returns true when two Arrow types have the same physical
// representation and can be safely used interchangeably (e.g. timestamp[us] vs
// timestamp[us, tz=UTC] — same unit, different timezone annotation).
func arrowTypesCompatible(a, b arrow.DataType) bool {
	if a.ID() != b.ID() {
		return false
	}
	switch at := a.(type) {
	case *arrow.TimestampType:
		bt, ok := b.(*arrow.TimestampType)
		return ok && at.Unit == bt.Unit
	}
	return arrow.TypeEqual(a, b)
}

func (r *splitRecordReader) closeCurrentFile() {
	if r.rowRdr != nil {
		r.rowRdr.Release()
		r.rowRdr = nil
	}
	r.fileRdr = nil
	if r.rdrClose != nil {
		r.rdrClose.Close()
		r.rdrClose = nil
	}
}

// isParquet returns true if the file name indicates a Parquet file.
func isParquet(name string) bool {
	lower := strings.ToLower(name)
	return strings.HasSuffix(lower, ".parquet") ||
		strings.HasSuffix(lower, ".snappy.parquet") ||
		strings.HasSuffix(lower, ".gz.parquet") ||
		strings.HasSuffix(lower, ".zstd.parquet") ||
		strings.HasSuffix(lower, ".lz4.parquet") ||
		// Paimon sometimes uses just ".parquet" or "-{uuid}.parquet"
		strings.Contains(lower, ".parquet")
}

// toReaderAtSeeker wraps an io.ReadCloser into a parquet ReaderAtSeeker by reading all bytes.
// For streaming GCS / local files we buffer into memory; for large files a seekable wrapper
// would be preferable, but this keeps the implementation simple for v1.
type memReaderAt struct {
	data []byte
	pos  int64
}

func toReaderAtSeeker(rc io.ReadCloser) (*memReaderAt, error) {
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	return &memReaderAt{data: data}, nil
}

func (m *memReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(m.data)) {
		return 0, io.EOF
	}
	n := copy(p, m.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (m *memReaderAt) Read(p []byte) (int, error) {
	if m.pos >= int64(len(m.data)) {
		return 0, io.EOF
	}
	n := copy(p, m.data[m.pos:])
	m.pos += int64(n)
	return n, nil
}

func (m *memReaderAt) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = m.pos + offset
	case io.SeekEnd:
		abs = int64(len(m.data)) + offset
	default:
		return 0, fmt.Errorf("memReaderAt: invalid whence %d", whence)
	}
	if abs < 0 {
		return 0, fmt.Errorf("memReaderAt: negative seek")
	}
	m.pos = abs
	return abs, nil
}

// ensure unused import is used
