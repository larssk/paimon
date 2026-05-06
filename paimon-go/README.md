# paimon-go

A minimalistic, pure-Go reader for [Apache Paimon](https://paimon.apache.org/) tables. No JVM required.

## Status

Early development — read-only, append-only tables, Parquet data files only.

## Features

- Filesystem catalog (local path or GCS)
- Latest snapshot resolution
- Manifest-list + manifest-entry reading (Avro)
- Partition and column statistics-based file pruning
- Column projection
- Output as Apache Arrow `RecordReader` or `arrow.Table`
- Predicate builder for filter push-down

## Not yet supported

- ORC data files (Paimon default format — requires `file.format=parquet` in table options)
- Primary-key (merge-on-read) tables
- Deletion vectors
- Schema evolution across data files
- Streaming / incremental scans
- REST catalog
- Write path

## Installation

```sh
go get github.com/apache/paimon/paimon-go
```

## Usage

### Local filesystem

```go
import (
    "context"
    "github.com/apache/paimon/paimon-go"
    "github.com/apache/paimon/paimon-go/read"
)

ctx := context.Background()

cat, err := paimon.NewCatalog(ctx, paimon.Options{
    Warehouse: "/path/to/warehouse",
})
tbl, err := cat.GetTable(ctx, "mydb", "mytable")

rb := read.NewReadBuilder(tbl).
    WithProjection([]string{"event_time", "user_id", "amount"})

plan, err := rb.NewScan().Plan(ctx)

reader, err := rb.NewRead().ToArrowReader(ctx, plan.Splits)
defer reader.Release()

for reader.Next() {
    rec := reader.RecordBatch()
    // use rec ...
    rec.Release()
}
```

### GCS

```go
cat, err := paimon.NewCatalog(ctx, paimon.Options{
    Warehouse: "gs://my-bucket/warehouse",
    FileIOOptions: []paimon.FileIOOption{
        paimon.WithCredentialsFile("/path/to/sa.json"),
    },
})
```

### With filter

```go
rb := read.NewReadBuilder(tbl)
pb := rb.NewPredicateBuilder()

filter, err := pb.GreaterOrEqual("event_date", int32(20240101))

plan, err := rb.WithFilter(filter).NewScan().Plan(ctx)
```

### Read as arrow.Table

```go
tbl, err := rb.NewRead().ToArrow(ctx, plan.Splits)
```

## Architecture

```
paimon-go/
├── paimon.go               # Entry point: NewCatalog()
├── catalog/                # FileSystemCatalog
├── fileio/                 # FileIO interface (local + GCS)
├── snapshot/               # Snapshot JSON + SnapshotManager
├── schema/                 # TableSchema JSON + Arrow type mapping
├── manifest/               # Manifest-list + manifest-entry Avro readers
├── table/                  # FileStoreTable + PathFactory
├── read/                   # ReadBuilder, TableScan, TableRead, Parquet reader
├── predicate/              # Predicate, PredicateBuilder, stats pruning
└── internal/
    └── binaryrow/          # Paimon BinaryRow binary format decoder
```

### Read pipeline

```
NewCatalog → GetTable → NewReadBuilder
    → NewScan().Plan(ctx)
        resolves latest snapshot (JSON)
        reads manifest-list (Avro) → []ManifestFileMeta
        reads manifest entries in parallel (Avro) → []ManifestEntry
        prunes files by partition + column stats (BinaryRow decoded on demand)
        returns Plan{[]DataSplit}
    → NewRead().ToArrowReader(ctx, plan.Splits)
        for each split: opens Parquet file via FileIO
        applies column projection
        fills missing columns with null arrays (schema evolution)
        yields arrow.RecordBatch chunks (65 536 rows default)
```

## Dependencies

| Package | Purpose |
|---|---|
| `github.com/apache/arrow-go/v18` | Arrow in-memory format + Parquet reader |
| `github.com/hamba/avro/v2` | Read manifest Avro files |
| `cloud.google.com/go/storage` | GCS storage backend |
