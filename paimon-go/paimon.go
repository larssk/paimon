// Package paimon provides a minimalistic Go reader for Apache Paimon tables.
//
// # Quick start
//
//	cat, err := paimon.NewCatalog(ctx, paimon.Options{
//	    Warehouse: "/path/to/warehouse",
//	})
//	tbl, err := cat.GetTable(ctx, "mydb", "mytable")
//
//	rb := read.NewReadBuilder(tbl).
//	    WithProjection([]string{"event_time", "user_id", "amount"})
//
//	plan, err := rb.NewScan().Plan(ctx)
//	reader, err := rb.NewRead().ToArrowReader(ctx, plan.Splits)
//	defer reader.Release()
//	for reader.Next() {
//	    rec := reader.Record()
//	    // use rec ...
//	}
package paimon

import (
	"context"

	"github.com/apache/paimon/paimon-go/catalog"
	"github.com/apache/paimon/paimon-go/fileio"
)

// Options configures a Paimon catalog.
type Options = catalog.Options

// NewCatalog creates a Catalog from the provided options.
// This is the main entry point for reading Paimon tables.
//
// Example — local filesystem:
//
//	cat, err := paimon.NewCatalog(ctx, paimon.Options{Warehouse: "/data/warehouse"})
//
// Example — GCS:
//
//	cat, err := paimon.NewCatalog(ctx, paimon.Options{
//	    Warehouse:     "gs://my-bucket/warehouse",
//	    FileIOOptions: []fileio.Option{fileio.WithCredentialsFile("/sa.json")},
//	})
func NewCatalog(ctx context.Context, opts Options) (catalog.Catalog, error) {
	return catalog.New(ctx, opts)
}

// Re-export fileio.Option so callers don't need to import fileio directly.
type FileIOOption = fileio.Option

// WithCredentialsFile is a convenience re-export.
var WithCredentialsFile = fileio.WithCredentialsFile

// WithCredentialsJSON is a convenience re-export.
var WithCredentialsJSON = fileio.WithCredentialsJSON
