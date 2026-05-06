package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/apache/paimon/paimon-go"
	"github.com/apache/paimon/paimon-go/read"
)

func main() {
	warehouse := flag.String("warehouse", "", "Warehouse path, local or gs://bucket/path  (required)")
	database := flag.String("database", "default", "Database name")
	table := flag.String("table", "", "Table name  (required)")
	limit := flag.Int64("limit", 0, "Stop after this many rows (0 = no limit)")
	credsFile := flag.String("gcs-creds", "", "Path to GCS service-account JSON key file (GCS only)")
	flag.Parse()

	if *warehouse == "" || *table == "" {
		flag.Usage()
		log.Fatal("--warehouse and --table are required")
	}

	ctx := context.Background()

	// Build catalog options.
	opts := paimon.Options{Warehouse: *warehouse}
	if *credsFile != "" {
		opts.FileIOOptions = append(opts.FileIOOptions, paimon.WithCredentialsFile(*credsFile))
	}

	cat, err := paimon.NewCatalog(ctx, opts)
	if err != nil {
		log.Fatalf("open catalog: %v", err)
	}
	defer cat.Close()

	tbl, err := cat.GetTable(ctx, *database, *table)
	if err != nil {
		log.Fatalf("get table %s.%s: %v", *database, *table, err)
	}

	rb := read.NewReadBuilder(tbl)
	if *limit > 0 {
		rb = rb.WithLimit(*limit)
	}

	plan, err := rb.NewScan().Plan(ctx)
	if err != nil {
		log.Fatalf("plan: %v", err)
	}

	fmt.Printf("snapshot:  %d\n", plan.SnapshotID)
	fmt.Printf("splits:    %d\n", len(plan.Splits))

	reader, err := rb.NewRead().ToArrowReader(ctx, plan.Splits)
	if err != nil {
		log.Fatalf("open reader: %v", err)
	}
	defer reader.Release()

	// Print schema.
	schema := reader.Schema()
	fmt.Printf("schema:    %s\n\n", formatSchema(schema))

	var totalRows int64
	for reader.Next() {
		batch := reader.RecordBatch()

		nRows := batch.NumRows()
		for row := int64(0); row < nRows; row++ {
			if *limit > 0 && totalRows >= *limit {
				break
			}
			fmt.Println(formatRow(batch, row))
			totalRows++
		}
		batch.Release()

		if *limit > 0 && totalRows >= *limit {
			break
		}
	}
	if err := reader.Err(); err != nil {
		log.Fatalf("read error: %v", err)
	}

	fmt.Printf("\n%d row(s)\n", totalRows)
}

// formatSchema returns a compact one-line schema description.
func formatSchema(s *arrow.Schema) string {
	parts := make([]string, s.NumFields())
	for i := 0; i < s.NumFields(); i++ {
		f := s.Field(i)
		nullable := ""
		if f.Nullable {
			nullable = "?"
		}
		parts[i] = fmt.Sprintf("%s:%s%s", f.Name, f.Type, nullable)
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

// formatRow returns a single row as "col=value  col=value ..." text.
func formatRow(batch arrow.RecordBatch, row int64) string {
	schema := batch.Schema()
	parts := make([]string, batch.NumCols())
	for col := 0; col < int(batch.NumCols()); col++ {
		name := schema.Field(col).Name
		val := formatValue(batch.Column(col), row)
		parts[col] = fmt.Sprintf("%s=%s", name, val)
	}
	return strings.Join(parts, "  ")
}

// formatValue extracts a single cell value as a string.
func formatValue(col arrow.Array, row int64) string {
	if col.IsNull(int(row)) {
		return "NULL"
	}
	switch c := col.(type) {
	case *array.Boolean:
		return fmt.Sprintf("%v", c.Value(int(row)))
	case *array.Int8:
		return fmt.Sprintf("%d", c.Value(int(row)))
	case *array.Int16:
		return fmt.Sprintf("%d", c.Value(int(row)))
	case *array.Int32:
		return fmt.Sprintf("%d", c.Value(int(row)))
	case *array.Int64:
		return fmt.Sprintf("%d", c.Value(int(row)))
	case *array.Uint8:
		return fmt.Sprintf("%d", c.Value(int(row)))
	case *array.Uint16:
		return fmt.Sprintf("%d", c.Value(int(row)))
	case *array.Uint32:
		return fmt.Sprintf("%d", c.Value(int(row)))
	case *array.Uint64:
		return fmt.Sprintf("%d", c.Value(int(row)))
	case *array.Float32:
		return fmt.Sprintf("%g", c.Value(int(row)))
	case *array.Float64:
		return fmt.Sprintf("%g", c.Value(int(row)))
	case *array.String:
		return c.Value(int(row))
	case *array.LargeString:
		return c.Value(int(row))
	case *array.Binary:
		return fmt.Sprintf("<binary %d bytes>", len(c.Value(int(row))))
	case *array.Date32:
		return c.Value(int(row)).ToTime().Format("2006-01-02")
	case *array.Date64:
		return c.Value(int(row)).ToTime().Format("2006-01-02")
	case *array.Timestamp:
		dt := col.DataType().(*arrow.TimestampType)
		t := c.Value(int(row)).ToTime(dt.Unit)
		return t.UTC().Format("2006-01-02T15:04:05.999999Z")
	case *array.Decimal128:
		dt := col.DataType().(*arrow.Decimal128Type)
		return c.Value(int(row)).ToString(dt.Scale)
	default:
		return fmt.Sprintf("%v", col)
	}
}
