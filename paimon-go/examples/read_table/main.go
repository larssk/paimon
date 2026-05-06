package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/apache/paimon/paimon-go"
	"github.com/apache/paimon/paimon-go/predicate"
	"github.com/apache/paimon/paimon-go/read"
	"github.com/apache/paimon/paimon-go/table"
)

func main() {
	warehouse := flag.String("warehouse", "", "Warehouse path, local or gs://bucket/path  (required)")
	database  := flag.String("database", "default", "Database name")
	tblName   := flag.String("table", "", "Table name  (required)")
	limit     := flag.Int64("limit", 0, "Stop after this many rows (0 = no limit)")
	credsFile := flag.String("gcs-creds", "", "Path to GCS service-account JSON key file (GCS only)")

	filterCol := flag.String("filter-col", "", "Column name to filter on")
	filterOp  := flag.String("filter-op", "eq", "Filter operator: eq, ne, lt, le, gt, ge, in")
	filterVal := flag.String("filter-val", "", "Filter value; comma-separated list for 'in'")

	stream       := flag.Bool("stream", false, "Enable continuous stream mode (polls for new snapshots)")
	pollInterval := flag.Duration("poll", 2*time.Second, "Poll interval in stream mode (e.g. 5s, 500ms)")
	fromEarliest := flag.Bool("from-earliest", false, "In stream mode, replay all existing snapshots first")
	flag.Parse()

	if *warehouse == "" || *tblName == "" {
		flag.Usage()
		log.Fatal("--warehouse and --table are required")
	}
	if (*filterCol == "") != (*filterVal == "") {
		log.Fatal("--filter-col and --filter-val must be used together")
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

	tbl, err := cat.GetTable(ctx, *database, *tblName)
	if err != nil {
		log.Fatalf("get table %s.%s: %v", *database, *tblName, err)
	}

	rb := read.NewReadBuilder(tbl)
	if *limit > 0 {
		rb = rb.WithLimit(*limit)
	}

	var filter *predicate.Predicate
	if *filterCol != "" {
		p, err := buildFilter(tbl, *filterCol, *filterOp, *filterVal)
		if err != nil {
			log.Fatalf("build filter: %v", err)
		}
		filter = p
		rb = rb.WithFilter(p)
		fmt.Printf("filter:    %s %s %s\n", *filterCol, *filterOp, *filterVal)
	}

	if *stream {
		runStream(ctx, tbl, filter, *pollInterval, *fromEarliest)
		return
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
	arrowSchema := reader.Schema()
	fmt.Printf("schema:    %s\n\n", formatSchema(arrowSchema))

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

// runStream runs a continuous stream read, printing each new row as it arrives.
// It blocks until the context is cancelled (Ctrl-C).
func runStream(ctx context.Context, tbl *table.FileStoreTable, filter *predicate.Predicate, poll time.Duration, fromEarliest bool) {
	sb := read.NewStreamReadBuilder(tbl).WithPollInterval(poll)
	if filter != nil {
		sb = sb.WithFilter(filter)
	}
	if fromEarliest {
		sb = sb.WithStartingFrom(read.StartingFromEarliest)
		fmt.Println("mode:      stream (from-earliest)")
	} else {
		fmt.Println("mode:      stream (from-latest)")
	}
	fmt.Printf("poll:      %s\n", poll)

	// Handle Ctrl-C gracefully.
	ctx, cancel := withSignal(ctx)
	defer cancel()

	rdr := read.NewStreamReader(ctx, sb)
	defer rdr.Release()

	fmt.Printf("schema:    %s\n\n", formatSchema(rdr.Schema()))

	var totalRows int64
	for rdr.Next() {
		batch := rdr.RecordBatch()
		snapID := rdr.SnapshotID()
		nRows := batch.NumRows()
		for row := int64(0); row < nRows; row++ {
			fmt.Printf("[snap=%d] %s\n", snapID, formatRow(batch, row))
			totalRows++
		}
		batch.Release()
	}
	if err := rdr.Err(); err != nil && err != context.Canceled {
		log.Fatalf("stream error: %v", err)
	}
	fmt.Printf("\n%d row(s) received\n", totalRows)
}

// buildFilter constructs a Predicate from the --filter-* flags.
func buildFilter(tbl *table.FileStoreTable, col, op, val string) (*predicate.Predicate, error) {
	f, ok := tbl.Schema.FieldByName(col)
	if !ok {
		return nil, fmt.Errorf("column %q not found in schema", col)
	}

	pb := predicate.NewBuilder(tbl.Schema.Fields)
	baseType := strings.ToUpper(strings.SplitN(f.Type.Type, "(", 2)[0])
	baseType = strings.TrimSuffix(baseType, " NOT NULL")

	if op == "in" {
		parts := strings.Split(val, ",")
		vals := make([]interface{}, 0, len(parts))
		for _, raw := range parts {
			v, err := parseFilterValue(baseType, strings.TrimSpace(raw))
			if err != nil {
				return nil, err
			}
			vals = append(vals, v)
		}
		return pb.In(col, vals...)
	}

	typed, err := parseFilterValue(baseType, val)
	if err != nil {
		return nil, err
	}

	switch op {
	case "eq":
		return pb.Equal(col, typed)
	case "ne":
		return pb.NotEqual(col, typed)
	case "lt":
		return pb.LessThan(col, typed)
	case "le":
		return pb.LessOrEqual(col, typed)
	case "gt":
		return pb.GreaterThan(col, typed)
	case "ge":
		return pb.GreaterOrEqual(col, typed)
	default:
		return nil, fmt.Errorf("unknown operator %q; valid: eq, ne, lt, le, gt, ge, in", op)
	}
}

// parseFilterValue coerces a CLI string to the Go type that the predicate stats
// engine uses for the given Paimon base type.
func parseFilterValue(paimonType, raw string) (interface{}, error) {
	switch paimonType {
	case "TINYINT":
		v, err := strconv.ParseInt(raw, 10, 8)
		return int8(v), err
	case "SMALLINT":
		v, err := strconv.ParseInt(raw, 10, 16)
		return int16(v), err
	case "INT":
		v, err := strconv.ParseInt(raw, 10, 32)
		return int32(v), err
	case "BIGINT":
		v, err := strconv.ParseInt(raw, 10, 64)
		return v, err
	case "FLOAT":
		v, err := strconv.ParseFloat(raw, 32)
		return float32(v), err
	case "DOUBLE":
		v, err := strconv.ParseFloat(raw, 64)
		return v, err
	case "BOOLEAN":
		v, err := strconv.ParseBool(raw)
		return v, err
	case "DATE":
		// Accept YYYY-MM-DD; convert to days since Unix epoch (int32).
		t, err := time.Parse("2006-01-02", raw)
		if err != nil {
			return nil, fmt.Errorf("DATE value %q must be YYYY-MM-DD: %w", raw, err)
		}
		epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
		days := int32(t.UTC().Sub(epoch).Hours() / 24)
		return days, nil
	case "TIMESTAMP", "TIMESTAMP_LTZ":
		// Accept RFC3339 / ISO-8601 with or without timezone.
		for _, layout := range []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02T15:04:05",
			"2006-01-02 15:04:05",
			"2006-01-02",
		} {
			if t, err := time.Parse(layout, raw); err == nil {
				return t.UnixMicro(), nil
			}
		}
		return nil, fmt.Errorf("TIMESTAMP value %q: use YYYY-MM-DDTHH:MM:SS or RFC3339", raw)
	default:
		// CHAR, VARCHAR, STRING, BINARY, VARBINARY, DECIMAL, unknown — pass as string.
		return raw, nil
	}
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
	s := batch.Schema()
	parts := make([]string, batch.NumCols())
	for col := 0; col < int(batch.NumCols()); col++ {
		name := s.Field(col).Name
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

// withSignal returns a context that is cancelled on SIGINT or SIGTERM (Ctrl-C).
func withSignal(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		select {
		case <-ch:
			fmt.Fprintln(os.Stderr, "\ninterrupted")
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}


