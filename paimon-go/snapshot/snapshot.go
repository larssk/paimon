// Package snapshot reads Paimon snapshot metadata.
package snapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/apache/paimon/paimon-go/fileio"
	"github.com/apache/paimon/paimon-go/internal/pathutil"
)

// CommitKind matches Paimon's CommitKind enum.
type CommitKind string

const (
	CommitAppend    CommitKind = "APPEND"
	CommitOverwrite CommitKind = "OVERWRITE"
	CommitCompact   CommitKind = "COMPACT"
	CommitAnalyze   CommitKind = "ANALYZE"
)

// Snapshot represents a Paimon snapshot (stored as JSON at snapshot/<id>).
type Snapshot struct {
	Version                int        `json:"version"`
	ID                     int64      `json:"id"`
	SchemaID               int64      `json:"schemaId"`
	BaseManifestList       string     `json:"baseManifestList"`
	DeltaManifestList      string     `json:"deltaManifestList"`
	TotalRecordCount       int64      `json:"totalRecordCount"`
	DeltaRecordCount       int64      `json:"deltaRecordCount"`
	CommitUser             string     `json:"commitUser"`
	CommitIdentifier       int64      `json:"commitIdentifier"`
	CommitKind             CommitKind `json:"commitKind"`
	TimeMillis             int64      `json:"timeMillis"`
	ChangelogManifestList  *string    `json:"changelogManifestList,omitempty"`
	IndexManifest          *string    `json:"indexManifest,omitempty"`
	ChangelogRecordCount   *int64     `json:"changelogRecordCount,omitempty"`
	Watermark              *int64     `json:"watermark,omitempty"`
	NextRowID              *int64     `json:"nextRowId,omitempty"`
	Statistics             *string    `json:"statistics,omitempty"`
}

// Manager resolves snapshots from a table's snapshot/ directory.
type Manager struct {
	tableRoot string
	io        fileio.FileIO
}

// NewManager creates a SnapshotManager for the given table root path.
func NewManager(tableRoot string, io fileio.FileIO) *Manager {
	return &Manager{tableRoot: tableRoot, io: io}
}

const snapshotPrefix = "snapshot-"

func (m *Manager) snapshotDir() string {
	return pathutil.Join(m.tableRoot, "snapshot")
}

// Latest returns the snapshot with the highest ID.
func (m *Manager) Latest(ctx context.Context) (*Snapshot, error) {
	entries, err := m.io.List(ctx, m.snapshotDir())
	if err != nil {
		return nil, fmt.Errorf("snapshot: list dir: %w", err)
	}

	var ids []int64
	for _, e := range entries {
		if e.IsDir {
			continue
		}
		name := pathutil.Base(e.Path)
		if !strings.HasPrefix(name, snapshotPrefix) {
			continue
		}
		id, err := strconv.ParseInt(name[len(snapshotPrefix):], 10, 64)
		if err != nil {
			continue
		}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return nil, fmt.Errorf("snapshot: no snapshots found in %s", m.snapshotDir())
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return m.Read(ctx, ids[len(ids)-1])
}

// Read loads a specific snapshot by ID.
func (m *Manager) Read(ctx context.Context, id int64) (*Snapshot, error) {
	p := pathutil.Join(m.snapshotDir(), snapshotPrefix+strconv.FormatInt(id, 10))
	data, err := m.io.ReadAll(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("snapshot: read %d: %w", id, err)
	}
	var s Snapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("snapshot: parse %d: %w", id, err)
	}
	return &s, nil
}

// EarliestID returns the smallest snapshot ID available, or -1 if none.
func (m *Manager) EarliestID(ctx context.Context) (int64, error) {
	// Paimon may write an "EARLIEST" hint file; fall back to listing.
	hintPath := pathutil.Join(m.snapshotDir(), "EARLIEST")
	if data, err := m.io.ReadAll(ctx, hintPath); err == nil {
		id, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
		if err == nil {
			return id, nil
		}
	}
	entries, err := m.io.List(ctx, m.snapshotDir())
	if err != nil {
		return -1, err
	}
	var min int64 = -1
	for _, e := range entries {
		name := pathutil.Base(e.Path)
		if !strings.HasPrefix(name, snapshotPrefix) {
			continue
		}
		id, err := strconv.ParseInt(name[len(snapshotPrefix):], 10, 64)
		if err != nil {
			continue
		}
		if min < 0 || id < min {
			min = id
		}
	}
	return min, nil
}
