package san_pgpartition

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"
)

// createEventTable creates a log-shaped table whose key may be NULL (no
// primary key to extend), so DEFAULT can hold NULL and infinite keys too.
func createEventTable(t *testing.T, db *gorm.DB, schema, name string) TableName {
	t.Helper()

	events := TableName{Schema: schema, Name: name}
	mustExec(t, db, "CREATE TABLE "+events.quoted()+" (id bigserial, note text, created_at timestamptz)")
	mustExec(t, db, "CREATE INDEX "+quoteIdentifier(name+"_created_at_idx")+" ON "+events.quoted()+" (created_at)")
	return events
}

// insertEvent inserts one row with created_at given as SQL text (NULL for
// nil) and returns its id.
func insertEvent(t *testing.T, db *gorm.DB, events TableName, createdAt any) int64 {
	t.Helper()

	var id int64
	err := db.
		Raw("INSERT INTO "+events.quoted()+" (note, created_at) VALUES (?, ?::timestamptz) RETURNING id", fmt.Sprint(createdAt), createdAt).
		Row().
		Scan(&id)
	if err != nil {
		t.Fatalf("insert an event at %v: %v", createdAt, err)
	}
	return id
}

func mustEqualReports(t *testing.T, got, want []MaintainReport) {
	t.Helper()

	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("reports =\n  %v\nwant\n  %v", got, want)
	}
}

func TestManagerMaintainPartitionRefusesBadInput(t *testing.T) {
	var nilManager *Manager
	_, err := nilManager.MaintainPartition(context.Background())
	if err == nil {
		t.Fatal("a nil manager maintained")
	}

	_, err = NewManager(dummyDB(), "").MaintainPartition(context.Background())
	if !errors.Is(err, ErrInvalidTableName) {
		t.Fatalf("an empty schemaconf = %v, want ErrInvalidTableName", err)
	}
}

func TestManagerMaintainPartitionWithoutARegistryDoesNothing(t *testing.T) {
	db := openTestDB(t)
	m := newTestManager(t, db)

	reports, err := m.MaintainPartition(context.Background())
	if err != nil || reports != nil {
		t.Fatalf("MaintainPartition = %v, %v; want nil, nil", reports, err)
	}
	if relkind(t, db, m.registry()) != "" {
		t.Fatal("MaintainPartition created the registry")
	}
}

// A monthly table with children for October and November 2026, maintained
// on 2027-01-10 after the cron missed December and January: DEFAULT rows
// from the oldest child (October) to the horizon (the end of January) are
// moved, each month's child created; the rest stay and are counted.
func TestManagerMaintainPartitionMovesRowsBetweenTheOldestChildAndTheHorizon(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)
	jakarta := mustLoadLocation(t, "Asia/Jakarta")
	m.now = clockAt(t, jakarta, 2026, time.November, 17, 9)

	events := createEventTable(t, db, schema, "events")
	insertEvent(t, db, events, "2026-10-05 08:00+07")
	insertEvent(t, db, events, "2026-11-05 08:00+07")
	err := m.PartitionExistTable(ctx, events, jakartaConfig(t, RangeTypeMonthly))
	if err != nil {
		t.Fatalf("PartitionExistTable: %v", err)
	}

	m.now = clockAt(t, jakarta, 2027, time.January, 10, 9)
	moved := map[int64]string{
		insertEvent(t, db, events, "2026-12-05 10:00+07"):    "events_202612",
		insertEvent(t, db, events, "2027-01-01 00:00+07"):    "events_202701",
		insertEvent(t, db, events, "2027-01-31 23:59:59+07"): "events_202701",
	}
	var left []int64
	for _, at := range []any{
		"2026-09-30 23:59:59+07", // before the oldest child
		"2019-03-10 10:00+07",    // a back-dated import
		"2027-02-01 00:00+07",    // past the horizon
		"2062-11-01 10:00+07",    // a typo
		nil,                      // no key
		"infinity",
		"-infinity",
	} {
		left = append(left, insertEvent(t, db, events, at))
	}

	reports, err := m.MaintainPartition(ctx)
	if err != nil {
		t.Fatalf("MaintainPartition: %v", err)
	}
	mustEqualReports(t, reports, []MaintainReport{{Table: events, Moved: 3, LeftInDefault: 7}})

	mustEqualStrings(t, "partitions", partitionNames(t, db, events), []string{"events_202610", "events_202611", "events_202612", "events_202701", "events_default"})
	for id, want := range moved {
		if got := partitionOfRow(t, db, events, id); got != want {
			t.Fatalf("row %d is in %s, want %s", id, got, want)
		}
	}
	for _, id := range left {
		if got := partitionOfRow(t, db, events, id); got != "events_default" {
			t.Fatalf("row %d is in %s, want events_default", id, got)
		}
	}
	mustHaveBound(t, db, TableName{Schema: schema, Name: "events_202612"}, "2026-12-01 00:00+07", "2027-01-01 00:00+07", "timestamptz")

	// Twice: nothing more to move, the same rows still reported.
	before := partitionsOf(t, db, events)
	reports, err = m.MaintainPartition(ctx)
	if err != nil {
		t.Fatalf("second MaintainPartition: %v", err)
	}
	mustEqualReports(t, reports, []MaintainReport{{Table: events, Moved: 0, LeftInDefault: 7}})
	mustEqualStrings(t, "partitions after the second call", partitionsOf(t, db, events), before)
}

// The horizon is the newest child's end when provisioning reached past the
// current period: a DEFAULT row in a gap before it is moved.
func TestManagerMaintainPartitionHorizonIsTheNewestChildsEnd(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)
	jakarta := mustLoadLocation(t, "Asia/Jakarta")
	m.now = clockAt(t, jakarta, 2026, time.November, 17, 9)

	events := createEventTable(t, db, schema, "events")
	insertEvent(t, db, events, "2026-10-05 08:00+07")
	err := m.PartitionExistTable(ctx, events, jakartaConfig(t, RangeTypeMonthly))
	if err != nil {
		t.Fatalf("PartitionExistTable: %v", err)
	}
	err = m.MaintainPartitionTableProvision(ctx, 4)
	if err != nil {
		t.Fatalf("MaintainPartitionTableProvision: %v", err)
	}
	// February 2027's child undone, then a row for it arrives in DEFAULT.
	dropped, err := PartitionDropTable(ctx, db, events, TableName{Schema: schema, Name: "events_202702"})
	if err != nil || !dropped {
		t.Fatalf("PartitionDropTable(events_202702) = %v, %v", dropped, err)
	}
	february := insertEvent(t, db, events, "2027-02-14 10:00+07")
	insertEvent(t, db, events, "2027-04-01 00:00+07") // past the newest child, events_202703

	reports, err := m.MaintainPartition(ctx)
	if err != nil {
		t.Fatalf("MaintainPartition: %v", err)
	}
	mustEqualReports(t, reports, []MaintainReport{{Table: events, Moved: 1, LeftInDefault: 1}})
	if got := partitionOfRow(t, db, events, february); got != "events_202702" {
		t.Fatalf("the February row is in %s, want events_202702", got)
	}
}

// A table with no children at all: only the current period's rows move.
func TestManagerMaintainPartitionWithoutChildren(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)
	jakarta := mustLoadLocation(t, "Asia/Jakarta")
	m.now = clockAt(t, jakarta, 2026, time.November, 17, 9)

	events := createEventTable(t, db, schema, "events")
	err := m.PartitionExistTable(ctx, events, jakartaConfig(t, RangeTypeMonthly))
	if err != nil {
		t.Fatalf("PartitionExistTable: %v", err)
	}
	items := TableName{Schema: schema, Name: "items"}
	mustExec(t, db, "CREATE TABLE "+items.quoted()+" (id bigserial PRIMARY KEY, note text)")
	err = m.PartitionExistTable(ctx, items, &PartitionRangeConfig{RangeType: FixedRange, BlockSize: 4, Key: "id"})
	if err != nil {
		t.Fatalf("PartitionExistTable(items): %v", err)
	}

	november := insertEvent(t, db, events, "2026-11-20 10:00+07")
	insertEvent(t, db, events, "2026-10-05 10:00+07")
	mustExec(t, db, "INSERT INTO "+items.quoted()+" (note) VALUES ('a'), ('b')")

	reports, err := m.MaintainPartition(ctx)
	if err != nil {
		t.Fatalf("MaintainPartition: %v", err)
	}
	mustEqualReports(t, reports, []MaintainReport{
		{Table: events, Moved: 1, LeftInDefault: 1},
		{Table: items, Moved: 0, LeftInDefault: 2},
	})
	if got := partitionOfRow(t, db, events, november); got != "events_202611" {
		t.Fatalf("the November row is in %s, want events_202611", got)
	}
	mustEqualStrings(t, "items partitions", partitionNames(t, db, items), []string{"items_default"})
}

// FixedRange: rows of a missing block between the oldest child and the
// newest child's end move; rows below or above stay.
func TestManagerMaintainPartitionFixed(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)

	items := TableName{Schema: schema, Name: "items"}
	mustExec(t, db, "CREATE TABLE "+items.quoted()+" (id bigint PRIMARY KEY, note text)")
	mustExec(t, db, "INSERT INTO "+items.quoted()+" (id, note) VALUES (1, 'a'), (2, 'b'), (9, 'c')")
	err := m.PartitionExistTable(ctx, items, &PartitionRangeConfig{RangeType: FixedRange, BlockSize: 4, Key: "id"})
	if err != nil {
		t.Fatalf("PartitionExistTable: %v", err)
	}
	dropped, err := PartitionDropTable(ctx, db, items, TableName{Schema: schema, Name: "items_4_7"})
	if err != nil || !dropped {
		t.Fatalf("PartitionDropTable(items_4_7) = %v, %v", dropped, err)
	}
	mustExec(t, db, "INSERT INTO "+items.quoted()+" (id, note) VALUES (5, 'gap'), (6, 'gap'), (100, 'above'), (-3, 'below')")

	reports, err := m.MaintainPartition(ctx)
	if err != nil {
		t.Fatalf("MaintainPartition: %v", err)
	}
	mustEqualReports(t, reports, []MaintainReport{{Table: items, Moved: 2, LeftInDefault: 2}})
	mustEqualStrings(t, "partitions", partitionsOf(t, db, items), []string{
		"items_0_3 FOR VALUES FROM ('0') TO ('4')",
		"items_4_7 FOR VALUES FROM ('4') TO ('8')",
		"items_8_11 FOR VALUES FROM ('8') TO ('12')",
		"items_default DEFAULT",
	})
	if got := countRows(t, db, TableName{Schema: schema, Name: "items_4_7"}); got != 2 {
		t.Fatalf("items_4_7 holds %d rows, want 5 and 6", got)
	}
}

// One report per registered table, zeros included, sorted by schema and then
// name.
func TestManagerMaintainPartitionReportsEveryTableSorted(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	first := createTestSchema(t, db)
	second := createTestSchema(t, db)
	m := newTestManager(t, db)

	var tables []TableName
	for _, table := range []TableName{{Schema: second, Name: "alpha"}, {Schema: first, Name: "zeta"}, {Schema: first, Name: "beta"}} {
		events := createEventTable(t, db, table.Schema, table.Name)
		err := m.PartitionExistTable(ctx, events, jakartaConfig(t, RangeTypeMonthly))
		if err != nil {
			t.Fatalf("PartitionExistTable(%s): %v", events, err)
		}
		tables = append(tables, events)
	}

	reports, err := m.MaintainPartition(ctx)
	if err != nil {
		t.Fatalf("MaintainPartition: %v", err)
	}
	mustEqualReports(t, reports, []MaintainReport{{Table: tables[2]}, {Table: tables[1]}, {Table: tables[0]}})
}

// A registered table that was dropped since: its error comes back, naming
// it, and the tables after it are still maintained and reported.
func TestManagerMaintainPartitionReportsABrokenTableAndGoesOn(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)
	jakarta := mustLoadLocation(t, "Asia/Jakarta")
	m.now = clockAt(t, jakarta, 2026, time.November, 17, 9)

	a, b := convertTwo(t, db, m, schema)
	mustExec(t, db, "DROP TABLE "+TableName{Schema: schema, Name: "logs_a_old"}.quoted()+", "+a.quoted())
	insertLog(t, db, b, 1, "2026-11-20 10:00+07")

	reports, err := m.MaintainPartition(ctx)
	if !errors.Is(err, ErrParentNotPartitioned) {
		t.Fatalf("MaintainPartition = %v, want ErrParentNotPartitioned for %s", err, a)
	}
	mustContain(t, err.Error(), a.String())
	mustEqualReports(t, reports, []MaintainReport{{Table: b, Moved: 1, LeftInDefault: 0}})
}

// Two crons maintaining the same table at once: the per-table lock makes the
// second wait and then find nothing left to move.
func TestManagerMaintainPartitionConcurrentCallersMoveOnce(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)
	jakarta := mustLoadLocation(t, "Asia/Jakarta")
	m.now = clockAt(t, jakarta, 2026, time.November, 17, 9)

	logs, _ := convertedOctober(t, db, m, schema)

	holder := db.Begin()
	t.Cleanup(func() {
		holder.Rollback()
	})
	mustExec(t, holder, "LOCK TABLE "+logs.quoted()+" IN ACCESS EXCLUSIVE MODE")

	const callers = 2
	reports := make([][]MaintainReport, callers)
	errs := make([]error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			reports[i], errs[i] = m.MaintainPartition(ctx)
		}(i)
	}

	waitForLockWaiters(t, db, callers)
	err := holder.Commit().Error
	if err != nil {
		t.Fatalf("release the table: %v", err)
	}
	wg.Wait()

	total := int64(0)
	for i := range errs {
		if errs[i] != nil {
			t.Fatalf("caller %d: %v", i, errs[i])
		}
		total += reports[i][0].Moved
	}
	// November's two rows; December's (past the horizon, the end of
	// November) and the 2019 row stay.
	if total != 2 {
		t.Fatalf("the callers moved %d rows together, want 2 (reports %v)", total, reports)
	}
	mustEqualStrings(t, "partitions", partitionNames(t, db, logs), []string{"logs_202610", "logs_202611", "logs_default"})
}
