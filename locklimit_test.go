package san_pgpartition

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
)

// fillLockTable takes advisory locks in one transaction of its own until
// PostgreSQL's shared lock table is full, then gives back about free·100 of
// them, so the next transaction needing more locks than that fails with
// SQLSTATE 53200 — the limit a call creating many children runs into,
// reached in a moment instead of with a thousand children. It returns the
// function that releases everything (also run at cleanup).
func fillLockTable(t *testing.T, db *gorm.DB, free int) func() {
	t.Helper()

	holder := db.Begin()
	released := false
	release := func() {
		if !released {
			released = true
			holder.Rollback()
		}
	}
	t.Cleanup(release)

	const base = int64(7_000_000_000)
	batches := 0
	for {
		mustExec(t, holder, fmt.Sprintf("SAVEPOINT fill_%d", batches))
		err := holder.
			Exec("SELECT pg_catalog.pg_advisory_xact_lock(CAST(? AS bigint) + g) FROM pg_catalog.generate_series(0, 99) AS g", base+int64(batches)*100).
			Error
		if err != nil {
			if !isLockLimit(err) {
				t.Fatalf("fill the lock table: %v", err)
			}
			mustExec(t, holder, fmt.Sprintf("ROLLBACK TO SAVEPOINT fill_%d", batches))
			break
		}
		batches++
		if batches > 100000 {
			t.Fatal("the lock table never filled")
		}
	}
	if batches <= free {
		t.Fatalf("only %d batches of locks fit", batches)
	}
	mustExec(t, holder, fmt.Sprintf("ROLLBACK TO SAVEPOINT fill_%d", batches-free))
	return release
}

// A conversion that runs out of PostgreSQL's lock table says why and what to
// do, keeps SQLSTATE 53200 reachable through errors.As, and changes nothing.
func TestManagerPartitionExistTableExplainsTheLockLimit(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)

	logs := createLogTable(t, db, schema, "logs")
	mustExec(t, db, "INSERT INTO "+logs.quoted()+` (team_id, product_id, note, created_at)
SELECT 1, g, 'note ' || g, timestamptz '2026-08-01 12:00+07' + g * interval '1 day'
FROM generate_series(0, 79) AS g`)
	// The registry first, so the conversion below is the only work.
	warmup := createLogTable(t, db, schema, "warmup")
	err := m.PartitionExistTable(ctx, warmup, jakartaConfig(t, RangeTypeMonthly))
	if err != nil {
		t.Fatalf("warm-up PartitionExistTable: %v", err)
	}
	before := snapshotTable(t, db, m, logs)

	release := fillLockTable(t, db, 3)
	err = m.PartitionExistTable(ctx, logs, jakartaConfig(t, RangeTypeDaily))
	release()

	if !isLockLimit(err) {
		t.Fatalf("PartitionExistTable = %v, want SQLSTATE 53200", err)
	}
	mustContain(t, err.Error(), "80 children")
	mustContain(t, err.Error(), "max_locks_per_transaction")
	mustContain(t, err.Error(), "RangeTypeMonthly")
	mustEqualStrings(t, "the table after the failure", snapshotTable(t, db, m, logs), before)
}

// Provisioning that runs out of the lock table explains it the same way.
func TestManagerMaintainPartitionTableProvisionExplainsTheLockLimit(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)
	jakarta := mustLoadLocation(t, "Asia/Jakarta")
	m.now = clockAt(t, jakarta, 2026, time.November, 17, 9)

	logs := createLogTable(t, db, schema, "logs")
	insertLog(t, db, logs, 1, "2026-11-17 08:00+07")
	err := m.PartitionExistTable(ctx, logs, jakartaConfig(t, RangeTypeDaily))
	if err != nil {
		t.Fatalf("PartitionExistTable: %v", err)
	}

	release := fillLockTable(t, db, 3)
	err = m.MaintainPartitionTableProvision(ctx, 90)
	release()

	if !isLockLimit(err) {
		t.Fatalf("MaintainPartitionTableProvision = %v, want SQLSTATE 53200", err)
	}
	mustContain(t, err.Error(), logs.String())
	mustContain(t, err.Error(), "90 children")
	mustContain(t, err.Error(), "max_locks_per_transaction")
	mustEqualStrings(t, "partitions after the failure", partitionNames(t, db, logs), []string{"logs_20261117", "logs_default"})
}

// MaintainPartition moving rows into many missing children explains the
// limit the same way.
func TestManagerMaintainPartitionExplainsTheLockLimit(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)
	jakarta := mustLoadLocation(t, "Asia/Jakarta")
	m.now = clockAt(t, jakarta, 2026, time.November, 17, 9)

	events := createEventTable(t, db, schema, "events")
	insertEvent(t, db, events, "2026-08-01 12:00+07")
	insertEvent(t, db, events, "2026-10-20 12:00+07")
	err := m.PartitionExistTable(ctx, events, jakartaConfig(t, RangeTypeDaily))
	if err != nil {
		t.Fatalf("PartitionExistTable: %v", err)
	}
	// Every day between the two rows loses its child, then gets a row in
	// DEFAULT: 79 children (2026-08-02 … 2026-10-19) for MaintainPartition
	// to create.
	var drop []string
	for _, name := range partitionNames(t, db, events) {
		if name != "events_20260801" && name != "events_20261020" && name != "events_default" {
			drop = append(drop, TableName{Schema: schema, Name: name}.quoted())
		}
	}
	mustExec(t, db, "DROP TABLE "+strings.Join(drop, ", "))
	mustExec(t, db, "INSERT INTO "+events.quoted()+` (note, created_at)
SELECT 'gap', timestamptz '2026-08-02 12:00+07' + g * interval '1 day' FROM generate_series(0, 78) AS g`)

	release := fillLockTable(t, db, 3)
	reports, err := m.MaintainPartition(ctx)
	release()

	if !isLockLimit(err) {
		t.Fatalf("MaintainPartition = %v, %v; want SQLSTATE 53200", reports, err)
	}
	mustContain(t, err.Error(), events.String())
	mustContain(t, err.Error(), "79 children")
	mustContain(t, err.Error(), "max_locks_per_transaction")
	if got := countRows(t, db, TableName{Schema: schema, Name: "events_default"}); got != 79 {
		t.Fatalf("events_default holds %d rows after the failure, want 79", got)
	}
}

func TestLockLimitHintLeavesOtherErrorsAlone(t *testing.T) {
	plain := errors.New("san_pgpartition: something else")
	if got := lockLimitHint(plain, 5); got != plain {
		t.Fatalf("lockLimitHint changed an unrelated error: %v", got)
	}
	if got := lockLimitHint(nil, 5); got != nil {
		t.Fatalf("lockLimitHint(nil) = %v", got)
	}
}
