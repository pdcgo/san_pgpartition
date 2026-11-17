package san_pgpartition

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"
)

// waitForLockWaiters polls pg_locks until want backends wait for a lock, and
// fails the test if they never do.
func waitForLockWaiters(t *testing.T, db *gorm.DB, want int64) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for {
		var waiting int64
		err := db.
			Raw("SELECT count(*) FROM pg_catalog.pg_locks WHERE NOT granted").
			Row().
			Scan(&waiting)
		if err != nil {
			t.Fatalf("count waiting locks: %v", err)
		}
		if waiting == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d backends wait for a lock, want %d", waiting, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Three callers converting the same table: the per-table lock makes the
// second and third wait for the first, then find its registry row.
func TestManagerPartitionExistTableConcurrentCallersConvertOnce(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)

	// Create the registry first, so only the per-table lock is in play.
	warmup := createLogTable(t, db, schema, "warmup")
	err := m.PartitionExistTable(ctx, warmup, jakartaConfig(t, RangeTypeMonthly))
	if err != nil {
		t.Fatalf("warm-up PartitionExistTable: %v", err)
	}

	logs := createLogTable(t, db, schema, "logs")
	insertLog(t, db, logs, 1, "2026-10-31 23:30+07")
	insertLog(t, db, logs, 2, "2026-11-01 00:10+07")

	// Park every caller behind a lock on the table, so they race for real
	// once it is released instead of running one after another.
	holder := db.Begin()
	t.Cleanup(func() {
		holder.Rollback()
	})
	mustExec(t, holder, "LOCK TABLE "+logs.quoted()+" IN ACCESS EXCLUSIVE MODE")

	const callers = 3
	errs := make([]error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = m.PartitionExistTable(ctx, logs, jakartaConfig(t, RangeTypeMonthly))
		}(i)
	}

	waitForLockWaiters(t, db, callers)
	err = holder.Commit().Error
	if err != nil {
		t.Fatalf("release the table: %v", err)
	}
	wg.Wait()

	converted := 0
	for i, err := range errs {
		switch {
		case err == nil:
			converted++
		case errors.Is(err, ErrAlreadyPartitioned):
		default:
			t.Fatalf("caller %d: %v", i, err)
		}
	}
	if converted != 1 {
		t.Fatalf("%d callers converted the table, want exactly 1 (errors: %v)", converted, errs)
	}
	if got := countRows(t, db, logs); got != 2 {
		t.Fatalf("the table holds %d rows, want 2", got)
	}
	if got := len(registryRows(t, db, m, logs)); got != 1 {
		t.Fatalf("%d registry rows, want 1", got)
	}
}

// Three first calls of a fresh manager, each on its own table, all create
// the registry: the registry lock lets exactly one create it.
func TestManagerFirstCallsCreateTheRegistryOnce(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)

	tables := []TableName{
		createLogTable(t, db, schema, "logs_a"),
		createLogTable(t, db, schema, "logs_b"),
		createLogTable(t, db, schema, "logs_c"),
	}

	holder := db.Begin()
	t.Cleanup(func() {
		holder.Rollback()
	})
	for _, table := range tables {
		mustExec(t, holder, "LOCK TABLE "+table.quoted()+" IN ACCESS EXCLUSIVE MODE")
	}

	errs := make([]error, len(tables))
	var wg sync.WaitGroup
	for i, table := range tables {
		wg.Add(1)
		go func(i int, table TableName) {
			defer wg.Done()
			errs[i] = m.PartitionExistTable(ctx, table, jakartaConfig(t, RangeTypeMonthly))
		}(i, table)
	}

	waitForLockWaiters(t, db, int64(len(tables)))
	err := holder.Commit().Error
	if err != nil {
		t.Fatalf("release the tables: %v", err)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("%s: %v", tables[i], err)
		}
	}
	for _, table := range tables {
		if got := len(registryRows(t, db, m, table)); got != 1 {
			t.Fatalf("%s: %d registry rows, want 1", table, got)
		}
	}
}

// insertRow inserts like the app, from its own connection, and reports the
// new id; usable from a goroutine.
func insertRow(ctx context.Context, db *gorm.DB, table TableName, createdAt string) (int64, error) {
	var id int64
	err := db.
		WithContext(ctx).
		Raw("INSERT INTO "+table.quoted()+" (team_id, created_at) VALUES (1, ?::timestamptz) RETURNING id", createdAt).
		Row().
		Scan(&id)
	return id, err
}

// An app insert issued while the conversion is past the renames waits, then
// lands in the new partitioned table — nothing written meanwhile is lost.
func TestManagerPartitionExistTableAppInsertDuringTheCopyLandsInTheNewTable(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)

	logs := createLogTable(t, db, schema, "logs")
	insertLog(t, db, logs, 1, "2026-10-31 23:30+07")
	insertLog(t, db, logs, 2, "2026-11-01 00:10+07")

	paused := make(chan struct{})
	resume := make(chan struct{})
	m.hooks.afterRenames = func() {
		close(paused)
		<-resume
	}

	converted := make(chan error, 1)
	go func() {
		converted <- m.PartitionExistTable(ctx, logs, jakartaConfig(t, RangeTypeMonthly))
	}()
	select {
	case <-paused:
	case err := <-converted:
		t.Fatalf("the conversion ended before the renames hook: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("the conversion never reached the renames")
	}

	type inserted struct {
		id  int64
		err error
	}
	app := make(chan inserted, 1)
	go func() {
		id, err := insertRow(ctx, db, logs, "2026-11-15 10:00+07")
		app <- inserted{id: id, err: err}
	}()

	waitForLockWaiters(t, db, 1)
	select {
	case r := <-app:
		t.Fatalf("the app insert did not wait for the conversion: %d, %v", r.id, r.err)
	default:
	}

	close(resume)
	err := <-converted
	if err != nil {
		t.Fatalf("PartitionExistTable: %v", err)
	}
	r := <-app
	if r.err != nil {
		t.Fatalf("the app insert: %v", r.err)
	}

	if got := partitionOfRow(t, db, logs, r.id); got != "logs_202611" {
		t.Fatalf("the app's row is in %s, want logs_202611", got)
	}
	var inOld int64
	err = db.
		Raw("SELECT count(*) FROM "+TableName{Schema: schema, Name: "logs_old"}.quoted()+" WHERE id = ?", r.id).
		Row().
		Scan(&inOld)
	if err != nil {
		t.Fatalf("look for the row in logs_old: %v", err)
	}
	if inOld != 0 {
		t.Fatal("the app's row went into logs_old")
	}
	if got := countRows(t, db, logs); got != 3 {
		t.Fatalf("the new table holds %d rows, want 3", got)
	}
}

// DDL on the table issued while the conversion reads it (here CREATE INDEX)
// waits for the conversion and then applies to the new table — it is never
// left behind on <t>_old.
func TestManagerPartitionExistTableHoldsOffDDLWhileItReadsTheTable(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)

	logs := createLogTable(t, db, schema, "logs")
	insertLog(t, db, logs, 1, "2026-10-31 23:30+07")

	paused := make(chan struct{})
	resume := make(chan struct{})
	m.hooks.afterChecks = func() {
		close(paused)
		<-resume
	}

	converted := make(chan error, 1)
	go func() {
		converted <- m.PartitionExistTable(ctx, logs, jakartaConfig(t, RangeTypeMonthly))
	}()
	select {
	case <-paused:
	case err := <-converted:
		t.Fatalf("the conversion ended before the checks hook: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("the conversion never finished its checks")
	}

	ddl := make(chan error, 1)
	go func() {
		ddl <- db.
			WithContext(ctx).
			Exec("CREATE INDEX logs_late_idx ON " + logs.quoted() + " (note)").
			Error
	}()

	// Either the index waits for the conversion, or (without the lock) it
	// is built at once on the table the conversion has already read.
	deadline := time.Now().Add(10 * time.Second)
	var ddlErr error
	ddlDone := false
	for !ddlDone {
		var waiting int64
		err := db.
			Raw("SELECT count(*) FROM pg_catalog.pg_locks WHERE NOT granted").
			Row().
			Scan(&waiting)
		if err != nil {
			t.Fatalf("count waiting locks: %v", err)
		}
		if waiting == 1 {
			break
		}
		select {
		case ddlErr = <-ddl:
			ddlDone = true
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("CREATE INDEX neither waited nor finished")
		}
		time.Sleep(20 * time.Millisecond)
	}

	close(resume)
	err := <-converted
	if err != nil {
		t.Fatalf("PartitionExistTable: %v", err)
	}
	if !ddlDone {
		ddlErr = <-ddl
	}
	if ddlErr != nil {
		t.Fatalf("CREATE INDEX: %v", ddlErr)
	}

	if _, ok := indexDefs(t, db, logs)["logs_late_idx"]; !ok {
		t.Fatalf("the new table lacks logs_late_idx; logs_old has %v", indexNames(t, db, TableName{Schema: schema, Name: "logs_old"}))
	}
}
