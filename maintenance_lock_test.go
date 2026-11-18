package san_pgpartition

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"
)

// pauseAfterFirstChild makes m's maintenance calls pause once they created
// their first child, holding every lock they took. The returned channel is
// closed at the pause; resume lets the call go on (it also runs when the
// test ends).
func pauseAfterFirstChild(t *testing.T, m *Manager) (<-chan struct{}, func()) {
	t.Helper()

	paused := make(chan struct{})
	resumed := make(chan struct{})
	var pause, release sync.Once
	m.hooks.afterChild = func() {
		pause.Do(func() {
			close(paused)
			<-resumed
		})
	}
	resume := func() {
		release.Do(func() {
			close(resumed)
		})
	}
	t.Cleanup(resume)
	return paused, resume
}

// waitForPause waits until the call reaches its hook, failing the test if it
// ends or stalls before.
func waitForPause(t *testing.T, paused <-chan struct{}, done <-chan error) {
	t.Helper()

	select {
	case <-paused:
	case err := <-done:
		t.Fatalf("the call ended before the hook: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("the call never reached the hook")
	}
}

// waitForCall returns the call's error, failing the test if it never ends.
func waitForCall(t *testing.T, done <-chan error) error {
	t.Helper()

	select {
	case err := <-done:
		return err
	case <-time.After(30 * time.Second):
		t.Fatal("the call never finished")
	}
	return nil
}

// readWithin counts the rows of table in a transaction of its own that gives
// up on any lock after limit, and says how long the read took.
func readWithin(db *gorm.DB, table TableName, limit time.Duration) (int64, time.Duration, error) {
	var err error

	started := time.Now()
	var count int64
	err = db.
		Transaction(func(tx *gorm.DB) error {
			var txErr error

			txErr = tx.
				Exec(fmt.Sprintf("SET LOCAL lock_timeout = '%dms'", limit.Milliseconds())).
				Error
			if txErr != nil {
				return txErr
			}
			return tx.
				Raw("SELECT count(*) FROM " + table.quoted()).
				Row().
				Scan(&count)
		})
	return count, time.Since(started), err
}

// convertedUsersAndOrders converts users (threeUsers, monthly in Jakarta, on
// 2026-11-17) while a plain orders table, with an order for users 1 and 2,
// points at it ON DELETE CASCADE.
func convertedUsersAndOrders(t *testing.T, db *gorm.DB, m *Manager, schema string) (TableName, TableName) {
	t.Helper()

	users := createUsers(t, db, schema, threeUsers...)
	orders := TableName{Schema: schema, Name: "orders"}
	execIn(t, db, schema,
		"CREATE TABLE $S.orders (id BIGSERIAL PRIMARY KEY, user_id BIGINT REFERENCES $S.users (id) ON DELETE CASCADE, note TEXT)",
		"INSERT INTO $S.orders (user_id, note) VALUES (1, 'october'), (2, 'november')",
	)
	err := m.PartitionExistTable(context.Background(), users, jakartaConfig(t, RangeTypeMonthly))
	if err != nil {
		t.Fatalf("PartitionExistTable: %v", err)
	}
	return users, orders
}

// Provisioning a child DEFAULT holds no rows for locks each table pointing at
// the parent only SHARE ROW EXCLUSIVE — what PostgreSQL itself takes to
// create the child: while the call holds its locks, the app reads orders at
// once; its writes of orders wait for the commit. A DEFAULT row just past the
// planned child (it stays there) does not make the call lock harder.
func TestMaintenanceLocksLetTheAppReadAReferencingTableWhileOnlyCreating(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)
	jakarta := mustLoadLocation(t, "Asia/Jakarta")
	m.now = clockAt(t, jakarta, 2026, time.November, 17, 9)

	users, orders := convertedUsersAndOrders(t, db, m, schema)
	execIn(t, db, schema, "INSERT INTO $S.users (name, created_at) VALUES ('new year', '2027-01-01 00:00+07')")
	paused, resume := pauseAfterFirstChild(t, m)

	provisioned := make(chan error, 1)
	go func() {
		provisioned <- m.MaintainPartitionTableProvision(ctx, 1)
	}()
	waitForPause(t, paused, provisioned)

	count, took, err := readWithin(db, orders, 3*time.Second)
	if err != nil {
		t.Fatalf("the app's read of orders waited for provisioning (%s): %v", took, err)
	}
	if count != 2 {
		t.Fatalf("the app read %d orders, want 2", count)
	}
	t.Logf("the app read orders in %s while provisioning held its locks", took)

	written := make(chan error, 1)
	go func() {
		_, err := insertOrder(db, orders, 1)
		written <- err
	}()
	waitForLockWaiters(t, db, 1)
	if got := waitingOn(t, db); got != schema+".orders" {
		t.Fatalf("the app's write waits on %s, want %s.orders", got, schema)
	}

	resume()
	err = waitForCall(t, provisioned)
	if err != nil {
		t.Fatalf("MaintainPartitionTableProvision: %v", err)
	}
	err = waitForCall(t, written)
	if err != nil {
		t.Fatalf("the app's write of orders: %v", err)
	}
	mustEqualStrings(t, "partitions", partitionNames(t, db, users), []string{"users_202610", "users_202611", "users_202612", "users_default"})
	if got := partitionOfRow(t, db, users, 4); got != "users_default" {
		t.Fatalf("the new year's user is in %s, want users_default", got)
	}
	if got := countRows(t, db, orders); got != 3 {
		t.Fatalf("orders holds %d rows, want 3", got)
	}
}

// A move out of DEFAULT drops and adds back the keys pointing in, so the
// call holds each referencing table ACCESS EXCLUSIVE: the app's read of
// orders waits for the commit, then sees every order.
func TestMaintenanceLocksMakeTheAppWaitWhileMoving(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)
	jakarta := mustLoadLocation(t, "Asia/Jakarta")
	m.now = clockAt(t, jakarta, 2026, time.November, 17, 9)

	users, orders := convertedUsersWithOrders(t, db, m, schema, "ON DELETE CASCADE")
	paused, resume := pauseAfterFirstChild(t, m)

	provisioned := make(chan error, 1)
	go func() {
		provisioned <- m.MaintainPartitionTableProvision(ctx, 1)
	}()
	waitForPause(t, paused, provisioned)

	type read struct {
		count int64
		err   error
	}
	reads := make(chan read, 1)
	go func() {
		count, _, err := readWithin(db, orders, 30*time.Second)
		reads <- read{count: count, err: err}
	}()
	waitForLockWaiters(t, db, 1)
	if got := waitingOn(t, db); got != schema+".orders" {
		t.Fatalf("the app's read waits on %s, want %s.orders", got, schema)
	}

	resume()
	err := waitForCall(t, provisioned)
	if err != nil {
		t.Fatalf("MaintainPartitionTableProvision: %v", err)
	}
	r := <-reads
	if r.err != nil {
		t.Fatalf("the app's read of orders: %v", r.err)
	}
	if r.count != 3 {
		t.Fatalf("the app read %d orders, want 3", r.count)
	}
	if got := partitionOfRow(t, db, users, 4); got != "users_202612" {
		t.Fatalf("the December user is in %s, want users_202612", got)
	}
}

// appReadsReferencingThenParent runs call while an app transaction that has
// read referencing goes on to read parent. The call must wait on referencing
// without holding parent: the app's read of parent returns, the app commits,
// the call finishes. A call holding parent ACCESS EXCLUSIVE while it waits
// for referencing deadlocks with the app.
func appReadsReferencingThenParent(t *testing.T, db *gorm.DB, referencing, parent TableName, call func() error) {
	t.Helper()

	app := db.Begin()
	t.Cleanup(func() {
		app.Rollback()
	})
	mustExec(t, app, "SET LOCAL lock_timeout = '10s'")
	var count int64
	err := app.
		Raw("SELECT count(*) FROM " + referencing.quoted()).
		Row().
		Scan(&count)
	if err != nil {
		t.Fatalf("the app's read of %s: %v", referencing, err)
	}

	done := make(chan error, 1)
	go func() {
		done <- call()
	}()
	waitForLockWaiters(t, db, 1)
	if got := waitingOn(t, db); got != referencing.Schema+"."+referencing.Name {
		t.Fatalf("the call waits on %s, want %s", got, referencing)
	}

	err = app.
		Raw("SELECT count(*) FROM " + parent.quoted()).
		Row().
		Scan(&count)
	if err != nil {
		t.Fatalf("the app's read of %s: %v", parent, err)
	}
	err = app.
		Commit().
		Error
	if err != nil {
		t.Fatalf("the app's commit: %v", err)
	}

	err = waitForCall(t, done)
	if err != nil {
		t.Fatalf("the call: %v", err)
	}
}

// A call that first creates a child with nothing to move and then one with
// rows to move takes the strong locks before its first child: it never
// upgrades a referencing table's lock while it holds the parent, which would
// deadlock with an app that read orders and then reads users.
func TestMaintenanceLocksDecideBeforeTheFirstChild(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)
	jakarta := mustLoadLocation(t, "Asia/Jakarta")
	m.now = clockAt(t, jakarta, 2026, time.November, 17, 9)

	users, orders := convertedUsersAndOrders(t, db, m, schema)
	// The January user sits on users_202701's lower bound.
	execIn(t, db, schema,
		"INSERT INTO $S.users (name, created_at) VALUES ('january', '2027-01-01 00:00+07')",
		"INSERT INTO $S.orders (user_id, note) VALUES (4, 'january')",
	)
	beforeRows := orderRows(t, db, orders)
	beforeKeys := foreignKeysOf(t, db, orders)

	// n = 2: users_202612 (nothing to move), then users_202701 (the January
	// user moves).
	appReadsReferencingThenParent(t, db, orders, users, func() error {
		return m.MaintainPartitionTableProvision(ctx, 2)
	})

	mustEqualStrings(t, "partitions", partitionNames(t, db, users), []string{"users_202610", "users_202611", "users_202612", "users_202701", "users_default"})
	if got := partitionOfRow(t, db, users, 4); got != "users_202701" {
		t.Fatalf("the January user is in %s, want users_202701", got)
	}
	mustEqualStrings(t, "orders after the move", orderRows(t, db, orders), beforeRows)
	mustEqualStrings(t, "foreign keys of orders after the move", foreignKeysOf(t, db, orders), beforeKeys)
}

// relationOid returns table's OID.
func relationOid(t *testing.T, db *gorm.DB, table TableName) int64 {
	t.Helper()

	var oid int64
	err := db.
		Raw("SELECT ?::regclass::oid::bigint", table.quoted()).
		Row().
		Scan(&oid)
	if err != nil {
		t.Fatalf("find the OID of %s: %v", table, err)
	}
	return oid
}

// A table holding a key to itself is the parent, locked last: it is not
// locked again among the referencing tables in OID order. Locked there, ahead
// of a referencing table created after the conversion (a higher OID), it
// would deadlock with an app that read that table and then reads the parent.
func TestMaintenanceLocksTakeATableWithASelfKeyLast(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)
	jakarta := mustLoadLocation(t, "Asia/Jakarta")
	m.now = clockAt(t, jakarta, 2026, time.November, 17, 9)

	tasks := convertedTasksWithASelfKey(t, db, m, schema)
	notes := TableName{Schema: schema, Name: "notes"}
	execIn(t, db, schema,
		"CREATE TABLE $S.notes (id BIGSERIAL PRIMARY KEY, task_id BIGINT, task_created_at TIMESTAMPTZ, FOREIGN KEY (task_id, task_created_at) REFERENCES $S.tasks (id, created_at) ON DELETE CASCADE)",
		"INSERT INTO $S.notes (task_id, task_created_at) VALUES (3, '2026-12-05 08:00+07'), (4, '2027-01-06 08:00+07')",
	)
	if relationOid(t, db, notes) < relationOid(t, db, tasks) {
		t.Fatal("notes has a lower OID than tasks; the test needs it higher")
	}

	appReadsReferencingThenParent(t, db, notes, tasks, func() error {
		return m.MaintainPartitionTableProvision(ctx, 2)
	})

	mustEqualStrings(t, "tasks after the move", taskRows(t, db, tasks), []string{
		"1 october NULL tasks_202610",
		"2 november NULL tasks_202611",
		"3 december parent NULL tasks_202612",
		"4 january subtask 3 tasks_202701",
	})
	if got := countRows(t, db, notes); got != 2 {
		t.Fatalf("notes holds %d rows, want 2", got)
	}
}
