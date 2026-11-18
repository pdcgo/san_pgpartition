package san_pgpartition

import (
	"context"
	"testing"
	"time"

	"gorm.io/gorm"
)

// waitingOn names the relation the one backend waiting for a lock waits on.
func waitingOn(t *testing.T, db *gorm.DB) string {
	t.Helper()

	var names []string
	err := db.
		Raw("SELECT l.relation::regclass::text FROM pg_catalog.pg_locks AS l WHERE NOT l.granted AND l.relation IS NOT NULL").
		Scan(&names).
		Error
	if err != nil {
		t.Fatalf("read the waiting lock: %v", err)
	}
	if len(names) != 1 {
		t.Fatalf("%d backends wait on a relation, want 1: %v", len(names), names)
	}
	return names[0]
}

// An app transaction that wrote orders (holding orders, not yet users) and
// then writes an order for a user — whose check reads the table being
// converted — must not deadlock with the conversion: the conversion locks
// orders first and waits there, the app finishes, the conversion goes on.
// (Locking orders only after the renames deadlocks: the conversion holds
// users and waits for orders, the app holds orders and waits for users.)
func TestManagerPartitionExistTableWaitsForAnAppWritingAReferencingTable(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)

	users := createUsers(t, db, schema, threeUsers...)
	orders := TableName{Schema: schema, Name: "orders"}
	execIn(t, db, schema,
		"CREATE TABLE $S.orders (id BIGSERIAL PRIMARY KEY, user_id BIGINT REFERENCES $S.users (id), note TEXT)",
		"INSERT INTO $S.orders (user_id, note) VALUES (1, 'before')",
	)

	app := db.Begin()
	t.Cleanup(func() {
		app.Rollback()
	})
	mustExec(t, app, "INSERT INTO "+orders.quoted()+" (user_id, note) VALUES (NULL, 'app first')")

	converted := make(chan error, 1)
	go func() {
		converted <- m.PartitionExistTable(ctx, users, jakartaConfig(t, RangeTypeMonthly))
	}()
	waitForLockWaiters(t, db, 1)
	if got := waitingOn(t, db); got != schema+".orders" {
		t.Fatalf("the conversion waits on %s, want %s.orders", got, schema)
	}

	err := app.
		Exec("INSERT INTO " + orders.quoted() + " (user_id, note) VALUES (2, 'app second')").
		Error
	if err != nil {
		t.Fatalf("the app's order for user 2: %v", err)
	}
	err = app.
		Commit().
		Error
	if err != nil {
		t.Fatalf("the app's commit: %v", err)
	}

	select {
	case err = <-converted:
	case <-time.After(30 * time.Second):
		t.Fatal("the conversion never finished")
	}
	if err != nil {
		t.Fatalf("PartitionExistTable: %v", err)
	}
	if got := countRows(t, db, orders); got != 3 {
		t.Fatalf("orders holds %d rows, want 3", got)
	}
	if got := mismatchedReferences(t, db, orders, users, "user_id", "user_created_at"); got != 0 {
		t.Fatalf("%d orders hold another created_at than their user's", got)
	}
}
