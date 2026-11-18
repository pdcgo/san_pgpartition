package san_pgpartition

import (
	"context"
	"testing"
	"time"

	"gorm.io/gorm"
)

// orderRows lists every order as "<id> <user_id> <user_created_at>".
func orderRows(t *testing.T, db *gorm.DB, orders TableName) []string {
	t.Helper()

	var rows []string
	err := db.
		Raw("SELECT id || ' ' || COALESCE(user_id::text, 'NULL') || ' ' || COALESCE(user_created_at::text, 'NULL') FROM " + orders.quoted() + " ORDER BY id").
		Scan(&rows).
		Error
	if err != nil {
		t.Fatalf("read the orders: %v", err)
	}
	return rows
}

// foreignKeyOid returns the OID of table's foreign key name.
func foreignKeyOid(t *testing.T, db *gorm.DB, table TableName, name string) int64 {
	t.Helper()

	var oid int64
	err := db.
		Raw("SELECT oid::bigint FROM pg_catalog.pg_constraint WHERE conrelid = ?::regclass AND conname = ? AND convalidated", table.quoted(), name).
		Row().
		Scan(&oid)
	if err != nil {
		t.Fatalf("find the validated foreign key %s of %s: %v", name, table, err)
	}
	return oid
}

// convertedUsersWithOrders converts users (an October and two November
// users, monthly in Jakarta, on 2026-11-17) while orders points at it with
// the given ON DELETE clause, then adds a December user — who lands in
// users_default — with two orders, and an order for user 1.
func convertedUsersWithOrders(t *testing.T, db *gorm.DB, m *Manager, schema, onDelete string) (TableName, TableName) {
	t.Helper()

	ctx := context.Background()
	users := createUsers(t, db, schema, threeUsers...)
	orders := TableName{Schema: schema, Name: "orders"}
	execIn(t, db, schema, "CREATE TABLE $S.orders (id BIGSERIAL PRIMARY KEY, user_id BIGINT REFERENCES $S.users (id) "+onDelete+", note TEXT)")

	err := m.PartitionExistTable(ctx, users, jakartaConfig(t, RangeTypeMonthly))
	if err != nil {
		t.Fatalf("PartitionExistTable: %v", err)
	}

	execIn(t, db, schema,
		"INSERT INTO $S.users (name, created_at) VALUES ('december', '2026-12-05 08:00+07')",
		"INSERT INTO $S.orders (user_id, note) VALUES (4, 'december a'), (4, 'december b'), (1, 'october')",
	)
	if got := partitionOfRow(t, db, users, 4); got != "users_default" {
		t.Fatalf("the December user is in %s, want users_default", got)
	}
	return users, orders
}

// Moving a referenced user out of DEFAULT must keep every order with its
// user, whatever the key's ON DELETE says: the move lifts the keys pointing
// in, moves the rows, and adds the keys back, same names and definitions.
func TestMovesOutOfDefaultKeepTheRowsPointingIn(t *testing.T) {
	db := openTestDB(t)
	jakarta := mustLoadLocation(t, "Asia/Jakarta")

	moves := []struct {
		name string
		now  func() time.Time
		move func(ctx context.Context, m *Manager) error
	}{
		{
			name: "provisioning",
			now:  clockAt(t, jakarta, 2026, time.November, 17, 9),
			move: func(ctx context.Context, m *Manager) error {
				return m.MaintainPartitionTableProvision(ctx, 1)
			},
		},
		{
			name: "MaintainPartition",
			now:  clockAt(t, jakarta, 2026, time.December, 3, 9),
			move: func(ctx context.Context, m *Manager) error {
				reports, err := m.MaintainPartition(ctx)
				if err == nil && (len(reports) != 1 || reports[0].Moved != 1) {
					t.Fatalf("MaintainPartition reports %+v, want 1 row moved", reports)
				}
				return err
			},
		},
	}
	for _, mv := range moves {
		for _, onDelete := range []string{"", "ON DELETE CASCADE", "ON DELETE SET NULL"} {
			name := onDelete
			if name == "" {
				name = "NO ACTION"
			}
			t.Run(mv.name+" "+name, func(t *testing.T) {
				ctx := context.Background()
				schema := createTestSchema(t, db)
				m := newTestManager(t, db)
				m.now = clockAt(t, jakarta, 2026, time.November, 17, 9)
				users, orders := convertedUsersWithOrders(t, db, m, schema, onDelete)
				beforeRows := orderRows(t, db, orders)
				beforeKeys := foreignKeysOf(t, db, orders)

				m.now = mv.now
				err := mv.move(ctx, m)
				if err != nil {
					t.Fatalf("%s: %v", mv.name, err)
				}

				if got := partitionOfRow(t, db, users, 4); got != "users_202612" {
					t.Fatalf("the December user is in %s, want users_202612", got)
				}
				mustEqualStrings(t, "orders after the move", orderRows(t, db, orders), beforeRows)
				mustEqualStrings(t, "foreign keys of orders after the move", foreignKeysOf(t, db, orders), beforeKeys)
				foreignKeyOid(t, db, orders, "orders_user_id_fkey")
			})
		}
	}
}

// A call that moves nothing leaves the keys pointing in alone: provisioning a
// child DEFAULT holds no rows for keeps the very same constraint.
func TestMovesOutOfDefaultTouchNoKeyWhenNothingMoves(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)
	jakarta := mustLoadLocation(t, "Asia/Jakarta")
	m.now = clockAt(t, jakarta, 2026, time.November, 17, 9)

	users := createUsers(t, db, schema, threeUsers...)
	orders := TableName{Schema: schema, Name: "orders"}
	execIn(t, db, schema,
		"CREATE TABLE $S.orders (id BIGSERIAL PRIMARY KEY, user_id BIGINT REFERENCES $S.users (id) ON DELETE CASCADE)",
		"INSERT INTO $S.orders (user_id) VALUES (1), (2)",
	)
	err := m.PartitionExistTable(ctx, users, jakartaConfig(t, RangeTypeMonthly))
	if err != nil {
		t.Fatalf("PartitionExistTable: %v", err)
	}
	before := foreignKeyOid(t, db, orders, "orders_user_id_fkey")

	err = m.MaintainPartitionTableProvision(ctx, 1)
	if err != nil {
		t.Fatalf("MaintainPartitionTableProvision: %v", err)
	}
	mustEqualStrings(t, "partitions", partitionNames(t, db, users), []string{"users_202610", "users_202611", "users_202612", "users_default"})
	if after := foreignKeyOid(t, db, orders, "orders_user_id_fkey"); after != before {
		t.Fatalf("orders_user_id_fkey was re-created (OID %d → %d) by a call that moved nothing", before, after)
	}

	reports, err := m.MaintainPartition(ctx)
	if err != nil {
		t.Fatalf("MaintainPartition: %v", err)
	}
	if len(reports) != 1 || reports[0].Moved != 0 {
		t.Fatalf("MaintainPartition reports %+v, want nothing moved", reports)
	}
	if after := foreignKeyOid(t, db, orders, "orders_user_id_fkey"); after != before {
		t.Fatalf("orders_user_id_fkey was re-created (OID %d → %d) by a call that moved nothing", before, after)
	}
}

// An app transaction that wrote orders and then writes an order for a user
// must not deadlock with a move out of users' DEFAULT: provisioning locks
// orders before users and waits there, the app finishes, the move goes on.
// (Locking users first deadlocks: provisioning holds users and waits for
// orders, the app holds orders and its fill trigger waits for users.)
func TestMovesOutOfDefaultWaitForAnAppWritingAReferencingTable(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)
	jakarta := mustLoadLocation(t, "Asia/Jakarta")
	m.now = clockAt(t, jakarta, 2026, time.November, 17, 9)

	users, orders := convertedUsersWithOrders(t, db, m, schema, "ON DELETE CASCADE")

	app := db.Begin()
	t.Cleanup(func() {
		app.Rollback()
	})
	mustExec(t, app, "INSERT INTO "+orders.quoted()+" (user_id, note) VALUES (NULL, 'app first')")

	provisioned := make(chan error, 1)
	go func() {
		provisioned <- m.MaintainPartitionTableProvision(ctx, 1)
	}()
	waitForLockWaiters(t, db, 1)
	if got := waitingOn(t, db); got != schema+".orders" {
		t.Fatalf("provisioning waits on %s, want %s.orders", got, schema)
	}

	err := app.
		Exec("INSERT INTO " + orders.quoted() + " (user_id, note) VALUES (1, 'app second')").
		Error
	if err != nil {
		t.Fatalf("the app's order for user 1: %v", err)
	}
	err = app.
		Commit().
		Error
	if err != nil {
		t.Fatalf("the app's commit: %v", err)
	}

	select {
	case err = <-provisioned:
	case <-time.After(30 * time.Second):
		t.Fatal("provisioning never finished")
	}
	if err != nil {
		t.Fatalf("MaintainPartitionTableProvision: %v", err)
	}
	if got := partitionOfRow(t, db, users, 4); got != "users_202612" {
		t.Fatalf("the December user is in %s, want users_202612", got)
	}
	if got := countRows(t, db, orders); got != 5 {
		t.Fatalf("orders holds %d rows, want 5", got)
	}
}

// A partitioned table pointing at the managed one has its key lifted once,
// at its parent; the copies on its children go and come back with it.
func TestMovesOutOfDefaultLiftAPartitionedReferencingTablesKey(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)
	jakarta := mustLoadLocation(t, "Asia/Jakarta")
	m.now = clockAt(t, jakarta, 2026, time.November, 17, 9)

	users := createUsers(t, db, schema, threeUsers...)
	orders := createPartitionedOrders(t, db, schema, "ON DELETE CASCADE")
	err := m.PartitionExistTable(ctx, users, jakartaConfig(t, RangeTypeMonthly))
	if err != nil {
		t.Fatalf("PartitionExistTable: %v", err)
	}
	execIn(t, db, schema,
		"INSERT INTO $S.users (name, created_at) VALUES ('december', '2026-12-05 08:00+07')",
		"INSERT INTO $S.orders (id, user_id) VALUES (1, 4), (150, 4), (151, 1)",
	)
	beforeRows := orderRows(t, db, orders)
	beforeKeys := constraintDefs(t, db, TableName{Schema: schema, Name: "orders_b"}, "f")

	err = m.MaintainPartitionTableProvision(ctx, 1)
	if err != nil {
		t.Fatalf("MaintainPartitionTableProvision: %v", err)
	}
	if got := partitionOfRow(t, db, users, 4); got != "users_202612" {
		t.Fatalf("the December user is in %s, want users_202612", got)
	}
	mustEqualStrings(t, "orders after the move", orderRows(t, db, orders), beforeRows)
	// The child's copy of the key is back.
	mustEqualStrings(t, "keys of orders_b after the move", constraintDefs(t, db, TableName{Schema: schema, Name: "orders_b"}, "f"), beforeKeys)
	mustEqualStrings(t, "foreign keys of orders after the move", foreignKeysOf(t, db, orders), []string{
		"orders_user_id_fkey FOREIGN KEY (user_id, user_created_at) REFERENCES " + schema + ".users(id, created_at) ON UPDATE CASCADE ON DELETE CASCADE",
	})
}
