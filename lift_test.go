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

// convertedTasksWithASelfKey converts tasks (an October and a November task,
// monthly in Jakarta, on 2026-11-17), adds by hand a key the table holds on
// itself — a task's parent task, ON DELETE CASCADE — and then a December
// parent task (id 3) and a January subtask of it (id 4), both in
// tasks_default.
func convertedTasksWithASelfKey(t *testing.T, db *gorm.DB, m *Manager, schema string) TableName {
	t.Helper()

	tasks := TableName{Schema: schema, Name: "tasks"}
	execIn(t, db, schema,
		"CREATE TABLE $S.tasks (id BIGSERIAL PRIMARY KEY, name TEXT, created_at TIMESTAMPTZ NOT NULL, parent_id BIGINT, parent_created_at TIMESTAMPTZ)",
		"INSERT INTO $S.tasks (name, created_at) VALUES ('october', '2026-10-05 08:00+07'), ('november', '2026-11-10 08:00+07')",
	)
	err := m.PartitionExistTable(context.Background(), tasks, jakartaConfig(t, RangeTypeMonthly))
	if err != nil {
		t.Fatalf("PartitionExistTable: %v", err)
	}

	execIn(t, db, schema,
		"ALTER TABLE $S.tasks ADD CONSTRAINT tasks_parent_fkey FOREIGN KEY (parent_id, parent_created_at) REFERENCES $S.tasks (id, created_at) ON DELETE CASCADE",
		"INSERT INTO $S.tasks (name, created_at) VALUES ('december parent', '2026-12-05 08:00+07')",
		"INSERT INTO $S.tasks (name, created_at, parent_id, parent_created_at) VALUES ('january subtask', '2027-01-06 08:00+07', 3, '2026-12-05 08:00+07')",
	)
	for _, id := range []int64{3, 4} {
		if got := partitionOfRow(t, db, tasks, id); got != "tasks_default" {
			t.Fatalf("task %d is in %s, want tasks_default", id, got)
		}
	}
	return tasks
}

// taskRows lists every task as "<id> <name> <parent> <partition>", the parent
// being the id of the task its key points at (NULL for none).
func taskRows(t *testing.T, db *gorm.DB, tasks TableName) []string {
	t.Helper()

	var rows []string
	err := db.
		Raw(`SELECT r.id || ' ' || r.name || ' ' || COALESCE(p.id::text, 'NULL') || ' ' || c.relname
FROM ` + tasks.quoted() + ` AS r
JOIN pg_catalog.pg_class AS c ON c.oid = r.tableoid
LEFT JOIN ` + tasks.quoted() + ` AS p ON p.id = r.parent_id AND p.created_at = r.parent_created_at
ORDER BY r.id`).
		Scan(&rows).
		Error
	if err != nil {
		t.Fatalf("read the tasks: %v", err)
	}
	return rows
}

// A move out of DEFAULT lifts a key the table holds on itself too: moving the
// December parent task would otherwise delete (ON DELETE CASCADE) its
// January subtask. Both tasks move, the subtask still points at its parent,
// and the key is back unchanged — deleting the parent still deletes the
// subtask.
func TestMovesOutOfDefaultLiftTheTablesKeyToItself(t *testing.T) {
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
				return m.MaintainPartitionTableProvision(ctx, 2)
			},
		},
		{
			name: "MaintainPartition",
			now:  clockAt(t, jakarta, 2027, time.January, 5, 9),
			move: func(ctx context.Context, m *Manager) error {
				reports, err := m.MaintainPartition(ctx)
				if err == nil && (len(reports) != 1 || reports[0].Moved != 2 || reports[0].LeftInDefault != 0) {
					t.Fatalf("MaintainPartition reports %+v, want 2 rows moved and none left", reports)
				}
				return err
			},
		},
	}
	for _, mv := range moves {
		t.Run(mv.name, func(t *testing.T) {
			ctx := context.Background()
			schema := createTestSchema(t, db)
			m := newTestManager(t, db)
			m.now = clockAt(t, jakarta, 2026, time.November, 17, 9)
			tasks := convertedTasksWithASelfKey(t, db, m, schema)
			beforeKeys := foreignKeysOf(t, db, tasks)

			m.now = mv.now
			err := mv.move(ctx, m)
			if err != nil {
				t.Fatalf("%s: %v", mv.name, err)
			}

			mustEqualStrings(t, "tasks after the move", taskRows(t, db, tasks), []string{
				"1 october NULL tasks_202610",
				"2 november NULL tasks_202611",
				"3 december parent NULL tasks_202612",
				"4 january subtask 3 tasks_202701",
			})
			mustEqualStrings(t, "foreign keys of tasks after the move", foreignKeysOf(t, db, tasks), beforeKeys)
			foreignKeyOid(t, db, tasks, "tasks_parent_fkey")

			execIn(t, db, schema, "DELETE FROM $S.tasks WHERE id = 3")
			if got := countRows(t, db, tasks); got != 2 {
				t.Fatalf("tasks holds %d rows after deleting the parent task, want 2", got)
			}
		})
	}
}
