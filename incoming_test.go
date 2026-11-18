package san_pgpartition

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm"
)

// execIn runs each statement with $S replaced by the quoted schema.
func execIn(t *testing.T, db *gorm.DB, schema string, statements ...string) {
	t.Helper()

	for _, statement := range statements {
		mustExec(t, db, strings.ReplaceAll(statement, "$S", quoteIdentifier(schema)))
	}
}

// createUsers creates <schema>.users — a sequence-fed id, a name and
// created_at — with one user per createdAt, ids 1, 2, … in that order.
func createUsers(t *testing.T, db *gorm.DB, schema string, createdAts ...string) TableName {
	t.Helper()

	users := TableName{Schema: schema, Name: "users"}
	mustExec(t, db, "CREATE TABLE "+users.quoted()+" (id BIGSERIAL PRIMARY KEY, name TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT now())")
	for i, at := range createdAts {
		mustExec(t, db, "INSERT INTO "+users.quoted()+" (name, created_at) VALUES (?, ?::timestamptz)", fmt.Sprintf("user %d", i+1), at)
	}
	return users
}

// threeUsers are an October and two November users (ids 1, 2, 3): a monthly
// conversion in Jakarta gives users_202610, users_202611 and users_default.
var threeUsers = []string{"2026-10-05 08:00+07", "2026-11-10 08:00+07", "2026-11-20 08:00+07"}

// columnInfo describes a column as "<type> NOT NULL" or "<type> NULL"; ""
// when table has no such column.
func columnInfo(t *testing.T, db *gorm.DB, table TableName, column string) string {
	t.Helper()

	var infos []string
	err := db.
		Raw(`SELECT pg_catalog.format_type(a.atttypid, a.atttypmod) || CASE WHEN a.attnotnull THEN ' NOT NULL' ELSE ' NULL' END
FROM pg_catalog.pg_attribute AS a
WHERE a.attrelid = ?::regclass
  AND a.attname = ?
  AND a.attnum > 0
  AND NOT a.attisdropped`, table.quoted(), column).
		Scan(&infos).
		Error
	if err != nil {
		t.Fatalf("read column %s of %s: %v", column, table, err)
	}
	if len(infos) == 0 {
		return ""
	}
	return infos[0]
}

// foreignKeysOf lists table's own foreign keys as "<name> <definition>",
// sorted — only the ones declared on it, not the copies PostgreSQL keeps for
// each partition of a partitioned table they point at.
func foreignKeysOf(t *testing.T, db *gorm.DB, table TableName) []string {
	t.Helper()

	var rows []string
	err := db.
		Raw(`SELECT conname || ' ' || pg_catalog.pg_get_constraintdef(oid)
FROM pg_catalog.pg_constraint
WHERE conrelid = ?::regclass
  AND contype = 'f'
  AND conparentid = 0
ORDER BY conname`, table.quoted()).
		Scan(&rows).
		Error
	if err != nil {
		t.Fatalf("list the foreign keys of %s: %v", table, err)
	}
	return rows
}

// mismatchedReferences counts the rows of orders whose column
// newColumn differs from the created_at of the user fkColumn points at
// (NULL fk rows must have a NULL newColumn).
func mismatchedReferences(t *testing.T, db *gorm.DB, orders, users TableName, fkColumn, newColumn string) int64 {
	t.Helper()

	var count int64
	err := db.
		Raw("SELECT count(*) FROM " + orders.quoted() + " AS o LEFT JOIN " + users.quoted() + " AS u ON u.id = o." + quoteIdentifier(fkColumn) +
			" WHERE o." + quoteIdentifier(newColumn) + " IS DISTINCT FROM u.created_at").
		Row().
		Scan(&count)
	if err != nil {
		t.Fatalf("compare %s.%s with %s: %v", orders, newColumn, users, err)
	}
	return count
}

func TestReferenceColumnName(t *testing.T) {
	cases := []struct {
		fk, key, want string
	}{
		{fk: "user_id", key: "created_at", want: "user_created_at"},
		{fk: "take_transaction_id", key: "created_at", want: "take_transaction_created_at"},
		{fk: "owner", key: "created_at", want: "owner_created_at"},
		{fk: "id", key: "created_at", want: "id_created_at"},
		{fk: "user_ID", key: "created_at", want: "user_ID_created_at"},
		{fk: "batch_id", key: "num", want: "batch_num"},
	}
	for _, tc := range cases {
		if got := referenceColumnName(tc.fk, tc.key); got != tc.want {
			t.Errorf("referenceColumnName(%q, %q) = %q, want %q", tc.fk, tc.key, got, tc.want)
		}
	}
}

// Another table's foreign key to the table no longer stops the conversion:
// the referencing table gets the key column, filled, and its key moves to
// (fk, new column) → (id, created_at) under the same name.
func TestManagerPartitionExistTableReshapesAKeyPointingIn(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)

	users := createUsers(t, db, schema, threeUsers...)
	orders := TableName{Schema: schema, Name: "orders"}
	execIn(t, db, schema,
		"CREATE TABLE $S.orders (id BIGSERIAL PRIMARY KEY, user_id BIGINT NOT NULL REFERENCES $S.users (id), note TEXT)",
		"INSERT INTO $S.orders (user_id, note) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (2, 'd')",
	)

	err := m.PartitionExistTable(ctx, users, jakartaConfig(t, RangeTypeMonthly))
	if err != nil {
		t.Fatalf("PartitionExistTable: %v", err)
	}

	if got := columnInfo(t, db, orders, "user_created_at"); got != "timestamp with time zone NOT NULL" {
		t.Fatalf("orders.user_created_at is %q, want timestamp with time zone NOT NULL", got)
	}
	if got := countRows(t, db, orders); got != 4 {
		t.Fatalf("orders holds %d rows, want 4", got)
	}
	if got := mismatchedReferences(t, db, orders, users, "user_id", "user_created_at"); got != 0 {
		t.Fatalf("%d orders hold another created_at than their user's", got)
	}
	mustEqualStrings(t, "foreign keys of orders", foreignKeysOf(t, db, orders), []string{
		"orders_user_id_fkey FOREIGN KEY (user_id, user_created_at) REFERENCES " + schema + ".users(id, created_at) ON UPDATE CASCADE",
	})
}

// The new key keeps the old one's ON DELETE action and DEFERRABLE flags; its
// ON UPDATE is always CASCADE. A nullable foreign-key column gets a nullable
// new column.
func TestManagerPartitionExistTableKeepsTheOldKeysActions(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	cases := []struct {
		name   string
		clause string // the old key's actions
		want   string // the new key's, after REFERENCES …(id, created_at)
		// deleted says what deleting user 1 does to its order: "refused",
		// "deleted" or "unlinked"; "" skips that check.
		deleted string
	}{
		{name: "no action", clause: "", want: " ON UPDATE CASCADE", deleted: "refused"},
		{name: "on update restrict becomes cascade", clause: "ON UPDATE RESTRICT", want: " ON UPDATE CASCADE"},
		{name: "restrict", clause: "ON DELETE RESTRICT", want: " ON UPDATE CASCADE ON DELETE RESTRICT", deleted: "refused"},
		{name: "cascade", clause: "ON DELETE CASCADE", want: " ON UPDATE CASCADE ON DELETE CASCADE", deleted: "deleted"},
		{name: "set null", clause: "ON DELETE SET NULL", want: " ON UPDATE CASCADE ON DELETE SET NULL", deleted: "unlinked"},
		{name: "set null of the fk column", clause: "ON DELETE SET NULL (user_id)", want: " ON UPDATE CASCADE ON DELETE SET NULL (user_id)", deleted: "unlinked"},
		{name: "set default", clause: "ON DELETE SET DEFAULT", want: " ON UPDATE CASCADE ON DELETE SET DEFAULT"},
		{name: "deferrable", clause: "DEFERRABLE", want: " ON UPDATE CASCADE DEFERRABLE"},
		{name: "initially deferred", clause: "ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED", want: " ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED", deleted: "deleted"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			schema := createTestSchema(t, db)
			m := newTestManager(t, db)
			users := createUsers(t, db, schema, threeUsers...)
			orders := TableName{Schema: schema, Name: "orders"}
			execIn(t, db, schema,
				"CREATE TABLE $S.orders (id BIGSERIAL PRIMARY KEY, user_id BIGINT, note TEXT, CONSTRAINT orders_user_fk FOREIGN KEY (user_id) REFERENCES $S.users (id) "+tc.clause+")",
				"INSERT INTO $S.orders (user_id, note) VALUES (1, 'a'), (2, 'b'), (NULL, 'c')",
			)

			err := m.PartitionExistTable(ctx, users, jakartaConfig(t, RangeTypeMonthly))
			if err != nil {
				t.Fatalf("PartitionExistTable: %v", err)
			}

			if got := columnInfo(t, db, orders, "user_created_at"); got != "timestamp with time zone NULL" {
				t.Fatalf("orders.user_created_at is %q, want a nullable timestamp with time zone", got)
			}
			if got := mismatchedReferences(t, db, orders, users, "user_id", "user_created_at"); got != 0 {
				t.Fatalf("%d orders hold another created_at than their user's", got)
			}
			mustEqualStrings(t, "foreign keys of orders", foreignKeysOf(t, db, orders), []string{
				"orders_user_fk FOREIGN KEY (user_id, user_created_at) REFERENCES " + schema + ".users(id, created_at)" + tc.want,
			})

			if tc.deleted == "" {
				return
			}
			err = db.
				Exec("DELETE FROM " + users.quoted() + " WHERE id = 1").
				Error
			var left []struct {
				UserID        *int64
				UserCreatedAt *string
			}
			scanErr := db.
				Raw("SELECT user_id, user_created_at::text FROM " + orders.quoted() + " WHERE note = 'a'").
				Scan(&left).
				Error
			if scanErr != nil {
				t.Fatalf("read the order: %v", scanErr)
			}
			switch tc.deleted {
			case "refused":
				// PostgreSQL names the copy of the key it keeps for the
				// user's child (orders_user_id_user_created_at_fkey…), not
				// orders_user_fk.
				if err == nil || !strings.Contains(err.Error(), `violates foreign key constraint`) || !strings.Contains(err.Error(), `on table "orders"`) || len(left) != 1 {
					t.Fatalf("deleting a user with an order = %v (%d orders left), want refused by the key on orders", err, len(left))
				}
			case "deleted":
				if err != nil || len(left) != 0 {
					t.Fatalf("deleting a user with an order = %v, %d orders left, want the order deleted", err, len(left))
				}
			case "unlinked":
				if err != nil || len(left) != 1 || left[0].UserID != nil || left[0].UserCreatedAt != nil {
					t.Fatalf("deleting a user with an order = %v, order %+v, want it kept with both columns NULL", err, left)
				}
			}
		})
	}
}

// sqlState returns the SQLSTATE PostgreSQL gave err, "" for none.
func sqlState(err error) string {
	var pgErr interface{ SQLState() string }
	if errors.As(err, &pgErr) {
		return pgErr.SQLState()
	}
	return ""
}

// insertOrder inserts an order for userID (nil: none) and returns its id.
func insertOrder(db *gorm.DB, orders TableName, userID any) (int64, error) {
	var id int64
	err := db.
		Raw("INSERT INTO "+orders.quoted()+" (user_id) VALUES (?) RETURNING id", userID).
		Row().
		Scan(&id)
	return id, err
}

// orderUser reads an order's user_id and user_created_at (as text).
func orderUser(t *testing.T, db *gorm.DB, orders TableName, id int64) (*int64, *string) {
	t.Helper()

	var userID *int64
	var createdAt *string
	err := db.
		Raw("SELECT user_id, user_created_at::text FROM "+orders.quoted()+" WHERE id = ?", id).
		Row().
		Scan(&userID, &createdAt)
	if err != nil {
		t.Fatalf("read order %d: %v", id, err)
	}
	return userID, createdAt
}

// userCreatedAt reads a user's created_at as text.
func userCreatedAt(t *testing.T, db *gorm.DB, users TableName, id int64) string {
	t.Helper()

	var createdAt string
	err := db.
		Raw("SELECT created_at::text FROM "+users.quoted()+" WHERE id = ?", id).
		Row().
		Scan(&createdAt)
	if err != nil {
		t.Fatalf("read user %d: %v", id, err)
	}
	return createdAt
}

// After the conversion the app keeps writing orders the old way — only
// user_id — and the fill trigger keeps the new column right.
func TestManagerPartitionExistTableFillsTheNewColumnOnLaterWrites(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)

	users := createUsers(t, db, schema, threeUsers...)
	orders := TableName{Schema: schema, Name: "orders"}
	execIn(t, db, schema,
		"CREATE TABLE $S.orders (id BIGSERIAL PRIMARY KEY, user_id BIGINT REFERENCES $S.users (id), note TEXT)",
		"INSERT INTO $S.orders (user_id, note) VALUES (1, 'a'), (2, 'b')",
	)

	err := m.PartitionExistTable(ctx, users, jakartaConfig(t, RangeTypeMonthly))
	if err != nil {
		t.Fatalf("PartitionExistTable: %v", err)
	}

	t.Run("the trigger and its function", func(t *testing.T) {
		var triggers []string
		err := db.
			Raw("SELECT pg_catalog.pg_get_triggerdef(oid) FROM pg_catalog.pg_trigger WHERE tgrelid = ?::regclass AND NOT tgisinternal", orders.quoted()).
			Scan(&triggers).
			Error
		if err != nil {
			t.Fatalf("read the triggers of orders: %v", err)
		}
		mustEqualStrings(t, "triggers of orders", triggers, []string{
			"CREATE TRIGGER orders_user_id_fkey_fill BEFORE INSERT OR UPDATE OF user_id ON " + schema + ".orders FOR EACH ROW EXECUTE FUNCTION " + schema + ".orders_user_id_fkey_fill()",
		})

		var comment *string
		err = db.
			Raw("SELECT pg_catalog.obj_description(pg_catalog.to_regprocedure(?::text), 'pg_proc')", quoteIdentifier(schema)+".orders_user_id_fkey_fill()").
			Row().
			Scan(&comment)
		if err != nil {
			t.Fatalf("read the function's comment: %v", err)
		}
		if comment == nil || *comment != createdMarker {
			t.Fatalf("the fill function's comment is %v, want %q", comment, createdMarker)
		}
	})

	t.Run("an existing user fills the column", func(t *testing.T) {
		id, err := insertOrder(db, orders, 2)
		if err != nil {
			t.Fatalf("insert an order for user 2: %v", err)
		}
		_, got := orderUser(t, db, orders, id)
		if want := userCreatedAt(t, db, users, 2); got == nil || *got != want {
			t.Fatalf("user_created_at = %v, want %s", got, want)
		}
	})

	t.Run("a missing user is a foreign key violation", func(t *testing.T) {
		_, err := insertOrder(db, orders, 999)
		if sqlState(err) != "23503" {
			t.Fatalf("insert an order for user 999 = %v, want SQLSTATE 23503", err)
		}
		mustContain(t, err.Error(), `insert or update on table "orders" violates foreign key constraint "orders_user_id_fkey"`)

		// The same fields PostgreSQL's own foreign-key check fills.
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) {
			t.Fatalf("the error is %T, want a *pgconn.PgError", err)
		}
		got := []string{pgErr.Detail, pgErr.ConstraintName, pgErr.SchemaName + "." + pgErr.TableName}
		mustEqualStrings(t, "the error's fields", got, []string{
			`Key (user_id)=(999) is not present in table "users".`, "orders_user_id_fkey", schema + ".orders",
		})
	})

	t.Run("no user leaves both NULL", func(t *testing.T) {
		id, err := insertOrder(db, orders, nil)
		if err != nil {
			t.Fatalf("insert an order without a user: %v", err)
		}
		if userID, got := orderUser(t, db, orders, id); userID != nil || got != nil {
			t.Fatalf("an order without a user holds %v, %v", userID, got)
		}
	})

	t.Run("a wrong value from the app is overwritten", func(t *testing.T) {
		var id int64
		err := db.
			Raw("INSERT INTO " + orders.quoted() + " (user_id, user_created_at) VALUES (2, '2000-01-01 00:00+00') RETURNING id").
			Row().
			Scan(&id)
		if err != nil {
			t.Fatalf("insert an order with a wrong user_created_at: %v", err)
		}
		_, got := orderUser(t, db, orders, id)
		if want := userCreatedAt(t, db, users, 2); got == nil || *got != want {
			t.Fatalf("user_created_at = %v, want %s", got, want)
		}
	})

	t.Run("changing the user refills the column", func(t *testing.T) {
		id, err := insertOrder(db, orders, 1)
		if err != nil {
			t.Fatalf("insert an order for user 1: %v", err)
		}
		mustExec(t, db, "UPDATE "+orders.quoted()+" SET user_id = 3 WHERE id = ?", id)
		_, got := orderUser(t, db, orders, id)
		if want := userCreatedAt(t, db, users, 3); got == nil || *got != want {
			t.Fatalf("after moving the order to user 3, user_created_at = %v, want %s", got, want)
		}
		mustExec(t, db, "UPDATE "+orders.quoted()+" SET user_id = NULL WHERE id = ?", id)
		if userID, got := orderUser(t, db, orders, id); userID != nil || got != nil {
			t.Fatalf("after unlinking the order it holds %v, %v", userID, got)
		}
	})

	t.Run("a user's key change across children cascades", func(t *testing.T) {
		mustExec(t, db, "UPDATE "+users.quoted()+" SET created_at = '2026-10-20 08:00+07' WHERE id = 2")
		if got := partitionOfRow(t, db, users, 2); got != "users_202610" {
			t.Fatalf("user 2 is in %s, want users_202610", got)
		}
		if got := mismatchedReferences(t, db, orders, users, "user_id", "user_created_at"); got != 0 {
			t.Fatalf("%d orders hold another created_at than their user's", got)
		}
	})

	t.Run("the caller can drop the old table", func(t *testing.T) {
		mustExec(t, db, "DROP TABLE "+TableName{Schema: schema, Name: "users_old"}.quoted())
	})
}

// fillFunctions lists the functions in schema, sorted.
func fillFunctions(t *testing.T, db *gorm.DB, schema string) []string {
	t.Helper()

	var names []string
	err := db.
		Raw(`SELECT p.proname::text
FROM pg_catalog.pg_proc AS p
JOIN pg_catalog.pg_namespace AS n ON n.oid = p.pronamespace
WHERE n.nspname = ?
ORDER BY 1`, schema).
		Scan(&names).
		Error
	if err != nil {
		t.Fatalf("list the functions of %s: %v", schema, err)
	}
	return names
}

// Every table pointing at the converted one is reshaped — two keys on one
// table, a table in another schema — each new column named after its key's
// column, each fill function in its referencing table's schema.
func TestManagerPartitionExistTableReshapesEveryReferencingTable(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	other := createTestSchema(t, db)
	m := newTestManager(t, db)

	users := createUsers(t, db, schema, threeUsers...)
	moves := TableName{Schema: schema, Name: "stock_moves"}
	documents := TableName{Schema: other, Name: "documents"}
	execIn(t, db, schema,
		`CREATE TABLE $S.stock_moves (
    id BIGSERIAL PRIMARY KEY,
    take_transaction_id BIGINT NOT NULL REFERENCES $S.users (id),
    put_transaction_id BIGINT REFERENCES $S.users (id) ON DELETE SET NULL
)`,
		"INSERT INTO $S.stock_moves (take_transaction_id, put_transaction_id) VALUES (1, 2), (3, NULL)",
	)
	mustExec(t, db, "CREATE TABLE "+documents.quoted()+" (id BIGSERIAL PRIMARY KEY, owner BIGINT NOT NULL REFERENCES "+users.quoted()+" (id) ON DELETE CASCADE)")
	mustExec(t, db, "INSERT INTO "+documents.quoted()+" (owner) VALUES (2), (3)")

	err := m.PartitionExistTable(ctx, users, jakartaConfig(t, RangeTypeMonthly))
	if err != nil {
		t.Fatalf("PartitionExistTable: %v", err)
	}

	for _, c := range []struct {
		table          TableName
		fk, newColumn  string
		wantColumnInfo string
	}{
		{table: moves, fk: "take_transaction_id", newColumn: "take_transaction_created_at", wantColumnInfo: "timestamp with time zone NOT NULL"},
		{table: moves, fk: "put_transaction_id", newColumn: "put_transaction_created_at", wantColumnInfo: "timestamp with time zone NULL"},
		{table: documents, fk: "owner", newColumn: "owner_created_at", wantColumnInfo: "timestamp with time zone NOT NULL"},
	} {
		if got := columnInfo(t, db, c.table, c.newColumn); got != c.wantColumnInfo {
			t.Fatalf("%s.%s is %q, want %s", c.table, c.newColumn, got, c.wantColumnInfo)
		}
		if got := mismatchedReferences(t, db, c.table, users, c.fk, c.newColumn); got != 0 {
			t.Fatalf("%d rows of %s hold another %s than their user's created_at", got, c.table, c.newColumn)
		}
	}
	mustEqualStrings(t, "foreign keys of stock_moves", foreignKeysOf(t, db, moves), []string{
		"stock_moves_put_transaction_id_fkey FOREIGN KEY (put_transaction_id, put_transaction_created_at) REFERENCES " + schema + ".users(id, created_at) ON UPDATE CASCADE ON DELETE SET NULL",
		"stock_moves_take_transaction_id_fkey FOREIGN KEY (take_transaction_id, take_transaction_created_at) REFERENCES " + schema + ".users(id, created_at) ON UPDATE CASCADE",
	})
	mustEqualStrings(t, "foreign keys of documents", foreignKeysOf(t, db, documents), []string{
		"documents_owner_fkey FOREIGN KEY (owner, owner_created_at) REFERENCES " + schema + ".users(id, created_at) ON UPDATE CASCADE ON DELETE CASCADE",
	})
	mustEqualStrings(t, "functions of "+schema, fillFunctions(t, db, schema), []string{
		"stock_moves_put_transaction_id_fkey_fill", "stock_moves_take_transaction_id_fkey_fill",
	})
	mustEqualStrings(t, "functions of "+other, fillFunctions(t, db, other), []string{"documents_owner_fkey_fill"})

	// The other schema's trigger fills from the converted table.
	var createdAt string
	err = db.
		Raw("INSERT INTO " + documents.quoted() + " (owner) VALUES (1) RETURNING owner_created_at::text").
		Row().
		Scan(&createdAt)
	if err != nil {
		t.Fatalf("insert a document: %v", err)
	}
	if want := userCreatedAt(t, db, users, 1); createdAt != want {
		t.Fatalf("a new document's owner_created_at = %s, want %s", createdAt, want)
	}
}

// snapshotReferencing describes what a failed or refused conversion must
// leave as it was on a referencing table: its columns, its constraints
// (copies included), its triggers and the functions of its schema.
func snapshotReferencing(t *testing.T, db *gorm.DB, table TableName) []string {
	t.Helper()

	var lines []string
	err := db.
		Raw(`SELECT 'column ' || a.attname || ' ' || pg_catalog.format_type(a.atttypid, a.atttypmod) || CASE WHEN a.attnotnull THEN ' NOT NULL' ELSE '' END
FROM pg_catalog.pg_attribute AS a
WHERE a.attrelid = ?::regclass
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum`, table.quoted()).
		Scan(&lines).
		Error
	if err != nil {
		t.Fatalf("read the columns of %s: %v", table, err)
	}
	for _, def := range constraintDefs(t, db, table, "pufcx") {
		lines = append(lines, "constraint "+def)
	}
	var triggers []string
	err = db.
		Raw("SELECT 'trigger ' || pg_catalog.pg_get_triggerdef(oid) FROM pg_catalog.pg_trigger WHERE tgrelid = ?::regclass AND NOT tgisinternal ORDER BY tgname", table.quoted()).
		Scan(&triggers).
		Error
	if err != nil {
		t.Fatalf("read the triggers of %s: %v", table, err)
	}
	lines = append(lines, triggers...)
	for _, name := range fillFunctions(t, db, table.Schema) {
		lines = append(lines, "function "+name)
	}
	return lines
}

// A new column (or fill trigger or function) whose name is taken, or longer
// than PostgreSQL's 63 bytes, refuses the conversion before anything is
// created — on the converted table and on every referencing table.
func TestManagerPartitionExistTableRefusesAReshapeItCannotName(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	long56 := strings.Repeat("o", 56)
	long60 := strings.Repeat("k", 60)
	cases := []struct {
		name       string
		statements []string // run after users exists; $S is the schema
		want       error
		names      []string // the message must name each
	}{
		{
			name: "the column is taken",
			statements: []string{
				"CREATE TABLE $S.orders (id BIGSERIAL PRIMARY KEY, user_id BIGINT NOT NULL REFERENCES $S.users (id), user_created_at DATE)",
			},
			want:  ErrReferenceColumnTaken,
			names: []string{".orders", "user_created_at"},
		},
		{
			name: "two keys need the same column",
			statements: []string{
				`CREATE TABLE $S.orders (id BIGSERIAL PRIMARY KEY, user_id BIGINT REFERENCES $S.users (id), "user" BIGINT REFERENCES $S.users (id))`,
			},
			want:  ErrReferenceColumnTaken,
			names: []string{".orders", "user_created_at", "orders_user_id_fkey", "orders_user_fkey"},
		},
		{
			name: "the column name is too long",
			statements: []string{
				"CREATE TABLE $S.orders (id BIGSERIAL PRIMARY KEY, " + long56 + " BIGINT REFERENCES $S.users (id), user_id BIGINT REFERENCES $S.users (id))",
			},
			want:  ErrInvalidTableName,
			names: []string{".orders", long56 + "_created_at"},
		},
		{
			name: "the fill trigger's name is too long",
			statements: []string{
				"CREATE TABLE $S.orders (id BIGSERIAL PRIMARY KEY, user_id BIGINT, CONSTRAINT " + long60 + " FOREIGN KEY (user_id) REFERENCES $S.users (id))",
			},
			want:  ErrInvalidTableName,
			names: []string{".orders", long60 + "_fill"},
		},
		{
			name: "the fill trigger's name is taken",
			statements: []string{
				"CREATE TABLE $S.orders (id BIGSERIAL PRIMARY KEY, user_id BIGINT REFERENCES $S.users (id))",
				"CREATE FUNCTION $S.touch() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NEW; END'",
				"CREATE TRIGGER orders_user_id_fkey_fill BEFORE INSERT ON $S.orders FOR EACH ROW EXECUTE FUNCTION $S.touch()",
			},
			want:  ErrReferenceColumnTaken,
			names: []string{"trigger orders_user_id_fkey_fill"},
		},
		{
			name: "the fill function's name is taken",
			statements: []string{
				"CREATE TABLE $S.orders (id BIGSERIAL PRIMARY KEY, user_id BIGINT REFERENCES $S.users (id))",
				"CREATE FUNCTION $S.orders_user_id_fkey_fill() RETURNS integer LANGUAGE sql AS 'SELECT 1'",
			},
			want:  ErrReferenceColumnTaken,
			names: []string{"function ", ".orders_user_id_fkey_fill()"},
		},
		{
			name: "two keys of one schema share a name",
			statements: []string{
				"CREATE TABLE $S.orders (id BIGSERIAL PRIMARY KEY, user_id BIGINT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES $S.users (id))",
				"CREATE TABLE $S.invoices (id BIGSERIAL PRIMARY KEY, user_id BIGINT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES $S.users (id))",
			},
			want:  ErrReferenceColumnTaken,
			names: []string{"function ", ".fk_user_fill()"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			schema := createTestSchema(t, db)
			m := newTestManager(t, db)
			users := createUsers(t, db, schema, threeUsers...)
			execIn(t, db, schema, tc.statements...)
			referencing := []TableName{{Schema: schema, Name: "orders"}}
			if strings.Contains(strings.Join(tc.statements, " "), "invoices") {
				referencing = append(referencing, TableName{Schema: schema, Name: "invoices"})
			}

			before := snapshotTable(t, db, m, users)
			var beforeRefs [][]string
			for _, table := range referencing {
				beforeRefs = append(beforeRefs, snapshotReferencing(t, db, table))
			}

			err := m.PartitionExistTable(ctx, users, jakartaConfig(t, RangeTypeMonthly))
			if !errors.Is(err, tc.want) {
				t.Fatalf("PartitionExistTable = %v, want %v", err, tc.want)
			}
			for _, name := range tc.names {
				mustContain(t, err.Error(), name)
			}
			mustEqualStrings(t, "users after the refusal", snapshotTable(t, db, m, users), before)
			for i, table := range referencing {
				mustEqualStrings(t, table.Name+" after the refusal", snapshotReferencing(t, db, table), beforeRefs[i])
			}
		})
	}
}

// A key pointing at a unique key that already holds the partition key needs
// no new column: it is only moved to the new table, same name and
// definition, and gets no trigger.
func TestManagerPartitionExistTableRepointsAKeyThatHoldsThePartitionKey(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)

	users := TableName{Schema: schema, Name: "users"}
	orders := TableName{Schema: schema, Name: "orders"}
	execIn(t, db, schema,
		`CREATE TABLE $S.users (
    id BIGSERIAL,
    code TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (id, created_at),
    CONSTRAINT users_code_key UNIQUE (code, created_at)
)`,
		"INSERT INTO $S.users (code, created_at) VALUES ('a', '2026-10-05 08:00+07'), ('b', '2026-11-10 08:00+07')",
		`CREATE TABLE $S.orders (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT,
    user_created_at TIMESTAMPTZ,
    user_code TEXT,
    code_at TIMESTAMPTZ,
    CONSTRAINT orders_user_fk FOREIGN KEY (user_id, user_created_at) REFERENCES $S.users (id, created_at) ON DELETE CASCADE,
    CONSTRAINT orders_code_fk FOREIGN KEY (user_code, code_at) REFERENCES $S.users (code, created_at) DEFERRABLE
)`,
		"INSERT INTO $S.orders (user_id, user_created_at, user_code, code_at) VALUES (1, '2026-10-05 08:00+07', 'b', '2026-11-10 08:00+07')",
	)
	before := snapshotReferencing(t, db, orders)
	beforeKeys := foreignKeysOf(t, db, orders)

	err := m.PartitionExistTable(ctx, users, jakartaConfig(t, RangeTypeMonthly))
	if err != nil {
		t.Fatalf("PartitionExistTable: %v", err)
	}

	mustEqualStrings(t, "foreign keys of orders", foreignKeysOf(t, db, orders), beforeKeys)
	// Columns, constraints and triggers as before, apart from the copies
	// PostgreSQL adds for each of the new table's children.
	var after []string
	for _, line := range snapshotReferencing(t, db, orders) {
		if !strings.Contains(line, "REFERENCES "+schema+".users_") {
			after = append(after, line)
		}
	}
	mustEqualStrings(t, "orders after the conversion", after, before)

	// The keys hold on the new table: users_old can go, and a missing user
	// is still refused.
	mustExec(t, db, "DROP TABLE "+TableName{Schema: schema, Name: "users_old"}.quoted())
	err = db.
		Exec("INSERT INTO " + orders.quoted() + " (user_id, user_created_at) VALUES (999, '2026-11-10 08:00+07')").
		Error
	if sqlState(err) != "23503" {
		t.Fatalf("an order for a missing user = %v, want SQLSTATE 23503", err)
	}
}

// A referencing row whose id is missing (its key was NOT VALID) fails the
// conversion midway — after another referencing table was already
// reshaped — and the rollback leaves every table as it was.
func TestManagerPartitionExistTableRollsBackEveryReferencingTable(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	for _, column := range []string{"user_id BIGINT", "user_id BIGINT NOT NULL"} {
		t.Run(column, func(t *testing.T) {
			schema := createTestSchema(t, db)
			m := newTestManager(t, db)
			users := createUsers(t, db, schema, threeUsers...)
			invoices := TableName{Schema: schema, Name: "invoices"}
			orders := TableName{Schema: schema, Name: "orders"}
			execIn(t, db, schema,
				// invoices sorts first: it is reshaped before orders fails.
				"CREATE TABLE $S.invoices (id BIGSERIAL PRIMARY KEY, user_id BIGINT NOT NULL REFERENCES $S.users (id))",
				"INSERT INTO $S.invoices (user_id) VALUES (1), (2)",
				"CREATE TABLE $S.orders (id BIGSERIAL PRIMARY KEY, "+column+")",
				"INSERT INTO $S.orders (user_id) VALUES (1), (999)",
				"ALTER TABLE $S.orders ADD CONSTRAINT orders_user_fk FOREIGN KEY (user_id) REFERENCES $S.users (id) NOT VALID",
			)
			before := snapshotTable(t, db, m, users)
			beforeInvoices := snapshotReferencing(t, db, invoices)
			beforeOrders := snapshotReferencing(t, db, orders)

			err := m.PartitionExistTable(ctx, users, jakartaConfig(t, RangeTypeMonthly))
			if err == nil {
				t.Fatal("PartitionExistTable succeeded with an order pointing at no user")
			}
			mustContain(t, err.Error(), "orders_user_fk")
			mustContain(t, err.Error(), "user_id = 999")

			mustEqualStrings(t, "users after the failure", snapshotTable(t, db, m, users), before)
			mustEqualStrings(t, "invoices after the failure", snapshotReferencing(t, db, invoices), beforeInvoices)
			mustEqualStrings(t, "orders after the failure", snapshotReferencing(t, db, orders), beforeOrders)
		})
	}
}

// An app role that may write orders but not read users could insert orders
// before the conversion (PostgreSQL checks a foreign key as the referenced
// table's owner); the fill trigger keeps that working by running as the
// conversion's role, like the key check.
func TestManagerPartitionExistTableFillsForARoleThatCannotReadTheTable(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)

	role := schema + "_app"
	mustExec(t, db, "CREATE ROLE "+quoteIdentifier(role))
	t.Cleanup(func() {
		_ = db.Exec("DROP OWNED BY " + quoteIdentifier(role)).Error
		_ = db.Exec("DROP ROLE IF EXISTS " + quoteIdentifier(role)).Error
	})

	users := createUsers(t, db, schema, threeUsers...)
	orders := TableName{Schema: schema, Name: "orders"}
	execIn(t, db, schema,
		"CREATE TABLE $S.orders (id BIGSERIAL PRIMARY KEY, user_id BIGINT NOT NULL REFERENCES $S.users (id))",
		"GRANT USAGE ON SCHEMA $S TO "+quoteIdentifier(role),
		"GRANT SELECT, INSERT, UPDATE ON $S.orders TO "+quoteIdentifier(role),
		"GRANT USAGE ON SEQUENCE $S.orders_id_seq TO "+quoteIdentifier(role),
	)

	insertAsApp := func(userID int64) error {
		return db.Transaction(func(tx *gorm.DB) error {
			var err error

			err = tx.
				Exec("SET LOCAL ROLE " + quoteIdentifier(role)).
				Error
			if err != nil {
				return err
			}
			return tx.
				Exec("INSERT INTO "+orders.quoted()+" (user_id) VALUES (?)", userID).
				Error
		})
	}

	err := insertAsApp(1)
	if err != nil {
		t.Fatalf("the app role's insert before the conversion: %v", err)
	}

	err = m.PartitionExistTable(ctx, users, jakartaConfig(t, RangeTypeMonthly))
	if err != nil {
		t.Fatalf("PartitionExistTable: %v", err)
	}

	err = insertAsApp(2)
	if err != nil {
		t.Fatalf("the app role's insert after the conversion: %v", err)
	}
	if got := mismatchedReferences(t, db, orders, users, "user_id", "user_created_at"); got != 0 {
		t.Fatalf("%d orders hold another created_at than their user's", got)
	}
}

// Names with quotes and percent signs reach the trigger's error message as
// they are: identifiers are quoted, the texts are literals, and a % is not
// read as a placeholder.
func TestManagerPartitionExistTableReshapesOddNames(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)

	users := createUsers(t, db, schema, threeUsers...)
	odd := TableName{Schema: schema, Name: `Order's %s "x"`}
	mustExec(t, db, "CREATE TABLE "+odd.quoted()+` (id BIGSERIAL PRIMARY KEY, "User%_id" BIGINT, CONSTRAINT "it's 100%" FOREIGN KEY ("User%_id") REFERENCES `+users.quoted()+" (id))")
	mustExec(t, db, "INSERT INTO "+odd.quoted()+` ("User%_id") VALUES (2)`)

	err := m.PartitionExistTable(ctx, users, jakartaConfig(t, RangeTypeMonthly))
	if err != nil {
		t.Fatalf("PartitionExistTable: %v", err)
	}
	if got := mismatchedReferences(t, db, odd, users, "User%_id", "User%_created_at"); got != 0 {
		t.Fatalf("%d rows hold another created_at than their user's", got)
	}

	err = db.
		Exec("INSERT INTO " + odd.quoted() + ` ("User%_id") VALUES (999)`).
		Error
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("an insert for a missing user = %v, want a PostgreSQL error", err)
	}
	got := []string{pgErr.Code, pgErr.Message, pgErr.Detail, pgErr.ConstraintName, pgErr.TableName}
	mustEqualStrings(t, "the error", got, []string{
		"23503",
		`insert or update on table "Order's %s "x"" violates foreign key constraint "it's 100%"`,
		`Key (User%_id)=(999) is not present in table "users".`,
		"it's 100%",
		`Order's %s "x"`,
	})
}

// The reshape works on the converted table's own columns: a primary key not
// named id, a date key — the new column takes the key's type.
func TestManagerPartitionExistTableReshapesOnTheTablesOwnColumns(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)

	members := TableName{Schema: schema, Name: "members"}
	cards := TableName{Schema: schema, Name: "cards"}
	execIn(t, db, schema,
		"CREATE TABLE $S.members (no BIGSERIAL PRIMARY KEY, joined_on DATE NOT NULL)",
		"INSERT INTO $S.members (joined_on) VALUES ('2026-10-05'), ('2026-11-10')",
		"CREATE TABLE $S.cards (id BIGSERIAL PRIMARY KEY, member_no BIGINT NOT NULL REFERENCES $S.members (no))",
		"INSERT INTO $S.cards (member_no) VALUES (1), (2)",
	)

	err := m.PartitionExistTable(ctx, members, &PartitionRangeConfig{Location: mustLoadLocation(t, "Asia/Jakarta"), RangeType: RangeTypeMonthly, Key: "joined_on"})
	if err != nil {
		t.Fatalf("PartitionExistTable: %v", err)
	}

	if got := columnInfo(t, db, cards, "member_no_joined_on"); got != "date NOT NULL" {
		t.Fatalf("cards.member_no_joined_on is %q, want date NOT NULL", got)
	}
	mustEqualStrings(t, "foreign keys of cards", foreignKeysOf(t, db, cards), []string{
		"cards_member_no_fkey FOREIGN KEY (member_no, member_no_joined_on) REFERENCES " + schema + ".members(no, joined_on) ON UPDATE CASCADE",
	})
	var joined string
	err = db.
		Raw("INSERT INTO " + cards.quoted() + " (member_no) VALUES (2) RETURNING member_no_joined_on::text").
		Row().
		Scan(&joined)
	if err != nil {
		t.Fatalf("insert a card: %v", err)
	}
	if joined != "2026-11-10" {
		t.Fatalf("a new card's member_no_joined_on = %s, want 2026-11-10", joined)
	}
}

// createPartitionedOrders creates <schema>.orders partitioned by id (two
// children) with user_id pointing at users: PostgreSQL keeps a copy of the
// key on each child, also pointing at users.
func createPartitionedOrders(t *testing.T, db *gorm.DB, schema, onDelete string) TableName {
	t.Helper()

	execIn(t, db, schema,
		"CREATE TABLE $S.orders (id BIGINT NOT NULL PRIMARY KEY, user_id BIGINT REFERENCES $S.users (id) "+onDelete+", note TEXT) PARTITION BY RANGE (id)",
		"CREATE TABLE $S.orders_a PARTITION OF $S.orders FOR VALUES FROM (0) TO (100)",
		"CREATE TABLE $S.orders_b PARTITION OF $S.orders FOR VALUES FROM (100) TO (200)",
	)
	return TableName{Schema: schema, Name: "orders"}
}

// A partitioned referencing table is reshaped once, at its parent: the
// copies of its key on its children follow the parent's.
func TestManagerPartitionExistTableReshapesAPartitionedReferencingTable(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)

	users := createUsers(t, db, schema, threeUsers...)
	orders := createPartitionedOrders(t, db, schema, "ON DELETE CASCADE")
	execIn(t, db, schema, "INSERT INTO $S.orders (id, user_id) VALUES (1, 1), (150, 2)")

	err := m.PartitionExistTable(ctx, users, jakartaConfig(t, RangeTypeMonthly))
	if err != nil {
		t.Fatalf("PartitionExistTable: %v", err)
	}
	mustEqualStrings(t, "foreign keys of orders", foreignKeysOf(t, db, orders), []string{
		"orders_user_id_fkey FOREIGN KEY (user_id, user_created_at) REFERENCES " + schema + ".users(id, created_at) ON UPDATE CASCADE ON DELETE CASCADE",
	})
	if got := mismatchedReferences(t, db, orders, users, "user_id", "user_created_at"); got != 0 {
		t.Fatalf("%d orders hold another created_at than their user's", got)
	}

	// The fill trigger works through the parent into either child.
	execIn(t, db, schema, "INSERT INTO $S.orders (id, user_id) VALUES (2, 3), (151, 1)")
	if got := mismatchedReferences(t, db, orders, users, "user_id", "user_created_at"); got != 0 {
		t.Fatalf("%d orders hold another created_at than their user's", got)
	}
}
