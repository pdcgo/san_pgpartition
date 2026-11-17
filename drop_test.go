package san_pgpartition

import (
	"context"
	"errors"
	"testing"
	"time"

	"gorm.io/gorm"
)

func relationExists(t *testing.T, db *gorm.DB, table TableName) bool {
	t.Helper()

	var exists bool
	err := db.
		Raw(`SELECT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_class AS c
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    WHERE n.nspname = ? AND c.relname = ?
)`, table.Schema, table.Name).
		Row().
		Scan(&exists)
	if err != nil {
		t.Fatalf("look up %s: %v", table, err)
	}
	return exists
}

func tableComment(t *testing.T, db *gorm.DB, table TableName) string {
	t.Helper()

	var comment *string
	err := db.
		Raw(`SELECT pg_catalog.obj_description(c.oid, 'pg_class')
FROM pg_catalog.pg_class AS c
JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
WHERE n.nspname = ? AND c.relname = ?`, table.Schema, table.Name).
		Row().
		Scan(&comment)
	if err != nil {
		t.Fatalf("read the comment of %s: %v", table, err)
	}
	if comment == nil {
		return ""
	}
	return *comment
}

func TestPartitionNewTableMarksOnlyWhatItCreates(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)

	parent := TableName{Schema: schema, Name: "orders"}
	byPackage := TableName{Schema: schema, Name: "orders_2026_01"}
	byMigration := TableName{Schema: schema, Name: "orders_2026_02"}
	mustExec(t, db, "CREATE TABLE "+parent.quoted()+" (id bigint NOT NULL, created_at date NOT NULL) PARTITION BY RANGE (created_at)")
	mustExec(t, db, "CREATE TABLE "+byMigration.quoted()+" PARTITION OF "+parent.quoted()+" FOR VALUES FROM ('2026-02-01') TO ('2026-03-01')")

	_, err := PartitionNewTable(ctx, db, parent, byPackage, PartitionBound{
		Range: &RangeBound{From: []any{"2026-01-01"}, To: []any{"2026-02-01"}},
	})
	if err != nil {
		t.Fatalf("PartitionNewTable: %v", err)
	}
	if got := tableComment(t, db, byPackage); got != createdMarker {
		t.Fatalf("comment of a created partition = %q, want %q", got, createdMarker)
	}

	created, err := PartitionNewTable(ctx, db, parent, byMigration, PartitionBound{
		Range: &RangeBound{From: []any{"2026-02-01"}, To: []any{"2026-03-01"}},
	})
	if err != nil || created {
		t.Fatalf("retry on a migration's partition = %v, %v; want false, nil", created, err)
	}
	if got := tableComment(t, db, byMigration); got != "" {
		t.Fatalf("a partition the package did not create got the comment %q", got)
	}
}

func TestPartitionDropTable(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)

	orders := TableName{Schema: schema, Name: "orders"}
	payments := TableName{Schema: schema, Name: "payments"}
	mustExec(t, db, "CREATE TABLE "+orders.quoted()+" (id bigint NOT NULL, created_at date NOT NULL, PRIMARY KEY (id, created_at)) PARTITION BY RANGE (created_at)")
	mustExec(t, db, "CREATE TABLE "+payments.quoted()+" (id bigint NOT NULL, created_at date NOT NULL) PARTITION BY RANGE (created_at)")

	month := func(name, from, to string) TableName {
		t.Helper()
		child := TableName{Schema: schema, Name: name}
		_, err := PartitionNewTable(ctx, db, orders, child, PartitionBound{
			Range: &RangeBound{From: []any{from}, To: []any{to}},
		})
		if err != nil {
			t.Fatalf("create %s: %v", child, err)
		}
		return child
	}

	t.Run("undoes a partition it created", func(t *testing.T) {
		child := month("orders_2026_01", "2026-01-01", "2026-02-01")

		dropped, err := PartitionDropTable(ctx, db, orders, child)
		if err != nil || !dropped {
			t.Fatalf("PartitionDropTable = %v, %v; want true, nil", dropped, err)
		}
		if relationExists(t, db, child) {
			t.Fatal("the partition still exists")
		}

		dropped, err = PartitionDropTable(ctx, db, orders, child)
		if err != nil || dropped {
			t.Fatalf("repeat = %v, %v; want false, nil", dropped, err)
		}
	})

	t.Run("refuses a partition the package did not create", func(t *testing.T) {
		child := TableName{Schema: schema, Name: "orders_2026_02"}
		mustExec(t, db, "CREATE TABLE "+child.quoted()+" PARTITION OF "+orders.quoted()+" FOR VALUES FROM ('2026-02-01') TO ('2026-03-01')")

		dropped, err := PartitionDropTable(ctx, db, orders, child)
		if !errors.Is(err, ErrPartitionNotOwned) || dropped {
			t.Fatalf("PartitionDropTable = %v, %v; want false, ErrPartitionNotOwned", dropped, err)
		}
		if !relationExists(t, db, child) {
			t.Fatal("the partition was removed")
		}
	})

	t.Run("refuses a partition whose mark was edited", func(t *testing.T) {
		child := month("orders_2026_03", "2026-03-01", "2026-04-01")
		mustExec(t, db, "COMMENT ON TABLE "+child.quoted()+" IS 'March, keep'")

		dropped, err := PartitionDropTable(ctx, db, orders, child)
		if !errors.Is(err, ErrPartitionNotOwned) || dropped {
			t.Fatalf("PartitionDropTable = %v, %v; want false, ErrPartitionNotOwned", dropped, err)
		}
		if !relationExists(t, db, child) {
			t.Fatal("the partition was removed")
		}
	})

	t.Run("refuses a partition holding rows", func(t *testing.T) {
		child := month("orders_2026_04", "2026-04-01", "2026-05-01")
		mustExec(t, db, "INSERT INTO "+orders.quoted()+" (id, created_at) VALUES (1, '2026-04-15')")

		dropped, err := PartitionDropTable(ctx, db, orders, child)
		if !errors.Is(err, ErrPartitionNotEmpty) || dropped {
			t.Fatalf("PartitionDropTable = %v, %v; want false, ErrPartitionNotEmpty", dropped, err)
		}
		if got := countRows(t, db, child); got != 1 {
			t.Fatalf("the partition holds %d rows, want 1", got)
		}
	})

	t.Run("refuses a name that is not a partition of the parent", func(t *testing.T) {
		plain := TableName{Schema: schema, Name: "orders_plain"}
		paymentsJan := TableName{Schema: schema, Name: "payments_2026_01"}
		mustExec(t, db, "CREATE TABLE "+plain.quoted()+" (id bigint)")
		mustExec(t, db, "CREATE TABLE "+paymentsJan.quoted()+" PARTITION OF "+payments.quoted()+" FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')")

		for _, child := range []TableName{plain, paymentsJan} {
			dropped, err := PartitionDropTable(ctx, db, orders, child)
			if !errors.Is(err, ErrPartitionConflict) || dropped {
				t.Fatalf("%s: PartitionDropTable = %v, %v; want false, ErrPartitionConflict", child, dropped, err)
			}
			if !relationExists(t, db, child) {
				t.Fatalf("%s was removed", child)
			}
		}
	})

	t.Run("works while another table's foreign key references the parent", func(t *testing.T) {
		items := TableName{Schema: schema, Name: "order_items"}
		mustExec(t, db, "CREATE TABLE "+items.quoted()+" (order_id bigint NOT NULL, order_created_at date NOT NULL, FOREIGN KEY (order_id, order_created_at) REFERENCES "+orders.quoted()+" (id, created_at))")
		child := month("orders_2026_05", "2026-05-01", "2026-06-01")

		dropped, err := PartitionDropTable(ctx, db, orders, child)
		if err != nil || !dropped {
			t.Fatalf("PartitionDropTable = %v, %v; want true, nil", dropped, err)
		}

		var foreignKeys int64
		err = db.
			Raw("SELECT count(*) FROM pg_catalog.pg_constraint WHERE conrelid = ?::regclass AND contype = 'f' AND conparentid = 0", items.quoted()).
			Row().
			Scan(&foreignKeys)
		if err != nil {
			t.Fatalf("count foreign keys: %v", err)
		}
		if foreignKeys != 1 {
			t.Fatalf("order_items has %d foreign keys, want its 1 kept", foreignKeys)
		}
	})

	t.Run("changes nothing when a foreign key points at the partition itself", func(t *testing.T) {
		child := month("orders_2026_06", "2026-06-01", "2026-07-01")
		direct := TableName{Schema: schema, Name: "direct_ref"}
		mustExec(t, db, "CREATE TABLE "+direct.quoted()+" (order_id bigint, order_created_at date, FOREIGN KEY (order_id, order_created_at) REFERENCES "+child.quoted()+" (id, created_at))")

		dropped, err := PartitionDropTable(ctx, db, orders, child)
		if err == nil || dropped {
			t.Fatalf("PartitionDropTable = %v, %v; want false and an error", dropped, err)
		}
		exists, err := PartitionAttached(ctx, db, orders, child)
		if err != nil || !exists {
			t.Fatalf("the partition is no longer attached: %v, %v", exists, err)
		}
	})

	t.Run("refuses bad input", func(t *testing.T) {
		child := TableName{Schema: schema, Name: "orders_2026_07"}
		_, err := PartitionDropTable(ctx, db, TableName{Schema: schema, Name: "nope"}, child)
		if !errors.Is(err, ErrParentNotPartitioned) {
			t.Fatalf("missing parent: error = %v, want ErrParentNotPartitioned", err)
		}
		_, err = PartitionDropTable(ctx, db, orders, TableName{Name: "orders_2026_07"})
		if !errors.Is(err, ErrInvalidTableName) {
			t.Fatalf("empty schema: error = %v, want ErrInvalidTableName", err)
		}
	})
}

// An insert in flight when the undo starts must either finish first — and
// then the undo sees its row and refuses — or never land. A row must never be
// dropped with the partition.
func TestPartitionDropTableWaitsForAnInsertInFlight(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)

	parent := TableName{Schema: schema, Name: "orders"}
	child := TableName{Schema: schema, Name: "orders_2026_01"}
	mustExec(t, db, "CREATE TABLE "+parent.quoted()+" (id bigint NOT NULL, created_at date NOT NULL) PARTITION BY RANGE (created_at)")
	_, err := PartitionNewTable(ctx, db, parent, child, PartitionBound{
		Range: &RangeBound{From: []any{"2026-01-01"}, To: []any{"2026-02-01"}},
	})
	if err != nil {
		t.Fatalf("PartitionNewTable: %v", err)
	}

	inserter := db.Begin()
	t.Cleanup(func() {
		inserter.Rollback()
	})
	mustExec(t, inserter, "INSERT INTO "+parent.quoted()+" (id, created_at) VALUES (1, '2026-01-15')")

	type result struct {
		dropped bool
		err     error
	}
	done := make(chan result, 1)
	go func() {
		dropped, err := PartitionDropTable(ctx, db, parent, child)
		done <- result{dropped: dropped, err: err}
	}()

	deadline := time.Now().Add(10 * time.Second)
	for {
		var waiting int64
		err := db.
			Raw("SELECT count(*) FROM pg_catalog.pg_locks WHERE NOT granted AND relation IN (?::regclass, ?::regclass)", parent.quoted(), child.quoted()).
			Row().
			Scan(&waiting)
		if err != nil {
			t.Fatalf("count waiting locks: %v", err)
		}
		if waiting > 0 {
			break
		}
		select {
		case r := <-done:
			t.Fatalf("the undo finished without waiting for the insert: %v, %v", r.dropped, r.err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("the undo never waited for the insert in flight")
		}
		time.Sleep(20 * time.Millisecond)
	}

	err = inserter.Commit().Error
	if err != nil {
		t.Fatalf("commit the insert: %v", err)
	}

	r := <-done
	if !errors.Is(r.err, ErrPartitionNotEmpty) || r.dropped {
		t.Fatalf("PartitionDropTable = %v, %v; want false, ErrPartitionNotEmpty", r.dropped, r.err)
	}
	if got := countRows(t, db, child); got != 1 {
		t.Fatalf("the partition holds %d rows, want the inserted 1", got)
	}
}
