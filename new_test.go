package san_pgpartition

import (
	"context"
	"testing"
)

func TestPartitionNewTableCreatesRangePartitionAndRoutesRows(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)

	parent := TableName{Schema: schema, Name: "orders"}
	child := TableName{Schema: schema, Name: "orders_2026_01"}
	mustExec(t, db, "CREATE TABLE "+parent.quoted()+" (id bigint NOT NULL, created_at date NOT NULL) PARTITION BY RANGE (created_at)")

	created, err := PartitionNewTable(ctx, db, parent, child, PartitionBound{
		Range: &RangeBound{From: []any{"2026-01-01"}, To: []any{"2026-02-01"}},
	})
	if err != nil {
		t.Fatalf("PartitionNewTable: %v", err)
	}
	if !created {
		t.Fatal("created = false, want true for a fresh partition")
	}

	exists, err := PartitionAttached(ctx, db, parent, child)
	if err != nil {
		t.Fatalf("PartitionAttached: %v", err)
	}
	if !exists {
		t.Fatal("the new child is not attached to its parent")
	}

	mustExec(t, db, "INSERT INTO "+parent.quoted()+" (id, created_at) VALUES (?, ?)", int64(17), "2026-01-15")
	if got := countRows(t, db, child); got != 1 {
		t.Fatalf("child holds %d rows, want 1", got)
	}
}
