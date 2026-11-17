package san_pgpartition

import (
	"context"
	"errors"
	"testing"
)

func TestPartitionNewTableStrategies(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)

	t.Run("composite RANGE key", func(t *testing.T) {
		parent := TableName{Schema: schema, Name: "sales"}
		child := TableName{Schema: schema, Name: "sales_jkt_2026_01"}
		mustExec(t, db, "CREATE TABLE "+parent.quoted()+" (region text NOT NULL, sold_on date NOT NULL) PARTITION BY RANGE (region, sold_on)")

		created, err := PartitionNewTable(ctx, db, parent, child, PartitionBound{
			Range: &RangeBound{From: []any{"jkt", "2026-01-01"}, To: []any{"jkt", "2026-02-01"}},
		})
		if err != nil || !created {
			t.Fatalf("PartitionNewTable = %v, %v; want true, nil", created, err)
		}
		mustContain(t, partitionBoundText(t, db, child), "FROM ('jkt', '2026-01-01') TO ('jkt', '2026-02-01')")
	})

	t.Run("LIST with a NULL value", func(t *testing.T) {
		parent := TableName{Schema: schema, Name: "shipments"}
		child := TableName{Schema: schema, Name: "shipments_java"}
		mustExec(t, db, "CREATE TABLE "+parent.quoted()+" (id bigint NOT NULL, island text) PARTITION BY LIST (island)")

		created, err := PartitionNewTable(ctx, db, parent, child, PartitionBound{
			List: &ListBound{Values: []any{"java", "bali", nil}},
		})
		if err != nil || !created {
			t.Fatalf("PartitionNewTable = %v, %v; want true, nil", created, err)
		}

		mustExec(t, db, "INSERT INTO "+parent.quoted()+" (id, island) VALUES (1, 'bali'), (2, NULL)")
		if got := countRows(t, db, child); got != 2 {
			t.Fatalf("child holds %d rows, want 2", got)
		}
	})

	t.Run("LIST values that look like SQL or placeholders", func(t *testing.T) {
		parent := TableName{Schema: schema, Name: "tags"}
		child := TableName{Schema: schema, Name: "tags_odd"}
		mustExec(t, db, "CREATE TABLE "+parent.quoted()+" (id bigint NOT NULL, tag text NOT NULL) PARTITION BY LIST (tag)")

		odd := []any{"o'brien", "what?", "$1", "@name", `back\slash`, "x'); DROP TABLE tags; --"}
		created, err := PartitionNewTable(ctx, db, parent, child, PartitionBound{List: &ListBound{Values: odd}})
		if err != nil || !created {
			t.Fatalf("PartitionNewTable = %v, %v; want true, nil", created, err)
		}

		for i, v := range odd {
			mustExec(t, db, "INSERT INTO "+parent.quoted()+" (id, tag) VALUES (?, ?)", int64(i), v)
		}
		if got := countRows(t, db, child); got != int64(len(odd)) {
			t.Fatalf("child holds %d rows, want %d", got, len(odd))
		}

		created, err = PartitionNewTable(ctx, db, parent, child, PartitionBound{List: &ListBound{Values: odd}})
		if err != nil || created {
			t.Fatalf("retry = %v, %v; want false, nil", created, err)
		}
	})

	t.Run("HASH", func(t *testing.T) {
		parent := TableName{Schema: schema, Name: "events"}
		mustExec(t, db, "CREATE TABLE "+parent.quoted()+" (id bigint NOT NULL, team_id bigint NOT NULL) PARTITION BY HASH (id, team_id)")

		for remainder := 0; remainder < 2; remainder++ {
			child := TableName{Schema: schema, Name: "events_h" + string(rune('0'+remainder))}
			created, err := PartitionNewTable(ctx, db, parent, child, PartitionBound{
				Hash: &HashBound{Modulus: 2, Remainder: remainder},
			})
			if err != nil || !created {
				t.Fatalf("PartitionNewTable remainder %d = %v, %v; want true, nil", remainder, created, err)
			}
		}

		mustExec(t, db, "INSERT INTO "+parent.quoted()+" (id, team_id) SELECT g, g % 7 FROM generate_series(1, 100) AS g")
		total := countRows(t, db, TableName{Schema: schema, Name: "events_h0"}) + countRows(t, db, TableName{Schema: schema, Name: "events_h1"})
		if total != 100 {
			t.Fatalf("hash partitions hold %d rows, want 100", total)
		}
	})
}

func TestPartitionNewTableRejectsBadInput(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)

	rangeParent := TableName{Schema: schema, Name: "orders"}
	listParent := TableName{Schema: schema, Name: "shipments"}
	plain := TableName{Schema: schema, Name: "plain"}
	mustExec(t, db, "CREATE TABLE "+rangeParent.quoted()+" (id bigint NOT NULL, created_at date NOT NULL) PARTITION BY RANGE (created_at)")
	mustExec(t, db, "CREATE TABLE "+listParent.quoted()+" (id bigint NOT NULL, island text) PARTITION BY LIST (island)")
	mustExec(t, db, "CREATE TABLE "+plain.quoted()+" (id bigint)")

	child := TableName{Schema: schema, Name: "child"}
	january := &RangeBound{From: []any{"2026-01-01"}, To: []any{"2026-02-01"}}

	cases := []struct {
		name   string
		parent TableName
		child  TableName
		bound  PartitionBound
		want   error
	}{
		{name: "no bound", parent: rangeParent, child: child, bound: PartitionBound{}, want: ErrInvalidBound},
		{name: "two strategies", parent: rangeParent, child: child, bound: PartitionBound{Range: january, List: &ListBound{Values: []any{"x"}}}, want: ErrInvalidBound},
		{name: "LIST bound for a RANGE parent", parent: rangeParent, child: child, bound: PartitionBound{List: &ListBound{Values: []any{"x"}}}, want: ErrInvalidBound},
		{name: "RANGE bound for a LIST parent", parent: listParent, child: child, bound: PartitionBound{Range: january}, want: ErrInvalidBound},
		{name: "too many RANGE values", parent: rangeParent, child: child, bound: PartitionBound{Range: &RangeBound{From: []any{"2026-01-01", 1}, To: []any{"2026-02-01", 2}}}, want: ErrInvalidBound},
		{name: "NULL in a RANGE bound", parent: rangeParent, child: child, bound: PartitionBound{Range: &RangeBound{From: []any{nil}, To: []any{"2026-02-01"}}}, want: ErrInvalidBound},
		{name: "empty LIST", parent: listParent, child: child, bound: PartitionBound{List: &ListBound{}}, want: ErrInvalidBound},
		{name: "HASH modulus zero", parent: rangeParent, child: child, bound: PartitionBound{Hash: &HashBound{Modulus: 0, Remainder: 0}}, want: ErrInvalidBound},
		{name: "HASH remainder not below modulus", parent: rangeParent, child: child, bound: PartitionBound{Hash: &HashBound{Modulus: 4, Remainder: 4}}, want: ErrInvalidBound},
		{name: "parent is an ordinary table", parent: plain, child: child, bound: PartitionBound{Range: january}, want: ErrParentNotPartitioned},
		{name: "parent does not exist", parent: TableName{Schema: schema, Name: "nope"}, child: child, bound: PartitionBound{Range: january}, want: ErrParentNotPartitioned},
		{name: "empty schema", parent: TableName{Name: "orders"}, child: child, bound: PartitionBound{Range: january}, want: ErrInvalidTableName},
		{name: "name over 63 bytes", parent: rangeParent, child: TableName{Schema: schema, Name: "orders_partition_name_that_is_far_too_long_for_postgres_identifiers"}, bound: PartitionBound{Range: january}, want: ErrInvalidTableName},
		{name: "child equals parent", parent: rangeParent, child: rangeParent, bound: PartitionBound{Range: january}, want: ErrInvalidTableName},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			created, err := PartitionNewTable(ctx, db, tc.parent, tc.child, tc.bound)
			if !errors.Is(err, tc.want) {
				t.Fatalf("PartitionNewTable error = %v, want %v", err, tc.want)
			}
			if created {
				t.Fatal("created = true on a rejected call")
			}
		})
	}

	exists, err := PartitionAttached(ctx, db, rangeParent, child)
	if err != nil {
		t.Fatalf("PartitionAttached: %v", err)
	}
	if exists {
		t.Fatal("a rejected call left a partition behind")
	}
}
