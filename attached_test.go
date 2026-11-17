package san_pgpartition

import (
	"context"
	"testing"
)

func TestPartitionAttached(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)

	orders := TableName{Schema: schema, Name: "orders"}
	payments := TableName{Schema: schema, Name: "payments"}
	ordersJan := TableName{Schema: schema, Name: "orders_2026_01"}
	paymentsJan := TableName{Schema: schema, Name: "payments_2026_01"}
	plain := TableName{Schema: schema, Name: "orders_plain"}

	mustExec(t, db, "CREATE TABLE "+orders.quoted()+" (id bigint NOT NULL, created_at date NOT NULL) PARTITION BY RANGE (created_at)")
	mustExec(t, db, "CREATE TABLE "+payments.quoted()+" (id bigint NOT NULL, created_at date NOT NULL) PARTITION BY RANGE (created_at)")
	mustExec(t, db, "CREATE TABLE "+ordersJan.quoted()+" PARTITION OF "+orders.quoted()+" FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')")
	mustExec(t, db, "CREATE TABLE "+paymentsJan.quoted()+" PARTITION OF "+payments.quoted()+" FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')")
	mustExec(t, db, "CREATE TABLE "+plain.quoted()+" (id bigint)")

	cases := []struct {
		name  string
		child TableName
		want  bool
	}{
		{name: "attached to the parent", child: ordersJan, want: true},
		{name: "missing", child: TableName{Schema: schema, Name: "orders_2026_02"}, want: false},
		{name: "partition of another parent", child: paymentsJan, want: false},
		{name: "ordinary table with no parent", child: plain, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := PartitionAttached(ctx, db, orders, tc.child)
			if err != nil {
				t.Fatalf("PartitionAttached: %v", err)
			}
			if got != tc.want {
				t.Fatalf("PartitionAttached = %v, want %v", got, tc.want)
			}
		})
	}
}
