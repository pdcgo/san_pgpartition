package san_pgpartition

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestPartitionNewTableIsSafeToRetry(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)

	parent := TableName{Schema: schema, Name: "orders"}
	child := TableName{Schema: schema, Name: "orders_2026_01"}
	mustExec(t, db, "CREATE TABLE "+parent.quoted()+" (id bigint NOT NULL, created_at date NOT NULL) PARTITION BY RANGE (created_at)")

	january := PartitionBound{Range: &RangeBound{From: []any{"2026-01-01"}, To: []any{"2026-02-01"}}}
	created, err := PartitionNewTable(ctx, db, parent, child, january)
	if err != nil || !created {
		t.Fatalf("first call = %v, %v; want true, nil", created, err)
	}

	t.Run("the same bound again", func(t *testing.T) {
		created, err := PartitionNewTable(ctx, db, parent, child, january)
		if err != nil || created {
			t.Fatalf("retry = %v, %v; want false, nil", created, err)
		}
	})

	t.Run("the same bound written another way", func(t *testing.T) {
		wib := time.FixedZone("WIB", 7*60*60)
		sameDays := PartitionBound{Range: &RangeBound{
			From: []any{time.Date(2026, 1, 1, 0, 0, 0, 0, wib)},
			To:   []any{"2026-02-01"},
		}}
		created, err := PartitionNewTable(ctx, db, parent, child, sameDays)
		if err != nil || created {
			t.Fatalf("retry = %v, %v; want false, nil", created, err)
		}
	})

	t.Run("inside the caller's transaction, leaving no probe tables", func(t *testing.T) {
		err := db.Transaction(func(tx *gorm.DB) error {
			created, err := PartitionNewTable(ctx, tx, parent, child, january)
			if err != nil || created {
				t.Fatalf("retry = %v, %v; want false, nil", created, err)
			}

			var probes int64
			err = tx.
				Raw("SELECT count(*) FROM pg_catalog.pg_class WHERE relname LIKE 'san_pgpartition_probe%'").
				Row().
				Scan(&probes)
			if err != nil {
				t.Fatalf("count probe tables: %v", err)
			}
			if probes != 0 {
				t.Fatalf("%d probe tables were left behind", probes)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("caller transaction: %v", err)
		}
	})
}

func TestPartitionNewTableRefusesConflicts(t *testing.T) {
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

	january := PartitionBound{Range: &RangeBound{From: []any{"2026-01-01"}, To: []any{"2026-02-01"}}}
	cases := []struct {
		name  string
		child TableName
		bound PartitionBound
	}{
		{name: "same name, different bounds", child: ordersJan, bound: PartitionBound{Range: &RangeBound{From: []any{"2026-01-01"}, To: []any{"2026-03-01"}}}},
		{name: "name taken by an ordinary table", child: plain, bound: PartitionBound{Range: &RangeBound{From: []any{"2026-05-01"}, To: []any{"2026-06-01"}}}},
		{name: "name taken by a partition of another parent", child: paymentsJan, bound: january},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			created, err := PartitionNewTable(ctx, db, orders, tc.child, tc.bound)
			if !errors.Is(err, ErrPartitionConflict) {
				t.Fatalf("error = %v, want ErrPartitionConflict", err)
			}
			if created {
				t.Fatal("created = true on a conflict")
			}
		})
	}

	mustContain(t, partitionBoundText(t, db, ordersJan), "TO ('2026-02-01')")
}

func TestPartitionNewTableConcurrentCallersCreateOnce(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)

	parent := TableName{Schema: schema, Name: "orders"}
	child := TableName{Schema: schema, Name: "orders_2026_01"}
	mustExec(t, db, "CREATE TABLE "+parent.quoted()+" (id bigint NOT NULL, created_at date NOT NULL) PARTITION BY RANGE (created_at)")

	const callers = 8
	january := PartitionBound{Range: &RangeBound{From: []any{"2026-01-01"}, To: []any{"2026-02-01"}}}

	// Hold the parent so every caller reads "child missing" and then waits;
	// releasing it makes them race for real instead of running one by one.
	holder := db.Begin()
	t.Cleanup(func() {
		holder.Rollback()
	})
	mustExec(t, holder, "LOCK TABLE "+parent.quoted()+" IN ACCESS EXCLUSIVE MODE")

	var wg sync.WaitGroup
	results := make([]bool, callers)
	errs := make([]error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = PartitionNewTable(ctx, db, parent, child, january)
		}(i)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		var waiting int64
		err := db.
			Raw("SELECT count(*) FROM pg_catalog.pg_locks WHERE NOT granted AND relation = ?::regclass", parent.quoted()).
			Row().
			Scan(&waiting)
		if err != nil {
			t.Fatalf("count waiting callers: %v", err)
		}
		if waiting == callers {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d callers reached the parent lock", waiting, callers)
		}
		time.Sleep(20 * time.Millisecond)
	}

	err := holder.Commit().Error
	if err != nil {
		t.Fatalf("release the parent: %v", err)
	}
	wg.Wait()

	createdCount := 0
	for i := 0; i < callers; i++ {
		if errs[i] != nil {
			t.Fatalf("caller %d: %v", i, errs[i])
		}
		if results[i] {
			createdCount++
		}
	}
	if createdCount != 1 {
		t.Fatalf("%d callers report created, want exactly 1", createdCount)
	}
}
