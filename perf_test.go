package san_pgpartition

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"gorm.io/gorm"
)

// The timings below run only with SAN_PGPARTITION_PERF=1 (and the test DSN):
//
//	SAN_PGPARTITION_PERF=1 go test . -run Perf -v -count=1 -p 1
func skipUnlessPerf(t *testing.T) {
	t.Helper()

	if os.Getenv("SAN_PGPARTITION_PERF") == "" {
		t.Skip("SAN_PGPARTITION_PERF is not set")
	}
}

// TestPerfConvertDailyChildren converts one row a day over
// SAN_PGPARTITION_PERF_DAYS days (default 1200) into as many daily children.
// Each child with the test table's 5 indexes and foreign key holds about 14
// locks until the commit; on PostgreSQL's default max_locks_per_transaction
// (64) the lock table runs out near 1,100 children ("out of shared memory").
func TestPerfConvertDailyChildren(t *testing.T) {
	skipUnlessPerf(t)
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)

	days := 1200
	if text := os.Getenv("SAN_PGPARTITION_PERF_DAYS"); text != "" {
		parsed, err := strconv.Atoi(text)
		if err != nil {
			t.Fatalf("SAN_PGPARTITION_PERF_DAYS: %v", err)
		}
		days = parsed
	}

	logs := createLogTable(t, db, schema, "logs")
	mustExec(t, db, "INSERT INTO "+logs.quoted()+` (team_id, product_id, note, created_at)
SELECT 1 + g % 2, g, 'note ' || g, timestamptz '2023-08-06 12:00+07' + g * interval '1 day'
FROM generate_series(0, ? - 1) AS g`, days)
	// Dropping the schema in one go would need as many locks as the
	// conversion; drop the children a few hundred at a time first.
	t.Cleanup(func() {
		dropChildrenInBatches(t, db, logs)
	})

	var maxLocks string
	err := db.
		Raw("SHOW max_locks_per_transaction").
		Row().
		Scan(&maxLocks)
	if err != nil {
		t.Fatalf("read max_locks_per_transaction: %v", err)
	}

	started := time.Now()
	err = m.PartitionExistTable(ctx, logs, jakartaConfig(t, RangeTypeDaily))
	elapsed := time.Since(started)
	if err != nil {
		t.Fatalf("PartitionExistTable (%d days, max_locks_per_transaction %s) after %s: %v", days, maxLocks, elapsed, err)
	}

	children := len(partitionNames(t, db, logs)) - 1
	t.Logf("daily: %d children + DEFAULT, %d rows, 5 indexes and 1 foreign key each, max_locks_per_transaction %s: %s", children, days, maxLocks, elapsed)
	if children != days {
		t.Fatalf("%d children, want %d", children, days)
	}
}

func TestPerfConvert200kRows(t *testing.T) {
	skipUnlessPerf(t)
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)

	for _, tc := range []struct {
		rangeType RangeType
		days      int
	}{
		{rangeType: RangeTypeMonthly, days: 730},
		{rangeType: RangeTypeDaily, days: 730},
	} {
		m := newTestManager(t, db)
		logs := createLogTable(t, db, schema, "logs_"+string(tc.rangeType))
		mustExec(t, db, "INSERT INTO "+logs.quoted()+` (team_id, product_id, note, created_at)
SELECT 1 + g % 2, g % 5000, 'note ' || g, timestamptz '2024-11-17 00:00+07' + (g % ?) * interval '1 day' + (g % 86400) * interval '1 second'
FROM generate_series(1, 200000) AS g`, tc.days)

		started := time.Now()
		err := m.PartitionExistTable(ctx, logs, jakartaConfig(t, tc.rangeType))
		elapsed := time.Since(started)
		if err != nil {
			t.Fatalf("%s: PartitionExistTable: %v", tc.rangeType, err)
		}

		children := len(partitionNames(t, db, logs)) - 1
		t.Logf("%s: 200,000 rows over %d days into %d children + DEFAULT: %s", tc.rangeType, tc.days, children, elapsed)
		if got := countRows(t, db, logs); got != 200000 {
			t.Fatalf("%s: %d rows after the conversion, want 200000", tc.rangeType, got)
		}
	}
}

// dropChildrenInBatches drops parent's partitions 200 per transaction.
func dropChildrenInBatches(t *testing.T, db *gorm.DB, parent TableName) {
	t.Helper()

	for {
		var names []string
		err := db.
			Raw(`SELECT c.relname
FROM pg_catalog.pg_inherits AS i
JOIN pg_catalog.pg_class AS c ON c.oid = i.inhrelid AND c.relkind = 'r'
WHERE i.inhparent = pg_catalog.to_regclass(?)
LIMIT 200`, parent.quoted()).
			Scan(&names).
			Error
		if err != nil {
			t.Errorf("list the children of %s: %v", parent, err)
			return
		}
		if len(names) == 0 {
			return
		}

		err = db.Transaction(func(tx *gorm.DB) error {
			for _, name := range names {
				txErr := tx.
					Exec("DROP TABLE " + TableName{Schema: parent.Schema, Name: name}.quoted()).
					Error
				if txErr != nil {
					return txErr
				}
			}
			return nil
		})
		if err != nil {
			t.Errorf("drop children of %s: %v", parent, err)
			return
		}
	}
}

// TestPerfProvisionAndMaintain times the cron's two calls: provisioning a
// year of daily children; creating 6 empty future months while DEFAULT holds
// 200,000 back-dated rows (each create scans DEFAULT); and MaintainPartition
// moving 200,000 DEFAULT rows of the current month into its new child.
func TestPerfProvisionAndMaintain(t *testing.T) {
	skipUnlessPerf(t)
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	jakarta := mustLoadLocation(t, "Asia/Jakarta")

	m := newTestManager(t, db)
	m.now = clockAt(t, jakarta, 2026, time.November, 17, 9)
	daily := createLogTable(t, db, schema, "daily")
	insertLog(t, db, daily, 1, "2026-11-17 08:00+07")
	err := m.PartitionExistTable(ctx, daily, jakartaConfig(t, RangeTypeDaily))
	if err != nil {
		t.Fatalf("PartitionExistTable(daily): %v", err)
	}
	t.Cleanup(func() {
		dropChildrenInBatches(t, db, daily)
	})
	started := time.Now()
	err = m.MaintainPartitionTableProvision(ctx, 365)
	if err != nil {
		t.Fatalf("provision 365 days: %v", err)
	}
	t.Logf("provision 365 daily children (5 indexes + 1 foreign key each): %s", time.Since(started))

	m = newTestManager(t, db)
	m.now = clockAt(t, jakarta, 2026, time.November, 17, 9)
	monthly := createLogTable(t, db, schema, "monthly")
	insertLog(t, db, monthly, 1, "2026-10-05 08:00+07")
	err = m.PartitionExistTable(ctx, monthly, jakartaConfig(t, RangeTypeMonthly))
	if err != nil {
		t.Fatalf("PartitionExistTable(monthly): %v", err)
	}
	mustExec(t, db, "INSERT INTO "+monthly.quoted()+` (team_id, product_id, note, created_at)
SELECT 1 + g % 2, g, 'old ' || g, timestamptz '2019-03-01 00:00+07' + (g % 86400) * interval '1 second'
FROM generate_series(1, 200000) AS g`)
	mustExec(t, db, "ANALYZE "+monthly.quoted())

	started = time.Now()
	err = m.MaintainPartitionTableProvision(ctx, 6)
	if err != nil {
		t.Fatalf("provision 6 months: %v", err)
	}
	t.Logf("provision 7 empty monthly children, DEFAULT holding 200,000 other rows: %s", time.Since(started))

	mustExec(t, db, "DELETE FROM "+TableName{Schema: schema, Name: "monthly_default"}.quoted())
	dropped, err := PartitionDropTable(ctx, db, monthly, TableName{Schema: schema, Name: "monthly_202611"})
	if err != nil || !dropped {
		t.Fatalf("PartitionDropTable(monthly_202611) = %v, %v", dropped, err)
	}
	mustExec(t, db, "INSERT INTO "+monthly.quoted()+` (team_id, product_id, note, created_at)
SELECT 1 + g % 2, g, 'november ' || g, timestamptz '2026-11-01 00:00+07' + (g % 2592000) * interval '1 second'
FROM generate_series(1, 200000) AS g`)

	started = time.Now()
	reports, err := m.MaintainPartition(ctx)
	if err != nil {
		t.Fatalf("MaintainPartition: %v", err)
	}
	t.Logf("MaintainPartition: %v in %s", reports, time.Since(started))
}

// TestPerfReshape converts users (20,000 rows over 40 months) while orders
// (200,000 rows) points at it, then times 20,000 app inserts into orders
// through the fill trigger against 20,000 into a copy of orders without it.
func TestPerfReshape(t *testing.T) {
	skipUnlessPerf(t)
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)

	users := TableName{Schema: schema, Name: "users"}
	orders := TableName{Schema: schema, Name: "orders"}
	plain := TableName{Schema: schema, Name: "orders_plain"}
	execIn(t, db, schema,
		"CREATE TABLE $S.users (id BIGSERIAL PRIMARY KEY, created_at TIMESTAMPTZ NOT NULL)",
		"INSERT INTO $S.users (created_at) SELECT timestamptz '2023-08-01 12:00+07' + (g % 40) * interval '1 month' FROM generate_series(1, 20000) AS g",
		"CREATE TABLE $S.orders (id BIGSERIAL PRIMARY KEY, user_id BIGINT NOT NULL REFERENCES $S.users (id), note TEXT)",
		"INSERT INTO $S.orders (user_id, note) SELECT 1 + g % 20000, 'order ' || g FROM generate_series(1, 200000) AS g",
	)

	started := time.Now()
	err := m.PartitionExistTable(ctx, users, jakartaConfig(t, RangeTypeMonthly))
	elapsed := time.Since(started)
	if err != nil {
		t.Fatalf("PartitionExistTable: %v", err)
	}
	t.Logf("users 20,000 rows into %d monthly children, orders 200,000 rows reshaped: %s", len(partitionNames(t, db, users))-1, elapsed)

	// The same table without the trigger: its key on (user_id,
	// user_created_at) checks the same, the app fills the column itself.
	execIn(t, db, schema,
		"CREATE TABLE $S.orders_plain (id BIGSERIAL PRIMARY KEY, user_id BIGINT NOT NULL, user_created_at TIMESTAMPTZ NOT NULL, note TEXT, FOREIGN KEY (user_id, user_created_at) REFERENCES $S.users (id, created_at))",
	)
	var createdAts []time.Time
	err = db.
		Raw("SELECT created_at FROM " + users.quoted() + " ORDER BY id").
		Scan(&createdAts).
		Error
	if err != nil {
		t.Fatalf("read the users: %v", err)
	}

	started = time.Now()
	for i := 0; i < 20000; i++ {
		mustExec(t, db, "INSERT INTO "+orders.quoted()+" (user_id, note) VALUES (?, 'app')", 1+i)
	}
	t.Logf("20,000 single-row inserts with the fill trigger (the app sends user_id only): %s", time.Since(started))

	started = time.Now()
	for i := 0; i < 20000; i++ {
		mustExec(t, db, "INSERT INTO "+plain.quoted()+" (user_id, user_created_at, note) VALUES (?, ?, 'app')", 1+i, createdAts[i])
	}
	t.Logf("20,000 single-row inserts without a trigger (the app sends both columns): %s", time.Since(started))
}
