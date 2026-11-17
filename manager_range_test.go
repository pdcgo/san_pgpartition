package san_pgpartition

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestManagerPartitionExistTableDaily(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)

	logs := createLogTable(t, db, schema, "logs")
	lateMonday := insertLog(t, db, logs, 1, "2026-11-16 23:59+07")
	tuesday := insertLog(t, db, logs, 1, "2026-11-17 00:00+07")
	thursday := insertLog(t, db, logs, 2, "2026-11-19 12:00+07")

	err := m.PartitionExistTable(ctx, logs, jakartaConfig(t, RangeTypeDaily))
	if err != nil {
		t.Fatalf("PartitionExistTable: %v", err)
	}

	// Every day from the oldest row to the newest, the empty Wednesday too.
	mustEqualStrings(t, "partitions", partitionNames(t, db, logs), []string{"logs_20261116", "logs_20261117", "logs_20261118", "logs_20261119", "logs_default"})
	for id, want := range map[int64]string{lateMonday: "logs_20261116", tuesday: "logs_20261117", thursday: "logs_20261119"} {
		if got := partitionOfRow(t, db, logs, id); got != want {
			t.Fatalf("row %d is in %s, want %s", id, got, want)
		}
	}
}

func TestManagerPartitionExistTableYearly(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)

	logs := createLogTable(t, db, schema, "logs")
	lastOf2025 := insertLog(t, db, logs, 1, "2025-12-31 23:00+07")
	firstOf2026 := insertLog(t, db, logs, 1, "2026-01-01 00:00+07")

	err := m.PartitionExistTable(ctx, logs, jakartaConfig(t, RangeTypeYearly))
	if err != nil {
		t.Fatalf("PartitionExistTable: %v", err)
	}

	mustEqualStrings(t, "partitions", partitionNames(t, db, logs), []string{"logs_2025", "logs_2026", "logs_default"})
	if got := partitionOfRow(t, db, logs, lastOf2025); got != "logs_2025" {
		t.Fatalf("the last row of 2025 (Jakarta) is in %s", got)
	}
	if got := partitionOfRow(t, db, logs, firstOf2026); got != "logs_2026" {
		t.Fatalf("the first row of 2026 (Jakarta) is in %s", got)
	}
}

func TestManagerPartitionExistTableFixed(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)

	t.Run("blocks of 4 on the id", func(t *testing.T) {
		items := TableName{Schema: schema, Name: "items"}
		mustExec(t, db, "CREATE TABLE "+items.quoted()+" (id bigserial PRIMARY KEY, note text)")
		mustExec(t, db, "INSERT INTO "+items.quoted()+" (note) SELECT 'n' || g FROM generate_series(1, 7) AS g")

		err := m.PartitionExistTable(ctx, items, &PartitionRangeConfig{RangeType: FixedRange, BlockSize: 4, Key: "id"})
		if err != nil {
			t.Fatalf("PartitionExistTable: %v", err)
		}

		mustEqualStrings(t, "partitions", partitionsOf(t, db, items), []string{
			"items_0_3 FOR VALUES FROM ('0') TO ('4')",
			"items_4_7 FOR VALUES FROM ('4') TO ('8')",
			"items_default DEFAULT",
		})
		if got := countRows(t, db, TableName{Schema: schema, Name: "items_0_3"}); got != 3 {
			t.Fatalf("items_0_3 holds %d rows, want ids 1-3", got)
		}
		if got := countRows(t, db, TableName{Schema: schema, Name: "items_4_7"}); got != 4 {
			t.Fatalf("items_4_7 holds %d rows, want ids 4-7", got)
		}
		// A primary key that already holds the key is kept as it is.
		mustEqualStrings(t, "primary key", constraintDefs(t, db, items, "p"), []string{"items_pkey PRIMARY KEY (id)"})
	})

	t.Run("negative numbers", func(t *testing.T) {
		nums := TableName{Schema: schema, Name: "nums"}
		mustExec(t, db, "CREATE TABLE "+nums.quoted()+" (id bigserial PRIMARY KEY, num integer)")
		mustExec(t, db, "INSERT INTO "+nums.quoted()+" (num) VALUES (-5), (-1), (0), (2)")

		err := m.PartitionExistTable(ctx, nums, &PartitionRangeConfig{RangeType: FixedRange, BlockSize: 4, Key: "num"})
		if err != nil {
			t.Fatalf("PartitionExistTable: %v", err)
		}

		mustEqualStrings(t, "partitions", partitionsOf(t, db, nums), []string{
			"nums_0_3 FOR VALUES FROM (0) TO (4)",
			"nums_default DEFAULT",
			"nums_m4_m1 FOR VALUES FROM ('-4') TO (0)",
			"nums_m8_m5 FOR VALUES FROM ('-8') TO ('-4')",
		})
		if got := countRows(t, db, TableName{Schema: schema, Name: "nums_default"}); got != 0 {
			t.Fatalf("nums_default holds %d rows, want 0", got)
		}
		mustEqualStrings(t, "primary key", constraintDefs(t, db, nums, "p"), []string{"nums_pkey PRIMARY KEY (id, num)"})
	})

	t.Run("a child name past 63 bytes", func(t *testing.T) {
		long := TableName{Schema: schema, Name: strings.Repeat("f", 50)}
		mustExec(t, db, "CREATE TABLE "+long.quoted()+" (num bigint)")
		mustExec(t, db, "INSERT INTO "+long.quoted()+" VALUES (1500000)")
		before := snapshotTable(t, db, m, long)

		// The child would be <50 bytes>_1000000_1999999: 66 bytes.
		err := m.PartitionExistTable(ctx, long, &PartitionRangeConfig{RangeType: FixedRange, BlockSize: 1000000, Key: "num"})
		if !errors.Is(err, ErrInvalidTableName) {
			t.Fatalf("PartitionExistTable = %v, want ErrInvalidTableName", err)
		}
		mustEqualStrings(t, "the table after the refusal", snapshotTable(t, db, m, long), before)
	})
}

func TestManagerPartitionExistTableTimestampAndDateKeys(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)

	t.Run("timestamp without time zone is cut at its own wall clock", func(t *testing.T) {
		visits := TableName{Schema: schema, Name: "visits"}
		mustExec(t, db, "CREATE TABLE "+visits.quoted()+" (id bigserial PRIMARY KEY, seen_at timestamp NOT NULL)")
		mustExec(t, db, "INSERT INTO "+visits.quoted()+" (seen_at) VALUES ('2026-10-31 23:30'), ('2026-11-01 00:10')")

		err := m.PartitionExistTable(ctx, visits, &PartitionRangeConfig{Location: mustLoadLocation(t, "Asia/Jakarta"), RangeType: RangeTypeMonthly, Key: "seen_at"})
		if err != nil {
			t.Fatalf("PartitionExistTable: %v", err)
		}

		mustEqualStrings(t, "partitions", partitionsOf(t, db, visits), []string{
			"visits_202610 FOR VALUES FROM ('2026-10-01 00:00:00') TO ('2026-11-01 00:00:00')",
			"visits_202611 FOR VALUES FROM ('2026-11-01 00:00:00') TO ('2026-12-01 00:00:00')",
			"visits_default DEFAULT",
		})
	})

	t.Run("date, with NULL and infinity going to DEFAULT", func(t *testing.T) {
		// No primary key: extended to (id, day) it would make day NOT NULL.
		days := TableName{Schema: schema, Name: "days"}
		mustExec(t, db, "CREATE TABLE "+days.quoted()+" (id bigint, day date)")
		mustExec(t, db, "INSERT INTO "+days.quoted()+" (day) VALUES ('2026-01-15'), ('2026-03-01'), (NULL), ('infinity')")

		err := m.PartitionExistTable(ctx, days, &PartitionRangeConfig{Location: mustLoadLocation(t, "Asia/Jakarta"), RangeType: RangeTypeMonthly, Key: "day"})
		if err != nil {
			t.Fatalf("PartitionExistTable: %v", err)
		}

		mustEqualStrings(t, "partitions", partitionsOf(t, db, days), []string{
			"days_202601 FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')",
			"days_202602 FOR VALUES FROM ('2026-02-01') TO ('2026-03-01')",
			"days_202603 FOR VALUES FROM ('2026-03-01') TO ('2026-04-01')",
			"days_default DEFAULT",
		})
		if got := countRows(t, db, TableName{Schema: schema, Name: "days_default"}); got != 2 {
			t.Fatalf("days_default holds %d rows, want the NULL and the infinity", got)
		}
	})
}

// An extended primary key makes the key column NOT NULL, so a row with a NULL
// key cannot be copied: the conversion fails and changes nothing.
func TestManagerPartitionExistTableFailsOnANullKeyUnderAnExtendedPrimaryKey(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)

	days := TableName{Schema: schema, Name: "days"}
	mustExec(t, db, "CREATE TABLE "+days.quoted()+" (id bigserial PRIMARY KEY, day date)")
	mustExec(t, db, "INSERT INTO "+days.quoted()+" (day) VALUES ('2026-01-15'), (NULL)")
	before := snapshotTable(t, db, m, days)

	err := m.PartitionExistTable(ctx, days, &PartitionRangeConfig{Location: mustLoadLocation(t, "Asia/Jakarta"), RangeType: RangeTypeMonthly, Key: "day"})
	if err == nil {
		t.Fatal("PartitionExistTable copied a NULL key under PRIMARY KEY (id, day)")
	}
	mustContain(t, err.Error(), "null value in column \"day\"")
	mustEqualStrings(t, "the table after the failure", snapshotTable(t, db, m, days), before)
}

func TestManagerPartitionExistTableMarksEveryTableItCreates(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)

	logs := createLogTable(t, db, schema, "logs")
	mustExec(t, db, "COMMENT ON TABLE "+logs.quoted()+" IS 'append-only team log'")
	insertLog(t, db, logs, 1, "2026-10-31 23:30+07")
	insertLog(t, db, logs, 1, "2026-12-01 10:00+07")

	err := m.PartitionExistTable(ctx, logs, jakartaConfig(t, RangeTypeMonthly))
	if err != nil {
		t.Fatalf("PartitionExistTable: %v", err)
	}

	for _, name := range partitionNames(t, db, logs) {
		if got := tableComment(t, db, TableName{Schema: schema, Name: name}); got != createdMarker {
			t.Fatalf("%s has comment %q, want %q", name, got, createdMarker)
		}
	}
	// The table itself keeps its own comment.
	if got := tableComment(t, db, logs); got != "append-only team log" {
		t.Fatalf("the new table's comment = %q, want the original", got)
	}

	// The empty November child is the package's, so PartitionDropTable can
	// undo it.
	november := TableName{Schema: schema, Name: "logs_202611"}
	dropped, err := PartitionDropTable(ctx, db, logs, november)
	if err != nil || !dropped {
		t.Fatalf("PartitionDropTable(%s) = %v, %v; want true, nil", november, dropped, err)
	}
}
