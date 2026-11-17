package san_pgpartition

import (
	"context"
	"testing"
	"time"
)

func TestNewManagerTouchesNothing(t *testing.T) {
	db := openTestDB(t)

	conf := confSchemaName(t, db)
	m := NewManager(db, conf)
	if m == nil {
		t.Fatal("NewManager returned nil")
	}

	var schemas int64
	err := db.
		Raw("SELECT count(*) FROM pg_catalog.pg_namespace WHERE nspname = ?", conf).
		Row().
		Scan(&schemas)
	if err != nil {
		t.Fatalf("look up the conf schema: %v", err)
	}
	if schemas != 0 {
		t.Fatalf("NewManager created schema %q", conf)
	}
}

func TestManagerPartitionExistTableSwapsAnEmptyTable(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)

	logs := createLogTable(t, db, schema, "logs")
	old := TableName{Schema: schema, Name: "logs_old"}

	err := m.PartitionExistTable(ctx, logs, jakartaConfig(t, RangeTypeMonthly))
	if err != nil {
		t.Fatalf("PartitionExistTable: %v", err)
	}

	if got := relkind(t, db, logs); got != "p" {
		t.Fatalf("relkind of %s = %q, want p (partitioned)", logs, got)
	}
	if got := relkind(t, db, old); got != "r" {
		t.Fatalf("relkind of %s = %q, want r (the old table, kept)", old, got)
	}
	mustEqualStrings(t, "partitions", partitionsOf(t, db, logs), []string{"logs_default DEFAULT"})
	if got := len(registryRows(t, db, m, logs)); got != 1 {
		t.Fatalf("%d registry rows, want 1", got)
	}

	// The swapped-in table takes rows.
	insertLog(t, db, logs, 1, "2026-11-17 10:00+07")
	if got := countRows(t, db, TableName{Schema: schema, Name: "logs_default"}); got != 1 {
		t.Fatalf("logs_default holds %d rows, want 1", got)
	}
}

func TestManagerPartitionExistTableMonthlyInJakarta(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)

	logs := createLogTable(t, db, schema, "logs")
	early := insertLog(t, db, logs, 1, "2026-10-05 08:00+07")
	lastOctober := insertLog(t, db, logs, 1, "2026-10-31 23:30+07")
	firstNovember := insertLog(t, db, logs, 2, "2026-11-01 00:10+07")
	late := insertLog(t, db, logs, 2, "2026-11-20 12:00+07")

	err := m.PartitionExistTable(ctx, logs, jakartaConfig(t, RangeTypeMonthly))
	if err != nil {
		t.Fatalf("PartitionExistTable: %v", err)
	}

	mustEqualStrings(t, "partitions", partitionNames(t, db, logs), []string{"logs_202610", "logs_202611", "logs_default"})
	wantIn := map[int64]string{
		early:         "logs_202610",
		lastOctober:   "logs_202610",
		firstNovember: "logs_202611",
		late:          "logs_202611",
	}
	for id, want := range wantIn {
		if got := partitionOfRow(t, db, logs, id); got != want {
			t.Fatalf("row %d is in %s, want %s", id, got, want)
		}
	}

	old := TableName{Schema: schema, Name: "logs_old"}
	if got, want := countRows(t, db, logs), countRows(t, db, old); got != want || got != 4 {
		t.Fatalf("the new table holds %d rows and %s %d, want 4 in both", got, old, want)
	}

	// The bounds are 00:00 in Jakarta, whatever the session's time zone.
	insertLog(t, db, logs, 1, "2026-11-01 00:00+07")
	insertLog(t, db, logs, 1, "2026-10-31 23:59:59.999999+07")
	if got := countRows(t, db, TableName{Schema: schema, Name: "logs_default"}); got != 0 {
		t.Fatalf("logs_default holds %d rows, want 0: a boundary row missed its month", got)
	}
}

func TestManagerPartitionExistTableKeepsIndexNames(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)

	logs := createLogTable(t, db, schema, "logs")
	mustExec(t, db, "ALTER TABLE "+logs.quoted()+" ADD CONSTRAINT logs_note_day_key UNIQUE (note, created_at)")
	mustExec(t, db, "CREATE UNIQUE INDEX logs_product_day_uq ON "+logs.quoted()+" (product_id, created_at)")
	insertLog(t, db, logs, 1, "2026-10-31 23:30+07")
	before := indexNames(t, db, logs)

	err := m.PartitionExistTable(ctx, logs, jakartaConfig(t, RangeTypeMonthly))
	if err != nil {
		t.Fatalf("PartitionExistTable: %v", err)
	}

	old := TableName{Schema: schema, Name: "logs_old"}
	mustEqualStrings(t, "indexes of the new table", indexNames(t, db, logs), before)
	var wantOld []string
	for _, name := range before {
		wantOld = append(wantOld, name+"_old")
	}
	mustEqualStrings(t, "indexes of logs_old", indexNames(t, db, old), wantOld)

	defs := indexDefs(t, db, logs)
	mustContain(t, defs["logs_pkey"], "UNIQUE INDEX logs_pkey ON ONLY "+schema+".logs USING btree (id, created_at)")
	mustContain(t, defs["logs_product_partial_idx"], "(product_id) WHERE (product_id IS NOT NULL)")
	mustContain(t, defs["logs_note_lower_idx"], "(lower(note))")
	mustContain(t, defs["logs_note_day_key"], "UNIQUE INDEX logs_note_day_key ON ONLY "+schema+".logs USING btree (note, created_at)")
	mustContain(t, defs["logs_product_day_uq"], "UNIQUE INDEX logs_product_day_uq ON ONLY "+schema+".logs USING btree (product_id, created_at)")

	mustEqualStrings(t, "constraints of the new table", constraintDefs(t, db, logs, "pu"), []string{
		"logs_note_day_key UNIQUE (note, created_at)",
		"logs_pkey PRIMARY KEY (id, created_at)",
	})
	mustEqualStrings(t, "constraints of logs_old", constraintDefs(t, db, old, "pu"), []string{
		"logs_note_day_key_old UNIQUE (note, created_at)",
		"logs_pkey_old PRIMARY KEY (id)",
	})

	// Every child carries the indexes too.
	if got := len(indexNames(t, db, TableName{Schema: schema, Name: "logs_202610"})); got != len(before) {
		t.Fatalf("logs_202610 has %d indexes, want %d", got, len(before))
	}
}

func TestManagerPartitionExistTableMovesItsOwnForeignKeys(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)

	logs := createLogTable(t, db, schema, "logs")
	insertLog(t, db, logs, 1, "2026-10-31 23:30+07")
	before := constraintDefs(t, db, logs, "f")

	err := m.PartitionExistTable(ctx, logs, jakartaConfig(t, RangeTypeMonthly))
	if err != nil {
		t.Fatalf("PartitionExistTable: %v", err)
	}

	mustEqualStrings(t, "foreign keys of the new table", constraintDefs(t, db, logs, "f"), before)
	mustEqualStrings(t, "foreign keys of logs_old", constraintDefs(t, db, TableName{Schema: schema, Name: "logs_old"}, "f"), nil)

	// The moved key holds on the new table.
	err = db.
		Exec("INSERT INTO " + logs.quoted() + " (team_id, created_at) VALUES (999, '2026-11-01 10:00+07')").
		Error
	if err == nil {
		t.Fatal("a row for a missing team was accepted: the foreign key did not move")
	}
}

func TestManagerPartitionExistTableRollsBackEverythingOnAFailureMidway(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)

	logs := createLogTable(t, db, schema, "logs")
	insertLog(t, db, logs, 1, "2026-10-31 23:30+07")
	// A NOT VALID foreign key with a row breaking it: the conversion re-adds
	// it validated (a partitioned table refuses NOT VALID), so the seed fails
	// after the indexes were renamed, the keys dropped and the tables swapped.
	mustExec(t, db, "INSERT INTO "+logs.quoted()+" (team_id, product_id, created_at) VALUES (1, 777, '2026-11-01 10:00+07')")
	mustExec(t, db, "ALTER TABLE "+logs.quoted()+" ADD CONSTRAINT logs_product_fkey FOREIGN KEY (product_id) REFERENCES "+TableName{Schema: schema, Name: "teams"}.quoted()+" (id) NOT VALID")
	before := snapshotTable(t, db, m, logs)

	err := m.PartitionExistTable(ctx, logs, jakartaConfig(t, RangeTypeMonthly))
	if err == nil {
		t.Fatal("PartitionExistTable succeeded with a row breaking a foreign key")
	}
	// The seed is what failed — after the renames, the dropped keys and the
	// renamed indexes — and the rollback undid all of them.
	mustContain(t, err.Error(), "violates foreign key constraint \"logs_product_fkey\"")

	mustEqualStrings(t, "the table after the failure", snapshotTable(t, db, m, logs), before)
}

func TestManagerPartitionExistTableValidatesANotValidForeignKey(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)

	logs := createLogTable(t, db, schema, "logs")
	insertLog(t, db, logs, 1, "2026-10-31 23:30+07")
	mustExec(t, db, "ALTER TABLE "+logs.quoted()+" ADD CONSTRAINT logs_product_fkey FOREIGN KEY (product_id) REFERENCES "+TableName{Schema: schema, Name: "teams"}.quoted()+" (id) NOT VALID")
	mustExec(t, db, "UPDATE "+logs.quoted()+" SET product_id = 2")

	err := m.PartitionExistTable(ctx, logs, jakartaConfig(t, RangeTypeMonthly))
	if err != nil {
		t.Fatalf("PartitionExistTable: %v", err)
	}

	mustEqualStrings(t, "foreign keys of the new table", constraintDefs(t, db, logs, "f"), []string{
		"logs_product_fkey FOREIGN KEY (product_id) REFERENCES " + schema + ".teams(id)",
		"logs_team_id_fkey FOREIGN KEY (team_id) REFERENCES " + schema + ".teams(id)",
	})
}

func TestManagerPartitionExistTableLetsTheCallerDropTheOldTable(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)

	logs := createLogTable(t, db, schema, "logs")
	insertLog(t, db, logs, 1, "2026-10-31 23:30+07")
	last := insertLog(t, db, logs, 1, "2026-11-01 00:10+07")

	err := m.PartitionExistTable(ctx, logs, jakartaConfig(t, RangeTypeMonthly))
	if err != nil {
		t.Fatalf("PartitionExistTable: %v", err)
	}

	mustExec(t, db, "DROP TABLE "+TableName{Schema: schema, Name: "logs_old"}.quoted())

	next := insertLog(t, db, logs, 2, "2026-11-02 09:00+07")
	if next != last+1 {
		t.Fatalf("the next insert got id %d, want %d", next, last+1)
	}
}

func TestManagerPartitionExistTableAnalyzesTheNewTable(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)

	logs := createLogTable(t, db, schema, "logs")
	insertLog(t, db, logs, 1, "2026-10-31 23:30+07")
	insertLog(t, db, logs, 2, "2026-11-01 00:10+07")

	err := m.PartitionExistTable(ctx, logs, jakartaConfig(t, RangeTypeMonthly))
	if err != nil {
		t.Fatalf("PartitionExistTable: %v", err)
	}

	// Autovacuum never analyzes a partitioned parent; only ANALYZE on it
	// writes its whole-table (inherited) statistics.
	var columns int64
	err = db.
		Raw("SELECT count(*) FROM pg_catalog.pg_stats WHERE schemaname = ? AND tablename = ? AND inherited", schema, logs.Name).
		Row().
		Scan(&columns)
	if err != nil {
		t.Fatalf("read pg_stats: %v", err)
	}
	if columns == 0 {
		t.Fatal("the new table has no statistics: it was not analyzed")
	}
}

func TestManagerPartitionExistTableWritesTheRegistryRow(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)
	convertedAt := time.Date(2026, 11, 17, 16, 40, 0, 0, mustLoadLocation(t, "Asia/Jakarta"))
	m.now = func() time.Time { return convertedAt }

	logs := createLogTable(t, db, schema, "logs")
	numbered := createLogTable(t, db, schema, "numbered")

	err := m.PartitionExistTable(ctx, logs, &PartitionRangeConfig{
		Location:  mustLoadLocation(t, "Asia/Jakarta"),
		RangeType: RangeTypeMonthly,
		BlockSize: 9, // ignored by a date range
		Key:       "created_at",
	})
	if err != nil {
		t.Fatalf("PartitionExistTable monthly: %v", err)
	}
	err = m.PartitionExistTable(ctx, numbered, &PartitionRangeConfig{
		Location:  time.UTC, // ignored by a fixed range
		RangeType: FixedRange,
		BlockSize: 1000,
		Key:       "id",
	})
	if err != nil {
		t.Fatalf("PartitionExistTable fixed: %v", err)
	}

	cases := []struct {
		table TableName
		want  registryRow
	}{
		{table: logs, want: registryRow{TableSchema: schema, TableName: "logs", Key: "created_at", RangeType: "month", Location: "Asia/Jakarta", BlockSize: 0}},
		{table: numbered, want: registryRow{TableSchema: schema, TableName: "numbered", Key: "id", RangeType: "fixed", Location: "", BlockSize: 1000}},
	}
	for _, tc := range cases {
		rows := registryRows(t, db, m, tc.table)
		if len(rows) != 1 {
			t.Fatalf("%s: %d registry rows, want 1", tc.table, len(rows))
		}
		got := rows[0]
		if !got.ConvertedAt.Equal(convertedAt) {
			t.Fatalf("%s: converted_at = %s, want %s", tc.table, got.ConvertedAt, convertedAt)
		}
		got.ConvertedAt = time.Time{}
		if got != tc.want {
			t.Fatalf("%s: registry row = %+v, want %+v", tc.table, got, tc.want)
		}
	}
}
