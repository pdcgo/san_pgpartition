package san_pgpartition

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"
)

// clockAt returns a manager clock fixed at the given wall time in loc.
func clockAt(t *testing.T, loc *time.Location, year int, month time.Month, day, hour int) func() time.Time {
	t.Helper()

	at := time.Date(year, month, day, hour, 0, 0, 0, loc)
	return func() time.Time {
		return at
	}
}

// mustHaveBound checks child's bound is FROM (from) TO (to), both read as
// keyType by PostgreSQL in this session.
func mustHaveBound(t *testing.T, db *gorm.DB, child TableName, from, to, keyType string) {
	t.Helper()

	var want string
	err := db.
		Raw("SELECT pg_catalog.format('FOR VALUES FROM (%L) TO (%L)', CAST(?::text AS "+keyType+"), CAST(?::text AS "+keyType+"))", from, to).
		Row().
		Scan(&want)
	if err != nil {
		t.Fatalf("render the bound of %s: %v", child, err)
	}
	if got := partitionBoundText(t, db, child); got != want {
		t.Fatalf("%s is %s, want %s", child, got, want)
	}
}

func TestManagerMaintainPartitionTableProvisionRefusesBadInput(t *testing.T) {
	ctx := context.Background()
	m := NewManager(dummyDB(), "san_pgpartition")

	for _, n := range []int{0, -1} {
		err := m.MaintainPartitionTableProvision(ctx, n)
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("MaintainPartitionTableProvision(ctx, %d) = %v, want ErrInvalidConfig", n, err)
		}
	}

	var nilManager *Manager
	err := nilManager.MaintainPartitionTableProvision(ctx, 6)
	if err == nil {
		t.Fatal("a nil manager provisioned")
	}
}

func TestManagerMaintainPartitionTableProvisionWithoutARegistryCreatesNothing(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	m := newTestManager(t, db)

	err := m.MaintainPartitionTableProvision(ctx, 6)
	if err != nil {
		t.Fatalf("MaintainPartitionTableProvision: %v", err)
	}

	var schemas int64
	err = db.
		Raw("SELECT count(*) FROM pg_catalog.pg_namespace WHERE nspname = ?", m.schemaconf).
		Row().
		Scan(&schemas)
	if err != nil {
		t.Fatalf("look up the conf schema: %v", err)
	}
	if schemas != 0 {
		t.Fatalf("provisioning created schemaconf %q", m.schemaconf)
	}
}

// On 2026-11-17 in Jakarta, provisioning 6 keeps this month and the next 6
// ready on a monthly table: _202611 through _202705, beside the child the
// conversion made for October's rows.
func TestManagerMaintainPartitionTableProvisionMonthly(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)
	jakarta := mustLoadLocation(t, "Asia/Jakarta")
	m.now = clockAt(t, jakarta, 2026, time.November, 17, 9)

	logs := createLogTable(t, db, schema, "logs")
	insertLog(t, db, logs, 1, "2026-10-05 08:00+07")
	err := m.PartitionExistTable(ctx, logs, jakartaConfig(t, RangeTypeMonthly))
	if err != nil {
		t.Fatalf("PartitionExistTable: %v", err)
	}

	err = m.MaintainPartitionTableProvision(ctx, 6)
	if err != nil {
		t.Fatalf("MaintainPartitionTableProvision: %v", err)
	}

	mustEqualStrings(t, "partitions", partitionNames(t, db, logs), []string{
		"logs_202610", "logs_202611", "logs_202612", "logs_202701", "logs_202702", "logs_202703", "logs_202704", "logs_202705", "logs_default",
	})
	mustHaveBound(t, db, TableName{Schema: schema, Name: "logs_202611"}, "2026-11-01 00:00+07", "2026-12-01 00:00+07", "timestamptz")
	for _, name := range partitionNames(t, db, logs) {
		if got := tableComment(t, db, TableName{Schema: schema, Name: name}); got != createdMarker {
			t.Fatalf("%s has comment %q, want %q", name, got, createdMarker)
		}
	}

	// A row of the new month lands in its child, not in DEFAULT.
	id := insertLog(t, db, logs, 1, "2026-12-01 00:00+07")
	if got := partitionOfRow(t, db, logs, id); got != "logs_202612" {
		t.Fatalf("a December row is in %s, want logs_202612", got)
	}
}

// Each table counts n in its own periods: 6 days, 6 years, 6 blocks.
func TestManagerMaintainPartitionTableProvisionDailyYearlyFixed(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)
	jakarta := mustLoadLocation(t, "Asia/Jakarta")
	m.now = clockAt(t, jakarta, 2026, time.November, 17, 9)

	daily := createLogTable(t, db, schema, "daily")
	insertLog(t, db, daily, 1, "2026-11-16 08:00+07")
	err := m.PartitionExistTable(ctx, daily, jakartaConfig(t, RangeTypeDaily))
	if err != nil {
		t.Fatalf("PartitionExistTable(daily): %v", err)
	}

	yearly := createLogTable(t, db, schema, "yearly")
	insertLog(t, db, yearly, 1, "2025-06-01 08:00+07")
	err = m.PartitionExistTable(ctx, yearly, jakartaConfig(t, RangeTypeYearly))
	if err != nil {
		t.Fatalf("PartitionExistTable(yearly): %v", err)
	}

	items := TableName{Schema: schema, Name: "items"}
	mustExec(t, db, "CREATE TABLE "+items.quoted()+" (id bigserial PRIMARY KEY, note text)")
	mustExec(t, db, "INSERT INTO "+items.quoted()+" (note) SELECT 'n' || g FROM generate_series(1, 9) AS g")
	err = m.PartitionExistTable(ctx, items, &PartitionRangeConfig{RangeType: FixedRange, BlockSize: 4, Key: "id"})
	if err != nil {
		t.Fatalf("PartitionExistTable(items): %v", err)
	}
	// A typo id in DEFAULT does not count: the highest value is read from
	// the children only.
	mustExec(t, db, "INSERT INTO "+items.quoted()+" (id, note) VALUES (1000, 'typo')")

	err = m.MaintainPartitionTableProvision(ctx, 6)
	if err != nil {
		t.Fatalf("MaintainPartitionTableProvision: %v", err)
	}

	mustEqualStrings(t, "daily partitions", partitionNames(t, db, daily), []string{
		"daily_20261116", "daily_20261117", "daily_20261118", "daily_20261119", "daily_20261120", "daily_20261121", "daily_20261122", "daily_20261123", "daily_default",
	})
	mustEqualStrings(t, "yearly partitions", partitionNames(t, db, yearly), []string{
		"yearly_2025", "yearly_2026", "yearly_2027", "yearly_2028", "yearly_2029", "yearly_2030", "yearly_2031", "yearly_2032", "yearly_default",
	})
	mustEqualStrings(t, "fixed partitions", partitionsOf(t, db, items), []string{
		"items_0_3 FOR VALUES FROM ('0') TO ('4')",
		"items_12_15 FOR VALUES FROM ('12') TO ('16')",
		"items_16_19 FOR VALUES FROM ('16') TO ('20')",
		"items_20_23 FOR VALUES FROM ('20') TO ('24')",
		"items_24_27 FOR VALUES FROM ('24') TO ('28')",
		"items_28_31 FOR VALUES FROM ('28') TO ('32')",
		"items_32_35 FOR VALUES FROM ('32') TO ('36')",
		"items_4_7 FOR VALUES FROM ('4') TO ('8')",
		"items_8_11 FOR VALUES FROM ('8') TO ('12')",
		"items_default DEFAULT",
	})
	mustHaveBound(t, db, TableName{Schema: schema, Name: "daily_20261123"}, "2026-11-23 00:00+07", "2026-11-24 00:00+07", "timestamptz")
	mustHaveBound(t, db, TableName{Schema: schema, Name: "yearly_2032"}, "2032-01-01 00:00+07", "2033-01-01 00:00+07", "timestamptz")
}

// A cron that stopped for months: every month from the newest child up to
// the current one is created, then the next n. A gap further back in
// history is left alone.
func TestManagerMaintainPartitionTableProvisionFillsAMissedCron(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)
	jakarta := mustLoadLocation(t, "Asia/Jakarta")
	m.now = clockAt(t, jakarta, 2026, time.November, 17, 9)

	logs := createLogTable(t, db, schema, "logs")
	insertLog(t, db, logs, 1, "2026-08-05 08:00+07")
	insertLog(t, db, logs, 1, "2026-11-05 08:00+07")
	err := m.PartitionExistTable(ctx, logs, jakartaConfig(t, RangeTypeMonthly))
	if err != nil {
		t.Fatalf("PartitionExistTable: %v", err)
	}
	// An older gap: September's empty child undone.
	dropped, err := PartitionDropTable(ctx, db, logs, TableName{Schema: schema, Name: "logs_202609"})
	if err != nil || !dropped {
		t.Fatalf("PartitionDropTable(logs_202609) = %v, %v", dropped, err)
	}

	m.now = clockAt(t, jakarta, 2027, time.March, 5, 8)
	err = m.MaintainPartitionTableProvision(ctx, 1)
	if err != nil {
		t.Fatalf("MaintainPartitionTableProvision: %v", err)
	}

	mustEqualStrings(t, "partitions", partitionNames(t, db, logs), []string{
		"logs_202608", "logs_202610", "logs_202611", "logs_202612", "logs_202701", "logs_202702", "logs_202703", "logs_202704", "logs_default",
	})
}

func TestManagerMaintainPartitionTableProvisionTwiceCreatesNothingNew(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)
	jakarta := mustLoadLocation(t, "Asia/Jakarta")
	m.now = clockAt(t, jakarta, 2026, time.November, 17, 9)

	logs := createLogTable(t, db, schema, "logs")
	insertLog(t, db, logs, 1, "2026-10-05 08:00+07")
	err := m.PartitionExistTable(ctx, logs, jakartaConfig(t, RangeTypeMonthly))
	if err != nil {
		t.Fatalf("PartitionExistTable: %v", err)
	}
	items := TableName{Schema: schema, Name: "items"}
	mustExec(t, db, "CREATE TABLE "+items.quoted()+" (id bigserial PRIMARY KEY, note text)")
	err = m.PartitionExistTable(ctx, items, &PartitionRangeConfig{RangeType: FixedRange, BlockSize: 4, Key: "id"})
	if err != nil {
		t.Fatalf("PartitionExistTable(items): %v", err)
	}

	err = m.MaintainPartitionTableProvision(ctx, 3)
	if err != nil {
		t.Fatalf("first MaintainPartitionTableProvision: %v", err)
	}
	logsAfterFirst := partitionsOf(t, db, logs)
	itemsAfterFirst := partitionsOf(t, db, items)
	// An empty fixed table starts at block 0.
	mustEqualStrings(t, "items after the first call", partitionNames(t, db, items), []string{"items_0_3", "items_12_15", "items_4_7", "items_8_11", "items_default"})

	err = m.MaintainPartitionTableProvision(ctx, 3)
	if err != nil {
		t.Fatalf("second MaintainPartitionTableProvision: %v", err)
	}
	mustEqualStrings(t, "logs after the second call", partitionsOf(t, db, logs), logsAfterFirst)
	mustEqualStrings(t, "items after the second call", partitionsOf(t, db, items), itemsAfterFirst)
}

// convertedOctober returns a monthly logs table converted on 2026-11-17 with
// one October row (child logs_202610 + DEFAULT), then given rows that land in
// DEFAULT: two in November, one in December and one back-dated to 2019.
func convertedOctober(t *testing.T, db *gorm.DB, m *Manager, schema string) (TableName, map[string]int64) {
	t.Helper()

	ctx := context.Background()
	logs := createLogTable(t, db, schema, "logs")
	ids := map[string]int64{}
	ids["october"] = insertLog(t, db, logs, 1, "2026-10-05 08:00+07")
	err := m.PartitionExistTable(ctx, logs, jakartaConfig(t, RangeTypeMonthly))
	if err != nil {
		t.Fatalf("PartitionExistTable: %v", err)
	}
	ids["november first"] = insertLog(t, db, logs, 1, "2026-11-01 00:00+07")
	ids["november last"] = insertLog(t, db, logs, 2, "2026-11-30 23:59:59+07")
	ids["december"] = insertLog(t, db, logs, 2, "2026-12-05 10:00+07")
	ids["2019"] = insertLog(t, db, logs, 1, "2019-03-01 10:00+07")
	if got := countRows(t, db, TableName{Schema: schema, Name: "logs_default"}); got != 4 {
		t.Fatalf("logs_default holds %d rows before provisioning, want 4", got)
	}
	return logs, ids
}

// Creating a child whose period DEFAULT holds rows of moves those rows into
// it in the same transaction (PostgreSQL refuses the child otherwise).
func TestManagerMaintainPartitionTableProvisionMovesDefaultRowsIn(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)
	jakarta := mustLoadLocation(t, "Asia/Jakarta")
	m.now = clockAt(t, jakarta, 2026, time.November, 17, 9)

	logs, ids := convertedOctober(t, db, m, schema)

	err := m.MaintainPartitionTableProvision(ctx, 1)
	if err != nil {
		t.Fatalf("MaintainPartitionTableProvision: %v", err)
	}

	mustEqualStrings(t, "partitions", partitionNames(t, db, logs), []string{"logs_202610", "logs_202611", "logs_202612", "logs_default"})
	for name, want := range map[string]string{
		"october":        "logs_202610",
		"november first": "logs_202611",
		"november last":  "logs_202611",
		"december":       "logs_202612",
		"2019":           "logs_default",
	} {
		if got := partitionOfRow(t, db, logs, ids[name]); got != want {
			t.Fatalf("the %s row is in %s, want %s", name, got, want)
		}
	}
	if got := countRows(t, db, logs); got != 5 {
		t.Fatalf("the table holds %d rows, want 5", got)
	}
}

// When a later child fails, the rows already moved go back: the table's
// transaction rolls back whole.
func TestManagerMaintainPartitionTableProvisionMovesAllOrNothing(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)
	jakarta := mustLoadLocation(t, "Asia/Jakarta")
	m.now = clockAt(t, jakarta, 2026, time.November, 17, 9)

	logs, ids := convertedOctober(t, db, m, schema)
	// December's name is taken by an ordinary table: its create fails after
	// November's rows were moved.
	mustExec(t, db, "CREATE TABLE "+TableName{Schema: schema, Name: "logs_202612"}.quoted()+" (id bigint)")
	before := partitionsOf(t, db, logs)

	err := m.MaintainPartitionTableProvision(ctx, 1)
	if err == nil {
		t.Fatal("MaintainPartitionTableProvision succeeded over a taken name")
	}
	mustContain(t, err.Error(), logs.String())
	mustContain(t, err.Error(), "logs_202612")

	mustEqualStrings(t, "partitions after the failure", partitionsOf(t, db, logs), before)
	if got := partitionOfRow(t, db, logs, ids["november first"]); got != "logs_default" {
		t.Fatalf("a November row is in %s after the rollback, want logs_default", got)
	}
	if got := countRows(t, db, TableName{Schema: schema, Name: "logs_default"}); got != 4 {
		t.Fatalf("logs_default holds %d rows after the rollback, want 4", got)
	}
}

// A move never loses rows: when fewer rows go back in than came out of
// DEFAULT (here an insert trigger, added after the conversion, swallows
// them), the table's transaction fails and rolls back.
func TestManagerMaintainPartitionTableProvisionNeverLosesMovedRows(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)
	jakarta := mustLoadLocation(t, "Asia/Jakarta")
	m.now = clockAt(t, jakarta, 2026, time.November, 17, 9)

	logs, _ := convertedOctober(t, db, m, schema)
	swallow := TableName{Schema: schema, Name: "swallow"}
	mustExec(t, db, "CREATE FUNCTION "+swallow.quoted()+"() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NULL; END'")
	mustExec(t, db, "CREATE TRIGGER swallow BEFORE INSERT ON "+logs.quoted()+" FOR EACH ROW EXECUTE FUNCTION "+swallow.quoted()+"()")

	err := m.MaintainPartitionTableProvision(ctx, 1)
	if err == nil {
		t.Fatal("MaintainPartitionTableProvision succeeded while the moved rows vanished")
	}
	mustContain(t, err.Error(), "moved 2 rows out of")
	if got := countRows(t, db, TableName{Schema: schema, Name: "logs_default"}); got != 4 {
		t.Fatalf("logs_default holds %d rows after the failure, want 4", got)
	}
}

// An app insert issued while provisioning holds the table waits, then lands
// in the child provisioning just created — not in DEFAULT.
func TestManagerMaintainPartitionTableProvisionAppInsertWaitsAndLandsInTheNewChild(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)
	jakarta := mustLoadLocation(t, "Asia/Jakarta")
	m.now = clockAt(t, jakarta, 2026, time.November, 17, 9)

	logs, _ := convertedOctober(t, db, m, schema)

	paused := make(chan struct{})
	resume := make(chan struct{})
	m.hooks.afterTableLock = func() {
		close(paused)
		<-resume
	}

	provisioned := make(chan error, 1)
	go func() {
		provisioned <- m.MaintainPartitionTableProvision(ctx, 1)
	}()
	select {
	case <-paused:
	case err := <-provisioned:
		t.Fatalf("provisioning ended before the lock hook: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("provisioning never locked the table")
	}

	type inserted struct {
		id  int64
		err error
	}
	app := make(chan inserted, 1)
	go func() {
		id, err := insertRow(ctx, db, logs, "2026-11-20 10:00+07")
		app <- inserted{id: id, err: err}
	}()

	waitForLockWaiters(t, db, 1)
	select {
	case r := <-app:
		t.Fatalf("the app insert did not wait for provisioning: %d, %v", r.id, r.err)
	default:
	}

	close(resume)
	err := <-provisioned
	if err != nil {
		t.Fatalf("MaintainPartitionTableProvision: %v", err)
	}
	r := <-app
	if r.err != nil {
		t.Fatalf("the app insert: %v", r.err)
	}
	if got := partitionOfRow(t, db, logs, r.id); got != "logs_202611" {
		t.Fatalf("the app's row is in %s, want logs_202611", got)
	}
}

// convertTwo converts logs_a and logs_b (monthly, one October row each) on
// 2026-11-17.
func convertTwo(t *testing.T, db *gorm.DB, m *Manager, schema string) (TableName, TableName) {
	t.Helper()

	ctx := context.Background()
	var tables []TableName
	for _, name := range []string{"logs_a", "logs_b"} {
		logs := createLogTable(t, db, schema, name)
		insertLog(t, db, logs, 1, "2026-10-05 08:00+07")
		err := m.PartitionExistTable(ctx, logs, jakartaConfig(t, RangeTypeMonthly))
		if err != nil {
			t.Fatalf("PartitionExistTable(%s): %v", logs, err)
		}
		tables = append(tables, logs)
	}
	return tables[0], tables[1]
}

// Two crons provisioning the same tables at once: the per-table lock makes
// the second wait for the first and then find every child there — no
// error, each child created once.
func TestManagerMaintainPartitionTableProvisionConcurrentCallersCreateEachChildOnce(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)
	jakarta := mustLoadLocation(t, "Asia/Jakarta")
	m.now = clockAt(t, jakarta, 2026, time.November, 17, 9)

	a, b := convertTwo(t, db, m, schema)

	// Park both callers behind the tables, so they race for real once the
	// holder lets go.
	holder := db.Begin()
	t.Cleanup(func() {
		holder.Rollback()
	})
	mustExec(t, holder, "LOCK TABLE "+a.quoted()+", "+b.quoted()+" IN ACCESS EXCLUSIVE MODE")

	const callers = 2
	errs := make([]error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = m.MaintainPartitionTableProvision(ctx, 3)
		}(i)
	}

	waitForLockWaiters(t, db, callers)
	err := holder.Commit().Error
	if err != nil {
		t.Fatalf("release the tables: %v", err)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("caller %d: %v", i, err)
		}
	}
	for _, logs := range []TableName{a, b} {
		mustEqualStrings(t, logs.Name+" partitions", partitionNames(t, db, logs), []string{
			logs.Name + "_202610", logs.Name + "_202611", logs.Name + "_202612", logs.Name + "_202701", logs.Name + "_202702", logs.Name + "_default",
		})
	}
}

// A registered table that was dropped since: its error comes back, naming
// it, and the tables after it are still provisioned.
func TestManagerMaintainPartitionTableProvisionReportsABrokenTableAndGoesOn(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)
	jakarta := mustLoadLocation(t, "Asia/Jakarta")
	m.now = clockAt(t, jakarta, 2026, time.November, 17, 9)

	a, b := convertTwo(t, db, m, schema)
	mustExec(t, db, "DROP TABLE "+TableName{Schema: schema, Name: "logs_a_old"}.quoted()+", "+a.quoted())

	err := m.MaintainPartitionTableProvision(ctx, 1)
	if !errors.Is(err, ErrParentNotPartitioned) {
		t.Fatalf("MaintainPartitionTableProvision = %v, want ErrParentNotPartitioned for %s", err, a)
	}
	mustContain(t, err.Error(), a.String())

	mustEqualStrings(t, "logs_b partitions", partitionNames(t, db, b), []string{"logs_b_202610", "logs_b_202611", "logs_b_202612", "logs_b_default"})
}
