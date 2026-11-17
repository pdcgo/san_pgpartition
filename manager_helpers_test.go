package san_pgpartition

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
)

// confSchemaName returns a fresh name for a manager's schemaconf without
// creating it — the manager must create it on its first call. It is dropped
// with everything in it when the test ends.
func confSchemaName(t *testing.T, db *gorm.DB) string {
	t.Helper()

	schema := fmt.Sprintf("spg_%d_%d_%d_conf", os.Getpid(), time.Now().UnixNano(), schemaCounter.Add(1))
	t.Cleanup(func() {
		err := db.
			Exec("DROP SCHEMA IF EXISTS " + quoteIdentifier(schema) + " CASCADE").
			Error
		if err != nil {
			t.Errorf("drop conf schema %q: %v", schema, err)
		}
	})
	return schema
}

// newTestManager returns a manager on a fresh, not yet created schemaconf.
func newTestManager(t *testing.T, db *gorm.DB) *Manager {
	t.Helper()

	return NewManager(db, confSchemaName(t, db))
}

// createTeams creates <schema>.teams with two rows (ids 1 and 2), the table
// the log tables' foreign key points at.
func createTeams(t *testing.T, db *gorm.DB, schema string) TableName {
	t.Helper()

	teams := TableName{Schema: schema, Name: "teams"}
	mustExec(t, db, "CREATE TABLE "+teams.quoted()+" (id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL)")
	mustExec(t, db, "INSERT INTO "+teams.quoted()+" (name) VALUES ('one'), ('two')")
	return teams
}

// createLogTable creates a table shaped like revamp's append-only logs: a
// sequence-fed id, an outgoing foreign key to teams, created_at, and ordinary,
// partial and expression indexes.
func createLogTable(t *testing.T, db *gorm.DB, schema, name string) TableName {
	t.Helper()

	teams := TableName{Schema: schema, Name: "teams"}
	if !relationExists(t, db, teams) {
		createTeams(t, db, schema)
	}

	logs := TableName{Schema: schema, Name: name}
	mustExec(t, db, "CREATE TABLE "+logs.quoted()+` (
    id BIGSERIAL PRIMARY KEY,
    team_id BIGINT NOT NULL REFERENCES `+teams.quoted()+` (id),
    product_id BIGINT,
    note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`)
	mustExec(t, db, "CREATE INDEX "+quoteIdentifier(name+"_team_id_idx")+" ON "+logs.quoted()+" (team_id)")
	mustExec(t, db, "CREATE INDEX "+quoteIdentifier(name+"_created_at_idx")+" ON "+logs.quoted()+" (created_at)")
	mustExec(t, db, "CREATE INDEX "+quoteIdentifier(name+"_product_partial_idx")+" ON "+logs.quoted()+" (product_id) WHERE product_id IS NOT NULL")
	mustExec(t, db, "CREATE INDEX "+quoteIdentifier(name+"_note_lower_idx")+" ON "+logs.quoted()+" (lower(note))")
	return logs
}

// insertLog inserts one row at createdAt (any form PostgreSQL reads as a
// timestamptz) and returns its id.
func insertLog(t *testing.T, db *gorm.DB, logs TableName, teamID int64, createdAt string) int64 {
	t.Helper()

	var id int64
	err := db.
		Raw("INSERT INTO "+logs.quoted()+" (team_id, product_id, note, created_at) VALUES (?, ?, ?, ?::timestamptz) RETURNING id", teamID, teamID*10, "note "+createdAt, createdAt).
		Row().
		Scan(&id)
	if err != nil {
		t.Fatalf("insert a log at %s: %v", createdAt, err)
	}
	return id
}

func jakartaConfig(t *testing.T, rangeType RangeType) *PartitionRangeConfig {
	t.Helper()

	return &PartitionRangeConfig{
		Location:  mustLoadLocation(t, "Asia/Jakarta"),
		RangeType: rangeType,
		Key:       "created_at",
	}
}

// relkind returns pg_class.relkind of table, or "" when it does not exist.
func relkind(t *testing.T, db *gorm.DB, table TableName) string {
	t.Helper()

	var kinds []string
	err := db.
		Raw(`SELECT c.relkind::text
FROM pg_catalog.pg_class AS c
JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
WHERE n.nspname = ? AND c.relname = ?`, table.Schema, table.Name).
		Scan(&kinds).
		Error
	if err != nil {
		t.Fatalf("read relkind of %s: %v", table, err)
	}
	if len(kinds) == 0 {
		return ""
	}
	return kinds[0]
}

// partitionsOf lists the partitions attached to parent, sorted by name, each
// as "<name> <bound>".
func partitionsOf(t *testing.T, db *gorm.DB, parent TableName) []string {
	t.Helper()

	var rows []string
	err := db.
		Raw(`SELECT c.relname || ' ' || pg_catalog.pg_get_expr(c.relpartbound, c.oid)
FROM pg_catalog.pg_inherits AS i
JOIN pg_catalog.pg_class AS c ON c.oid = i.inhrelid
WHERE i.inhparent = ?::regclass
ORDER BY c.relname`, parent.quoted()).
		Scan(&rows).
		Error
	if err != nil {
		t.Fatalf("list partitions of %s: %v", parent, err)
	}
	return rows
}

// partitionNames lists only the names of parent's partitions, sorted.
func partitionNames(t *testing.T, db *gorm.DB, parent TableName) []string {
	t.Helper()

	var names []string
	for _, row := range partitionsOf(t, db, parent) {
		names = append(names, strings.SplitN(row, " ", 2)[0])
	}
	sort.Strings(names)
	return names
}

type registryRow struct {
	TableSchema string
	TableName   string
	Key         string
	RangeType   string
	Location    string
	BlockSize   int
	ConvertedAt time.Time
}

// registryRows reads every row of <schemaconf>.partitioned_tables for table.
func registryRows(t *testing.T, db *gorm.DB, m *Manager, table TableName) []registryRow {
	t.Helper()

	if relkind(t, db, TableName{Schema: m.schemaconf, Name: registryTable}) == "" {
		return nil
	}

	var rows []registryRow
	err := db.
		Raw(`SELECT table_schema, table_name, key, range_type, location, block_size, converted_at
FROM `+quoteIdentifier(m.schemaconf)+`.partitioned_tables
WHERE table_schema = ? AND table_name = ?`, table.Schema, table.Name).
		Scan(&rows).
		Error
	if err != nil {
		t.Fatalf("read the registry of %s: %v", table, err)
	}
	return rows
}

// partitionOfRow names the partition holding the row with this id.
func partitionOfRow(t *testing.T, db *gorm.DB, parent TableName, id int64) string {
	t.Helper()

	var name string
	err := db.
		Raw("SELECT c.relname FROM "+parent.quoted()+" AS r JOIN pg_catalog.pg_class AS c ON c.oid = r.tableoid WHERE r.id = ?", id).
		Row().
		Scan(&name)
	if err != nil {
		t.Fatalf("find row %d of %s: %v", id, parent, err)
	}
	return name
}

// indexNames lists the indexes of table, sorted.
func indexNames(t *testing.T, db *gorm.DB, table TableName) []string {
	t.Helper()

	var names []string
	err := db.
		Raw(`SELECT c.relname
FROM pg_catalog.pg_index AS i
JOIN pg_catalog.pg_class AS c ON c.oid = i.indexrelid
WHERE i.indrelid = ?::regclass
ORDER BY c.relname`, table.quoted()).
		Scan(&names).
		Error
	if err != nil {
		t.Fatalf("list the indexes of %s: %v", table, err)
	}
	return names
}

// indexDefs maps each index of table to its pg_get_indexdef.
func indexDefs(t *testing.T, db *gorm.DB, table TableName) map[string]string {
	t.Helper()

	var rows []struct {
		Name string
		Def  string
	}
	err := db.
		Raw(`SELECT c.relname AS name, pg_catalog.pg_get_indexdef(i.indexrelid) AS def
FROM pg_catalog.pg_index AS i
JOIN pg_catalog.pg_class AS c ON c.oid = i.indexrelid
WHERE i.indrelid = ?::regclass`, table.quoted()).
		Scan(&rows).
		Error
	if err != nil {
		t.Fatalf("read the indexes of %s: %v", table, err)
	}
	defs := map[string]string{}
	for _, row := range rows {
		defs[row.Name] = row.Def
	}
	return defs
}

// constraintDefs lists table's constraints of the given contype letters
// ("p", "u", "f", "c" …) as "<name> <definition>", sorted.
func constraintDefs(t *testing.T, db *gorm.DB, table TableName, types string) []string {
	t.Helper()

	var rows []string
	err := db.
		Raw(`SELECT conname || ' ' || pg_catalog.pg_get_constraintdef(oid)
FROM pg_catalog.pg_constraint
WHERE conrelid = ?::regclass
  AND pg_catalog.strpos(?, contype::text) > 0
ORDER BY conname`, table.quoted(), types).
		Scan(&rows).
		Error
	if err != nil {
		t.Fatalf("list the constraints of %s: %v", table, err)
	}
	return rows
}

// snapshotTable describes everything a failed conversion must leave as it
// was: the table's kind and row count, its indexes and constraints, the
// tables a conversion would create, and its registry row.
func snapshotTable(t *testing.T, db *gorm.DB, m *Manager, table TableName) []string {
	t.Helper()

	var lines []string
	lines = append(lines, "relkind "+relkind(t, db, table))
	if relkind(t, db, table) != "" {
		lines = append(lines, fmt.Sprintf("rows %d", countRows(t, db, table)))
		for _, name := range indexNames(t, db, table) {
			lines = append(lines, "index "+name)
		}
		for _, def := range constraintDefs(t, db, table, "pufcx") {
			lines = append(lines, "constraint "+def)
		}
	}
	for _, suffix := range []string{suffixNew, suffixOld, suffixDefault} {
		other := TableName{Schema: table.Schema, Name: table.Name + suffix}
		lines = append(lines, other.Name+" "+relkind(t, db, other))
	}
	lines = append(lines, fmt.Sprintf("registry rows %d", len(registryRows(t, db, m, table))))
	return lines
}

// dummyDB is a *gorm.DB with no connection, for calls that must fail
// before they reach the database.
func dummyDB() *gorm.DB {
	return &gorm.DB{}
}

func mustEqualStrings(t *testing.T, what string, got, want []string) {
	t.Helper()

	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("%s =\n  %s\nwant\n  %s", what, strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}
