package san_pgpartition

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestManagerPartitionExistTableRefusesATableAlreadyPartitioned(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)

	t.Run("listed in partitioned_tables", func(t *testing.T) {
		logs := createLogTable(t, db, schema, "logs")
		insertLog(t, db, logs, 1, "2026-10-31 23:30+07")
		err := m.PartitionExistTable(ctx, logs, jakartaConfig(t, RangeTypeMonthly))
		if err != nil {
			t.Fatalf("first PartitionExistTable: %v", err)
		}
		before := snapshotTable(t, db, m, logs)

		err = m.PartitionExistTable(ctx, logs, jakartaConfig(t, RangeTypeMonthly))
		if !errors.Is(err, ErrAlreadyPartitioned) {
			t.Fatalf("second PartitionExistTable = %v, want ErrAlreadyPartitioned", err)
		}
		mustContain(t, err.Error(), m.schemaconf+".partitioned_tables")
		mustEqualStrings(t, "the table after the refusal", snapshotTable(t, db, m, logs), before)
	})

	t.Run("partitioned by hand", func(t *testing.T) {
		byHand := TableName{Schema: schema, Name: "by_hand"}
		mustExec(t, db, "CREATE TABLE "+byHand.quoted()+" (id bigint NOT NULL, created_at timestamptz NOT NULL) PARTITION BY RANGE (created_at)")
		before := snapshotTable(t, db, m, byHand)

		err := m.PartitionExistTable(ctx, byHand, jakartaConfig(t, RangeTypeMonthly))
		if !errors.Is(err, ErrAlreadyPartitioned) {
			t.Fatalf("PartitionExistTable = %v, want ErrAlreadyPartitioned", err)
		}
		mustEqualStrings(t, "the table after the refusal", snapshotTable(t, db, m, byHand), before)
	})
}

func TestManagerPartitionExistTableRefusesWhatIsNotAPlainTable(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)

	logs := createLogTable(t, db, schema, "logs")
	view := TableName{Schema: schema, Name: "logs_view"}
	mustExec(t, db, "CREATE VIEW "+view.quoted()+" AS SELECT id, created_at FROM "+logs.quoted())

	for _, table := range []TableName{{Schema: schema, Name: "missing"}, view} {
		before := snapshotTable(t, db, m, table)
		err := m.PartitionExistTable(ctx, table, jakartaConfig(t, RangeTypeMonthly))
		if !errors.Is(err, ErrNotPlainTable) {
			t.Fatalf("%s: PartitionExistTable = %v, want ErrNotPlainTable", table, err)
		}
		mustEqualStrings(t, table.Name+" after the refusal", snapshotTable(t, db, m, table), before)
	}
}

func TestManagerPartitionExistTableRefusesABadKey(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)

	logs := createLogTable(t, db, schema, "logs")
	mustExec(t, db, "ALTER TABLE "+logs.quoted()+" ADD COLUMN created_day date GENERATED ALWAYS AS ((created_at AT TIME ZONE 'UTC')::date) STORED")
	insertLog(t, db, logs, 1, "2026-10-31 23:30+07")
	before := snapshotTable(t, db, m, logs)

	cases := []struct {
		name    string
		conf    *PartitionRangeConfig
		wantErr string
	}{
		{name: "missing column", conf: &PartitionRangeConfig{Location: mustLoadLocation(t, "Asia/Jakarta"), RangeType: RangeTypeMonthly, Key: "made_at"}, wantErr: `no column "made_at"`},
		{name: "text column for a month range", conf: &PartitionRangeConfig{Location: mustLoadLocation(t, "Asia/Jakarta"), RangeType: RangeTypeMonthly, Key: "note"}, wantErr: "text"},
		{name: "number column for a month range", conf: &PartitionRangeConfig{Location: mustLoadLocation(t, "Asia/Jakarta"), RangeType: RangeTypeMonthly, Key: "product_id"}, wantErr: "bigint"},
		{name: "timestamptz column for a fixed range", conf: &PartitionRangeConfig{RangeType: FixedRange, BlockSize: 4, Key: "created_at"}, wantErr: "timestamp with time zone"},
		{name: "generated column", conf: &PartitionRangeConfig{Location: mustLoadLocation(t, "Asia/Jakarta"), RangeType: RangeTypeMonthly, Key: "created_day"}, wantErr: "generated"},
		{name: "column name in another case", conf: &PartitionRangeConfig{Location: mustLoadLocation(t, "Asia/Jakarta"), RangeType: RangeTypeMonthly, Key: "Created_At"}, wantErr: `no column "Created_At"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := m.PartitionExistTable(ctx, logs, tc.conf)
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("PartitionExistTable = %v, want ErrInvalidConfig", err)
			}
			mustContain(t, err.Error(), tc.wantErr)
			mustEqualStrings(t, "the table after the refusal", snapshotTable(t, db, m, logs), before)
		})
	}
}

func TestManagerPartitionExistTableRefusesNamesPastPostgreSQLsLimit(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)

	t.Run("a table whose _default name is too long", func(t *testing.T) {
		// No index: its own derived names must be what is refused.
		long := TableName{Schema: schema, Name: strings.Repeat("t", 56)}
		mustExec(t, db, "CREATE TABLE "+long.quoted()+" (created_at timestamptz NOT NULL)")
		mustExec(t, db, "INSERT INTO "+long.quoted()+" VALUES ('2026-11-01 10:00+07')")
		before := snapshotTable(t, db, m, long)

		err := m.PartitionExistTable(ctx, long, jakartaConfig(t, RangeTypeMonthly))
		if !errors.Is(err, ErrInvalidTableName) {
			t.Fatalf("PartitionExistTable = %v, want ErrInvalidTableName", err)
		}
		mustEqualStrings(t, "the table after the refusal", snapshotTable(t, db, m, long), before)
	})

	t.Run("an index whose _old name is too long", func(t *testing.T) {
		logs := createLogTable(t, db, schema, "logs")
		mustExec(t, db, "CREATE INDEX "+quoteIdentifier(strings.Repeat("i", 60))+" ON "+logs.quoted()+" (note)")
		before := snapshotTable(t, db, m, logs)

		err := m.PartitionExistTable(ctx, logs, jakartaConfig(t, RangeTypeMonthly))
		if !errors.Is(err, ErrInvalidTableName) {
			t.Fatalf("PartitionExistTable = %v, want ErrInvalidTableName", err)
		}
		mustContain(t, err.Error(), strings.Repeat("i", 60)+"_old")
		mustEqualStrings(t, "the table after the refusal", snapshotTable(t, db, m, logs), before)
	})
}

func TestManagerPartitionExistTableRefusesBadInput(t *testing.T) {
	ctx := context.Background()
	jakarta := mustLoadLocation(t, "Asia/Jakarta")
	conf := &PartitionRangeConfig{Location: jakarta, RangeType: RangeTypeMonthly, Key: "created_at"}
	table := TableName{Schema: "public", Name: "logs"}

	var nilManager *Manager
	err := nilManager.PartitionExistTable(ctx, table, conf)
	if err == nil {
		t.Fatal("a nil manager was accepted")
	}

	// None of these reach the database: a nil *gorm.DB would panic if they did.
	err = NewManager(nil, "conf").PartitionExistTable(ctx, table, conf)
	if err == nil || !strings.Contains(err.Error(), "database is nil") {
		t.Fatalf("nil db: error = %v", err)
	}

	cases := []struct {
		name  string
		m     *Manager
		table TableName
		conf  *PartitionRangeConfig
		want  error
	}{
		{name: "empty schemaconf", m: &Manager{db: dummyDB(), schemaconf: ""}, table: table, conf: conf, want: ErrInvalidTableName},
		{name: "empty table schema", m: &Manager{db: dummyDB(), schemaconf: "conf"}, table: TableName{Name: "logs"}, conf: conf, want: ErrInvalidTableName},
		{name: "nil config", m: &Manager{db: dummyDB(), schemaconf: "conf"}, table: table, conf: nil, want: ErrInvalidConfig},
		{name: "a daily child name past 63 bytes", m: &Manager{db: dummyDB(), schemaconf: "conf"}, table: TableName{Schema: "public", Name: strings.Repeat("t", 55)}, conf: &PartitionRangeConfig{Location: jakarta, RangeType: RangeTypeDaily, Key: "created_at"}, want: ErrInvalidTableName},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.m.PartitionExistTable(ctx, tc.table, tc.conf)
			if !errors.Is(err, tc.want) {
				t.Fatalf("PartitionExistTable = %v, want %v", err, tc.want)
			}
		})
	}
}

// Every refusal must leave the table exactly as it was: no _new, no _old, no
// renamed index, no dropped foreign key, no registry row.
func TestManagerPartitionExistTableRefusals(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	type setup func(t *testing.T, schema string) TableName

	logsWith := func(statements ...string) setup {
		return func(t *testing.T, schema string) TableName {
			logs := createLogTable(t, db, schema, "logs")
			insertLog(t, db, logs, 1, "2026-10-31 23:30+07")
			for _, statement := range statements {
				mustExec(t, db, strings.ReplaceAll(statement, "$S", quoteIdentifier(schema)))
			}
			return logs
		}
	}

	cases := []struct {
		name  string
		setup setup
		want  []error
		names []string // every offender the message must name
	}{
		{
			name:  "its own foreign key points at itself",
			setup: logsWith("ALTER TABLE $S.logs ADD COLUMN parent_id bigint REFERENCES $S.logs (id)"),
			want:  []error{ErrReferencedByForeignKey},
			names: []string{".logs.logs_parent_id_fkey"},
		},
		{
			name: "unique keys without the partition key",
			setup: logsWith(
				"ALTER TABLE $S.logs ADD CONSTRAINT logs_note_key UNIQUE (note)",
				"CREATE UNIQUE INDEX logs_product_uq ON $S.logs (product_id)",
				"CREATE UNIQUE INDEX logs_note_day_uq ON $S.logs (note) INCLUDE (created_at)",
			),
			want:  []error{ErrUniqueKeyWithoutPartitionKey},
			names: []string{"logs_note_key", "logs_product_uq", "logs_note_day_uq"},
		},
		{
			name: "a primary key the application fills",
			setup: func(t *testing.T, schema string) TableName {
				events := TableName{Schema: schema, Name: "event_logs"}
				mustExec(t, db, "CREATE TABLE "+events.quoted()+" (event_id text PRIMARY KEY, payload text, created_at timestamptz NOT NULL)")
				mustExec(t, db, "INSERT INTO "+events.quoted()+" VALUES ('e-1', 'x', '2026-11-01 10:00+07')")
				return events
			},
			want:  []error{ErrPrimaryKeyNotExtendable},
			names: []string{"event_logs_pkey (event_id)"},
		},
		{
			name: "a composite primary key",
			setup: func(t *testing.T, schema string) TableName {
				stock := TableName{Schema: schema, Name: "stock_logs"}
				mustExec(t, db, "CREATE TABLE "+stock.quoted()+" (warehouse_id bigint, product_id bigint, created_at timestamptz NOT NULL, PRIMARY KEY (warehouse_id, product_id))")
				return stock
			},
			want:  []error{ErrPrimaryKeyNotExtendable},
			names: []string{"stock_logs_pkey (warehouse_id, product_id)"},
		},
		{
			name: "a trigger",
			setup: logsWith(
				"CREATE FUNCTION $S.touch() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NEW; END'",
				"CREATE TRIGGER logs_touch BEFORE INSERT ON $S.logs FOR EACH ROW EXECUTE FUNCTION $S.touch()",
			),
			want:  []error{ErrUnsupportedTableFeature},
			names: []string{"trigger logs_touch"},
		},
		{
			name: "views and a rule",
			setup: logsWith(
				"CREATE VIEW $S.logs_recent AS SELECT id FROM $S.logs",
				"CREATE MATERIALIZED VIEW $S.logs_count AS SELECT count(*) FROM $S.logs",
				"CREATE RULE logs_keep AS ON DELETE TO $S.logs DO INSTEAD NOTHING",
			),
			want:  []error{ErrUnsupportedTableFeature},
			names: []string{"view ", ".logs_recent", "materialized view ", ".logs_count", "rule logs_keep"},
		},
		{
			name: "row level security",
			setup: logsWith(
				"ALTER TABLE $S.logs ENABLE ROW LEVEL SECURITY",
				"CREATE POLICY logs_by_team ON $S.logs USING (team_id = 1)",
			),
			want:  []error{ErrUnsupportedTableFeature},
			names: []string{"row level security", "policy logs_by_team"},
		},
		{
			name: "a publication",
			setup: func(t *testing.T, schema string) TableName {
				logs := logsWith()(t, schema)
				publication := schema + "_pub"
				mustExec(t, db, "CREATE PUBLICATION "+quoteIdentifier(publication)+" FOR TABLE "+logs.quoted())
				t.Cleanup(func() {
					_ = db.Exec("DROP PUBLICATION IF EXISTS " + quoteIdentifier(publication)).Error
				})
				return logs
			},
			want:  []error{ErrUnsupportedTableFeature},
			names: []string{"publication ", "_pub"},
		},
		{
			name: "an identity column",
			setup: logsWith(
				"ALTER TABLE $S.logs ADD COLUMN seq_no bigint GENERATED BY DEFAULT AS IDENTITY",
			),
			want:  []error{ErrUnsupportedTableFeature},
			names: []string{"identity column seq_no"},
		},
		{
			name:  "a table inheriting from it",
			setup: logsWith("CREATE TABLE $S.logs_extra (extra text) INHERITS ($S.logs)"),
			want:  []error{ErrUnsupportedTableFeature},
			names: []string{"inherited by ", ".logs_extra"},
		},
		{
			name: "a table it inherits from",
			setup: func(t *testing.T, schema string) TableName {
				base := TableName{Schema: schema, Name: "base"}
				child := TableName{Schema: schema, Name: "child"}
				mustExec(t, db, "CREATE TABLE "+base.quoted()+" (created_at timestamptz NOT NULL)")
				mustExec(t, db, "CREATE TABLE "+child.quoted()+" (id bigint) INHERITS ("+base.quoted()+")")
				return child
			},
			want:  []error{ErrUnsupportedTableFeature},
			names: []string{"inherits from ", ".base"},
		},
		{
			name: "a partition of another table",
			setup: func(t *testing.T, schema string) TableName {
				parted := TableName{Schema: schema, Name: "parted"}
				part := TableName{Schema: schema, Name: "parted_2026"}
				mustExec(t, db, "CREATE TABLE "+parted.quoted()+" (id bigint, created_at timestamptz NOT NULL) PARTITION BY RANGE (created_at)")
				mustExec(t, db, "CREATE TABLE "+part.quoted()+" PARTITION OF "+parted.quoted()+" FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')")
				return part
			},
			want:  []error{ErrUnsupportedTableFeature},
			names: []string{"inherits from ", ".parted"},
		},
		{
			name:  "an exclusion constraint",
			setup: logsWith("ALTER TABLE $S.logs ADD CONSTRAINT logs_one_note EXCLUDE USING btree (note WITH =)"),
			want:  []error{ErrUnsupportedTableFeature},
			names: []string{"exclusion constraint logs_one_note"},
		},
		{
			name: "privileges granted on it",
			setup: logsWith(
				"GRANT SELECT ON $S.logs TO PUBLIC",
				"GRANT UPDATE (note) ON $S.logs TO PUBLIC",
			),
			want:  []error{ErrUnsupportedTableFeature},
			names: []string{"privileges granted on the table", "privileges granted on column note"},
		},
		{
			name:  "extended statistics",
			setup: logsWith("CREATE STATISTICS $S.logs_team_product ON team_id, product_id FROM $S.logs"),
			want:  []error{ErrUnsupportedTableFeature},
			names: []string{"statistics ", ".logs_team_product"},
		},
		{
			name:  "an unlogged table",
			setup: logsWith("ALTER TABLE $S.logs SET UNLOGGED"),
			want:  []error{ErrUnsupportedTableFeature},
			names: []string{"unlogged"},
		},
		{
			name: "several at once, each named",
			setup: logsWith(
				"ALTER TABLE $S.logs ADD COLUMN parent_id bigint REFERENCES $S.logs (id)",
				"ALTER TABLE $S.logs ADD CONSTRAINT logs_note_key UNIQUE (note)",
				"CREATE FUNCTION $S.touch() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NEW; END'",
				"CREATE TRIGGER logs_touch BEFORE INSERT ON $S.logs FOR EACH ROW EXECUTE FUNCTION $S.touch()",
			),
			want:  []error{ErrReferencedByForeignKey, ErrUniqueKeyWithoutPartitionKey, ErrUnsupportedTableFeature},
			names: []string{"logs_parent_id_fkey", "logs_note_key", "trigger logs_touch"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			schema := createTestSchema(t, db)
			m := newTestManager(t, db)
			table := tc.setup(t, schema)
			before := snapshotTable(t, db, m, table)

			err := m.PartitionExistTable(ctx, table, jakartaConfig(t, RangeTypeMonthly))
			if err == nil {
				t.Fatal("PartitionExistTable converted the table")
			}
			for _, want := range tc.want {
				if !errors.Is(err, want) {
					t.Fatalf("PartitionExistTable = %v, want it to wrap %v", err, want)
				}
			}
			for _, name := range tc.names {
				mustContain(t, err.Error(), name)
			}
			mustEqualStrings(t, "the table after the refusal", snapshotTable(t, db, m, table), before)
		})
	}
}

// A conversion run by another role than the table's owner would leave the
// new table owned by that role.
func TestManagerPartitionExistTableRefusesATableItDoesNotOwn(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	schema := createTestSchema(t, db)
	m := newTestManager(t, db)

	role := schema + "_owner"
	mustExec(t, db, "CREATE ROLE "+quoteIdentifier(role))
	t.Cleanup(func() {
		_ = db.Exec("DROP OWNED BY " + quoteIdentifier(role)).Error
		_ = db.Exec("DROP ROLE IF EXISTS " + quoteIdentifier(role)).Error
	})

	logs := createLogTable(t, db, schema, "logs")
	mustExec(t, db, "ALTER TABLE "+logs.quoted()+" OWNER TO "+quoteIdentifier(role))
	before := snapshotTable(t, db, m, logs)

	err := m.PartitionExistTable(ctx, logs, jakartaConfig(t, RangeTypeMonthly))
	if !errors.Is(err, ErrUnsupportedTableFeature) {
		t.Fatalf("PartitionExistTable = %v, want ErrUnsupportedTableFeature", err)
	}
	mustContain(t, err.Error(), "owned by "+role)
	mustEqualStrings(t, "the table after the refusal", snapshotTable(t, db, m, logs), before)
}
