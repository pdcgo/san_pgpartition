package san_pgpartition

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"gorm.io/gorm"
)

// PartitionExistTable converts the existing plain table into a table
// partitioned by RANGE on pconf.Key, under the same name, and records it with
// its whole config in <schemaconf>.partitioned_tables (created on the first
// call when missing). Everything runs in ONE transaction (a savepoint inside
// a caller's): any error rolls all of it back, leaving the table, its indexes
// and foreign keys — and every table pointing at it — exactly as they were,
// with no <table>_new, no <table>_old and no registry row.
//
// In order:
//
//  1. Conversions of the same table through the same schemaconf wait for
//     each other (a transaction-scoped advisory lock); a second caller then
//     finds the first one's registry row.
//  2. Refused with ErrAlreadyPartitioned when partitioned_tables lists the
//     table or PostgreSQL already shows it as partitioned; ErrNotPlainTable
//     when it is missing or not an ordinary table; ErrInvalidConfig when the
//     key column is missing, generated, or of a type the range type cannot
//     cut (timestamptz, timestamp or date for the date types; smallint,
//     integer or bigint for FixedRange).
//  3. Refused, before anything is created, with every offender named:
//     ErrReferencedByForeignKey (its own foreign key points back at it),
//     ErrUniqueKeyWithoutPartitionKey (a unique key other than
//     the primary key lacks the key), ErrPrimaryKeyNotExtendable (the primary
//     key lacks the key and is not one sequence-fed column),
//     ErrUnsupportedTableFeature (a trigger, view, materialized view, rule,
//     row level security, publication, identity column, inheritance,
//     exclusion constraint, granted privileges, extended statistics, an
//     unlogged table, or another owner than the current user). The errors are
//     joined: errors.Is matches each.
//  4. Every other table holding a foreign key that points at the table is
//     locked ACCESS EXCLUSIVE, one at a time in OID order — before any
//     strong lock on the table itself, so an app transaction that wrote such
//     a table and then checks a key against this one is waited for instead
//     of deadlocking. They stay closed until the commit. Then the names the
//     reshape (step 13) adds are checked: the new column, and the fill
//     trigger and function <constraint>_fill — taken, or needed twice, is
//     ErrReferenceColumnTaken; over 63 bytes, ErrInvalidTableName.
//  5. <table>_new is created LIKE the table (defaults, CHECK and NOT NULL
//     constraints, generated columns, storage, comments; the table comment
//     too), PARTITION BY RANGE (Key).
//  6. Each index of the table is renamed <name>_old and re-created on
//     <table>_new under its original name — the primary key extended to
//     (<column>, Key) when it lacked the key.
//  7. The table's own foreign keys are dropped from it.
//  8. <table> is renamed <table>_old and <table>_new is renamed <table>.
//     From here to the commit the app's reads and writes of the table wait;
//     afterwards they run on the new partitioned table — nothing is lost.
//  9. The foreign keys are added back to <table> under their names
//     (validated: PostgreSQL refuses NOT VALID on a partitioned table).
//  10. One child per day, month, year or block from the oldest to the newest
//     key value in <table>_old (<table>_20261117, <table>_202611,
//     <table>_2026, <table>_0_3; a negative block <table>_m4_m1), and
//     <table>_default for the rest (NULL and infinite keys). An empty table
//     gets only the DEFAULT. Each carries the comment "created by
//     san_pgpartition", so [PartitionDropTable] can undo an empty one.
//  11. The rows are copied from <table>_old (generated columns left out).
//  12. Each sequence a default of <table>_old draws from and that it owns is
//     handed to <table>, so the caller can later DROP TABLE <table>_old.
//  13. Every foreign key pointing at the table moves to <table> under its
//     name. One that points at a unique key already holding Key is re-added
//     as it was. Any other points at the primary key (<id>), which is now
//     (<id>, Key): its table gets a column <fk column minus _id>_<Key>
//     (user_id → user_created_at; owner → owner_created_at) of Key's type,
//     filled from <table> by the id, NOT NULL when the foreign-key column
//     is; a row whose id <table> lacks (possible under a NOT VALID key)
//     fails the conversion. The key becomes (fk, new column) → (<id>, Key)
//     ON UPDATE CASCADE, with the old key's ON DELETE and DEFERRABLE, and a
//     BEFORE INSERT OR UPDATE OF <fk> trigger <constraint>_fill keeps the new
//     column filled on later writes — the app keeps writing only the fk.
//  14. ANALYZE <table> (autovacuum never analyzes a partitioned parent).
//  15. The registry row is written last.
//
// <table>_old is kept, with every original row and its indexes renamed
// <name>_old; the caller drops it once satisfied. Day, month and year start
// at 00:00 in pconf.Location for a timestamptz key, at 00:00 of the wall
// clock for a timestamp key, and at the date for a date key.
//
// Every table and index it creates must fit PostgreSQL's 63-byte names, or
// it refuses with ErrInvalidTableName. Each child holds about a dozen locks
// until the commit, so one call can create only as many children as the
// server's lock table holds — on PostgreSQL's defaults roughly a thousand
// (1,000 daily children: 7 s; 1,200 needed max_locks_per_transaction 128).
// Running out fails with SQLSTATE 53200, which the error explains (how many
// children, what to raise). Prefer RangeTypeMonthly unless a table is huge
// and read by date.
//
// The fill trigger looks the id up across every child (≈ 0.3 ms a row on 40
// monthly children), and a table carrying it can no longer be converted
// itself (it has a trigger). A table's foreign key to itself stays refused.
//
// Afterwards the caller's cron keeps the table ready:
// [Manager.MaintainPartitionTableProvision], then [Manager.MaintainPartition].
func (m *Manager) PartitionExistTable(ctx context.Context, table TableName, pconf *PartitionRangeConfig) error {
	var err error

	err = m.check(ctx)
	if err != nil {
		return err
	}
	err = table.validate("table")
	if err != nil {
		return err
	}
	err = pconf.validate()
	if err != nil {
		return err
	}
	err = validateDerivedNames(table, pconf.RangeType)
	if err != nil {
		return err
	}

	c := newConversion(table, pconf)
	err = m.db.
		WithContext(ctx).
		Transaction(func(tx *gorm.DB) error {
			return m.convert(tx, c)
		})
	return lockLimitHint(err, c.children)
}

// conversion holds the names one conversion works with.
type conversion struct {
	table   TableName // the table being converted; the partitioned table at the end
	oid     int64     // the original table's OID
	newT    TableName // <t>_new, built beside it
	old     TableName // <t>_old, the original after the swap
	deflt   TableName // <t>_default
	pconf   *PartitionRangeConfig
	keyType string
	// keyColumnType is the key column's type with its modifier, for the
	// column a reshape adds to a referencing table.
	keyColumnType string
	indexes       []indexInfo
	fks           []foreignKey
	// incoming are the foreign keys other tables hold that point at it.
	incoming []incomingKey
	// children counts the children it planned, for the lock-limit hint.
	children int
}

func newConversion(table TableName, pconf *PartitionRangeConfig) *conversion {
	return &conversion{
		table: table,
		newT:  TableName{Schema: table.Schema, Name: table.Name + suffixNew},
		old:   TableName{Schema: table.Schema, Name: table.Name + suffixOld},
		deflt: TableName{Schema: table.Schema, Name: table.Name + suffixDefault},
		pconf: pconf,
	}
}

func (m *Manager) convert(tx *gorm.DB, c *conversion) error {
	var err error

	table := c.table
	pconf := c.pconf

	// One conversion per table at a time: a second caller waits here until
	// the first commits, then finds its registry row below.
	err = m.lockTable(tx, table)
	if err != nil {
		return err
	}

	err = m.ensureRegistry(tx)
	if err != nil {
		return err
	}

	listed, err := m.registered(tx, table)
	if err != nil {
		return err
	}
	if listed {
		return fmt.Errorf("%w: %s is listed in %s", ErrAlreadyPartitioned, table, m.registry())
	}

	err = c.checkRelation(tx)
	if err != nil {
		return err
	}

	err = c.lockDefinition(tx)
	if err != nil {
		return err
	}

	err = c.readKeyType(tx)
	if err != nil {
		return err
	}

	err = c.readIndexes(tx)
	if err != nil {
		return err
	}

	err = c.readForeignKeys(tx)
	if err != nil {
		return err
	}

	err = c.readIncomingKeys(tx)
	if err != nil {
		return err
	}

	err = c.checkRefusals(tx)
	if err != nil {
		return err
	}

	err = c.lockReferencing(tx)
	if err != nil {
		return err
	}

	err = c.planIncoming(tx)
	if err != nil {
		return err
	}

	if m.hooks.afterChecks != nil {
		m.hooks.afterChecks()
	}

	err = c.createNew(tx)
	if err != nil {
		return err
	}

	err = c.moveIndexes(tx)
	if err != nil {
		return err
	}

	err = c.dropOldForeignKeys(tx)
	if err != nil {
		return err
	}

	err = c.swap(tx)
	if err != nil {
		return err
	}

	if m.hooks.afterRenames != nil {
		m.hooks.afterRenames()
	}

	err = c.addForeignKeys(tx)
	if err != nil {
		return err
	}

	err = c.createChildren(tx)
	if err != nil {
		return err
	}

	err = c.createDefault(tx)
	if err != nil {
		return err
	}

	err = c.seed(tx)
	if err != nil {
		return err
	}

	err = c.moveSequences(tx)
	if err != nil {
		return err
	}

	err = c.reshapeIncoming(tx)
	if err != nil {
		return err
	}

	err = c.analyze(tx)
	if err != nil {
		return err
	}

	return m.register(tx, table, pconf)
}

// checkRelation refuses a table that is missing, already partitioned, or not
// an ordinary table.
func (c *conversion) checkRelation(tx *gorm.DB) error {
	var err error

	var found []struct {
		Oid     int64
		Relkind string
	}
	err = tx.
		Raw(`SELECT c.oid::bigint AS oid,
       c.relkind::text AS relkind
FROM pg_catalog.pg_class AS c
JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
WHERE n.nspname = ?
  AND c.relname = ?`, c.table.Schema, c.table.Name).
		Scan(&found).
		Error
	if err != nil {
		return fmt.Errorf("san_pgpartition: look up %s: %w", c.table, err)
	}

	switch {
	case len(found) == 0:
		return fmt.Errorf("%w: %s does not exist", ErrNotPlainTable, c.table)
	case found[0].Relkind == "p":
		return fmt.Errorf("%w: PostgreSQL already shows %s as a partitioned table", ErrAlreadyPartitioned, c.table)
	case found[0].Relkind != "r":
		return fmt.Errorf("%w: %s is a %s", ErrNotPlainTable, c.table, relkindName(found[0].Relkind))
	}

	c.oid = found[0].Oid
	return nil
}

func relkindName(kind string) string {
	switch kind {
	case "v":
		return "view"
	case "m":
		return "materialized view"
	case "f":
		return "foreign table"
	case "S":
		return "sequence"
	case "i", "I":
		return "index"
	case "c":
		return "composite type"
	case "t":
		return "TOAST table"
	}
	return "relation of kind " + kind
}

// lockDefinition takes ROW EXCLUSIVE on the table: the app's reads and writes
// go on, but DDL that would change what the conversion reads next — CREATE
// INDEX, CREATE TRIGGER, ALTER TABLE, another table's new foreign key to it
// — waits until the conversion commits, and then applies to the new table.
// It does not serialise two conversions (the mode does not conflict with
// itself); lockTable does.
func (c *conversion) lockDefinition(tx *gorm.DB) error {
	var err error

	err = tx.
		Exec("LOCK TABLE " + c.table.quoted() + " IN ROW EXCLUSIVE MODE").
		Error
	if err != nil {
		return fmt.Errorf("san_pgpartition: lock %s: %w", c.table, err)
	}
	return nil
}

// keyTypes are the key column types each kind of range accepts.
var keyTypes = map[bool][]string{
	true:  {keyTimestamptz, keyTimestamp, keyDate},
	false: {keySmallint, keyInteger, keyBigint},
}

// readKeyType reads the key column's type, which decides how bounds are
// written, and refuses a column the range type cannot cut.
func (c *conversion) readKeyType(tx *gorm.DB) error {
	var err error

	var columns []struct {
		TypeName  string
		FullType  string
		Generated string
	}
	err = tx.
		Raw(`SELECT pg_catalog.format_type(a.atttypid, NULL) AS type_name,
       pg_catalog.format_type(a.atttypid, a.atttypmod) AS full_type,
       a.attgenerated::text AS generated
FROM pg_catalog.pg_attribute AS a
WHERE a.attrelid = ?::regclass
  AND a.attname = ?
  AND a.attnum > 0
  AND NOT a.attisdropped`, c.table.quoted(), c.pconf.Key).
		Scan(&columns).
		Error
	if err != nil {
		return fmt.Errorf("san_pgpartition: read the key column of %s: %w", c.table, err)
	}
	if len(columns) == 0 {
		return fmt.Errorf("%w: %s has no column %q", ErrInvalidConfig, c.table, c.pconf.Key)
	}
	column := columns[0]

	if column.Generated != "" {
		return fmt.Errorf("%w: %s.%s is a generated column; PostgreSQL cannot partition on one", ErrInvalidConfig, c.table, c.pconf.Key)
	}

	accepted := keyTypes[c.pconf.RangeType.isDate()]
	if !slices.Contains(accepted, column.TypeName) {
		return fmt.Errorf("%w: %s.%s is %s; a %q range takes %s", ErrInvalidConfig, c.table, c.pconf.Key, column.TypeName, c.pconf.RangeType, strings.Join(accepted, ", "))
	}

	c.keyType = column.TypeName
	c.keyColumnType = column.FullType
	return nil
}

// createNew creates <t>_new with the old table's columns, defaults, CHECK
// and NOT NULL constraints, generated columns, storage and comments,
// partitioned by RANGE on the key.
func (c *conversion) createNew(tx *gorm.DB) error {
	var err error

	err = tx.
		Exec("CREATE TABLE " + c.newT.quoted() + " (LIKE " + c.table.quoted() +
			" INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING GENERATED INCLUDING STORAGE INCLUDING COMMENTS)" +
			" PARTITION BY RANGE (" + quoteIdentifier(c.pconf.Key) + ")").
		Error
	if err != nil {
		return fmt.Errorf("san_pgpartition: create %s: %w", c.newT, err)
	}

	return c.copyTableComment(tx)
}

// copyTableComment gives <t>_new the old table's own comment: LIKE … INCLUDING
// COMMENTS copies only the columns' and constraints' comments.
func (c *conversion) copyTableComment(tx *gorm.DB) error {
	var err error

	var comment *string
	err = tx.
		Raw("SELECT pg_catalog.obj_description(CAST(? AS bigint)::oid, 'pg_class')", c.oid).
		Row().
		Scan(&comment)
	if err != nil {
		return fmt.Errorf("san_pgpartition: read the comment of %s: %w", c.table, err)
	}
	if comment == nil {
		return nil
	}

	literal, err := formatLiterals(tx, "%L", []any{*comment})
	if err != nil {
		return err
	}

	err = tx.
		Exec("COMMENT ON TABLE " + c.newT.quoted() + " IS " + literal).
		Error
	if err != nil {
		return fmt.Errorf("san_pgpartition: copy the comment of %s to %s: %w", c.table, c.newT, err)
	}
	return nil
}

// swap renames <t> to <t>_old and <t>_new to <t>. From here to the commit,
// the app's reads and writes of <t> wait; once the conversion commits they
// run on the new partitioned table.
func (c *conversion) swap(tx *gorm.DB) error {
	var err error

	err = tx.
		Exec("ALTER TABLE " + c.table.quoted() + " RENAME TO " + quoteIdentifier(c.old.Name)).
		Error
	if err != nil {
		return fmt.Errorf("san_pgpartition: rename %s to %s: %w", c.table, c.old, err)
	}

	err = tx.
		Exec("ALTER TABLE " + c.newT.quoted() + " RENAME TO " + quoteIdentifier(c.table.Name)).
		Error
	if err != nil {
		return fmt.Errorf("san_pgpartition: rename %s to %s: %w", c.newT, c.table, err)
	}
	return nil
}

// createDefault creates <t>_default, the partition catching every row no
// child takes.
func (c *conversion) createDefault(tx *gorm.DB) error {
	return createMarkedPartition(tx, c.table, c.deflt, "DEFAULT")
}

// createChildren creates one child per period (or block) from the oldest to
// the newest key value in <t>_old. Rows whose key is NULL or infinite fit no
// period: they go to <t>_default. An empty table gets no child.
func (c *conversion) createChildren(tx *gorm.DB) error {
	var err error

	children, err := c.childRanges(tx)
	if err != nil {
		return err
	}
	c.children = len(children)

	for _, r := range children {
		child := TableName{Schema: c.table.Schema, Name: c.table.Name + r.suffix}
		err = validateIdentifier("child table name", child.Name)
		if err != nil {
			return err
		}

		clause, err := formatLiterals(tx, "FOR VALUES FROM (%L) TO (%L)", []any{r.from, r.to})
		if err != nil {
			return err
		}

		err = createMarkedPartition(tx, c.table, child, clause)
		if err != nil {
			return err
		}
	}
	return nil
}

// childRanges reads the key's range over <t>_old and cuts it into children.
func (c *conversion) childRanges(tx *gorm.DB) ([]childRange, error) {
	var err error

	key := quoteIdentifier(c.pconf.Key)

	if c.pconf.RangeType == FixedRange {
		var lowest, highest *int64
		err = tx.
			Raw("SELECT min("+key+")::bigint, max("+key+")::bigint FROM "+c.old.quoted()).
			Row().
			Scan(&lowest, &highest)
		if err != nil {
			return nil, fmt.Errorf("san_pgpartition: read the key range of %s: %w", c.old, err)
		}
		if lowest == nil {
			return nil, nil
		}
		return fixedChildren(c.pconf.BlockSize, *lowest, *highest)
	}

	var lowest, highest *time.Time
	err = tx.
		Raw("SELECT min("+key+"), max("+key+") FROM "+c.old.quoted()+" WHERE pg_catalog.isfinite("+key+")").
		Row().
		Scan(&lowest, &highest)
	if err != nil {
		return nil, fmt.Errorf("san_pgpartition: read the key range of %s: %w", c.old, err)
	}
	if lowest == nil {
		return nil, nil
	}
	return dateChildren(c.pconf.RangeType, c.keyType, c.pconf.Location, *lowest, *highest), nil
}

// seed copies every row of <t>_old into the new table, which routes each to
// its child. Generated columns are left out: the new table computes them.
func (c *conversion) seed(tx *gorm.DB) error {
	var err error

	var names []string
	err = tx.
		Raw(`SELECT a.attname
FROM pg_catalog.pg_attribute AS a
WHERE a.attrelid = ?::regclass
  AND a.attnum > 0
  AND NOT a.attisdropped
  AND a.attgenerated = ''
ORDER BY a.attnum`, c.old.quoted()).
		Scan(&names).
		Error
	if err != nil {
		return fmt.Errorf("san_pgpartition: read the columns of %s: %w", c.old, err)
	}

	columns := make([]string, len(names))
	for i, name := range names {
		columns[i] = quoteIdentifier(name)
	}
	list := strings.Join(columns, ", ")

	err = tx.
		Exec("INSERT INTO " + c.table.quoted() + " (" + list + ") SELECT " + list + " FROM " + c.old.quoted()).
		Error
	if err != nil {
		return fmt.Errorf("san_pgpartition: copy the rows of %s into %s: %w", c.old, c.table, err)
	}
	return nil
}

// moveSequences hands each sequence the old table owns, and that one of its
// column defaults draws from, to the same column of the new table. The new
// table's default already calls it; without the move, DROP TABLE <t>_old is
// refused ("default value … depends on sequence"), and CASCADE would strip
// the new table's default.
func (c *conversion) moveSequences(tx *gorm.DB) error {
	var err error

	var owned []struct {
		Sequence   string
		ColumnName string
	}
	err = tx.
		Raw(`SELECT pg_catalog.quote_ident(sn.nspname) || '.' || pg_catalog.quote_ident(s.relname) AS sequence,
       a.attname AS column_name
FROM pg_catalog.pg_depend AS d
JOIN pg_catalog.pg_class AS s ON s.oid = d.objid AND s.relkind = 'S'
JOIN pg_catalog.pg_namespace AS sn ON sn.oid = s.relnamespace
JOIN pg_catalog.pg_attribute AS a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
WHERE d.classid = 'pg_catalog.pg_class'::regclass
  AND d.refclassid = 'pg_catalog.pg_class'::regclass
  AND d.refobjid = ?::regclass
  AND d.deptype = 'a'
  AND EXISTS (
      SELECT 1
      FROM pg_catalog.pg_attrdef AS ad
      JOIN pg_catalog.pg_depend AS dd
        ON dd.classid = 'pg_catalog.pg_attrdef'::regclass
       AND dd.objid = ad.oid
       AND dd.refclassid = 'pg_catalog.pg_class'::regclass
       AND dd.refobjid = s.oid
      WHERE ad.adrelid = d.refobjid
  )
ORDER BY 1`, c.old.quoted()).
		Scan(&owned).
		Error
	if err != nil {
		return fmt.Errorf("san_pgpartition: read the sequences %s owns: %w", c.old, err)
	}

	for _, o := range owned {
		err = tx.
			Exec("ALTER SEQUENCE " + o.Sequence + " OWNED BY " + c.table.quoted() + "." + quoteIdentifier(o.ColumnName)).
			Error
		if err != nil {
			return fmt.Errorf("san_pgpartition: move sequence %s to %s.%s: %w", o.Sequence, c.table, o.ColumnName, err)
		}
	}
	return nil
}

// analyze collects the new table's statistics: autovacuum never analyzes a
// partitioned parent, so without it the planner knows nothing of the table
// as a whole.
func (c *conversion) analyze(tx *gorm.DB) error {
	var err error

	err = tx.
		Exec("ANALYZE " + c.table.quoted()).
		Error
	if err != nil {
		return fmt.Errorf("san_pgpartition: analyze %s: %w", c.table, err)
	}
	return nil
}
