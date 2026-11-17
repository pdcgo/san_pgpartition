package san_pgpartition

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"gorm.io/gorm"
)

// registryEntry is one row of <schemaconf>.partitioned_tables.
type registryEntry struct {
	TableSchema string
	TableName   string
	Key         string
	RangeType   string
	Location    string
	BlockSize   int
}

func (e registryEntry) table() TableName {
	return TableName{Schema: e.TableSchema, Name: e.TableName}
}

// config rebuilds the PartitionRangeConfig the table was converted with.
func (e registryEntry) config() (*PartitionRangeConfig, error) {
	var err error

	pconf := &PartitionRangeConfig{
		RangeType: RangeType(e.RangeType),
		BlockSize: e.BlockSize,
		Key:       e.Key,
	}
	if pconf.RangeType.isDate() {
		pconf.Location, err = time.LoadLocation(e.Location)
		if err != nil {
			return nil, fmt.Errorf("%w: the registry's location %q: %v", ErrInvalidConfig, e.Location, err)
		}
	}

	err = pconf.validate()
	if err != nil {
		return nil, err
	}
	return pconf, nil
}

// readRegistry returns every row of partitioned_tables, sorted by schema and
// then name; none when the registry does not exist (nothing converted yet).
// It creates nothing.
func (m *Manager) readRegistry(ctx context.Context) ([]registryEntry, error) {
	var err error

	db := m.db.WithContext(ctx)
	exists, err := m.registryExists(db)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}

	var entries []registryEntry
	err = db.
		Raw(`SELECT table_schema, table_name, key, range_type, location, block_size
FROM ` + m.registry().quoted() + `
ORDER BY table_schema COLLATE "C", table_name COLLATE "C"`).
		Scan(&entries).
		Error
	if err != nil {
		return nil, fmt.Errorf("san_pgpartition: read %s: %w", m.registry(), err)
	}
	return entries, nil
}

// managedTable is one registered table as a maintenance call finds it,
// inside that call's transaction for the table.
type managedTable struct {
	table   TableName
	pconf   *PartitionRangeConfig
	keyType string
	oid     int64
	// deflt is the DEFAULT partition; nil when the table has none.
	deflt *TableName
	// defaultOid is deflt's OID, 0 without one.
	defaultOid int64

	// present holds the bounds of every child, as "<from>|<to>" in the
	// literal form childRange uses, so a planned child is found by its bounds.
	present map[string]bool
	// The oldest child's start and the newest child's end (finite bounds
	// only); nil without children. Date tables fill the time fields, FixedRange
	// tables the block fields.
	oldestFrom, newestEnd           *time.Time
	oldestFromBlock, newestEndBlock *int64

	columns []string // the parent's stored columns, quoted, read on the first move
	locked  bool     // ACCESS EXCLUSIVE on the parent is held
	// afterLock runs once the parent is locked; tests set it.
	afterLock func()
}

// loadTable reads what a maintenance call needs to know about a registered
// table: it must still be a RANGE-partitioned table with the registry's key.
func loadTable(tx *gorm.DB, entry registryEntry) (*managedTable, error) {
	var err error

	table := entry.table()
	err = table.validate("table")
	if err != nil {
		return nil, err
	}

	pconf, err := entry.config()
	if err != nil {
		return nil, err
	}

	mt := &managedTable{
		table:   table,
		pconf:   pconf,
		present: map[string]bool{},
	}

	err = mt.readRelation(tx)
	if err != nil {
		return nil, err
	}

	c := newConversion(table, pconf)
	err = c.readKeyType(tx)
	if err != nil {
		return nil, err
	}
	mt.keyType = c.keyType

	err = mt.readChildren(tx)
	if err != nil {
		return nil, err
	}
	return mt, nil
}

// readRelation finds the table, its partitioning and its DEFAULT partition.
func (mt *managedTable) readRelation(tx *gorm.DB) error {
	var err error

	var found []struct {
		Oid        int64
		Relkind    string
		Strategy   string
		DefaultOid int64
		DefaultNsp string
		DefaultRel string
	}
	err = tx.
		Raw(`SELECT c.oid::bigint AS oid,
       c.relkind::text AS relkind,
       COALESCE(pt.partstrat::text, '') AS strategy,
       COALESCE(pt.partdefid, 0)::bigint AS default_oid,
       COALESCE(dn.nspname::text, '') AS default_nsp,
       COALESCE(d.relname::text, '') AS default_rel
FROM pg_catalog.pg_class AS c
JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
LEFT JOIN pg_catalog.pg_partitioned_table AS pt ON pt.partrelid = c.oid
LEFT JOIN pg_catalog.pg_class AS d ON d.oid = pt.partdefid
LEFT JOIN pg_catalog.pg_namespace AS dn ON dn.oid = d.relnamespace
WHERE n.nspname = ?
  AND c.relname = ?`, mt.table.Schema, mt.table.Name).
		Scan(&found).
		Error
	if err != nil {
		return fmt.Errorf("san_pgpartition: look up %s: %w", mt.table, err)
	}

	switch {
	case len(found) == 0:
		return fmt.Errorf("%w: %s is registered but does not exist", ErrParentNotPartitioned, mt.table)
	case found[0].Relkind != "p" || found[0].Strategy != "r":
		return fmt.Errorf("%w: %s is registered but is not partitioned by RANGE", ErrParentNotPartitioned, mt.table)
	}

	mt.oid = found[0].Oid
	if found[0].DefaultOid != 0 {
		mt.defaultOid = found[0].DefaultOid
		mt.deflt = &TableName{Schema: found[0].DefaultNsp, Name: found[0].DefaultRel}
	}
	return nil
}

// childBoundsQuery reads each RANGE child's bounds from pg_get_expr and casts
// them back to the key's type in the same session, so the text round-trips
// whatever the session's DateStyle and TimeZone. MINVALUE and MAXVALUE come
// back as NULL. The %s is the key's type, one of keyTypes.
const childBoundsQuery = `SELECT b.relname AS name,
       CAST(NULLIF(pg_catalog.btrim(b.bound[1], ''''), 'MINVALUE') AS %s) AS lo,
       CAST(NULLIF(pg_catalog.btrim(b.bound[2], ''''), 'MAXVALUE') AS %s) AS hi
FROM (
    SELECT c.relname,
           pg_catalog.regexp_match(pg_catalog.pg_get_expr(c.relpartbound, c.oid), '^FOR VALUES FROM \((.*)\) TO \((.*)\)$') AS bound
    FROM pg_catalog.pg_inherits AS i
    JOIN pg_catalog.pg_class AS c ON c.oid = i.inhrelid
    WHERE i.inhparent = CAST(? AS bigint)::oid
) AS b
WHERE b.bound IS NOT NULL`

// readChildren reads the bounds of the table's children.
func (mt *managedTable) readChildren(tx *gorm.DB) error {
	var err error

	if mt.pconf.RangeType == FixedRange {
		var children []struct {
			Name string
			Lo   *int64
			Hi   *int64
		}
		err = tx.
			Raw(fmt.Sprintf(childBoundsQuery, "bigint", "bigint"), mt.oid).
			Scan(&children).
			Error
		if err != nil {
			return fmt.Errorf("san_pgpartition: read the children of %s: %w", mt.table, err)
		}
		for _, child := range children {
			if child.Lo == nil || child.Hi == nil {
				continue
			}
			mt.present[strconv.FormatInt(*child.Lo, 10)+"|"+strconv.FormatInt(*child.Hi, 10)] = true
			if mt.oldestFromBlock == nil || *child.Lo < *mt.oldestFromBlock {
				mt.oldestFromBlock = child.Lo
			}
			if mt.newestEndBlock == nil || *child.Hi > *mt.newestEndBlock {
				mt.newestEndBlock = child.Hi
			}
		}
		return nil
	}

	var children []struct {
		Name string
		Lo   *time.Time
		Hi   *time.Time
	}
	err = tx.
		Raw(fmt.Sprintf(childBoundsQuery, mt.keyType, mt.keyType), mt.oid).
		Scan(&children).
		Error
	if err != nil {
		return fmt.Errorf("san_pgpartition: read the children of %s: %w", mt.table, err)
	}
	zone := keyZone(mt.keyType, mt.pconf.Location)
	for _, child := range children {
		if child.Lo == nil || child.Hi == nil {
			continue
		}
		mt.present[dateLiteral(mt.keyType, child.Lo.In(zone))+"|"+dateLiteral(mt.keyType, child.Hi.In(zone))] = true
		if mt.oldestFrom == nil || child.Lo.Before(*mt.oldestFrom) {
			mt.oldestFrom = child.Lo
		}
		if mt.newestEnd == nil || child.Hi.After(*mt.newestEnd) {
			mt.newestEnd = child.Hi
		}
	}
	return nil
}

// has reports whether a child with exactly r's bounds exists.
func (mt *managedTable) has(r childRange) bool {
	return mt.present[r.from+"|"+r.to]
}

// lock takes ACCESS EXCLUSIVE on the parent, once: the app's reads and
// writes of the table wait from here to the commit, so no row reaches
// DEFAULT while rows move out of it. ONLY: the children need no lock of
// their own (a lock on each would fill the lock table on a big table).
func (mt *managedTable) lock(tx *gorm.DB) error {
	var err error

	if mt.locked {
		return nil
	}
	err = tx.
		Exec("LOCK TABLE ONLY " + mt.table.quoted() + " IN ACCESS EXCLUSIVE MODE").
		Error
	if err != nil {
		return fmt.Errorf("san_pgpartition: lock %s: %w", mt.table, err)
	}
	mt.locked = true

	if mt.afterLock != nil {
		mt.afterLock()
	}
	return nil
}

// movingTable is the temporary table rows wait in while their child is
// created; dropped again before the next child.
const movingTable = "san_pgpartition_moving"

// createChild creates the child r under the table, moving the rows of its
// range out of DEFAULT first and back in through the parent after —
// PostgreSQL refuses to create a child while DEFAULT holds rows of its range.
// It returns how many rows it moved. The parent is locked first.
func (mt *managedTable) createChild(tx *gorm.DB, r childRange) (int64, error) {
	var err error

	child := TableName{Schema: mt.table.Schema, Name: mt.table.Name + r.suffix}
	err = validateIdentifier("child table name", child.Name)
	if err != nil {
		return 0, err
	}

	err = mt.lock(tx)
	if err != nil {
		return 0, err
	}

	moved := int64(0)
	if mt.deflt != nil {
		moved, err = mt.moveOut(tx, r)
		if err != nil {
			return 0, err
		}
	}

	clause, err := formatLiterals(tx, "FOR VALUES FROM (%L) TO (%L)", []any{r.from, r.to})
	if err != nil {
		return 0, err
	}
	err = createMarkedPartition(tx, mt.table, child, clause)
	if err != nil {
		return 0, err
	}
	mt.present[r.from+"|"+r.to] = true

	if moved > 0 {
		err = mt.moveBack(tx, moved)
		if err != nil {
			return 0, err
		}
	}
	return moved, nil
}

// moveOut deletes the DEFAULT rows of r's range into the moving table and
// returns how many there were. With none it creates nothing.
func (mt *managedTable) moveOut(tx *gorm.DB, r childRange) (int64, error) {
	var err error

	where, err := formatLiterals(tx, "%I >= %L AND %I < %L", []any{mt.pconf.Key, r.from, mt.pconf.Key, r.to})
	if err != nil {
		return 0, err
	}

	var found bool
	err = tx.
		Raw("SELECT EXISTS (SELECT 1 FROM " + mt.deflt.quoted() + " WHERE " + where + ")").
		Row().
		Scan(&found)
	if err != nil {
		return 0, fmt.Errorf("san_pgpartition: look for rows of %s in %s: %w", r.suffix, mt.deflt, err)
	}
	if !found {
		return 0, nil
	}

	columns, err := mt.storedColumns(tx)
	if err != nil {
		return 0, err
	}

	result := tx.
		Exec("CREATE TEMPORARY TABLE " + movingTable + " AS WITH moved AS (DELETE FROM " + mt.deflt.quoted() + " WHERE " + where + " RETURNING " + columns + ") SELECT * FROM moved")
	if result.Error != nil {
		return 0, fmt.Errorf("san_pgpartition: move the rows of %s out of %s: %w", r.suffix, mt.deflt, result.Error)
	}
	if result.RowsAffected == 0 {
		return 0, dropMovingTable(tx)
	}
	return result.RowsAffected, nil
}

// moveBack inserts the moved rows through the parent, which routes them to
// the new child, and drops the moving table.
func (mt *managedTable) moveBack(tx *gorm.DB, moved int64) error {
	var err error

	columns, err := mt.storedColumns(tx)
	if err != nil {
		return err
	}

	result := tx.
		Exec("INSERT INTO " + mt.table.quoted() + " (" + columns + ") SELECT " + columns + " FROM pg_temp." + movingTable)
	if result.Error != nil {
		return fmt.Errorf("san_pgpartition: move rows back into %s: %w", mt.table, result.Error)
	}
	if result.RowsAffected != moved {
		return fmt.Errorf("san_pgpartition: moved %d rows out of %s but %d back into %s", moved, mt.deflt, result.RowsAffected, mt.table)
	}

	return dropMovingTable(tx)
}

func dropMovingTable(tx *gorm.DB) error {
	var err error

	err = tx.
		Exec("DROP TABLE pg_temp." + movingTable).
		Error
	if err != nil {
		return fmt.Errorf("san_pgpartition: drop the moving table: %w", err)
	}
	return nil
}

// storedColumns lists the parent's columns a row is written with — generated
// columns are left out, PostgreSQL computes them — quoted and joined.
func (mt *managedTable) storedColumns(tx *gorm.DB) (string, error) {
	var err error

	if mt.columns == nil {
		var names []string
		err = tx.
			Raw(`SELECT a.attname
FROM pg_catalog.pg_attribute AS a
WHERE a.attrelid = CAST(? AS bigint)::oid
  AND a.attnum > 0
  AND NOT a.attisdropped
  AND a.attgenerated = ''
ORDER BY a.attnum`, mt.oid).
			Scan(&names).
			Error
		if err != nil {
			return "", fmt.Errorf("san_pgpartition: read the columns of %s: %w", mt.table, err)
		}
		for _, name := range names {
			mt.columns = append(mt.columns, quoteIdentifier(name))
		}
	}
	return strings.Join(mt.columns, ", "), nil
}
