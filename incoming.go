package san_pgpartition

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"gorm.io/gorm"
)

// referenceColumnName names the column a reshape adds to a referencing
// table: the foreign-key column without a trailing "_id", then "_" and the
// partition key — user_id → user_created_at, owner → owner_created_at.
func referenceColumnName(fkColumn, key string) string {
	return strings.TrimSuffix(fkColumn, "_id") + "_" + key
}

// incomingKey is one foreign key another table holds that points at the
// table being converted, read before anything changes.
type incomingKey struct {
	Name      string // the constraint's name, kept by the new key
	RefSchema string // the referencing table
	RefTable  string
	RefOid    int64
	FkColumn  string // its (first) foreign-key column
	FkNotNull bool
	PkColumn  string // the column of the converted table it points at
	// OnDelete is the old key's ON DELETE clause with a leading space
	// (" ON DELETE SET NULL (user_id)"), "" for NO ACTION.
	OnDelete string
	// Deferral is " DEFERRABLE", " DEFERRABLE INITIALLY DEFERRED" or "".
	Deferral string
	// HoldsKey: it points at a unique key that already holds the partition
	// key, so it is only re-pointed at the new table, unchanged.
	HoldsKey bool
	// Def is its pg_get_constraintdef, read while the table still has its
	// name, so executed after the renames it names the new table.
	Def string

	// Set by planIncoming:
	NewColumn string // the column the reshape adds to the referencing table
	FillName  string // its fill trigger and function, <constraint>_fill
}

func (k incomingKey) table() TableName {
	return TableName{Schema: k.RefSchema, Name: k.RefTable}
}

const incomingKeysQuery = `
SELECT con.conname AS name,
       rn.nspname AS ref_schema,
       r.relname AS ref_table,
       con.conrelid::bigint AS ref_oid,
       fa.attname AS fk_column,
       fa.attnotnull AS fk_not_null,
       ka.attname AS pk_column,
       CASE con.confdeltype
           WHEN 'r' THEN ' ON DELETE RESTRICT'
           WHEN 'c' THEN ' ON DELETE CASCADE'
           WHEN 'n' THEN ' ON DELETE SET NULL'
           WHEN 'd' THEN ' ON DELETE SET DEFAULT'
           ELSE ''
       END || COALESCE(' (' || (
           SELECT pg_catalog.string_agg(pg_catalog.quote_ident(a.attname), ', ' ORDER BY s.ord)
           FROM pg_catalog.unnest(con.confdelsetcols) WITH ORDINALITY AS s(attnum, ord)
           JOIN pg_catalog.pg_attribute AS a ON a.attrelid = con.conrelid AND a.attnum = s.attnum
       ) || ')', '') AS on_delete,
       CASE
           WHEN con.condeferred THEN ' DEFERRABLE INITIALLY DEFERRED'
           WHEN con.condeferrable THEN ' DEFERRABLE'
           ELSE ''
       END AS deferral,
       EXISTS (
           SELECT 1
           FROM pg_catalog.unnest(con.confkey) AS k(attnum)
           JOIN pg_catalog.pg_attribute AS a ON a.attrelid = con.confrelid AND a.attnum = k.attnum
           WHERE a.attname = ?
       ) AS holds_key,
       pg_catalog.pg_get_constraintdef(con.oid) AS def
FROM pg_catalog.pg_constraint AS con
JOIN pg_catalog.pg_class AS r ON r.oid = con.conrelid
JOIN pg_catalog.pg_namespace AS rn ON rn.oid = r.relnamespace
JOIN pg_catalog.pg_attribute AS fa ON fa.attrelid = con.conrelid AND fa.attnum = con.conkey[1]
JOIN pg_catalog.pg_attribute AS ka ON ka.attrelid = con.confrelid AND ka.attnum = con.confkey[1]
WHERE con.contype = 'f'
  AND con.confrelid = CAST(? AS bigint)::oid
  AND con.conparentid = 0
  AND con.conrelid <> con.confrelid
ORDER BY rn.nspname COLLATE "C", r.relname COLLATE "C", con.conname COLLATE "C"`

// readIncomingKeys reads the foreign keys other tables hold that point at
// the table.
func (c *conversion) readIncomingKeys(tx *gorm.DB) error {
	var err error

	err = tx.
		Raw(incomingKeysQuery, c.pconf.Key, c.oid).
		Scan(&c.incoming).
		Error
	if err != nil {
		return fmt.Errorf("san_pgpartition: read the foreign keys pointing at %s: %w", c.table, err)
	}
	return nil
}

// lockReferencing locks every table holding a key that points at the table
// ACCESS EXCLUSIVE, one at a time in OID order, before the conversion takes
// any strong lock on the table itself. An app transaction that wrote such a
// table and then checks a key against the table (any insert or update of a
// referencing row) would otherwise deadlock with the conversion; this way
// the conversion waits for it instead. They stay locked until the commit.
func (c *conversion) lockReferencing(tx *gorm.DB) error {
	var err error

	var oids []int64
	tables := map[int64]TableName{}
	for _, k := range c.incoming {
		if _, ok := tables[k.RefOid]; !ok {
			oids = append(oids, k.RefOid)
			tables[k.RefOid] = k.table()
		}
	}
	slices.Sort(oids)

	for _, oid := range oids {
		err = tx.
			Exec("LOCK TABLE " + tables[oid].quoted() + " IN ACCESS EXCLUSIVE MODE").
			Error
		if err != nil {
			return fmt.Errorf("san_pgpartition: lock %s, which points at %s: %w", tables[oid], c.table, err)
		}
	}
	return nil
}

// planIncoming names what the reshape adds to each referencing table and
// refuses, before anything is created, a name longer than PostgreSQL's 63
// bytes (ErrInvalidTableName) or already taken (ErrReferenceColumnTaken):
// the new column on its table, the fill trigger on its table, the fill
// function in its schema — or one needed twice. Every offender is named.
func (c *conversion) planIncoming(tx *gorm.DB) error {
	var err error

	var refused []error
	columns := map[string]string{}   // "<table oid> <column>" → the key needing it
	functions := map[string]string{} // "<schema> <function>" → the key needing it
	for i := range c.incoming {
		k := &c.incoming[i]
		if k.HoldsKey {
			continue
		}
		ref := k.table()
		k.NewColumn = referenceColumnName(k.FkColumn, c.pconf.Key)
		k.FillName = k.Name + fillSuffix

		err = validateIdentifier(fmt.Sprintf("the column foreign key %s needs on %s", k.Name, ref), k.NewColumn)
		if err != nil {
			refused = append(refused, err)
		}
		err = validateIdentifier(fmt.Sprintf("the fill trigger and function of foreign key %s on %s", k.Name, ref), k.FillName)
		if err != nil {
			refused = append(refused, err)
		}

		column := fmt.Sprintf("%d %s", k.RefOid, k.NewColumn)
		if other, ok := columns[column]; ok {
			refused = append(refused, fmt.Errorf("%w: %s.%s is needed by foreign keys %s and %s", ErrReferenceColumnTaken, ref, k.NewColumn, other, k.Name))
		}
		columns[column] = k.Name
		function := k.RefSchema + " " + k.FillName
		if other, ok := functions[function]; ok {
			refused = append(refused, fmt.Errorf("%w: function %s.%s() is needed by foreign keys %s and %s", ErrReferenceColumnTaken, k.RefSchema, k.FillName, other, k.Name))
		}
		functions[function] = k.Name

		var taken struct {
			ColumnTaken   bool
			TriggerTaken  bool
			FunctionTaken bool
		}
		err = tx.
			Raw(`SELECT EXISTS (
           SELECT 1
           FROM pg_catalog.pg_attribute
           WHERE attrelid = CAST(? AS bigint)::oid
             AND attname = ?
             AND attnum > 0
             AND NOT attisdropped
       ) AS column_taken,
       EXISTS (
           SELECT 1
           FROM pg_catalog.pg_trigger
           WHERE tgrelid = CAST(? AS bigint)::oid
             AND tgname = ?
       ) AS trigger_taken,
       pg_catalog.to_regprocedure(pg_catalog.format('%I.%I()', ?::text, ?::text)) IS NOT NULL AS function_taken`,
				k.RefOid, k.NewColumn, k.RefOid, k.FillName, k.RefSchema, k.FillName).
			Scan(&taken).
			Error
		if err != nil {
			return fmt.Errorf("san_pgpartition: look up the names foreign key %s on %s needs: %w", k.Name, ref, err)
		}
		if taken.ColumnTaken {
			refused = append(refused, fmt.Errorf("%w: %s.%s, the column foreign key %s needs, already exists", ErrReferenceColumnTaken, ref, k.NewColumn, k.Name))
		}
		if taken.TriggerTaken {
			refused = append(refused, fmt.Errorf("%w: trigger %s on %s, the fill trigger of foreign key %s, already exists", ErrReferenceColumnTaken, k.FillName, ref, k.Name))
		}
		if taken.FunctionTaken {
			refused = append(refused, fmt.Errorf("%w: function %s.%s(), the fill function of foreign key %s on %s, already exists", ErrReferenceColumnTaken, k.RefSchema, k.FillName, k.Name, ref))
		}
	}

	if len(refused) > 0 {
		return fmt.Errorf("san_pgpartition: %s: the foreign keys pointing at it cannot be reshaped: %w", c.table, errors.Join(refused...))
	}
	return nil
}

// reshapeIncoming runs after the seed: each key pointing at the table moves
// to the new partitioned table — re-pointed as it is when it already holds
// the partition key, reshaped otherwise.
func (c *conversion) reshapeIncoming(tx *gorm.DB) error {
	var err error

	for _, k := range c.incoming {
		if k.HoldsKey {
			err = c.repoint(tx, k)
		} else {
			err = c.reshape(tx, k)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// reshape gives the referencing table the key column, fills it from the new
// table by the id, makes it NOT NULL when the foreign-key column is, and
// replaces the key under its name by one on (fk, new column) → (id, Key)
// with ON UPDATE CASCADE and the old ON DELETE and DEFERRABLE; then adds
// the fill trigger.
func (c *conversion) reshape(tx *gorm.DB, k incomingKey) error {
	var err error

	ref := k.table()
	newColumn := quoteIdentifier(k.NewColumn)
	fk := quoteIdentifier(k.FkColumn)
	pk := quoteIdentifier(k.PkColumn)
	key := quoteIdentifier(c.pconf.Key)

	err = c.execSteps(tx, k,
		"ALTER TABLE "+ref.quoted()+" ADD COLUMN "+newColumn+" "+c.keyColumnType,
		"UPDATE "+ref.quoted()+" AS r SET "+newColumn+" = k."+key+" FROM "+c.table.quoted()+" AS k WHERE k."+pk+" = r."+fk,
	)
	if err != nil {
		return err
	}

	err = c.refuseMissing(tx, k)
	if err != nil {
		return err
	}

	if k.FkNotNull {
		err = c.execSteps(tx, k, "ALTER TABLE "+ref.quoted()+" ALTER COLUMN "+newColumn+" SET NOT NULL")
		if err != nil {
			return err
		}
	}

	fill, err := c.fillTrigger(tx, k)
	if err != nil {
		return err
	}
	return c.execSteps(tx, k, append([]string{
		"ALTER TABLE " + ref.quoted() + " DROP CONSTRAINT " + quoteIdentifier(k.Name),
		"ALTER TABLE " + ref.quoted() + " ADD CONSTRAINT " + quoteIdentifier(k.Name) +
			" FOREIGN KEY (" + fk + ", " + newColumn + ") REFERENCES " + c.table.quoted() + " (" + pk + ", " + key + ")" +
			" ON UPDATE CASCADE" + k.OnDelete + k.Deferral,
	}, fill...)...)
}

// execSteps runs the statements reshaping one key, in order.
func (c *conversion) execSteps(tx *gorm.DB, k incomingKey, steps ...string) error {
	var err error

	for _, step := range steps {
		err = tx.
			Exec(step).
			Error
		if err != nil {
			return fmt.Errorf("san_pgpartition: reshape foreign key %s of %s: %w", k.Name, k.table(), err)
		}
	}
	return nil
}

// refuseMissing fails the conversion when a referencing row's id matched no
// row of the table, so the fill left its new column NULL — possible only
// under a NOT VALID key. The new key would never check such a row (MATCH
// SIMPLE skips a pair holding a NULL), so it would point at nothing forever.
func (c *conversion) refuseMissing(tx *gorm.DB, k incomingKey) error {
	var err error

	ref := k.table()
	fk := quoteIdentifier(k.FkColumn)
	var missing struct {
		Count   int64
		Example *string
	}
	err = tx.
		Raw("SELECT count(*) AS count, min(r." + fk + "::text) AS example FROM " + ref.quoted() + " AS r WHERE r." + fk + " IS NOT NULL AND r." + quoteIdentifier(k.NewColumn) + " IS NULL").
		Scan(&missing).
		Error
	if err != nil {
		return fmt.Errorf("san_pgpartition: check the rows foreign key %s of %s points at: %w", k.Name, ref, err)
	}
	if missing.Count > 0 {
		return fmt.Errorf("san_pgpartition: reshape foreign key %s of %s: %d rows point at no row of %s (e.g. %s = %s); the key is NOT VALID — fix or delete those rows first",
			k.Name, ref, missing.Count, c.table, k.FkColumn, *missing.Example)
	}
	return nil
}

// repoint moves a key that already holds the partition key from <t>_old,
// where the renames left it, to the new <t>: dropped and added again under
// its name with its definition.
func (c *conversion) repoint(tx *gorm.DB, k incomingKey) error {
	var err error

	ref := k.table()
	for _, step := range []string{
		"ALTER TABLE " + ref.quoted() + " DROP CONSTRAINT " + quoteIdentifier(k.Name),
		"ALTER TABLE " + ref.quoted() + " ADD CONSTRAINT " + quoteIdentifier(k.Name) + " " + k.Def,
	} {
		err = tx.
			Exec(step).
			Error
		if err != nil {
			return fmt.Errorf("san_pgpartition: move foreign key %s of %s to the new %s: %w", k.Name, ref, c.table, err)
		}
	}
	return nil
}

// fillSuffix ends the names of the fill trigger and its function:
// <constraint>_fill.
const fillSuffix = "_fill"

// fillTrigger returns the statements creating the fill function and trigger
// of one reshaped key: on every insert, and every update of the foreign-key
// column, the new column is set from the converted table by the id —
// whatever the app sent — or both stay NULL; an id the table lacks raises
// PostgreSQL's own foreign-key error.
func (c *conversion) fillTrigger(tx *gorm.DB, k incomingKey) ([]string, error) {
	var err error

	ref := k.table()
	function := TableName{Schema: k.RefSchema, Name: k.FillName}
	fk := "NEW." + quoteIdentifier(k.FkColumn)
	newColumn := "NEW." + quoteIdentifier(k.NewColumn)

	// RAISE and format() read % as a placeholder: double any in the names.
	message := strings.ReplaceAll(`insert or update on table "`+k.RefTable+`" violates foreign key constraint "`+k.Name+`"`, "%", "%%")
	detail := strings.ReplaceAll(`Key (`+k.FkColumn+`)=(`, "%", "%%") + "%s" + strings.ReplaceAll(`) is not present in table "`+c.table.Name+`".`, "%", "%%")

	literals := map[string]string{}
	for _, text := range []string{message, detail, k.Name, k.RefTable, k.RefSchema} {
		literals[text], err = formatLiterals(tx, "%L", []any{text})
		if err != nil {
			return nil, err
		}
	}

	body := `
BEGIN
    IF ` + fk + ` IS NULL THEN
        ` + newColumn + ` := NULL;
        RETURN NEW;
    END IF;
    SELECT k.` + quoteIdentifier(c.pconf.Key) + ` INTO ` + newColumn + `
    FROM ` + c.table.quoted() + ` AS k
    WHERE k.` + quoteIdentifier(k.PkColumn) + ` = ` + fk + `;
    IF NOT FOUND THEN
        RAISE EXCEPTION ` + literals[message] + `
            USING ERRCODE = 'foreign_key_violation',
                  DETAIL = pg_catalog.format(` + literals[detail] + `, ` + fk + `),
                  CONSTRAINT = ` + literals[k.Name] + `,
                  TABLE = ` + literals[k.RefTable] + `,
                  SCHEMA = ` + literals[k.RefSchema] + `;
    END IF;
    RETURN NEW;
END
`
	bodyLiteral, err := formatLiterals(tx, "%L", []any{body})
	if err != nil {
		return nil, err
	}

	return []string{
		// SECURITY DEFINER: like PostgreSQL's own key check, the lookup runs
		// as the table's owner (the role converting it), so an app role that
		// may write the referencing table but not read the converted one
		// keeps working. A fixed search_path keeps it from running anything
		// the caller put on theirs.
		"CREATE FUNCTION " + function.quoted() + "() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS " + bodyLiteral,
		"COMMENT ON FUNCTION " + function.quoted() + "() IS '" + createdMarker + "'",
		"CREATE TRIGGER " + quoteIdentifier(k.FillName) + " BEFORE INSERT OR UPDATE OF " + quoteIdentifier(k.FkColumn) +
			" ON " + ref.quoted() + " FOR EACH ROW EXECUTE FUNCTION " + function.quoted() + "()",
	}, nil
}
