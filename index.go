package san_pgpartition

import (
	"fmt"
	"strings"

	"gorm.io/gorm"
)

// indexInfo is one index of the table being converted, read before anything
// is changed.
type indexInfo struct {
	Name        string // as stored
	QuotedName  string // as pg_get_indexdef prints it
	Def         string // pg_get_indexdef
	IsPrimary   bool
	IsUnique    bool
	ConType     string // "p", "u" or "x" when the index backs a constraint, else ""
	ConDef      string // pg_get_constraintdef of that constraint
	HasKey      bool   // the partition key is one of its key columns (not inside an expression)
	KeyColumns  int    // indnkeyatts
	FirstColumn string // its first key column; "" when that is an expression
	Qualified   string // the table as pg_get_indexdef prints it
	QualifiedTo string // <t>_new printed the same way
}

const indexesQuery = `
SELECT ic.relname AS name,
       pg_catalog.quote_ident(ic.relname) AS quoted_name,
       pg_catalog.pg_get_indexdef(i.indexrelid) AS def,
       i.indisprimary AS is_primary,
       i.indisunique AS is_unique,
       COALESCE(con.contype::text, '') AS con_type,
       COALESCE(pg_catalog.pg_get_constraintdef(con.oid), '') AS con_def,
       EXISTS (
           SELECT 1
           FROM pg_catalog.unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord)
           JOIN pg_catalog.pg_attribute AS a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
           WHERE k.ord <= i.indnkeyatts
             AND a.attname = ?
       ) AS has_key,
       i.indnkeyatts::int AS key_columns,
       COALESCE((
           SELECT a.attname
           FROM pg_catalog.pg_attribute AS a
           WHERE a.attrelid = i.indrelid
             AND a.attnum = i.indkey[0]
             AND i.indkey[0] <> 0
       ), '') AS first_column,
       pg_catalog.quote_ident(n.nspname) || '.' || pg_catalog.quote_ident(t.relname) AS qualified,
       pg_catalog.quote_ident(n.nspname) || '.' || pg_catalog.quote_ident(t.relname || ?) AS qualified_to
FROM pg_catalog.pg_index AS i
JOIN pg_catalog.pg_class AS ic ON ic.oid = i.indexrelid
JOIN pg_catalog.pg_class AS t ON t.oid = i.indrelid
JOIN pg_catalog.pg_namespace AS n ON n.oid = t.relnamespace
LEFT JOIN pg_catalog.pg_constraint AS con
       ON con.conindid = i.indexrelid
      AND con.conrelid = i.indrelid
      AND con.contype IN ('p', 'u', 'x')
WHERE i.indrelid = ?::regclass
ORDER BY ic.relname`

// readIndexes reads the table's indexes, and refuses one whose "_old" name
// would pass PostgreSQL's 63 bytes.
func (c *conversion) readIndexes(tx *gorm.DB) error {
	var err error

	err = tx.
		Raw(indexesQuery, c.pconf.Key, suffixNew, c.table.quoted()).
		Scan(&c.indexes).
		Error
	if err != nil {
		return fmt.Errorf("san_pgpartition: read the indexes of %s: %w", c.table, err)
	}

	for _, index := range c.indexes {
		err = validateIdentifier("renamed index name", index.Name+suffixOld)
		if err != nil {
			return err
		}
	}
	return nil
}

// moveIndexes renames each index of the old table to <name>_old, then
// creates it on <t>_new under its original name: index names are unique per
// schema, and name-based tools look them up by name. A primary key without
// the partition key becomes (<column>, <key>) — the refusal checks have
// already made sure it is one sequence-fed column.
func (c *conversion) moveIndexes(tx *gorm.DB) error {
	var err error

	for _, index := range c.indexes {
		err = c.renameOldIndex(tx, index)
		if err != nil {
			return err
		}
	}

	for _, index := range c.indexes {
		err = c.createNewIndex(tx, index)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *conversion) renameOldIndex(tx *gorm.DB, index indexInfo) error {
	var err error

	renamed := quoteIdentifier(index.Name + suffixOld)
	if index.ConType != "" {
		// Renaming the constraint renames its index with it.
		err = tx.
			Exec("ALTER TABLE " + c.table.quoted() + " RENAME CONSTRAINT " + quoteIdentifier(index.Name) + " TO " + renamed).
			Error
	} else {
		err = tx.
			Exec("ALTER INDEX " + TableName{Schema: c.table.Schema, Name: index.Name}.quoted() + " RENAME TO " + renamed).
			Error
	}
	if err != nil {
		return fmt.Errorf("san_pgpartition: rename index %s of %s to %s_old: %w", index.Name, c.table, index.Name, err)
	}
	return nil
}

func (c *conversion) createNewIndex(tx *gorm.DB, index indexInfo) error {
	var err error

	var sql string
	switch {
	case index.ConType == "p" && !index.HasKey:
		sql = "ALTER TABLE " + c.newT.quoted() + " ADD CONSTRAINT " + quoteIdentifier(index.Name) +
			" PRIMARY KEY (" + quoteIdentifier(index.FirstColumn) + ", " + quoteIdentifier(c.pconf.Key) + ")"
	case index.ConType != "":
		sql = "ALTER TABLE " + c.newT.quoted() + " ADD CONSTRAINT " + quoteIdentifier(index.Name) + " " + index.ConDef
	default:
		sql, err = retargetIndexDef(index)
		if err != nil {
			return err
		}
	}

	err = tx.
		Exec(sql).
		Error
	if err != nil {
		return fmt.Errorf("san_pgpartition: create index %s on %s: %w", index.Name, c.newT, err)
	}
	return nil
}

// retargetIndexDef turns "CREATE [UNIQUE] INDEX name ON schema.t USING …"
// into the same statement on schema.t_new. The prefix is matched exactly, so
// nothing inside the definition (a predicate, an expression) is rewritten.
func retargetIndexDef(index indexInfo) (string, error) {
	head := "CREATE INDEX "
	if index.IsUnique {
		head = "CREATE UNIQUE INDEX "
	}
	from := head + index.QuotedName + " ON " + index.Qualified + " "
	to := head + index.QuotedName + " ON " + index.QualifiedTo + " "

	if !strings.HasPrefix(index.Def, from) {
		return "", fmt.Errorf("san_pgpartition: unexpected definition of index %s: %s", index.Name, index.Def)
	}
	return to + strings.TrimPrefix(index.Def, from), nil
}
