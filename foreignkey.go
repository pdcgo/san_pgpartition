package san_pgpartition

import (
	"fmt"
	"strings"

	"gorm.io/gorm"
)

// foreignKey is one foreign key the table being converted holds (pointing out
// at another table).
type foreignKey struct {
	Name      string
	Def       string // pg_get_constraintdef
	Validated bool
}

// readForeignKeys reads the table's own foreign keys before anything changes.
func (c *conversion) readForeignKeys(tx *gorm.DB) error {
	var err error

	err = tx.
		Raw(`SELECT conname AS name,
       pg_catalog.pg_get_constraintdef(oid) AS def,
       convalidated AS validated
FROM pg_catalog.pg_constraint
WHERE conrelid = ?::regclass
  AND contype = 'f'
ORDER BY conname`, c.table.quoted()).
		Scan(&c.fks).
		Error
	if err != nil {
		return fmt.Errorf("san_pgpartition: read the foreign keys of %s: %w", c.table, err)
	}
	return nil
}

// dropOldForeignKeys drops the table's own foreign keys before the renames;
// addForeignKeys puts them back, same names, on the new table after them.
func (c *conversion) dropOldForeignKeys(tx *gorm.DB) error {
	var err error

	for _, fk := range c.fks {
		err = tx.
			Exec("ALTER TABLE " + c.table.quoted() + " DROP CONSTRAINT " + quoteIdentifier(fk.Name)).
			Error
		if err != nil {
			return fmt.Errorf("san_pgpartition: drop foreign key %s from %s: %w", fk.Name, c.table, err)
		}
	}
	return nil
}

// addForeignKeys re-creates the old table's foreign keys on the new
// partitioned table. They are always added validated: PostgreSQL refuses
// NOT VALID on a partitioned table. The table is empty at this point, so the
// seed that follows is what checks every copied row against them.
func (c *conversion) addForeignKeys(tx *gorm.DB) error {
	var err error

	for _, fk := range c.fks {
		def := fk.Def
		if !fk.Validated {
			def = strings.TrimSuffix(def, " NOT VALID")
		}

		err = tx.
			Exec("ALTER TABLE " + c.table.quoted() + " ADD CONSTRAINT " + quoteIdentifier(fk.Name) + " " + def).
			Error
		if err != nil {
			return fmt.Errorf("san_pgpartition: add foreign key %s to %s: %w", fk.Name, c.table, err)
		}
	}
	return nil
}
