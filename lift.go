package san_pgpartition

import (
	"fmt"
	"slices"

	"gorm.io/gorm"
)

// liftedKey is a foreign key another table holds that points at a managed
// table: dropped while rows move out of DEFAULT, then added back.
type liftedKey struct {
	Name      string
	RefSchema string
	RefTable  string
	RefOid    int64
	Def       string // pg_get_constraintdef, read before the drop
}

func (k liftedKey) table() TableName {
	return TableName{Schema: k.RefSchema, Name: k.RefTable}
}

// readKeysPointingIn reads the foreign keys other tables hold that point at
// the table — the parent-level ones; their copies for each child go with
// them.
func (mt *managedTable) readKeysPointingIn(tx *gorm.DB) error {
	var err error

	err = tx.
		Raw(`SELECT con.conname AS name,
       rn.nspname AS ref_schema,
       r.relname AS ref_table,
       con.conrelid::bigint AS ref_oid,
       pg_catalog.pg_get_constraintdef(con.oid) AS def
FROM pg_catalog.pg_constraint AS con
JOIN pg_catalog.pg_class AS r ON r.oid = con.conrelid
JOIN pg_catalog.pg_namespace AS rn ON rn.oid = r.relnamespace
WHERE con.contype = 'f'
  AND con.confrelid = CAST(? AS bigint)::oid
  AND con.conparentid = 0
  AND con.conrelid <> con.confrelid
ORDER BY rn.nspname COLLATE "C", r.relname COLLATE "C", con.conname COLLATE "C"`, mt.oid).
		Scan(&mt.pointingIn).
		Error
	if err != nil {
		return fmt.Errorf("san_pgpartition: read the foreign keys pointing at %s: %w", mt.table, err)
	}
	return nil
}

// lockReferencing locks each table holding a key that points at the table
// ACCESS EXCLUSIVE, one at a time in OID order — the order the conversion
// uses too.
func (mt *managedTable) lockReferencing(tx *gorm.DB) error {
	var err error

	var oids []int64
	tables := map[int64]TableName{}
	for _, k := range mt.pointingIn {
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
			return fmt.Errorf("san_pgpartition: lock %s, which points at %s: %w", tables[oid], mt.table, err)
		}
	}
	return nil
}

// liftKeys drops, once per transaction, every foreign key another table
// holds that points at the table, keeping each one's name and definition.
// A move is a DELETE from DEFAULT and an INSERT back: with the keys in place
// the DELETE would fire them — ON DELETE CASCADE deleting the rows pointing
// at a moved row, SET NULL unlinking them, NO ACTION failing the move.
func (mt *managedTable) liftKeys(tx *gorm.DB) error {
	var err error

	if mt.keysLifted {
		return nil
	}
	for _, k := range mt.pointingIn {
		err = tx.
			Exec("ALTER TABLE " + k.table().quoted() + " DROP CONSTRAINT " + quoteIdentifier(k.Name)).
			Error
		if err != nil {
			return fmt.Errorf("san_pgpartition: drop foreign key %s of %s before moving rows of %s: %w", k.Name, k.table(), mt.table, err)
		}
	}
	mt.keysLifted = true
	return nil
}

// restoreKeys adds back every key liftKeys dropped, under its name with its
// definition. Adding a key checks every row of its table again.
func (mt *managedTable) restoreKeys(tx *gorm.DB) error {
	var err error

	if !mt.keysLifted {
		return nil
	}
	for _, k := range mt.pointingIn {
		err = tx.
			Exec("ALTER TABLE " + k.table().quoted() + " ADD CONSTRAINT " + quoteIdentifier(k.Name) + " " + k.Def).
			Error
		if err != nil {
			return fmt.Errorf("san_pgpartition: add foreign key %s back to %s after moving rows of %s: %w", k.Name, k.table(), mt.table, err)
		}
	}
	mt.keysLifted = false
	return nil
}
