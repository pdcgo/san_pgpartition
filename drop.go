package san_pgpartition

import (
	"context"
	"fmt"

	"gorm.io/gorm"
)

// createdMarker is the table comment PartitionNewTable writes on every
// partition it creates. PartitionDropTable removes only tables carrying it,
// exactly: partitions made by a migration or by hand never have it, and a
// table whose comment someone changed is left alone.
const createdMarker = "created by san_pgpartition"

// PartitionDropTable is the undo of PartitionNewTable: it removes child from
// parent, but only when that cannot lose data or break anything.
//
// It removes child only when all of these hold — otherwise it changes nothing:
//
//	child is a partition attached to parent       else ErrPartitionConflict
//	it carries PartitionNewTable's mark           else ErrPartitionNotOwned
//	it holds no rows                              else ErrPartitionNotEmpty
//	no other table's foreign key points at child  else PostgreSQL's error
//
// A missing child returns false, nil, so the call is safe to repeat. On
// success it returns true, nil. Removal is DETACH PARTITION, then DROP TABLE,
// in one transaction and never with CASCADE: a foreign key from another table
// to the parent stays in place (PostgreSQL refuses a plain DROP there, even of
// an empty partition), and one pointing at child itself makes the whole call
// fail and roll back. The child's own indexes and foreign keys go with it.
//
// It locks the parent, then child, ACCESS EXCLUSIVE — the same order an
// insert through the parent takes them, so it cannot deadlock with one — and
// checks for rows only under those locks: an insert in flight finishes first
// and its row makes the call refuse, it is never dropped with the partition.
// Reads and writes of the parent wait for the moment it runs; inside a
// caller's transaction, until the caller commits.
func PartitionDropTable(ctx context.Context, db *gorm.DB, parent, child TableName) (bool, error) {
	var err error

	err = checkCall(ctx, db, parent, child)
	if err != nil {
		return false, err
	}

	dropped := false
	err = db.
		WithContext(ctx).
		Transaction(func(tx *gorm.DB) error {
			var txErr error

			_, txErr = readParent(tx, parent)
			if txErr != nil {
				return txErr
			}

			txErr = tx.
				Exec("LOCK TABLE ONLY " + parent.quoted() + " IN ACCESS EXCLUSIVE MODE").
				Error
			if txErr != nil {
				return fmt.Errorf("san_pgpartition: lock parent %s: %w", parent, txErr)
			}

			state, txErr := readChild(tx, parent, child)
			if txErr != nil {
				return txErr
			}
			switch state {
			case childMissing:
				return nil
			case childElsewhere:
				return fmt.Errorf("%w: %s exists and is not a partition of %s", ErrPartitionConflict, child, parent)
			}

			txErr = tx.
				Exec("LOCK TABLE " + child.quoted() + " IN ACCESS EXCLUSIVE MODE").
				Error
			if txErr != nil {
				return fmt.Errorf("san_pgpartition: lock partition %s: %w", child, txErr)
			}

			owned, txErr := readOwned(tx, child)
			if txErr != nil {
				return txErr
			}
			if !owned {
				return fmt.Errorf("%w: %s has no %q comment", ErrPartitionNotOwned, child, createdMarker)
			}

			var hasRows bool
			txErr = tx.
				Raw("SELECT EXISTS (SELECT 1 FROM " + child.quoted() + ")").
				Row().
				Scan(&hasRows)
			if txErr != nil {
				return fmt.Errorf("san_pgpartition: check partition %s for rows: %w", child, txErr)
			}
			if hasRows {
				return fmt.Errorf("%w: %s holds rows", ErrPartitionNotEmpty, child)
			}

			txErr = tx.
				Exec("ALTER TABLE " + parent.quoted() + " DETACH PARTITION " + child.quoted()).
				Error
			if txErr != nil {
				return fmt.Errorf("san_pgpartition: detach partition %s from %s: %w", child, parent, txErr)
			}

			txErr = tx.
				Exec("DROP TABLE " + child.quoted() + " RESTRICT").
				Error
			if txErr != nil {
				return fmt.Errorf("san_pgpartition: drop partition %s: %w", child, txErr)
			}

			dropped = true
			return nil
		})
	if err != nil {
		return false, err
	}

	return dropped, nil
}

func readOwned(tx *gorm.DB, child TableName) (bool, error) {
	var err error
	var owned bool

	err = tx.
		Raw(ownedQuery, createdMarker, child.Schema, child.Name).
		Row().
		Scan(&owned)
	if err != nil {
		return false, fmt.Errorf("san_pgpartition: read the mark of %s: %w", child, err)
	}
	return owned, nil
}

const ownedQuery = `
SELECT pg_catalog.obj_description(c.oid, 'pg_class') IS NOT DISTINCT FROM ?::text
FROM pg_catalog.pg_class AS c
JOIN pg_catalog.pg_namespace AS cn ON cn.oid = c.relnamespace
WHERE cn.nspname = ?
  AND c.relname = ?`
