package san_pgpartition

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"gorm.io/gorm"
)

// PartitionNewTable creates child as a fresh partition of parent:
// CREATE TABLE child PARTITION OF parent FOR VALUES ….
//
// The parent must already be a declarative partitioned table. Its strategy is
// read from the catalog and bound must carry that strategy (RANGE, LIST or
// HASH), with one value per partition-key column for RANGE. The child inherits
// the parent's columns, indexes, constraints and foreign keys from PostgreSQL
// itself — nothing is copied by this package.
//
// It is safe to repeat. The result is:
//
//	created  err
//	true     nil                   the child was created now
//	false    nil                   an identical partition was already there
//	false    ErrPartitionConflict  the name is taken by anything else, or by
//	                               a partition of parent with other bounds
//	false    other                 bad input or a database error; nothing
//	                               was created
//
// "Identical" is judged by PostgreSQL, through the key's own types: a date
// bound written "2026-01-01" matches one written as a time.Time on that day.
//
// Each table it creates gets the comment "created by san_pgpartition", in the
// same transaction: [PartitionDropTable] removes only tables carrying it. A
// partition that already existed is left as it is, unmarked.
//
// It runs in a transaction (a savepoint, when db is already inside one) and
// first takes SHARE UPDATE EXCLUSIVE on the parent — reads and writes on the
// parent go on, but concurrent calls for the same parent wait for each other.
// When it does create, CREATE TABLE … PARTITION OF takes PostgreSQL's ACCESS
// EXCLUSIVE lock on the parent, which blocks every read and write of it until
// the transaction commits. On its own that is a moment; inside a caller's
// transaction it lasts until the caller commits, so call it outside long
// transactions. The bound comparison creates and rolls back two temporary
// tables, which needs the TEMP privilege on the database.
//
// It only creates fresh, empty children: attaching an existing table and
// DEFAULT partitions are not part of it; removing one is [PartitionDropTable].
func PartitionNewTable(ctx context.Context, db *gorm.DB, parent, child TableName, bound PartitionBound) (bool, error) {
	var err error

	err = checkCall(ctx, db, parent, child)
	if err != nil {
		return false, err
	}

	err = bound.validateShape()
	if err != nil {
		return false, err
	}

	created := false
	err = db.
		WithContext(ctx).
		Transaction(func(tx *gorm.DB) error {
			var txErr error

			info, txErr := readParent(tx, parent)
			if txErr != nil {
				return txErr
			}

			txErr = bound.validateFor(info.strategy, info.keyColumns)
			if txErr != nil {
				return txErr
			}

			clause, txErr := bound.clause(tx)
			if txErr != nil {
				return txErr
			}

			// Serialise callers creating partitions of this parent, so two
			// callers asking for the same child end as one create and one
			// "already there" — never a duplicate-table error. This mode lets
			// reads and writes on the parent continue.
			txErr = tx.
				Exec("LOCK TABLE ONLY " + parent.quoted() + " IN SHARE UPDATE EXCLUSIVE MODE").
				Error
			if txErr != nil {
				return fmt.Errorf("san_pgpartition: lock parent %s: %w", parent, txErr)
			}

			state, txErr := readChild(tx, parent, child)
			if txErr != nil {
				return txErr
			}

			switch state {
			case childElsewhere:
				return fmt.Errorf("%w: %s exists and is not a partition of %s", ErrPartitionConflict, child, parent)
			case childAttached:
				existing, wanted, txErr := compareBound(tx, parent, child, clause)
				if txErr != nil {
					return txErr
				}
				if existing != wanted {
					return fmt.Errorf("%w: %s is a partition of %s %s, not %s", ErrPartitionConflict, child, parent, existing, wanted)
				}
				return nil
			}

			txErr = createMarkedPartition(tx, parent, child, clause)
			if txErr != nil {
				return txErr
			}

			created = true
			return nil
		})
	if err != nil {
		return false, err
	}

	return created, nil
}

// createMarkedPartition runs CREATE TABLE child PARTITION OF parent <clause>
// (a FOR VALUES … clause, or DEFAULT) and marks child as the package's.
func createMarkedPartition(tx *gorm.DB, parent, child TableName, clause string) error {
	var err error

	err = tx.
		Exec("CREATE TABLE " + child.quoted() + " PARTITION OF " + parent.quoted() + " " + clause).
		Error
	if err != nil {
		return fmt.Errorf("san_pgpartition: create partition %s of %s: %w", child, parent, err)
	}

	// The mark that lets PartitionDropTable undo this create — and nothing
	// else. Written in the same transaction, so a created partition never
	// exists unmarked.
	err = tx.
		Exec("COMMENT ON TABLE " + child.quoted() + " IS '" + createdMarker + "'").
		Error
	if err != nil {
		return fmt.Errorf("san_pgpartition: mark partition %s: %w", child, err)
	}
	return nil
}

type parentInfo struct {
	strategy   string
	keyColumns int
}

func readParent(tx *gorm.DB, parent TableName) (parentInfo, error) {
	var err error
	var code string
	var info parentInfo

	err = tx.
		Raw(parentQuery, parent.Schema, parent.Name).
		Row().
		Scan(&code, &info.keyColumns)
	if errors.Is(err, sql.ErrNoRows) {
		return parentInfo{}, fmt.Errorf("%w: %s does not exist or is not partitioned", ErrParentNotPartitioned, parent)
	}
	if err != nil {
		return parentInfo{}, fmt.Errorf("san_pgpartition: read parent %s: %w", parent, err)
	}

	info.strategy = catalogStrategy(code)
	return info, nil
}

const parentQuery = `
SELECT pt.partstrat::text, pt.partnatts::int
FROM pg_catalog.pg_partitioned_table AS pt
JOIN pg_catalog.pg_class AS p ON p.oid = pt.partrelid
JOIN pg_catalog.pg_namespace AS pn ON pn.oid = p.relnamespace
WHERE pn.nspname = ?
  AND p.relname = ?`

type childState int

const (
	childMissing   childState = iota // no relation has the name: create it
	childAttached                    // a partition of this parent: compare bounds
	childElsewhere                   // the name is taken by anything else: conflict
)

// readChild says what, if anything, holds the child's name. Any relation
// counts — a table, a partition of another parent, an index, a view — since
// all of them block CREATE TABLE.
func readChild(tx *gorm.DB, parent, child TableName) (childState, error) {
	var err error
	var attached bool

	err = tx.
		Raw(childQuery, parent.Schema, parent.Name, child.Schema, child.Name).
		Row().
		Scan(&attached)
	if errors.Is(err, sql.ErrNoRows) {
		return childMissing, nil
	}
	if err != nil {
		return childMissing, fmt.Errorf("san_pgpartition: read child %s: %w", child, err)
	}

	if attached {
		return childAttached, nil
	}
	return childElsewhere, nil
}

const childQuery = `
SELECT c.relispartition AND EXISTS (
    SELECT 1
    FROM pg_catalog.pg_inherits AS i
    JOIN pg_catalog.pg_class AS p ON p.oid = i.inhparent
    JOIN pg_catalog.pg_namespace AS pn ON pn.oid = p.relnamespace
    WHERE i.inhrelid = c.oid
      AND pn.nspname = ?
      AND p.relname = ?
)
FROM pg_catalog.pg_class AS c
JOIN pg_catalog.pg_namespace AS cn ON cn.oid = c.relnamespace
WHERE cn.nspname = ?
  AND c.relname = ?`

const (
	probeSavepoint = "san_pgpartition_probe"
	probeParent    = "san_pgpartition_probe_parent"
	probeChild     = "san_pgpartition_probe_child"
)

// compareBound returns the existing child's bound and the requested one, both
// as PostgreSQL prints them, so equal strings mean the same partition.
//
// Text from the caller cannot be compared as typed: '2026-01-01' and
// 2026-01-01T00:00:00+07:00 are the same date bound. So the requested clause
// is applied to a throwaway temporary copy of the parent — same columns, same
// partition key — and PostgreSQL prints both bounds through the key's own
// types in this one session. The copy lives inside a savepoint that is always
// rolled back: nothing is left behind, even inside the caller's transaction.
func compareBound(tx *gorm.DB, parent, child TableName, clause string) (string, string, error) {
	var err error

	err = tx.
		Exec("SAVEPOINT " + probeSavepoint).
		Error
	if err != nil {
		return "", "", fmt.Errorf("san_pgpartition: open the bound probe: %w", err)
	}

	existing, wanted, probeErr := probeBound(tx, parent, child, clause)

	err = tx.
		Exec("ROLLBACK TO SAVEPOINT " + probeSavepoint).
		Error
	if err != nil {
		return "", "", fmt.Errorf("san_pgpartition: roll back the bound probe: %w", err)
	}

	err = tx.
		Exec("RELEASE SAVEPOINT " + probeSavepoint).
		Error
	if err != nil {
		return "", "", fmt.Errorf("san_pgpartition: release the bound probe: %w", err)
	}

	if probeErr != nil {
		return "", "", probeErr
	}
	return existing, wanted, nil
}

func probeBound(tx *gorm.DB, parent, child TableName, clause string) (string, string, error) {
	var err error
	var keyDef string

	err = tx.
		Raw(keyDefQuery, parent.Schema, parent.Name).
		Row().
		Scan(&keyDef)
	if err != nil {
		return "", "", fmt.Errorf("san_pgpartition: read the partition key of %s: %w", parent, err)
	}

	err = tx.
		Exec("CREATE TEMP TABLE " + probeParent + " (LIKE " + parent.quoted() + ") PARTITION BY " + keyDef).
		Error
	if err != nil {
		return "", "", fmt.Errorf("san_pgpartition: create the bound probe for %s: %w", parent, err)
	}

	err = tx.
		Exec("CREATE TEMP TABLE " + probeChild + " PARTITION OF pg_temp." + probeParent + " " + clause).
		Error
	if err != nil {
		return "", "", fmt.Errorf("san_pgpartition: apply the requested bound of %s: %w", child, err)
	}

	var existing, wanted string
	err = tx.
		Raw(boundTextQuery, child.Schema, child.Name).
		Row().
		Scan(&existing, &wanted)
	if err != nil {
		return "", "", fmt.Errorf("san_pgpartition: read the bound of %s: %w", child, err)
	}

	return existing, wanted, nil
}

const keyDefQuery = `
SELECT pg_catalog.pg_get_partkeydef(p.oid)
FROM pg_catalog.pg_class AS p
JOIN pg_catalog.pg_namespace AS pn ON pn.oid = p.relnamespace
WHERE pn.nspname = ?
  AND p.relname = ?`

const boundTextQuery = `
SELECT pg_catalog.pg_get_expr(c.relpartbound, c.oid),
       pg_catalog.pg_get_expr(probe.relpartbound, probe.oid)
FROM pg_catalog.pg_class AS c
JOIN pg_catalog.pg_namespace AS cn ON cn.oid = c.relnamespace
CROSS JOIN pg_catalog.pg_class AS probe
WHERE cn.nspname = ?
  AND c.relname = ?
  AND probe.oid = 'pg_temp.` + probeChild + `'::regclass`
