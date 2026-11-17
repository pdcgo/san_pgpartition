package san_pgpartition

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"
)

// PartitionAttached reports whether child is a partition attached directly
// to parent.
//
// It answers false — not an error — when child does not exist, is an ordinary
// table, or is a partition of another parent. It only reads the catalog and
// takes no locks.
//
// It never creates or repairs indexes or foreign keys: PostgreSQL keeps an
// attached partition's copies of them in step with the parent, and refuses to
// drop them from the partition.
//
// It was called PartitionExistTable before that name went to the manager's
// conversion, [Manager.PartitionExistTable].
func PartitionAttached(ctx context.Context, db *gorm.DB, parent, child TableName) (bool, error) {
	var err error

	err = checkCall(ctx, db, parent, child)
	if err != nil {
		return false, err
	}

	var exists bool
	err = db.
		WithContext(ctx).
		Raw(attachedQuery, parent.Schema, parent.Name, child.Schema, child.Name).
		Row().
		Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("san_pgpartition: check partition %s under %s: %w", child, parent, err)
	}

	return exists, nil
}

const attachedQuery = `
SELECT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_inherits AS i
    JOIN pg_catalog.pg_class AS p ON p.oid = i.inhparent
    JOIN pg_catalog.pg_namespace AS pn ON pn.oid = p.relnamespace
    JOIN pg_catalog.pg_class AS c ON c.oid = i.inhrelid
    JOIN pg_catalog.pg_namespace AS cn ON cn.oid = c.relnamespace
    WHERE pn.nspname = ?
      AND p.relname = ?
      AND cn.nspname = ?
      AND c.relname = ?
      AND c.relispartition
)`

// checkCall validates what every exported function takes.
func checkCall(ctx context.Context, db *gorm.DB, parent, child TableName) error {
	var err error

	if ctx == nil {
		return errors.New("san_pgpartition: context is nil")
	}
	if db == nil {
		return errors.New("san_pgpartition: gorm database is nil")
	}

	err = parent.validate("parent")
	if err != nil {
		return err
	}

	err = child.validate("child")
	if err != nil {
		return err
	}

	if parent == child {
		return fmt.Errorf("%w: parent and child are both %s", ErrInvalidTableName, parent)
	}

	return nil
}
