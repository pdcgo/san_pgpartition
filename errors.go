package san_pgpartition

import "errors"

// Errors returned by this package. Match them with errors.Is; the wrapped
// message carries the table names and the detail.
var (
	// ErrInvalidTableName: a schema or table name is empty, longer than
	// PostgreSQL's 63 bytes, or contains a NUL byte.
	ErrInvalidTableName = errors.New("san_pgpartition: invalid table name")

	// ErrInvalidBound: the bound does not fit the parent — no strategy or more
	// than one strategy set, a strategy other than the parent's, a value count
	// different from the partition key's column count, or a value that cannot
	// be written as a literal.
	ErrInvalidBound = errors.New("san_pgpartition: invalid partition bound")

	// ErrParentNotPartitioned: the parent does not exist, or exists but is not
	// a declarative partitioned table — for the manager's maintenance calls,
	// a registered table that was dropped or changed since its conversion.
	ErrParentNotPartitioned = errors.New("san_pgpartition: parent is not a partitioned table")

	// ErrPartitionConflict: the child name is taken by something other than an
	// identical partition — an ordinary table, a partition of another parent,
	// or a partition of this parent with different bounds.
	ErrPartitionConflict = errors.New("san_pgpartition: partition conflict")

	// ErrPartitionNotOwned: PartitionDropTable was asked to remove a partition
	// that does not carry the mark PartitionNewTable writes on the tables it
	// creates — one made by a migration or by hand, or whose comment was
	// changed. It is never removed.
	ErrPartitionNotOwned = errors.New("san_pgpartition: partition was not created by san_pgpartition")

	// ErrPartitionNotEmpty: PartitionDropTable was asked to remove a partition
	// that holds rows. It is never removed: that would delete them.
	ErrPartitionNotEmpty = errors.New("san_pgpartition: partition is not empty")

	// ErrInvalidConfig: the PartitionRangeConfig given to
	// Manager.PartitionExistTable is nil or unusable — an unknown RangeType,
	// an empty Key, a date range without a named Location, a FixedRange
	// BlockSize below 1, or a Key column the table lacks or whose type does
	// not fit the range type; or Manager.MaintainPartitionTableProvision was
	// given a number below 1.
	ErrInvalidConfig = errors.New("san_pgpartition: invalid partition config")

	// ErrAlreadyPartitioned: Manager.PartitionExistTable was asked to convert
	// a table the manager's partitioned_tables already lists, or one
	// PostgreSQL already shows as partitioned (made by hand or by a
	// migration). Nothing was changed.
	ErrAlreadyPartitioned = errors.New("san_pgpartition: table is already partitioned")

	// ErrNotPlainTable: Manager.PartitionExistTable was asked to convert a
	// table that does not exist, or a relation that is not an ordinary table
	// (a view, a materialized view, a foreign table, a sequence…).
	ErrNotPlainTable = errors.New("san_pgpartition: not an existing plain table")

	// ErrReferencedByForeignKey: Manager.PartitionExistTable refused the
	// table because foreign keys point at it — other tables', or its own
	// pointing back at itself. After the swap they would still reference
	// <table>_old, and the partitioned table cannot take them on (id) alone.
	// The error names each one; nothing was changed.
	ErrReferencedByForeignKey = errors.New("san_pgpartition: foreign keys reference the table")

	// ErrUniqueKeyWithoutPartitionKey: Manager.PartitionExistTable refused
	// the table because a unique constraint or unique index other than the
	// primary key lacks the partition key — PostgreSQL cannot keep it unique
	// across partitions. The error names each one; nothing was changed.
	ErrUniqueKeyWithoutPartitionKey = errors.New("san_pgpartition: unique key without the partition key")

	// ErrPrimaryKeyNotExtendable: Manager.PartitionExistTable refused the
	// table because its primary key lacks the partition key and is not one
	// column filled by a sequence (nextval) — only such a key is extended to
	// (<column>, <key>). Nothing was changed.
	ErrPrimaryKeyNotExtendable = errors.New("san_pgpartition: primary key cannot take the partition key")

	// ErrUnsupportedTableFeature: Manager.PartitionExistTable refused the
	// table because it has something the conversion would silently leave on
	// <table>_old or lose: a trigger, a view, materialized view or rule on
	// it, row level security, a publication, an identity column,
	// inheritance, an exclusion constraint, granted privileges, extended
	// statistics, an unlogged table, or an owner other than the current
	// user. The error names each one; nothing was changed.
	ErrUnsupportedTableFeature = errors.New("san_pgpartition: table has a feature the conversion does not carry over")
)
