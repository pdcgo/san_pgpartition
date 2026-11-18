// Package san_pgpartition manages PostgreSQL declarative partitions through a
// caller-supplied *gorm.DB.
//
// A [Manager] converts an existing plain table into one partitioned by RANGE
// on a date or number column ([Manager.PartitionExistTable]) — same name,
// rows, indexes and foreign keys, one transaction; every other table whose
// foreign key points at it is reshaped onto the new (id, key) primary key
// (a new column, filled, the key re-created, a fill trigger) — and lists each converted
// table with its [PartitionRangeConfig] in its own schema's
// partitioned_tables. The caller's cron then keeps every listed table ready:
// [Manager.MaintainPartitionTableProvision] creates the children n of each
// table's own periods ahead (moving any DEFAULT rows of a period it creates
// into the new child), and [Manager.MaintainPartition] moves the rows that
// still land in a table's DEFAULT partition into their children and reports,
// per table, what it moved and what it left ([MaintainReport]). A move lifts
// every foreign key pointing at the table around it — the table's own on
// itself included — so no ON DELETE action fires. Maintenance locks the
// tables holding those keys only as hard as the job needs: SHARE ROW
// EXCLUSIVE when it only creates children (the app keeps reading them),
// ACCESS EXCLUSIVE when it moves rows.
//
// Under it, plain functions work on any partitioned table: whether a child
// partition is attached to a parent ([PartitionAttached]), creating a fresh
// child under an existing partitioned parent ([PartitionNewTable]) and
// undoing such a create ([PartitionDropTable] — only for an empty partition
// the package itself created). The parent's strategy — RANGE, LIST or HASH —
// is read from the PostgreSQL catalog, never passed in.
//
// The package runs nothing on its own: no scheduler, no background goroutines.
// The caller (an application job, a cron, a CLI) decides when to call it.
//
// See README.md for the design, the locking it takes, the lock-table limit
// and what is out of scope.
package san_pgpartition
