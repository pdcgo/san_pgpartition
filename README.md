# san_pgpartition

Go library that manages PostgreSQL **declarative partitions** through a caller-supplied `*gorm.DB`.

```go
import sanpgpartition "github.com/pdcgo/san_pgpartition"
```

It does two jobs:

1. **A manager for range-partitioned tables.** It converts an existing plain table into one partitioned by a date or
   number column, remembers it with its config, and — called by your cron — keeps the children it needs ready ahead
   and moves rows that landed in its `DEFAULT` partition into their children.
2. **Plain functions for any partitioned table**: is a child attached, create a fresh child, undo such a create.

The library has **no scheduler**: no goroutines, no timers, no connections of its own. The caller — an app job, a
cron, a CLI — decides when to run it. Every call runs in a transaction (a savepoint inside the caller's).

| Function | Does |
| --- | --- |
| `NewManager(db, schemaconf) *Manager` | A manager keeping its registry (`partitioned_tables`) in the schema `schemaconf`. Touches nothing in the database. |
| `manager.PartitionExistTable(ctx, table, pconf) error` | **Converts** an existing plain table into one partitioned by RANGE on a date or number column — same name, rows, indexes and foreign keys — in one transaction, and records it with its config. |
| `manager.MaintainPartitionTableProvision(ctx, n) error` | For every recorded table: creates the children `n` of its own periods ahead (6 months, 6 days, 6 years, 6 blocks), filling a missed cron's gap, moving `DEFAULT` rows of a period it creates into the new child. |
| `manager.MaintainPartition(ctx) ([]MaintainReport, error)` | For every recorded table: moves the `DEFAULT` rows lying between the oldest child and the horizon into their children; reports per table the rows moved and the rows left in `DEFAULT`. |
| `PartitionAttached(ctx, db, parent, child) (bool, error)` | Is `child` a partition attached directly to `parent`? Reads the catalog only, no locks. |
| `PartitionNewTable(ctx, db, parent, child, bound) (created bool, err error)` | Creates `child` as a fresh partition of an existing partitioned `parent`, and marks it as the package's. Safe to repeat. |
| `PartitionDropTable(ctx, db, parent, child) (dropped bool, err error)` | The undo of `PartitionNewTable`: removes `child` only if the package created it, it is empty and nothing references it. Safe to repeat. |
| `TableName{Schema, Name}.String()` | `schema.name`, for messages. |

`PartitionAttached` was called `PartitionExistTable` before that name went to the manager's conversion.

Compile-checked versions of every snippet below are in [example_test.go](example_test.go) (`ExampleNewManager`,
`ExampleManager_PartitionExistTable`, `ExampleManager_MaintainPartitionTableProvision`,
`ExampleManager_MaintainPartition`, `ExampleManager_cron`, `ExamplePartitionAttached`, `ExamplePartitionNewTable`,
`ExamplePartitionDropTable`, `ExamplePartitionBound`, `ExamplePartitionRangeConfig_fixed`, `ExampleTableName_String`):
`go test` builds them, so they cannot drift from the API.

## The manager in one picture

```mermaid
flowchart LR
  MIG["your migration, once"] --> PET["manager.PartitionExistTable"]
  PET --> REG[("schemaconf.partitioned_tables")]
  CRON["your cron, daily"] --> PROV["manager.MaintainPartitionTableProvision ctx, 6"]
  PROV --> MAINT["manager.MaintainPartition ctx"]
  REG -.-> PROV
  REG -.-> MAINT
  MAINT --> REP["one MaintainReport per table: moved, left in DEFAULT"]
```

## `NewManager`

```go
manager := sanpgpartition.NewManager(db, "san_pgpartition") // the manager's OWN schema, not the tables'
```

`schemaconf` is where the manager keeps **its own** table: `partitioned_tables`, one row per converted table with its
whole config (schema, name, key, range type, location by IANA name, block size, converted at). Each call names its
table's schema in `TableName`. `NewManager` touches nothing — the first conversion creates the schema and the table when
they are missing; the maintenance calls never create them — so a caller inside a transaction can make a manager for
it: `NewManager(tx, "san_pgpartition")`.

## Converting an existing table: `manager.PartitionExistTable`

```go
jakarta, err := time.LoadLocation("Asia/Jakarta")
if err != nil {
	return err
}
logs := sanpgpartition.TableName{Schema: "public", Name: "stock_logs"}

err = manager.PartitionExistTable(ctx, logs, &sanpgpartition.PartitionRangeConfig{
	Location:  jakarta,                         // days, months, years start at 00:00 here
	RangeType: sanpgpartition.RangeTypeMonthly, // "daily" | "month" | "year" | "fixed"
	Key:       "created_at",                    // the partition key column
})
if errors.Is(err, sanpgpartition.ErrAlreadyPartitioned) {
	// listed in san_pgpartition.partitioned_tables, or already partitioned by hand
}
```

### Range types

| `RangeType` | Value stored | Key column | Child names | Needs |
| --- | --- | --- | --- | --- |
| `RangeTypeDaily` | `daily` | `timestamptz`, `timestamp`, `date` | `stock_logs_20261117` | `Location` |
| `RangeTypeMonthly` | `month` | same | `stock_logs_202611` | `Location` |
| `RangeTypeYearly` | `year` | same | `stock_logs_2026` | `Location` |
| `FixedRange` | `fixed` | `smallint`, `integer`, `bigint` | `items_0_3`, `items_4_7`; below zero `items_m4_m1` | `BlockSize` ≥ 1 |

```go
// Numbers cut into blocks of 1,000: items_0_999, items_1000_1999, …
err = manager.PartitionExistTable(ctx, items, &sanpgpartition.PartitionRangeConfig{
	RangeType: sanpgpartition.FixedRange,
	BlockSize: 1000,
	Key:       "id",
})
```

- A **`timestamptz`** key is cut at 00:00 in `Location`: in Jakarta, `2026-10-31 23:30+07` lands in `_202610` and
  `2026-11-01 00:10+07` in `_202611`, whatever the session's time zone. A `timestamp` key (no zone) is cut at 00:00
  of its own wall clock, a `date` key at the date.
- `Location` is stored by its IANA name (`Asia/Jakarta`), so it must be a named zone `time.LoadLocation` can load again:
  `time.Local` and unnamed `time.FixedZone`s are refused (`ErrInvalidConfig`). A program running where the OS has no
  zone database (a scratch container) needs `import _ "time/tzdata"`.
- `FixedRange` blocks are `[k·BlockSize, (k+1)·BlockSize)`, `k` rounded down, so `-1` is in `[-4, 0)` with
  `BlockSize: 4`. `BlockSize` is ignored by the date types, `Location` by `FixedRange`.
- **Prefer `month`.** A lookup without the key (by `id` alone) plans over every child: measured on ~1,200 daily
  children ≈ 30 ms warm and 257 ms on a new connection's first query, < 1 ms on 40 monthly children; with the key in the
  `WHERE`, 0.1 ms. GORM's `First(&row, id)` is that kind of lookup. Use `daily` only for a huge table read by date.

### What a conversion does

One transaction (a savepoint inside the caller's). Any error rolls all of it back: the table, its index names and
foreign keys are exactly as before, there is no `_new`, no `_old`, no registry row.

```mermaid
flowchart TD
  L[advisory lock on schemaconf + table] --> R{"listed in partitioned_tables, or relkind p?"}
  R -->|yes| AP[ErrAlreadyPartitioned]
  R -->|no| P{"missing or not a plain table?"}
  P -->|yes| NP[ErrNotPlainTable]
  P -->|no| K[LOCK table ROW EXCLUSIVE, check the key column]
  K --> X{"anything to refuse?"}
  X -->|yes| E["one error naming every offender"]
  X -->|no| N[CREATE TABLE t_new LIKE t PARTITION BY RANGE key]
  N --> I["rename each index to name_old, create it on t_new under its name"]
  I --> F[drop the table's own foreign keys]
  F --> S["rename t to t_old, t_new to t"]
  S --> FA[add the foreign keys back on t]
  FA --> C["one child per period of t_old's rows, plus t_default"]
  C --> SD[copy the rows from t_old]
  SD --> Q[hand t_old's sequences to t]
  Q --> AN[ANALYZE t]
  AN --> W[write the registry row]
```

- **Indexes keep their names.** Each index of the old table is renamed `<name>_old` (a primary or unique key through
  `RENAME CONSTRAINT`) and re-created on the new table under its original name and definition — partial and
  expression indexes too — so name-based tools (GORM `HasIndex`, `AutoMigrate`) still find them.
- **The primary key gains the key** when it lacks it: `PRIMARY KEY (id)` becomes `PRIMARY KEY (id, created_at)`,
  same name. Only a ONE-column key filled by a sequence (`BIGSERIAL`, a `nextval(…)` default) is extended. PostgreSQL
  then guarantees only the pair: `id` stays unique through its sequence (an explicit id is not checked), a lookup by
  `id` alone searches every child, `ON CONFLICT (id)` stops working. It also makes the key column `NOT NULL`: a row
  whose key is NULL fails the copy, and the conversion rolls back.
- **The table's own foreign keys move**: dropped from the old table before the renames, added back to the new one
  after them under the same names, always validated (PostgreSQL refuses `NOT VALID` on a partitioned table — a
  `NOT VALID` key with a row breaking it makes the copy fail and the conversion roll back).
- **The table is closed from the renames to the commit.** The app's reads and writes wait, then run on the new
  partitioned table: an insert issued meanwhile lands in the new child (tested). Converting a big, busy table is a
  maintenance-window job.
- **Children** cover every period from the oldest to the newest key value — empty ones in between too — plus
  `<table>_default` for the rest (NULL and infinite keys). An empty table gets only the DEFAULT and is still swapped.
  Each child carries the comment `created by san_pgpartition`, so `PartitionDropTable` can undo an empty one. The new
  table keeps the old table's own comment.
- **`<table>_old` is kept** with every original row. The id sequence is handed to the new table (`OWNED BY`), so your
  later migration can `DROP TABLE <table>_old`; until then the data takes its disk space twice.
- `ANALYZE` runs on the new table: autovacuum never analyzes a partitioned parent.

### What stops a conversion

Checked before anything is created; every check runs, and the error names every offender. Errors of several kinds
come back joined: `errors.Is` matches each.

| Error | The table has… | Why |
| --- | --- | --- |
| `ErrReferencedByForeignKey` | another table's foreign key pointing at it (named `schema.table.constraint`), or its own pointing at itself | after the swap it would still reference `<table>_old`; the partitioned table cannot take it on `(id)` alone |
| `ErrUniqueKeyWithoutPartitionKey` | a unique key or unique index, other than the primary key, without the partition key | PostgreSQL can only keep a key unique across partitions when it holds the partition key |
| `ErrPrimaryKeyNotExtendable` | a primary key without the partition key that is not one sequence-fed column (`event_id`, a composite key) | extending a key the application fills (a dedupe key) breaks `ON CONFLICT` and schema checks |
| `ErrUnsupportedTableFeature` | a trigger, a view / materialized view / rule on it, row level security or a policy, a publication, an identity column, inheritance (either way, a partition of another table too), an exclusion constraint, granted privileges, extended statistics, an unlogged table, an owner other than the current user | each would stay on `<table>_old` or be lost without a word |

## Keeping children ready: `manager.MaintainPartitionTableProvision`

```go
err := manager.MaintainPartitionTableProvision(ctx, 6)
if err != nil {
	// one table failing does not stop the others: err joins one error per
	// failed table, each naming it
	log.Printf("provision partitions: %v", err)
}
```

Every table in `partitioned_tables` gets its children `n` of **its own** periods ahead — one number from the cron, the
same for every table. On 2026-11-17 in Jakarta, `n = 6` keeps ready:

| `RangeType` | Children it makes sure exist |
| --- | --- |
| `month` | `_202611` (the current month) … `_202705` |
| `daily` | `_20261117` (today) … `_20261123` |
| `year` | `_2026` … `_2032` |
| `fixed`, `BlockSize: 4`, highest key in the children 9 | `_8_11` (the block holding 9) … `_32_35` |

- **The current period** is taken in the table's `Location` (a `timestamp` / `date` key: the wall clock there).
- **A missed cron is caught up**: every period from the table's newest child up to the current one is created too.
  Older gaps inside history (a child someone removed) are left alone — `MaintainPartition` deals with rows there.
- **`fixed`** counts from the highest key value in the table's **children** — `DEFAULT` never counts, so a typo id
  there cannot make it create blocks far ahead. Children holding no rows (a table converted empty) start from block 0.
- **Rows already waiting in `DEFAULT`** for a period it creates are moved into the new child in the same transaction
  (PostgreSQL refuses to create a child while `DEFAULT` holds rows of its range): the parent is locked `ACCESS
  EXCLUSIVE`, the rows are deleted from `DEFAULT` into a temporary table, the child is created, the rows go back in
  through the parent (which routes them to it). Fewer rows back than out fails the call — a move never loses rows.
- **Safe to repeat**: a child that exists (found by its bounds) is left alone; a second call creates nothing and never
  locks the table exclusively. New children get the same names and the `created by san_pgpartition` comment as the conversion's.
- `n` below 1 is `ErrInvalidConfig`. Without a registry (nothing converted yet) it returns `nil` and creates nothing.

## Emptying `DEFAULT`: `manager.MaintainPartition`

```go
reports, err := manager.MaintainPartition(ctx)
for _, report := range reports {
	if report.LeftInDefault > 0 {
		log.Printf("%s: %d moved, %d left in DEFAULT", report.Table, report.Moved, report.LeftInDefault)
	}
}
if err != nil {
	log.Printf("maintain partitions: %v", err)
}
```

```go
type MaintainReport struct {
	Table         TableName
	Moved         int64 // rows moved out of DEFAULT into their children by this call
	LeftInDefault int64 // rows still in DEFAULT afterwards, reported on every call
}
```

A row lands in `DEFAULT` when no child takes it: the cron did not run, a back-dated import, a typo year, a NULL key.
`MaintainPartition` moves a `DEFAULT` row only when its period (or block) lies **between the table's oldest child and
the horizon**, creating that period's child with the same move as provisioning:

| Range type | Moved when the period starts… | Horizon |
| --- | --- | --- |
| `daily` / `month` / `year` | at or after the oldest child's start, before the horizon | the later of the newest child's end and the end of the current period |
| `fixed` | at or after the oldest child's start, before the horizon | the newest child's end |
| a table with no children | dates: only the current period; `fixed`: nothing | — |

Monthly table, children `_202610` … `_202611`, maintained on 2027-01-10:

| Row in `DEFAULT` | Result |
| --- | --- |
| 2026-12-05, 2027-01-01, 2027-01-31 23:59:59 | moved into new `_202612` and `_202701` |
| 2026-09-30 (before the oldest child), 2019-03 (an import) | stays, `LeftInDefault` |
| 2027-02-01 (past the horizon), 2062-11 (a typo) | stays, `LeftInDefault` |
| NULL, `infinity`, `-infinity` | stays, `LeftInDefault` |

- Rows that stay create no odd child tables; they are reported on every call until someone deals with them. Every later
  child creation scans `DEFAULT` (measured: 7 empty monthly children with 200,000 rows in `DEFAULT`, 150 ms).
- One report per registered table — zeros included — sorted by schema, then name. A table that fails is left out of
  the reports; its error, naming it, is in the joined error. A second call moves nothing.
- Without a registry it returns `nil, nil`.

```mermaid
flowchart TD
  T["each row of partitioned_tables, sorted by schema and name"] --> LK["one transaction, the manager's advisory lock on the table"]
  LK --> RD["read the children's bounds and the DEFAULT partition"]
  RD --> G["group DEFAULT rows by period or block"]
  G --> W{"between the oldest child and the horizon?"}
  W -->|no| ST["stays in DEFAULT, counted in LeftInDefault"]
  W -->|yes| AX["LOCK parent ACCESS EXCLUSIVE, once"]
  AX --> MV["DELETE the period's rows from DEFAULT into a temporary table"]
  MV --> CC["CREATE the child, marked created by san_pgpartition"]
  CC --> IB["INSERT the rows back through the parent, same count or fail"]
  IB --> RP["report: Moved, LeftInDefault"]
  ST --> RP
```

## Maintenance locks

- Each table is **one transaction**, serialised with the conversion and the other maintenance call through the
  manager's advisory lock on (`schemaconf`, schema, table). Two crons at once: the second waits, then finds the
  children there and nothing to move (tested; with the lock removed the second fails with `relation … already exists`,
  or deadlocks).
- A table that fails is rolled back alone — the rows it moved are back in `DEFAULT` — and the next tables are still
  done. The returned error joins one error per failed table (`errors.Is` matches each; a registered table dropped since
  is `ErrParentNotPartitioned`).
- Working out what is missing blocks nobody: catalog reads, plus a plain `SELECT` of the table (`fixed`: its highest
  key) or of its `DEFAULT` (`MaintainPartition`). Only when something must be created does it take **`ACCESS
  EXCLUSIVE` on the parent** (`LOCK TABLE ONLY` — the children are not locked one by one), held to the commit: the
  app's reads and writes of the table wait meanwhile, then run on the new child. Measured: 200,000 rows moved out of `DEFAULT` into
  a new month in 4.0 s; 365 empty daily children created in 2.6 s.

```mermaid
sequenceDiagram
  participant C as cron, MaintainPartitionTableProvision
  participant P as PostgreSQL
  participant A as the app
  C->>P: advisory lock on the table, read its children
  C->>P: LOCK parent ACCESS EXCLUSIVE
  A->>P: INSERT a row of the new month
  Note over A,P: waits
  C->>P: move DEFAULT rows out, create the child, insert them back
  C->>P: commit
  P-->>A: the insert runs and lands in the new child
```

## The lock-table limit

Every child created in one transaction — a conversion, or one table's provisioning or maintenance — holds its locks
(about 14 with 5 indexes and a foreign key) until the commit, and the server's lock table holds about
`max_locks_per_transaction × max_connections` (64 × 100 by default). Measured on PostgreSQL 17's defaults: a conversion
of a table with 5 indexes ran out near 1,175 daily children; 1,000 daily children took 7.1 s; 1,200 took 9.2 s with
`max_locks_per_transaction = 128`. Running out is SQLSTATE 53200 *out of shared memory*: the whole call (or that
table) rolls back, and the error says how many children it was creating and what to do — raise
`max_locks_per_transaction` (a server setting, needs a restart) or create fewer children per call (**prefer `month`**,
or a smaller provisioning number). `errors.As` still finds the PostgreSQL error.

## Locks of the conversion

- Conversions of one table through one `schemaconf` wait for each other (the same advisory lock). Three callers released
  together end as one conversion and two `ErrAlreadyPartitioned` (a test forces that race; with the lock removed it
  deadlocks).
- The first calls of a fresh manager create `schemaconf` and `partitioned_tables` under an advisory lock on the
  `schemaconf`, taken only while the table is missing (with it removed, concurrent first calls fail on
  `pg_namespace_nspname_index`).
- While it reads the table it holds `ROW EXCLUSIVE` on it: the app's reads and writes go on, but DDL — `CREATE INDEX`,
  `CREATE TRIGGER`, `ALTER TABLE`, a new foreign key to it — waits and then applies to the new table (tested).
- Renaming the first index and the renames take `ACCESS EXCLUSIVE` until the commit: the table is closed to reads and
  writes from there to the commit.
- Measured: 200,000 rows into 25 monthly children 4.1 s, into 730 daily children 8.7 s.

```mermaid
sequenceDiagram
  participant M as PartitionExistTable
  participant P as PostgreSQL
  participant A as the app
  M->>P: rename table to table_old, table_new to table
  A->>P: insert or select on table
  Note over A,P: waits
  M->>P: children, seed, sequences, ANALYZE, registry row, commit
  P-->>A: runs on the new partitioned table
```

## Lower level: functions for any partitioned table

The parent is created by your own migration — these functions never create or alter it:

```sql
CREATE TABLE orders (
  id         bigint NOT NULL,
  created_at date   NOT NULL,
  PRIMARY KEY (id, created_at)   -- a unique key must include the partition key
) PARTITION BY RANGE (created_at);
```

```go
orders := sanpgpartition.TableName{Schema: "public", Name: "orders"}
march := sanpgpartition.TableName{Schema: "public", Name: "orders_2026_03"}
```

### `PartitionAttached`

Is `orders_2026_03` a partition of `orders`? Reads only; `false` when it is missing, an ordinary table or another
parent's partition.

```go
exists, err := sanpgpartition.PartitionAttached(ctx, db, orders, march)
if err != nil {
	return err
}
if !exists {
	// not there yet: create it
}
```

### `PartitionNewTable`

Safe to repeat: `created` is `false` when the same partition is already there.

```go
created, err := sanpgpartition.PartitionNewTable(ctx, db, orders, march, sanpgpartition.PartitionBound{
	Range: &sanpgpartition.RangeBound{From: []any{"2026-03-01"}, To: []any{"2026-04-01"}},
})
if errors.Is(err, sanpgpartition.ErrPartitionConflict) {
	// the name is taken by something that is not this exact partition
}
```

### `PartitionDropTable`

Removes it only if this package created it and it is still empty.

```go
dropped, err := sanpgpartition.PartitionDropTable(ctx, db, orders, march)
if errors.Is(err, sanpgpartition.ErrPartitionNotEmpty) {
	// rows already arrived: it stays
}
```

### `PartitionBound`

The parent's strategy is **read from the catalog**, never passed in. Set exactly one field of `PartitionBound` — the
one matching the parent; anything else is `ErrInvalidBound`.

```go
java := sanpgpartition.PartitionBound{
	List: &sanpgpartition.ListBound{Values: []any{"java", "bali", nil}}, // nil: rows whose key is NULL
}
quarter := sanpgpartition.PartitionBound{
	Hash: &sanpgpartition.HashBound{Modulus: 4, Remainder: 1},
}
```

| Parent | Bound | SQL | Key columns |
| --- | --- | --- | --- |
| `RANGE` | `Range: &RangeBound{From: []any{…}, To: []any{…}}` | `FOR VALUES FROM (…) TO (…)` — `From` inclusive, `To` exclusive | one value per key column, so composite keys work: `From: []any{"jkt", "2026-01-01"}` |
| `LIST` | `List: &ListBound{Values: []any{…}}` | `FOR VALUES IN (…)`; a `nil` value is `NULL` | PostgreSQL allows ONE key column |
| `HASH` | `Hash: &HashBound{Modulus: 4, Remainder: 1}` | `FOR VALUES WITH (MODULUS 4, REMAINDER 1)` | covers every key column |

**Values** are written as PostgreSQL literals and cast to the key column's type by PostgreSQL:

| Go value | Sent as |
| --- | --- |
| `string` | as is — the column type's input form: `"2026-01-01"`, `"42"` |
| `bool`, every `int`/`uint` kind, `float32`/`float64` | its decimal text (NaN and ±Inf refused) |
| `time.Time` | RFC 3339 with its own offset. A `date` / `timestamp` key keeps the wall-clock date/time as written; a `timestamptz` key gets the exact instant |
| `fmt.Stringer` (a UUID, a decimal) | `String()` |
| `nil` or a nil pointer | `NULL` — LIST only |

Every value is quoted by PostgreSQL's own `format('%L')`, and names are always double-quoted, so neither can inject
SQL. `MINVALUE` / `MAXVALUE` range bounds are not supported yet.

### Names

`TableName{Schema, Name}` — both required, so nothing depends on `search_path`. Names are **exact**: quoted, not
folded to lower case (`Orders` ≠ `orders`). A name over 63 bytes is refused (`ErrInvalidTableName`): PostgreSQL would
silently truncate it, and the table could then never be found by its full name. The caller chooses the child's name.

```go
log.Printf("converting %s", logs) // TableName.String(): "public.stock_logs"
```

### Safe to repeat

| `PartitionNewTable` finds the child name… | Result |
| --- | --- |
| free | creates it → `true, nil` |
| a partition of `parent` with the **same** bounds | `false, nil` |
| a partition of `parent` with **other** bounds | `false, ErrPartitionConflict` |
| an ordinary table, another parent's partition, an index, a view… | `false, ErrPartitionConflict` |

"Same bounds" is judged by PostgreSQL through the key's types, not by comparing text: `"2026-01-01"` and a
`time.Time` on that day are the same `date` bound. To do that, the requested bound is applied to a temporary copy of
the parent (same columns, same key) inside a savepoint that is always rolled back; both bounds are then printed by
`pg_get_expr` in the same session and compared. Nothing is left behind.

```mermaid
flowchart TD
  A[validate names and bound shape] --> B[read the parent's strategy and key from the catalog]
  B --> C[check the bound against the parent]
  C --> D[LOCK parent SHARE UPDATE EXCLUSIVE]
  D --> E{child name}
  E -->|free| F[CREATE TABLE child PARTITION OF parent] --> G[created = true]
  E -->|partition of parent| H[compare bounds in a rolled-back probe]
  H -->|same| I[created = false]
  H -->|different| J[ErrPartitionConflict]
  E -->|anything else| J
```

### Undo: `PartitionDropTable`

Every table `PartitionNewTable` (and the manager) creates gets the comment **`created by san_pgpartition`**, in the
same transaction as the create. `PartitionDropTable` removes a partition only when it can lose nothing; otherwise it
changes nothing:

| `PartitionDropTable` finds the child… | Result |
| --- | --- |
| missing | `false, nil` (safe to repeat) |
| not a partition of `parent` | `false, ErrPartitionConflict` |
| without the exact comment — made by a migration or by hand, or its comment edited | `false, ErrPartitionNotOwned` |
| holding rows | `false, ErrPartitionNotEmpty` |
| referenced by another table's foreign key pointing at the child itself | `false`, PostgreSQL's error — rolled back |
| marked, empty | `DETACH PARTITION`, then `DROP TABLE` → `true, nil` |

- **Detach, then drop, never `CASCADE`.** While another table's foreign key references the *parent*, PostgreSQL refuses a
  plain `DROP` even of an empty partition; detaching first removes the partition from that foreign key, which itself
  stays. `CASCADE` would delete the whole foreign key from the other table.
- **No row can slip in.** It locks the parent, then the child, `ACCESS EXCLUSIVE` — the order an insert through the parent
  takes them, so the two cannot deadlock — and looks for rows only under those locks. An insert in flight finishes
  first; its row then makes the undo refuse. A test holds an insert open while the undo starts; with the locks removed
  that test sees the partition dropped together with the committed row.
- Partitions your migrations create are never marked, so the package can never remove them.

```mermaid
flowchart TD
  A[validate names] --> B[read the parent from the catalog]
  B --> C[LOCK parent ACCESS EXCLUSIVE]
  C --> D{child name}
  D -->|missing| E[dropped = false]
  D -->|anything but a partition of parent| F[ErrPartitionConflict]
  D -->|partition of parent| G[LOCK child ACCESS EXCLUSIVE]
  G --> H{marked by the package}
  H -->|no| I[ErrPartitionNotOwned]
  H -->|yes| J{holds rows}
  J -->|yes| K[ErrPartitionNotEmpty]
  J -->|no| L[DETACH PARTITION then DROP TABLE] --> M[dropped = true]
```

### Locks and transactions of the plain functions

- Each call runs in its own transaction; given a `db` already inside one, GORM uses a savepoint instead.
- `PartitionNewTable` first takes **SHARE UPDATE EXCLUSIVE** on the parent (`LOCK TABLE ONLY`). Reads and writes
  continue; concurrent calls for the same parent wait for each other, so eight callers asking for the same child end as
  one create and seven `false, nil` — never a duplicate-table error. A test forces exactly that race.
- When it creates, `CREATE TABLE … PARTITION OF` takes **ACCESS EXCLUSIVE** on the parent until the transaction
  commits — every read and write of the parent waits for it. Alone that is a moment; inside a caller's long
  transaction it lasts until that commits. Call it outside long transactions.
- The bound comparison needs the `TEMP` privilege on the database (granted to everyone by default); so do the manager's
  moves out of `DEFAULT`.
- `PartitionDropTable` takes **ACCESS EXCLUSIVE** on the parent from its start (and then on the child), so reads and
  writes of the parent wait for it the same way.

### Indexes and foreign keys

The plain functions create, check and repair **no** index and no foreign key: PostgreSQL keeps every attached
partition in step with its parent (verified on PostgreSQL 17).

| | |
| --- | --- |
| New partition | `CREATE TABLE … PARTITION OF` gives it the parent's columns, indexes, primary/unique keys, `CHECK`/`NOT NULL` constraints and outgoing foreign keys |
| Existing partition | its copies cannot be dropped: *"cannot drop index … because index … requires it"*, *"cannot drop inherited constraint"* |
| Incoming foreign key (`order_items → orders`) | points at the parent, so it covers every partition, new ones included. It must reference a unique key that includes the partition key, e.g. `(id, created_at)` |

`PartitionAttached` is therefore a pure check and never rebuilds anything. The one gap PostgreSQL leaves is an index
started with `CREATE INDEX … ON ONLY parent` (the non-blocking way) and not finished: it stays invalid and the existing
partitions lack it. Finishing such an index belongs to a future index-build function, not to the existence check.

Inserts through the parent are routed to the matching child; a row no partition accepts is an error unless the table
has a `DEFAULT` partition (`PartitionNewTable` does not create one; the manager's conversion does).

## Errors

Match with `errors.Is`; the message carries the names and the detail.

| Error | When |
| --- | --- |
| `ErrInvalidTableName` | empty schema/name, over 63 bytes, a NUL byte, or `child == parent`; for `PartitionExistTable` and the maintenance calls, also a name they would create (`_new`, `_old`, `_default`, a child, an index's `_old`) over 63 bytes; an empty `schemaconf` |
| `ErrInvalidBound` | no strategy or two set, not the parent's strategy, wrong value count, `NULL` in a RANGE, empty LIST, bad HASH modulus/remainder, an unsupported value type |
| `ErrParentNotPartitioned` | the parent does not exist or is not a partitioned table; for the maintenance calls, a registered table dropped or changed since its conversion |
| `ErrPartitionConflict` | the child name is taken by anything but this exact partition; for `PartitionDropTable`, the child is not a partition of the parent |
| `ErrPartitionNotOwned` | `PartitionDropTable`: the partition does not carry the package's `created by san_pgpartition` comment |
| `ErrPartitionNotEmpty` | `PartitionDropTable`: the partition holds rows |
| `ErrInvalidConfig` | `PartitionExistTable`: a nil config, an unknown `RangeType`, an empty `Key`, a date range without a named `Location`, `BlockSize` < 1 for `fixed`, or a key column that is missing, generated, or of a type the range type cannot cut; `MaintainPartitionTableProvision`: `n` < 1 |
| `ErrAlreadyPartitioned` | `PartitionExistTable`: `partitioned_tables` lists the table, or PostgreSQL already shows it as partitioned |
| `ErrNotPlainTable` | `PartitionExistTable`: the table does not exist, or is a view, materialized view, foreign table… |
| `ErrReferencedByForeignKey`, `ErrUniqueKeyWithoutPartitionKey`, `ErrPrimaryKeyNotExtendable`, `ErrUnsupportedTableFeature` | `PartitionExistTable` refused the table — see *What stops a conversion* |

Anything else is a database error, wrapped: for a conversion, a row breaking a moved foreign key or a NULL key under an
extended primary key; for any call creating children, the lock table running out (SQLSTATE 53200, with the hint
above); for a move, `moved N rows out of … but M back` (something — a trigger added later — swallowed rows; rolled
back). On any error nothing of that call (or of that table, for the maintenance calls) was created, renamed, moved or
removed.

## Not in scope (yet)

- **Retention** — dropping old children that hold rows. Decided not now: it is decided when a table needs it (which
  tables, how long to keep, what happens to `_old`). `PartitionDropTable` only undoes empty children the package
  created.
- **Tables other tables point at** — a foreign key into the table stops the conversion (`ErrReferencedByForeignKey`);
  the caller removes or reshapes it first. The other refusals under *What stops a conversion* likewise.
- **Attaching an existing, filled table** (`ATTACH PARTITION`) — a separate API later: it validates every row and locks
  differently.
- **Non-blocking index building** — `ON ONLY` parent, `CONCURRENTLY` per partition, then `ATTACH` — including finishing
  an index left incomplete.
- `DEFAULT` partitions through `PartitionNewTable`, `MINVALUE`/`MAXVALUE` range bounds, LIST/HASH in the manager.
- Creating or altering the parent (outside the manager's conversion).

## Testing

The tests run against a real PostgreSQL. They create and drop schemas, so they refuse any database not named
`san_pgpartition_test`, and skip when `SAN_PGPARTITION_TEST_DSN` is unset (only the DB-free value tests run then).
Maintenance tests inject the manager's clock; the lock-limit tests fill the server's lock table with advisory locks for
a moment, so run nothing else against that server meanwhile.

```sh
docker run --rm -d --name san_pgpartition_test -e POSTGRES_USER=pgp -e POSTGRES_PASSWORD=localtest \
  -e POSTGRES_DB=san_pgpartition_test -p 127.0.0.1:55432:5432 postgres:17-alpine

SAN_PGPARTITION_TEST_DSN='postgres://pgp:localtest@127.0.0.1:55432/san_pgpartition_test?sslmode=disable' \
  go test . -race -count=1 -p 1

docker stop san_pgpartition_test
```

`SAN_PGPARTITION_PERF=1` also runs the timing tests (`TestPerf…`: 1,200 daily children — set
`SAN_PGPARTITION_PERF_DAYS` to change the count — 200,000 rows converted, provisioning 365 days, moving 200,000
`DEFAULT` rows).

Run from this folder: it is its own Go module (`github.com/pdcgo/san_pgpartition`), so a parent repo's `go test ./...`
does not enter it. Test `.`, not `./...`: `examples/ensure` is a hand-run program against its own database.
