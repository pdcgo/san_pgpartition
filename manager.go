package san_pgpartition

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
)

// registryTable is the table, inside the manager's schemaconf, that lists
// every table the manager has converted, one row each with its config.
const registryTable = "partitioned_tables"

// Manager converts plain tables into range-partitioned ones, keeps a
// registry of them with each table's config in its own schema, and keeps
// their children ready ahead ([Manager.MaintainPartitionTableProvision]) and
// their DEFAULT partitions emptied ([Manager.MaintainPartition]).
//
// A Manager holds no connection of its own and starts nothing in the
// background: every method runs on the *gorm.DB it was given, inside one
// transaction (a savepoint when that db is already in a transaction), and
// the caller decides when to call it. It is cheap — NewManager touches
// nothing in the database — so a caller holding a transaction can make a
// Manager for it: NewManager(tx, schemaconf).
type Manager struct {
	db         *gorm.DB
	schemaconf string

	// now stamps converted_at and decides the current period; tests set it.
	now func() time.Time

	// hooks pause a conversion at a step; tests set them.
	hooks managerHooks
}

// managerHooks are test-only pauses inside a call's transaction.
type managerHooks struct {
	// afterChecks runs once every check passed and the table's definition
	// was read, before anything is created.
	afterChecks func()
	// afterRenames runs once <t> is <t>_old and <t>_new is <t>, before the
	// seed — the window in which the app's reads and writes of <t> wait.
	afterRenames func()
	// afterTableLock runs once a maintenance call holds ACCESS EXCLUSIVE on
	// a table, before it creates the first child.
	afterTableLock func()
	// afterChild runs once a maintenance call created a child and moved its
	// rows in, holding every lock it has taken so far.
	afterChild func()
}

// NewManager returns a manager working through db that keeps its own tables
// in the schema schemaconf — its configuration and partitioned_tables, the
// list of converted tables. schemaconf is NOT where the converted tables
// live: each call names its table's schema in TableName.
//
// It touches nothing in the database. The first call that needs schemaconf
// and partitioned_tables creates them when they are missing.
func NewManager(db *gorm.DB, schemaconf string) *Manager {
	return &Manager{
		db:         db,
		schemaconf: schemaconf,
		now:        time.Now,
	}
}

// check validates what every Manager method needs.
func (m *Manager) check(ctx context.Context) error {
	var err error

	if m == nil {
		return errors.New("san_pgpartition: manager is nil")
	}
	if ctx == nil {
		return errors.New("san_pgpartition: context is nil")
	}
	if m.db == nil {
		return errors.New("san_pgpartition: gorm database is nil")
	}

	err = validateIdentifier("schemaconf", m.schemaconf)
	if err != nil {
		return err
	}

	return nil
}

func (m *Manager) registry() TableName {
	return TableName{Schema: m.schemaconf, Name: registryTable}
}

// ensureRegistry creates schemaconf and partitioned_tables when they are
// missing. Two first calls must not both try: CREATE SCHEMA IF NOT EXISTS
// is not safe against a concurrent create (the second fails on the catalog's
// unique index), so the create runs under a transaction-scoped advisory lock
// on the schemaconf. Once the table exists, no lock is taken.
func (m *Manager) ensureRegistry(tx *gorm.DB) error {
	var err error

	exists, err := m.registryExists(tx)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	err = tx.
		Exec("SELECT pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(pg_catalog.format('san_pgpartition registry %I', ?::text), 0))", m.schemaconf).
		Error
	if err != nil {
		return fmt.Errorf("san_pgpartition: lock the registry %s: %w", m.registry(), err)
	}

	err = tx.
		Exec("CREATE SCHEMA IF NOT EXISTS " + quoteIdentifier(m.schemaconf)).
		Error
	if err != nil {
		return fmt.Errorf("san_pgpartition: create schema %q: %w", m.schemaconf, err)
	}

	err = tx.
		Exec("CREATE TABLE IF NOT EXISTS " + m.registry().quoted() + ` (
    table_schema text NOT NULL,
    table_name text NOT NULL,
    key text NOT NULL,
    range_type text NOT NULL,
    location text NOT NULL,
    block_size integer NOT NULL,
    converted_at timestamptz NOT NULL,
    PRIMARY KEY (table_schema, table_name)
)`).
		Error
	if err != nil {
		return fmt.Errorf("san_pgpartition: create %s: %w", m.registry(), err)
	}

	return nil
}

func (m *Manager) registryExists(tx *gorm.DB) (bool, error) {
	var err error
	var exists bool

	err = tx.
		Raw("SELECT pg_catalog.to_regclass(?::text) IS NOT NULL", m.registry().quoted()).
		Row().
		Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("san_pgpartition: look up %s: %w", m.registry(), err)
	}
	return exists, nil
}

// lockTable serialises the manager's work on one table through this
// schemaconf — its conversion, provisioning and maintenance: a
// transaction-scoped advisory lock keyed on (schemaconf, schema, table). A
// second caller waits until the first commits or rolls back.
func (m *Manager) lockTable(tx *gorm.DB, table TableName) error {
	var err error

	err = tx.
		Exec("SELECT pg_catalog.pg_advisory_xact_lock("+tableLockKey+")", m.schemaconf, table.Schema, table.Name).
		Error
	if err != nil {
		return fmt.Errorf("san_pgpartition: take the manager's lock on %s: %w", table, err)
	}
	return nil
}

// tableLockKey is the advisory lock key of one table; it takes schemaconf,
// schema and table name. %I quotes each part, so no two different triples
// print alike. ("convert" in it is historical: every call on the table uses
// this one key.)
const tableLockKey = "pg_catalog.hashtextextended(pg_catalog.format('san_pgpartition convert %I %I %I', ?::text, ?::text, ?::text), 0)"

// registered reports whether partitioned_tables already lists table.
func (m *Manager) registered(tx *gorm.DB, table TableName) (bool, error) {
	var err error
	var found bool

	err = tx.
		Raw("SELECT EXISTS (SELECT 1 FROM "+m.registry().quoted()+" WHERE table_schema = ? AND table_name = ?)", table.Schema, table.Name).
		Row().
		Scan(&found)
	if err != nil {
		return false, fmt.Errorf("san_pgpartition: read %s for %s: %w", m.registry(), table, err)
	}
	return found, nil
}

// register writes table's row, the last step of a conversion.
func (m *Manager) register(tx *gorm.DB, table TableName, pconf *PartitionRangeConfig) error {
	var err error

	location := ""
	blockSize := 0
	if pconf.RangeType.isDate() {
		location = pconf.Location.String()
	} else {
		blockSize = pconf.BlockSize
	}

	err = tx.
		Exec("INSERT INTO "+m.registry().quoted()+" (table_schema, table_name, key, range_type, location, block_size, converted_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
			table.Schema, table.Name, pconf.Key, string(pconf.RangeType), location, blockSize, m.now()).
		Error
	if err != nil {
		return fmt.Errorf("san_pgpartition: register %s in %s: %w", table, m.registry(), err)
	}
	return nil
}
