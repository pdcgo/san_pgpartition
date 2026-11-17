package san_pgpartition

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// testDatabaseName is the only database the integration tests will touch:
// they create and drop schemas, so any other database is refused.
const testDatabaseName = "san_pgpartition_test"

var schemaCounter atomic.Int64

func openTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	dsn := os.Getenv("SAN_PGPARTITION_TEST_DSN")
	if dsn == "" {
		t.Skip("SAN_PGPARTITION_TEST_DSN is not set")
	}

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}

	rawDB, err := db.DB()
	if err != nil {
		t.Fatalf("get the test connection pool: %v", err)
	}
	t.Cleanup(func() {
		_ = rawDB.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var databaseName string
	err = db.
		WithContext(ctx).
		Raw("SELECT current_database()").
		Row().
		Scan(&databaseName)
	if err != nil {
		t.Fatalf("read the test database name: %v", err)
	}
	if databaseName != testDatabaseName {
		t.Fatalf("refusing to run against database %q: the tests create and drop schemas, use %q", databaseName, testDatabaseName)
	}

	return db
}

// createTestSchema creates a throwaway schema, dropped with everything in it
// when the test ends.
func createTestSchema(t *testing.T, db *gorm.DB) string {
	t.Helper()

	schema := fmt.Sprintf("spg_%d_%d_%d", os.Getpid(), time.Now().UnixNano(), schemaCounter.Add(1))
	mustExec(t, db, "CREATE SCHEMA "+quoteIdentifier(schema))
	t.Cleanup(func() {
		err := db.
			Exec("DROP SCHEMA IF EXISTS " + quoteIdentifier(schema) + " CASCADE").
			Error
		if err != nil {
			t.Errorf("drop test schema %q: %v", schema, err)
		}
	})

	return schema
}

func mustExec(t *testing.T, db *gorm.DB, sql string, args ...any) {
	t.Helper()

	err := db.
		Exec(sql, args...).
		Error
	if err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

func countRows(t *testing.T, db *gorm.DB, table TableName) int64 {
	t.Helper()

	var count int64
	err := db.
		Raw("SELECT count(*) FROM " + table.quoted()).
		Row().
		Scan(&count)
	if err != nil {
		t.Fatalf("count rows in %s: %v", table, err)
	}
	return count
}

func partitionBoundText(t *testing.T, db *gorm.DB, table TableName) string {
	t.Helper()

	var bound string
	err := db.
		Raw(`SELECT pg_catalog.pg_get_expr(c.relpartbound, c.oid)
FROM pg_catalog.pg_class AS c
JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
WHERE n.nspname = ? AND c.relname = ?`, table.Schema, table.Name).
		Row().
		Scan(&bound)
	if err != nil {
		t.Fatalf("read the bound of %s: %v", table, err)
	}
	return bound
}

func mustContain(t *testing.T, got, want string) {
	t.Helper()

	if !strings.Contains(got, want) {
		t.Fatalf("%q does not contain %q", got, want)
	}
}
