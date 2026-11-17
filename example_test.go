package san_pgpartition_test

import (
	"context"
	"errors"
	"log"
	"time"

	sanpgpartition "github.com/pdcgo/san_pgpartition"
	"gorm.io/gorm"
)

// A manager keeps its registry (partitioned_tables) in its own schema,
// "san_pgpartition" here — not where the converted tables live. NewManager
// touches nothing; the first call creates the schema and the table.
func ExampleNewManager() {
	var db *gorm.DB // the caller's connection
	manager := sanpgpartition.NewManager(db, "san_pgpartition")
	_ = manager

	// Inside a transaction of your own, make a manager for it:
	_ = db.Transaction(func(tx *gorm.DB) error {
		_ = sanpgpartition.NewManager(tx, "san_pgpartition")
		return nil
	})
}

// Converts public.stock_logs into a table partitioned by month on
// created_at, the months starting at 00:00 in Jakarta. One transaction: on
// any error nothing has changed. stock_logs_old is kept for you to drop.
func ExampleManager_PartitionExistTable() {
	var db *gorm.DB // the caller's connection
	ctx := context.Background()

	jakarta, err := time.LoadLocation("Asia/Jakarta")
	if err != nil {
		log.Printf("load the zone: %v", err)
		return
	}

	manager := sanpgpartition.NewManager(db, "san_pgpartition")
	logs := sanpgpartition.TableName{Schema: "public", Name: "stock_logs"}

	err = manager.PartitionExistTable(ctx, logs, &sanpgpartition.PartitionRangeConfig{
		Location:  jakarta,
		RangeType: sanpgpartition.RangeTypeMonthly,
		Key:       "created_at",
	})
	switch {
	case errors.Is(err, sanpgpartition.ErrAlreadyPartitioned):
		log.Printf("%s is already partitioned", logs)
	case errors.Is(err, sanpgpartition.ErrReferencedByForeignKey),
		errors.Is(err, sanpgpartition.ErrUniqueKeyWithoutPartitionKey),
		errors.Is(err, sanpgpartition.ErrPrimaryKeyNotExtendable),
		errors.Is(err, sanpgpartition.ErrUnsupportedTableFeature):
		log.Printf("%s needs a migration first: %v", logs, err) // the message names each offender
	case err != nil:
		log.Printf("convert %s: %v", logs, err)
	}
}

// The cron's first call: every converted table gets its children n of its
// own periods ahead — with 6, the next 6 months of a month table, 6 days of
// a daily one, 6 blocks of a FixedRange one. Rows already waiting in DEFAULT
// for a period it creates are moved into the new child. Safe to repeat.
func ExampleManager_MaintainPartitionTableProvision() {
	var db *gorm.DB // the caller's connection
	ctx := context.Background()

	manager := sanpgpartition.NewManager(db, "san_pgpartition")
	err := manager.MaintainPartitionTableProvision(ctx, 6)
	if err != nil {
		// One table failing does not stop the others: err joins one error
		// per failed table, each naming it.
		log.Printf("provision partitions: %v", err)
	}
}

// The cron's second call: rows that landed in a table's DEFAULT partition
// are moved into their children; the rest (a back-dated import, a typo
// year, NULL keys) stay and are reported every run.
func ExampleManager_MaintainPartition() {
	var db *gorm.DB // the caller's connection
	ctx := context.Background()

	manager := sanpgpartition.NewManager(db, "san_pgpartition")
	reports, err := manager.MaintainPartition(ctx)
	for _, report := range reports {
		if report.LeftInDefault > 0 {
			log.Printf("%s: %d rows moved, %d left in DEFAULT — look at them", report.Table, report.Moved, report.LeftInDefault)
		}
	}
	if err != nil {
		log.Printf("maintain partitions: %v", err)
	}
}

// A daily job runs both maintenance calls, provisioning first.
func ExampleManager_cron() {
	var db *gorm.DB // the caller's connection
	ctx := context.Background()

	manager := sanpgpartition.NewManager(db, "san_pgpartition")
	err := manager.MaintainPartitionTableProvision(ctx, 6)
	if err != nil {
		log.Printf("provision partitions: %v", err)
	}
	reports, err := manager.MaintainPartition(ctx)
	if err != nil {
		log.Printf("maintain partitions: %v", err)
	}
	log.Printf("maintained %d tables", len(reports))
}

// TableName prints as schema.name in messages; both parts are matched
// exactly (no case folding, no search_path).
func ExampleTableName_String() {
	logs := sanpgpartition.TableName{Schema: "public", Name: "stock_logs"}
	log.Printf("converting %s", logs) // converting public.stock_logs
}

// Numbers cut into blocks: BlockSize 1000 gives children <table>_0_999,
// <table>_1000_1999, … Location is not used.
func ExamplePartitionRangeConfig_fixed() {
	conf := &sanpgpartition.PartitionRangeConfig{
		RangeType: sanpgpartition.FixedRange,
		BlockSize: 1000,
		Key:       "id",
	}
	_ = conf
}

// Checks whether next month's partition is already attached to orders. It only
// reads the catalog: false for a missing table, an ordinary table, or a
// partition of another parent.
func ExamplePartitionAttached() {
	var db *gorm.DB // the caller's connection
	ctx := context.Background()

	orders := sanpgpartition.TableName{Schema: "public", Name: "orders"}
	march := sanpgpartition.TableName{Schema: "public", Name: "orders_2026_03"}

	exists, err := sanpgpartition.PartitionAttached(ctx, db, orders, march)
	if err != nil {
		log.Printf("check %s: %v", march, err)
		return
	}
	if !exists {
		log.Printf("%s is not a partition of %s yet", march, orders)
	}
}

// Creates next month's partition of a RANGE-partitioned orders table. Run it
// from a job as often as you like: a second run returns created == false.
func ExamplePartitionNewTable() {
	var db *gorm.DB // the caller's connection
	ctx := context.Background()

	orders := sanpgpartition.TableName{Schema: "public", Name: "orders"}
	march := sanpgpartition.TableName{Schema: "public", Name: "orders_2026_03"}

	created, err := sanpgpartition.PartitionNewTable(ctx, db, orders, march, sanpgpartition.PartitionBound{
		Range: &sanpgpartition.RangeBound{From: []any{"2026-03-01"}, To: []any{"2026-04-01"}},
	})
	if errors.Is(err, sanpgpartition.ErrPartitionConflict) {
		log.Printf("%s is taken by something that is not this partition: %v", march, err)
		return
	}
	if err != nil {
		log.Printf("create %s: %v", march, err)
		return
	}
	log.Printf("%s created now: %v", march, created)
}

// Undoes a create made by mistake. It refuses — and changes nothing — unless
// the partition was made by PartitionNewTable and is still empty.
func ExamplePartitionDropTable() {
	var db *gorm.DB // the caller's connection
	ctx := context.Background()

	orders := sanpgpartition.TableName{Schema: "public", Name: "orders"}
	march := sanpgpartition.TableName{Schema: "public", Name: "orders_2026_03"}

	dropped, err := sanpgpartition.PartitionDropTable(ctx, db, orders, march)
	switch {
	case errors.Is(err, sanpgpartition.ErrPartitionNotEmpty):
		log.Printf("%s already holds rows; it stays", march)
	case errors.Is(err, sanpgpartition.ErrPartitionNotOwned):
		log.Printf("%s was not created by san_pgpartition; it stays", march)
	case err != nil:
		log.Printf("undo %s: %v", march, err)
	default:
		log.Printf("%s removed: %v", march, dropped)
	}
}

// LIST and HASH bounds; the parent's own strategy decides which one is valid.
func ExamplePartitionBound() {
	java := sanpgpartition.PartitionBound{
		List: &sanpgpartition.ListBound{Values: []any{"java", "bali", nil}}, // nil: rows whose key is NULL
	}
	quarter := sanpgpartition.PartitionBound{
		Hash: &sanpgpartition.HashBound{Modulus: 4, Remainder: 1},
	}
	_, _ = java, quarter
}
