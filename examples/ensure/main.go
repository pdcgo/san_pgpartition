package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"time"

	"github.com/pdcgo/san_pgpartition"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type User struct {
	ID        uint `gorm:"primarykey"`
	Name      string
	Username  string `gorm:"uniqueIndex;not null"`
	CreatedAt time.Time
}

func main() {
	ctx := context.Background()

	// database
	dsn := "host=localhost user=user password=password dbname=parttest port=5433 sslmode=disable TimeZone=Asia/Jakarta"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}

	// migration
	// db.AutoMigrate(
	// 	&User{},
	// )

	t := time.Now().UnixMilli()
	user := User{
		Name:      fmt.Sprintf("name_%d", t),
		Username:  fmt.Sprintf("username_%d", t),
		CreatedAt: time.Now(),
	}

	err = db.Save(&user).Error

	if err != nil {
		panic(err)
	}

	check, err := san_pgpartition.PartitionExistTable(
		ctx,
		db,
		san_pgpartition.TableName{
			Schema: "public",
			Name:   "users",
		},
		san_pgpartition.TableName{
			Schema: "public",
			Name:   "users_0",
		},
	)

	slog.Info("check", "check", check)

	if err != nil {
		panic(err)
	}

}
