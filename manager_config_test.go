package san_pgpartition

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func mustLoadLocation(t *testing.T, name string) *time.Location {
	t.Helper()

	loc, err := time.LoadLocation(name)
	if err != nil {
		t.Fatalf("load location %q: %v", name, err)
	}
	return loc
}

func TestRangeTypeValuesAreTheStoredOnes(t *testing.T) {
	cases := map[RangeType]string{
		RangeTypeDaily:   "daily",
		RangeTypeMonthly: "month",
		RangeTypeYearly:  "year",
		FixedRange:       "fixed",
	}
	for rangeType, want := range cases {
		if string(rangeType) != want {
			t.Fatalf("RangeType %q, want %q", rangeType, want)
		}
	}
}

func TestPartitionRangeConfigValidate(t *testing.T) {
	jakarta := mustLoadLocation(t, "Asia/Jakarta")

	cases := []struct {
		name    string
		conf    *PartitionRangeConfig
		wantErr string // "" = valid
	}{
		{name: "monthly in Jakarta", conf: &PartitionRangeConfig{Location: jakarta, RangeType: RangeTypeMonthly, Key: "created_at"}},
		{name: "daily in UTC", conf: &PartitionRangeConfig{Location: time.UTC, RangeType: RangeTypeDaily, Key: "created_at"}},
		{name: "yearly", conf: &PartitionRangeConfig{Location: jakarta, RangeType: RangeTypeYearly, Key: "created_at"}},
		{name: "date type ignores BlockSize", conf: &PartitionRangeConfig{Location: jakarta, RangeType: RangeTypeMonthly, BlockSize: -3, Key: "created_at"}},
		{name: "fixed", conf: &PartitionRangeConfig{RangeType: FixedRange, BlockSize: 4, Key: "id"}},
		{name: "fixed ignores Location", conf: &PartitionRangeConfig{Location: time.Local, RangeType: FixedRange, BlockSize: 1, Key: "id"}},

		{name: "nil config", conf: nil, wantErr: "config is nil"},
		{name: "unknown range type", conf: &PartitionRangeConfig{Location: jakarta, RangeType: "monthly", Key: "created_at"}, wantErr: `range type "monthly"`},
		{name: "empty range type", conf: &PartitionRangeConfig{Location: jakarta, Key: "created_at"}, wantErr: `range type ""`},
		{name: "empty key", conf: &PartitionRangeConfig{Location: jakarta, RangeType: RangeTypeMonthly}, wantErr: "key is empty"},
		{name: "key with NUL", conf: &PartitionRangeConfig{Location: jakarta, RangeType: RangeTypeMonthly, Key: "a\x00b"}, wantErr: "NUL"},
		{name: "key too long", conf: &PartitionRangeConfig{Location: jakarta, RangeType: RangeTypeMonthly, Key: strings.Repeat("k", 64)}, wantErr: "64 bytes"},
		{name: "date type without location", conf: &PartitionRangeConfig{RangeType: RangeTypeDaily, Key: "created_at"}, wantErr: "needs a Location"},
		{name: "time.Local", conf: &PartitionRangeConfig{Location: time.Local, RangeType: RangeTypeDaily, Key: "created_at"}, wantErr: "Local"},
		{name: "unnamed fixed zone", conf: &PartitionRangeConfig{Location: time.FixedZone("WIB", 7*60*60), RangeType: RangeTypeDaily, Key: "created_at"}, wantErr: `"WIB"`},
		{name: "empty zone name", conf: &PartitionRangeConfig{Location: time.FixedZone("", 7*60*60), RangeType: RangeTypeDaily, Key: "created_at"}, wantErr: "has no name"},
		{name: "fixed zone posing as UTC", conf: &PartitionRangeConfig{Location: time.FixedZone("UTC", 7*60*60), RangeType: RangeTypeDaily, Key: "created_at"}, wantErr: `"UTC"`},
		{name: "fixed with BlockSize 0", conf: &PartitionRangeConfig{RangeType: FixedRange, Key: "id"}, wantErr: "BlockSize"},
		{name: "fixed with negative BlockSize", conf: &PartitionRangeConfig{RangeType: FixedRange, BlockSize: -4, Key: "id"}, wantErr: "BlockSize"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.conf.validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("validate: %v", err)
				}
				return
			}
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("validate = %v, want ErrInvalidConfig", err)
			}
			mustContain(t, err.Error(), tc.wantErr)
		})
	}
}

func TestDerivedNamesMustFitPostgreSQL(t *testing.T) {
	// 63 bytes is PostgreSQL's limit. "_default" adds 8 bytes, a daily child
	// "_20261117" adds 9.
	cases := []struct {
		name      string
		tableLen  int
		rangeType RangeType
		wantErr   bool
	}{
		{name: "monthly, 55 bytes", tableLen: 55, rangeType: RangeTypeMonthly},
		{name: "monthly, 56 bytes: _default is 64", tableLen: 56, rangeType: RangeTypeMonthly, wantErr: true},
		{name: "daily, 54 bytes", tableLen: 54, rangeType: RangeTypeDaily},
		{name: "daily, 55 bytes: a child is 64", tableLen: 55, rangeType: RangeTypeDaily, wantErr: true},
		{name: "fixed, 55 bytes", tableLen: 55, rangeType: FixedRange},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			table := TableName{Schema: "public", Name: strings.Repeat("t", tc.tableLen)}
			err := validateDerivedNames(table, tc.rangeType)
			if tc.wantErr {
				if !errors.Is(err, ErrInvalidTableName) {
					t.Fatalf("validateDerivedNames = %v, want ErrInvalidTableName", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("validateDerivedNames: %v", err)
			}
		})
	}
}
