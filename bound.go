package san_pgpartition

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"gorm.io/gorm"
)

// PartitionBound is the bound of one child partition. Set exactly one field,
// the one matching the parent's strategy — the parent's strategy is read from
// the catalog and a bound of another strategy is refused with ErrInvalidBound.
type PartitionBound struct {
	Range *RangeBound
	List  *ListBound
	Hash  *HashBound
}

// RangeBound is FOR VALUES FROM (From...) TO (To...). From is inclusive, To is
// exclusive. Each holds one value per partition-key column, in key order, so a
// composite key (region, created_at) takes two values on each side. NULL (nil)
// is not allowed in a range bound.
type RangeBound struct {
	From []any
	To   []any
}

// ListBound is FOR VALUES IN (Values...). PostgreSQL's LIST partitioning has a
// single key column, so each element is one value of it. A nil element is SQL
// NULL: the partition then receives the rows whose key is NULL.
type ListBound struct {
	Values []any
}

// HashBound is FOR VALUES WITH (MODULUS m, REMAINDER r), with 0 <= r < m. The
// hash covers every partition-key column; PostgreSQL also requires each
// modulus under one parent to divide the next larger one.
type HashBound struct {
	Modulus   int
	Remainder int
}

const (
	strategyRange = "RANGE"
	strategyList  = "LIST"
	strategyHash  = "HASH"
)

// catalogStrategy maps pg_partitioned_table.partstrat to a strategy name.
func catalogStrategy(code string) string {
	switch code {
	case "r":
		return strategyRange
	case "l":
		return strategyList
	case "h":
		return strategyHash
	}
	return "unknown strategy " + strconv.Quote(code)
}

// strategy names the one strategy set on the bound.
func (b PartitionBound) strategy() (string, error) {
	var set []string
	if b.Range != nil {
		set = append(set, strategyRange)
	}
	if b.List != nil {
		set = append(set, strategyList)
	}
	if b.Hash != nil {
		set = append(set, strategyHash)
	}

	switch len(set) {
	case 0:
		return "", fmt.Errorf("%w: no RANGE, LIST or HASH bound is set", ErrInvalidBound)
	case 1:
		return set[0], nil
	}
	return "", fmt.Errorf("%w: set exactly one strategy, got %s", ErrInvalidBound, strings.Join(set, " and "))
}

// validateShape checks everything that does not need the parent, so a
// malformed bound is refused before any SQL runs.
func (b PartitionBound) validateShape() error {
	var err error

	strategy, err := b.strategy()
	if err != nil {
		return err
	}

	switch strategy {
	case strategyRange:
		if len(b.Range.From) == 0 || len(b.Range.From) != len(b.Range.To) {
			return fmt.Errorf("%w: RANGE needs the same number of FROM and TO values, at least one, got %d and %d", ErrInvalidBound, len(b.Range.From), len(b.Range.To))
		}
		for _, v := range b.rangeValues() {
			text, err := literalText(v)
			if err != nil {
				return err
			}
			if text == nil {
				return fmt.Errorf("%w: a RANGE bound cannot hold NULL", ErrInvalidBound)
			}
		}
	case strategyList:
		if len(b.List.Values) == 0 {
			return fmt.Errorf("%w: LIST needs at least one value", ErrInvalidBound)
		}
		for _, v := range b.List.Values {
			_, err = literalText(v)
			if err != nil {
				return err
			}
		}
	case strategyHash:
		if b.Hash.Modulus <= 0 {
			return fmt.Errorf("%w: HASH modulus must be positive, got %d", ErrInvalidBound, b.Hash.Modulus)
		}
		if b.Hash.Remainder < 0 || b.Hash.Remainder >= b.Hash.Modulus {
			return fmt.Errorf("%w: HASH remainder must be in [0, %d), got %d", ErrInvalidBound, b.Hash.Modulus, b.Hash.Remainder)
		}
	}

	return nil
}

// validateFor checks the bound against the parent's catalog entry.
func (b PartitionBound) validateFor(parentStrategy string, keyColumns int) error {
	var err error

	strategy, err := b.strategy()
	if err != nil {
		return err
	}
	if strategy != parentStrategy {
		return fmt.Errorf("%w: the parent is partitioned by %s, got a %s bound", ErrInvalidBound, parentStrategy, strategy)
	}
	if strategy == strategyRange && len(b.Range.From) != keyColumns {
		return fmt.Errorf("%w: RANGE has %d values per side, the partition key has %d columns", ErrInvalidBound, len(b.Range.From), keyColumns)
	}

	return nil
}

func (b PartitionBound) rangeValues() []any {
	values := make([]any, 0, len(b.Range.From)+len(b.Range.To))
	values = append(values, b.Range.From...)
	values = append(values, b.Range.To...)
	return values
}

// clause renders the FOR VALUES clause. Every value is quoted by PostgreSQL's
// own format(%L), so no caller value reaches the DDL unquoted. A quoted
// literal is accepted for any key type: PostgreSQL casts it to the column.
func (b PartitionBound) clause(tx *gorm.DB) (string, error) {
	switch {
	case b.Range != nil:
		format := "FOR VALUES FROM (" + literalMarks(len(b.Range.From)) + ") TO (" + literalMarks(len(b.Range.To)) + ")"
		return formatLiterals(tx, format, b.rangeValues())
	case b.List != nil:
		format := "FOR VALUES IN (" + literalMarks(len(b.List.Values)) + ")"
		return formatLiterals(tx, format, b.List.Values)
	case b.Hash != nil:
		return fmt.Sprintf("FOR VALUES WITH (MODULUS %d, REMAINDER %d)", b.Hash.Modulus, b.Hash.Remainder), nil
	}
	return "", fmt.Errorf("%w: no RANGE, LIST or HASH bound is set", ErrInvalidBound)
}

func literalMarks(n int) string {
	marks := make([]string, n)
	for i := range marks {
		marks[i] = "%L"
	}
	return strings.Join(marks, ", ")
}

// formatLiterals runs SELECT format(format, values...) with every value sent
// as a text parameter (NULL for nil).
func formatLiterals(tx *gorm.DB, format string, values []any) (string, error) {
	var err error

	params := make([]string, len(values))
	args := make([]any, 0, len(values)+1)
	args = append(args, format)
	for i, v := range values {
		params[i] = "?::text"

		text, err := literalText(v)
		if err != nil {
			return "", err
		}
		if text == nil {
			args = append(args, nil)
			continue
		}
		args = append(args, *text)
	}

	var out string
	err = tx.
		Raw("SELECT pg_catalog.format(?::text, "+strings.Join(params, ", ")+")", args...).
		Row().
		Scan(&out)
	if err != nil {
		return "", fmt.Errorf("san_pgpartition: quote bound values: %w", err)
	}
	return out, nil
}

// literalText turns one bound value into the text PostgreSQL parses for the
// key column; nil (or a nil pointer) is NULL.
//
//   - string: as is — use the column type's input form ("2026-01-01", "42")
//   - bool, every int and uint kind, float32/float64 (NaN and ±Inf refused)
//   - time.Time: RFC 3339 with nanoseconds and its own offset. A date or
//     timestamp (without time zone) key keeps the wall-clock date/time as
//     written; a timestamptz key gets the exact instant.
//   - anything implementing fmt.Stringer (a UUID, a decimal): its String()
func literalText(v any) (*string, error) {
	if v == nil {
		return nil, nil
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Pointer && rv.IsNil() {
		return nil, nil
	}

	var text string
	switch x := v.(type) {
	case string:
		text = x
	case bool:
		text = strconv.FormatBool(x)
	case int:
		text = strconv.FormatInt(int64(x), 10)
	case int8:
		text = strconv.FormatInt(int64(x), 10)
	case int16:
		text = strconv.FormatInt(int64(x), 10)
	case int32:
		text = strconv.FormatInt(int64(x), 10)
	case int64:
		text = strconv.FormatInt(x, 10)
	case uint:
		text = strconv.FormatUint(uint64(x), 10)
	case uint8:
		text = strconv.FormatUint(uint64(x), 10)
	case uint16:
		text = strconv.FormatUint(uint64(x), 10)
	case uint32:
		text = strconv.FormatUint(uint64(x), 10)
	case uint64:
		text = strconv.FormatUint(x, 10)
	case float32:
		if math.IsNaN(float64(x)) || math.IsInf(float64(x), 0) {
			return nil, fmt.Errorf("%w: %v is not a finite number", ErrInvalidBound, x)
		}
		text = strconv.FormatFloat(float64(x), 'g', -1, 32)
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return nil, fmt.Errorf("%w: %v is not a finite number", ErrInvalidBound, x)
		}
		text = strconv.FormatFloat(x, 'g', -1, 64)
	case time.Time:
		text = x.Format(time.RFC3339Nano)
	case fmt.Stringer:
		text = x.String()
	default:
		return nil, fmt.Errorf("%w: a bound value of type %T is not supported", ErrInvalidBound, v)
	}

	if strings.ContainsRune(text, 0) {
		return nil, fmt.Errorf("%w: a bound value contains a NUL byte", ErrInvalidBound)
	}
	return &text, nil
}
