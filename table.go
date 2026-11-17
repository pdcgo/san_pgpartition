package san_pgpartition

import (
	"fmt"
	"strings"
)

// maxIdentifierBytes is PostgreSQL's identifier limit (NAMEDATALEN - 1).
// A longer name is silently truncated by the server, after which a lookup by
// the full name would never find the table again — so it is refused here.
const maxIdentifierBytes = 63

// TableName names a table exactly as it is stored in the PostgreSQL catalog.
//
// Both parts are required, so a call never depends on search_path. Names are
// matched exactly and quoted in DDL: they are not folded to lower case, so
// "Orders" and "orders" are two different tables.
type TableName struct {
	Schema string
	Name   string
}

// String returns schema.name, for messages.
func (t TableName) String() string {
	return t.Schema + "." + t.Name
}

func (t TableName) quoted() string {
	return quoteIdentifier(t.Schema) + "." + quoteIdentifier(t.Name)
}

func (t TableName) validate(label string) error {
	var err error

	err = validateIdentifier(label+" schema", t.Schema)
	if err != nil {
		return err
	}

	err = validateIdentifier(label+" table name", t.Name)
	if err != nil {
		return err
	}

	return nil
}

func validateIdentifier(label, value string) error {
	if value == "" {
		return fmt.Errorf("%w: %s is empty", ErrInvalidTableName, label)
	}
	if len(value) > maxIdentifierBytes {
		return fmt.Errorf("%w: %s %q is %d bytes, PostgreSQL allows %d", ErrInvalidTableName, label, value, len(value), maxIdentifierBytes)
	}
	if strings.ContainsRune(value, 0) {
		return fmt.Errorf("%w: %s contains a NUL byte", ErrInvalidTableName, label)
	}
	return nil
}

// quoteIdentifier double-quotes an identifier, doubling any quote inside it.
func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
