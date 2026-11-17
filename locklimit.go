package san_pgpartition

import (
	"errors"
	"fmt"
)

// isLockLimit reports whether err is PostgreSQL's SQLSTATE 53200, "out of
// shared memory" — the lock table is full.
func isLockLimit(err error) bool {
	var state interface{ SQLState() string }
	return errors.As(err, &state) && state.SQLState() == "53200"
}

// lockLimitHint explains SQLSTATE 53200 when a call creating children runs
// into it: every child holds its locks (about a dozen with a few indexes and
// a foreign key) until the commit, and the server's lock table holds about
// max_locks_per_transaction × max_connections. Any other error is returned
// as it is; the 53200 stays reachable through errors.As.
func lockLimitHint(err error, children int) error {
	if !isLockLimit(err) {
		return err
	}
	return fmt.Errorf("%w — PostgreSQL's lock table is full: creating %d children in one transaction holds about a dozen locks per child until the commit; raise max_locks_per_transaction (a server setting, needs a restart) or create fewer children per call (RangeTypeMonthly rather than RangeTypeDaily, or a smaller provisioning number)", err, children)
}
