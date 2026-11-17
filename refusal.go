package san_pgpartition

import (
	"errors"
	"fmt"
	"strings"

	"gorm.io/gorm"
)

// refusal is one thing that stops a conversion before anything is created.
// Each query lists the offenders, one text per row, from the original table's
// OID (@oid) and the partition key's column name (@key).
type refusal struct {
	sentinel error
	what     string
	queries  []string
}

// refusals run in this order; every one runs, so a single error names every
// offender of every kind.
var refusals = []refusal{
	{
		sentinel: ErrReferencedByForeignKey,
		what:     "these foreign keys reference it; after the swap they would still point at the old table — drop or reshape them first",
		queries:  []string{refuseIncomingForeignKeys},
	},
	{
		sentinel: ErrUniqueKeyWithoutPartitionKey,
		what:     "these unique keys lack the partition key, which PostgreSQL requires on a partitioned table — drop or reshape them first",
		queries:  []string{refuseUniqueWithoutKey},
	},
	{
		sentinel: ErrPrimaryKeyNotExtendable,
		what:     "its primary key lacks the partition key and is not one sequence-fed column, so it cannot be extended",
		queries:  []string{refusePrimaryKey},
	},
	{
		sentinel: ErrUnsupportedTableFeature,
		what:     "the conversion would leave these on the old table or lose them",
		queries: []string{
			refuseTriggers,
			refuseDependentViews,
			refuseRules,
			refuseRowSecurity,
			refusePolicies,
			refusePublications,
			refuseIdentity,
			refuseInheritsFrom,
			refuseInheritedBy,
			refuseExclusion,
			refuseTableGrants,
			refuseColumnGrants,
			refuseStatistics,
			refusePersistence,
			refuseOwner,
		},
	},
}

// checkRefusals runs every refusal and returns them joined: errors.Is matches
// each sentinel, and the message names every offender.
func (c *conversion) checkRefusals(tx *gorm.DB) error {
	var err error

	args := map[string]any{"oid": c.oid, "key": c.pconf.Key}

	var refused []error
	for _, r := range refusals {
		var offenders []string
		for _, query := range r.queries {
			var found []string
			err = tx.
				Raw(query, args).
				Scan(&found).
				Error
			if err != nil {
				return fmt.Errorf("san_pgpartition: check %s before converting it: %w", c.table, err)
			}
			offenders = append(offenders, found...)
		}
		if len(offenders) > 0 {
			refused = append(refused, fmt.Errorf("%w: %s: %s: %s", r.sentinel, c.table, r.what, strings.Join(offenders, ", ")))
		}
	}

	return errors.Join(refused...)
}

const refuseIncomingForeignKeys = `
SELECT n.nspname || '.' || r.relname || '.' || con.conname
FROM pg_catalog.pg_constraint AS con
JOIN pg_catalog.pg_class AS r ON r.oid = con.conrelid
JOIN pg_catalog.pg_namespace AS n ON n.oid = r.relnamespace
WHERE con.contype = 'f'
  AND con.confrelid = CAST(@oid AS bigint)::oid
  AND con.conparentid = 0
ORDER BY 1`

const refuseUniqueWithoutKey = `
SELECT ic.relname::text
FROM pg_catalog.pg_index AS i
JOIN pg_catalog.pg_class AS ic ON ic.oid = i.indexrelid
WHERE i.indrelid = CAST(@oid AS bigint)::oid
  AND i.indisunique
  AND NOT i.indisprimary
  AND NOT EXISTS (
      SELECT 1
      FROM pg_catalog.unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord)
      JOIN pg_catalog.pg_attribute AS a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
      WHERE k.ord <= i.indnkeyatts
        AND a.attname = @key
  )
ORDER BY 1`

// A primary key passes when it holds the key, or when it is one column whose
// default draws from a sequence (nextval) — the only kind extended to
// (<column>, <key>).
const refusePrimaryKey = `
SELECT con.conname || ' (' || pg_catalog.string_agg(pg_catalog.quote_ident(a.attname), ', ' ORDER BY k.ord) || ')'
FROM pg_catalog.pg_constraint AS con
JOIN pg_catalog.pg_index AS i ON i.indexrelid = con.conindid
CROSS JOIN LATERAL pg_catalog.unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord)
JOIN pg_catalog.pg_attribute AS a ON a.attrelid = con.conrelid AND a.attnum = k.attnum
WHERE con.conrelid = CAST(@oid AS bigint)::oid
  AND con.contype = 'p'
  AND k.ord <= i.indnkeyatts
GROUP BY con.conname
HAVING NOT pg_catalog.bool_or(a.attname = @key)
   AND NOT (
       pg_catalog.count(*) = 1
       AND pg_catalog.bool_and(EXISTS (
           SELECT 1
           FROM pg_catalog.pg_attrdef AS d
           WHERE d.adrelid = a.attrelid
             AND d.adnum = a.attnum
             AND pg_catalog.pg_get_expr(d.adbin, d.adrelid) LIKE 'nextval(%'
       ))
   )`

const refuseTriggers = `
SELECT 'trigger ' || tgname
FROM pg_catalog.pg_trigger
WHERE tgrelid = CAST(@oid AS bigint)::oid
  AND NOT tgisinternal
ORDER BY 1`

const refuseDependentViews = `
SELECT DISTINCT CASE v.relkind WHEN 'm' THEN 'materialized view ' ELSE 'view ' END || vn.nspname || '.' || v.relname
FROM pg_catalog.pg_depend AS d
JOIN pg_catalog.pg_rewrite AS rw ON rw.oid = d.objid
JOIN pg_catalog.pg_class AS v ON v.oid = rw.ev_class
JOIN pg_catalog.pg_namespace AS vn ON vn.oid = v.relnamespace
WHERE d.classid = 'pg_catalog.pg_rewrite'::regclass
  AND d.refclassid = 'pg_catalog.pg_class'::regclass
  AND d.refobjid = CAST(@oid AS bigint)::oid
  AND rw.ev_class <> CAST(@oid AS bigint)::oid
ORDER BY 1`

const refuseRules = `
SELECT 'rule ' || rulename
FROM pg_catalog.pg_rewrite
WHERE ev_class = CAST(@oid AS bigint)::oid
  AND rulename <> '_RETURN'
ORDER BY 1`

const refuseRowSecurity = `
SELECT 'row level security'
FROM pg_catalog.pg_class
WHERE oid = CAST(@oid AS bigint)::oid
  AND (relrowsecurity OR relforcerowsecurity)`

const refusePolicies = `
SELECT 'policy ' || polname
FROM pg_catalog.pg_policy
WHERE polrelid = CAST(@oid AS bigint)::oid
ORDER BY 1`

const refusePublications = `
SELECT 'publication ' || p.pubname
FROM pg_catalog.pg_publication_rel AS pr
JOIN pg_catalog.pg_publication AS p ON p.oid = pr.prpubid
WHERE pr.prrelid = CAST(@oid AS bigint)::oid
ORDER BY 1`

const refuseIdentity = `
SELECT 'identity column ' || attname
FROM pg_catalog.pg_attribute
WHERE attrelid = CAST(@oid AS bigint)::oid
  AND attnum > 0
  AND NOT attisdropped
  AND attidentity <> ''
ORDER BY 1`

const refuseInheritsFrom = `
SELECT 'inherits from ' || pn.nspname || '.' || p.relname
FROM pg_catalog.pg_inherits AS i
JOIN pg_catalog.pg_class AS p ON p.oid = i.inhparent
JOIN pg_catalog.pg_namespace AS pn ON pn.oid = p.relnamespace
WHERE i.inhrelid = CAST(@oid AS bigint)::oid
ORDER BY 1`

const refuseInheritedBy = `
SELECT 'inherited by ' || cn.nspname || '.' || ch.relname
FROM pg_catalog.pg_inherits AS i
JOIN pg_catalog.pg_class AS ch ON ch.oid = i.inhrelid
JOIN pg_catalog.pg_namespace AS cn ON cn.oid = ch.relnamespace
WHERE i.inhparent = CAST(@oid AS bigint)::oid
ORDER BY 1`

const refuseExclusion = `
SELECT 'exclusion constraint ' || conname
FROM pg_catalog.pg_constraint
WHERE conrelid = CAST(@oid AS bigint)::oid
  AND contype = 'x'
ORDER BY 1`

// An ACL equal to the owner-only default is no grant.
const refuseTableGrants = `
SELECT 'privileges granted on the table'
FROM pg_catalog.pg_class
WHERE oid = CAST(@oid AS bigint)::oid
  AND relacl IS NOT NULL
  AND relacl <> pg_catalog.acldefault('r', relowner)`

const refuseColumnGrants = `
SELECT 'privileges granted on column ' || attname
FROM pg_catalog.pg_attribute
WHERE attrelid = CAST(@oid AS bigint)::oid
  AND attnum > 0
  AND NOT attisdropped
  AND pg_catalog.cardinality(attacl) > 0
ORDER BY 1`

const refuseStatistics = `
SELECT 'statistics ' || sn.nspname || '.' || s.stxname
FROM pg_catalog.pg_statistic_ext AS s
JOIN pg_catalog.pg_namespace AS sn ON sn.oid = s.stxnamespace
WHERE s.stxrelid = CAST(@oid AS bigint)::oid
ORDER BY 1`

const refusePersistence = `
SELECT CASE relpersistence WHEN 'u' THEN 'unlogged table' ELSE 'temporary table' END
FROM pg_catalog.pg_class
WHERE oid = CAST(@oid AS bigint)::oid
  AND relpersistence <> 'p'`

// The new table and its children belong to whoever runs the conversion; a
// table owned by another role would change hands.
const refuseOwner = `
SELECT 'owned by ' || pg_catalog.pg_get_userbyid(relowner) || ', not by the current user ' || current_user
FROM pg_catalog.pg_class
WHERE oid = CAST(@oid AS bigint)::oid
  AND pg_catalog.pg_get_userbyid(relowner) <> current_user`
