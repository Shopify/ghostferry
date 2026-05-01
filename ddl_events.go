package ghostferry

import (
	"regexp"
	"strings"
	"sync"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"

	// test_driver registers the value-expression types the TiDB parser needs.
	// Despite the name, it is the standard driver every non-TiDB consumer of
	// the parser uses (sqlc, soar, bytebase, etc.) — the production driver
	// lives in pkg/types and pulls in the rest of TiDB.
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

// DDLStatement represents a parsed DDL statement extracted from a binlog
// QueryEvent. Only the fields needed to drive schema-change reactions are
// populated — we use the TiDB parser to extract them and discard the rest of
// the AST.
type DDLStatement struct {
	// SchemaName is the database the statement targets. Falls back to the
	// QueryEvent's default schema when the statement does not qualify the table.
	SchemaName string
	// TableName is the primary table the statement targets. For RENAME with
	// multiple pairs the parser populates RenamePairs and leaves this empty.
	TableName string
	// RawQuery is the original SQL text, useful for logging.
	RawQuery string
	// DDLType is one of "ALTER", "RENAME", "CREATE", "DROP", "TRUNCATE".
	DDLType string

	// RenamePairs is populated for RENAME TABLE statements. A single statement
	// can rename multiple tables (e.g. the gh-ost cutover swap):
	//   RENAME TABLE foo TO _foo_del, _foo_gho TO foo
	RenamePairs []RenamePair
}

// TableRef identifies a single table in a parsed DDL statement.
type TableRef struct {
	SchemaName string
	TableName  string
}

// RenamePair captures one rename clause (FROM → TO) within a RENAME TABLE
// statement.
type RenamePair struct {
	From TableRef
	To   TableRef
}

// SchemaImpact classifies how a schema change affects an in-progress copy.
type SchemaImpact int

const (
	// ImpactMetadataOnly means the cached schema can be refreshed in place and
	// no row recopy is required. Index changes, default value changes, comment
	// changes, and column reorders fall into this bucket.
	ImpactMetadataOnly SchemaImpact = iota
	// ImpactRequiresRecopy means rows already on the target are no longer
	// guaranteed to match source state and the table must be re-copied.
	ImpactRequiresRecopy
)

// leadingNoiseRe strips MySQL hint comments and SQL-style comments so the
// keyword sniff in IsDDLQuery can run without a full parse.
var leadingNoiseRe = regexp.MustCompile(`(?s)^(\s|/\*.*?\*/|--[^\n]*\n|#[^\n]*\n)+`)

// alterLegacyModifierRe strips MariaDB/legacy MySQL keywords (ONLINE, OFFLINE,
// IGNORE) that may appear between ALTER and TABLE. The TiDB parser does not
// accept them — real MySQL 5.7+ does not emit them in binlogs either, but
// older fixtures still carry the syntax so we normalize before parsing.
var alterLegacyModifierRe = regexp.MustCompile(`(?i)^ALTER(\s+(?:ONLINE|OFFLINE|IGNORE))+\s+TABLE\b`)

// Shadow-table suffixes — these are conventions, not SQL, so a regex is the
// right tool. gh-ost uses _<original>_gho/_del/_ghc, pt-osc uses _new/_old.
var (
	ghostShadowRe = regexp.MustCompile(`^_(.+)_(gho|del|ghc)$`)
	ptOscShadowRe = regexp.MustCompile(`^_(.+)_(new|old)$`)
)

// parserPool reuses TiDB parser instances. Per the TiDB docs, a parser is
// not goroutine-safe and not lightweight to construct, so a sync.Pool keeps
// allocations bounded under the binlog event rate.
var parserPool = sync.Pool{
	New: func() interface{} { return parser.New() },
}

// IsDDLQuery returns true when the QueryEvent payload is a DDL statement we
// might react to. Transactional control statements (BEGIN, COMMIT, SAVEPOINT,
// ROLLBACK) and other non-DDL queries return false so callers can drop them
// without invoking the full parser.
func IsDDLQuery(query []byte) bool {
	keyword := firstKeyword(query)
	if keyword == "" {
		return false
	}
	switch strings.ToUpper(keyword) {
	case "ALTER", "RENAME", "CREATE", "DROP", "TRUNCATE":
		return true
	}
	return false
}

// ParseDDLFromQueryEvent extracts a DDLStatement from a binlog QueryEvent.
// Returns nil for non-DDL statements (BEGIN, COMMIT, etc.) and for DDL the
// parser doesn't recognize or fails to parse.
func ParseDDLFromQueryEvent(ev *replication.QueryEvent) *DDLStatement {
	if ev == nil || len(ev.Query) == 0 {
		return nil
	}

	rawQuery := string(ev.Query)
	defaultSchema := string(ev.Schema)

	// Cheap keyword sniff first — skip the full parse for BEGIN/COMMIT/etc.
	// The binlog stream is mostly transactional control statements; running
	// the parser on every one would dominate CPU.
	if !IsDDLQuery(ev.Query) {
		return nil
	}

	p := parserPool.Get().(*parser.Parser)
	defer parserPool.Put(p)

	stmtNode, err := p.ParseOneStmt(normalizeForParser(rawQuery), "", "")
	if err != nil {
		// Statements the parser can't handle (vendor-specific syntax, etc.)
		// are dropped silently. Returning nil is consistent with the previous
		// behavior of returning nil for unrecognized DDL — callers are expected
		// to fall back to a conservative recopy if they care.
		return nil
	}

	switch s := stmtNode.(type) {
	case *ast.AlterTableStmt:
		schema, table := tableNameOrDefault(s.Table, defaultSchema)
		return &DDLStatement{
			RawQuery:   rawQuery,
			DDLType:    "ALTER",
			SchemaName: schema,
			TableName:  table,
		}
	case *ast.CreateTableStmt:
		schema, table := tableNameOrDefault(s.Table, defaultSchema)
		return &DDLStatement{
			RawQuery:   rawQuery,
			DDLType:    "CREATE",
			SchemaName: schema,
			TableName:  table,
		}
	case *ast.DropTableStmt:
		// DROP TABLE supports a comma-separated list. The detector only
		// inspects the primary table, so we surface the first; callers that
		// need every dropped table can extend AllAffectedTables.
		if len(s.Tables) == 0 {
			return nil
		}
		schema, table := tableNameOrDefault(s.Tables[0], defaultSchema)
		return &DDLStatement{
			RawQuery:   rawQuery,
			DDLType:    "DROP",
			SchemaName: schema,
			TableName:  table,
		}
	case *ast.TruncateTableStmt:
		schema, table := tableNameOrDefault(s.Table, defaultSchema)
		return &DDLStatement{
			RawQuery:   rawQuery,
			DDLType:    "TRUNCATE",
			SchemaName: schema,
			TableName:  table,
		}
	case *ast.RenameTableStmt:
		if len(s.TableToTables) == 0 {
			return nil
		}
		pairs := make([]RenamePair, 0, len(s.TableToTables))
		for _, t2t := range s.TableToTables {
			fromSchema, fromTable := tableNameOrDefault(t2t.OldTable, defaultSchema)
			toSchema, toTable := tableNameOrDefault(t2t.NewTable, defaultSchema)
			pairs = append(pairs, RenamePair{
				From: TableRef{SchemaName: fromSchema, TableName: fromTable},
				To:   TableRef{SchemaName: toSchema, TableName: toTable},
			})
		}
		stmt := &DDLStatement{
			RawQuery:    rawQuery,
			DDLType:     "RENAME",
			RenamePairs: pairs,
		}
		if len(pairs) == 1 {
			stmt.SchemaName = pairs[0].From.SchemaName
			stmt.TableName = pairs[0].From.TableName
		}
		return stmt
	}
	return nil
}

// AllAffectedTables returns every table referenced by the statement. For
// non-RENAME DDL this is a single-element slice; for RENAME it includes both
// sides of every pair so callers can react regardless of which side names a
// migrated table.
func (s *DDLStatement) AllAffectedTables() []TableRef {
	if s == nil {
		return nil
	}
	if len(s.RenamePairs) > 0 {
		out := make([]TableRef, 0, 2*len(s.RenamePairs))
		for _, p := range s.RenamePairs {
			out = append(out, p.From, p.To)
		}
		return out
	}
	if s.TableName == "" {
		return nil
	}
	return []TableRef{{SchemaName: s.SchemaName, TableName: s.TableName}}
}

// IsShadowTable reports whether the given table name matches a known
// online-schema-change shadow pattern (gh-ost or pt-osc).
func IsShadowTable(tableName string) bool {
	return ghostShadowRe.MatchString(tableName) || ptOscShadowRe.MatchString(tableName)
}

// OriginalTableFromShadow returns the original (unmangled) table name for a
// gh-ost or pt-osc shadow table, or "" if the name is not a recognised shadow
// pattern.
func OriginalTableFromShadow(tableName string) string {
	if m := ghostShadowRe.FindStringSubmatch(tableName); m != nil {
		return m[1]
	}
	if m := ptOscShadowRe.FindStringSubmatch(tableName); m != nil {
		return m[1]
	}
	return ""
}

// ClassifyImpact compares two schemas for the same logical table and decides
// whether a change between them needs a row recopy.
//
// Conservative by design: anything we can't positively identify as
// metadata-only is treated as ImpactRequiresRecopy. We can only inspect what
// go-mysql's schema package exposes (column names, raw types, index list, PK
// columns), so changes outside that set — for example NOT NULL ↔ NULL — fall
// through to recopy.
func ClassifyImpact(oldSchema, newSchema *TableSchema) SchemaImpact {
	if oldSchema == nil || newSchema == nil || oldSchema.Table == nil || newSchema.Table == nil {
		return ImpactRequiresRecopy
	}

	oldCols := oldSchema.Columns
	newCols := newSchema.Columns

	if len(oldCols) != len(newCols) {
		return ImpactRequiresRecopy
	}

	newByName := make(map[string]int, len(newCols))
	for i, c := range newCols {
		newByName[c.Name] = i
	}

	for _, oc := range oldCols {
		ni, ok := newByName[oc.Name]
		if !ok {
			return ImpactRequiresRecopy
		}
		nc := newCols[ni]
		if !columnTypesEquivalent(oc.RawType, nc.RawType) {
			return ImpactRequiresRecopy
		}
		if oc.IsUnsigned != nc.IsUnsigned {
			return ImpactRequiresRecopy
		}
	}

	if oldSchema.PaginationKeyIndex != newSchema.PaginationKeyIndex {
		return ImpactRequiresRecopy
	}
	if oldSchema.PaginationKeyColumn != nil && newSchema.PaginationKeyColumn != nil &&
		oldSchema.PaginationKeyColumn.Name != newSchema.PaginationKeyColumn.Name {
		return ImpactRequiresRecopy
	}

	return ImpactMetadataOnly
}

// normalizeForParser rewrites legacy ALTER syntax the TiDB parser rejects but
// older binlog fixtures may still contain.
func normalizeForParser(sql string) string {
	return alterLegacyModifierRe.ReplaceAllString(sql, "ALTER TABLE")
}

// tableNameOrDefault extracts (schema, table) from a parsed *ast.TableName,
// falling back to the QueryEvent's default schema when the SQL did not
// qualify the table.
func tableNameOrDefault(t *ast.TableName, defaultSchema string) (string, string) {
	if t == nil {
		return defaultSchema, ""
	}
	schema := t.Schema.O
	if schema == "" {
		schema = defaultSchema
	}
	return schema, t.Name.O
}

func stripLeadingNoise(q []byte) []byte {
	for {
		loc := leadingNoiseRe.FindIndex(q)
		if loc == nil || loc[0] != 0 {
			return q
		}
		q = q[loc[1]:]
	}
}

func firstKeyword(q []byte) string {
	stripped := stripLeadingNoise(q)
	end := 0
	for end < len(stripped) {
		c := stripped[end]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') {
			end++
			continue
		}
		break
	}
	if end == 0 {
		return ""
	}
	return string(stripped[:end])
}

// columnTypesEquivalent compares two RawType strings ignoring trivial
// whitespace and case differences. We don't try to decide whether e.g.
// VARCHAR(64) → VARCHAR(128) is safe — it isn't necessarily, since the binlog
// row payloads pre- and post-ALTER use different byte widths.
func columnTypesEquivalent(a, b string) bool {
	return strings.EqualFold(strings.TrimSpace(a), strings.TrimSpace(b))
}
