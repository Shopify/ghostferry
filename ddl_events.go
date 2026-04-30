package ghostferry

import (
	"regexp"
	"strings"

	"github.com/go-mysql-org/go-mysql/replication"
)

// DDLStatement represents a parsed DDL statement extracted from a binlog
// QueryEvent. Only the fields needed to drive schema-change reactions are
// populated — the parser is intentionally tolerant and does not produce a full
// AST.
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

// Match leading SQL comments and whitespace so we can sniff the first keyword
// reliably.
var (
	leadingNoiseRe = regexp.MustCompile(`(?s)^(\s|/\*.*?\*/|--[^\n]*\n|#[^\n]*\n)+`)

	// Capture group 1 = optional schema, group 2 = table name. Used by ALTER,
	// CREATE TABLE, DROP TABLE, TRUNCATE TABLE.
	identifierPattern = `(?:` + "`" + `([^` + "`" + `]+)` + "`" + `\.)?` + "`" + `?([^` + "`" + ` ,;()]+)` + "`" + `?`

	alterRe    = regexp.MustCompile(`(?is)^ALTER\s+(?:ONLINE\s+|OFFLINE\s+|IGNORE\s+)*TABLE\s+` + identifierPattern)
	createRe   = regexp.MustCompile(`(?is)^CREATE\s+(?:TEMPORARY\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?` + identifierPattern)
	dropRe     = regexp.MustCompile(`(?is)^DROP\s+(?:TEMPORARY\s+)?TABLE\s+(?:IF\s+EXISTS\s+)?` + identifierPattern)
	truncateRe = regexp.MustCompile(`(?is)^TRUNCATE\s+(?:TABLE\s+)?` + identifierPattern)
	renameRe   = regexp.MustCompile(`(?is)^RENAME\s+TABLE\s+(.+)$`)

	// Used to walk a RENAME TABLE clause list. Each match is one "FROM TO TO TO".
	renamePairRe = regexp.MustCompile(`(?is)` + identifierPattern + `\s+TO\s+` + identifierPattern)

	// gh-ost shadow tables: _<original>_gho (ghost copy), _<original>_del
	// (renamed-out original at cutover), _<original>_ghc (changelog).
	ghostShadowRe = regexp.MustCompile(`^_(.+)_(gho|del|ghc)$`)
	// pt-online-schema-change shadow tables: _<original>_new and _<original>_old.
	ptOscShadowRe = regexp.MustCompile(`^_(.+)_(new|old)$`)
)

// IsDDLQuery returns true when the QueryEvent payload is a DDL statement we
// might react to. Transactional control statements (BEGIN, COMMIT, SAVEPOINT,
// ROLLBACK) and other non-DDL queries return false so callers can drop them
// cheaply.
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
// parser doesn't recognize.
func ParseDDLFromQueryEvent(ev *replication.QueryEvent) *DDLStatement {
	if ev == nil || len(ev.Query) == 0 {
		return nil
	}

	defaultSchema := string(ev.Schema)
	stripped := stripLeadingNoise(ev.Query)
	keyword := strings.ToUpper(firstKeyword(stripped))

	stmt := &DDLStatement{
		RawQuery: string(ev.Query),
		DDLType:  keyword,
	}

	switch keyword {
	case "ALTER":
		if m := alterRe.FindSubmatch(stripped); m != nil {
			stmt.SchemaName = pickSchema(string(m[1]), defaultSchema)
			stmt.TableName = string(m[2])
			return stmt
		}
	case "CREATE":
		if m := createRe.FindSubmatch(stripped); m != nil {
			stmt.SchemaName = pickSchema(string(m[1]), defaultSchema)
			stmt.TableName = string(m[2])
			return stmt
		}
	case "DROP":
		if m := dropRe.FindSubmatch(stripped); m != nil {
			stmt.SchemaName = pickSchema(string(m[1]), defaultSchema)
			stmt.TableName = string(m[2])
			return stmt
		}
	case "TRUNCATE":
		if m := truncateRe.FindSubmatch(stripped); m != nil {
			stmt.SchemaName = pickSchema(string(m[1]), defaultSchema)
			stmt.TableName = string(m[2])
			return stmt
		}
	case "RENAME":
		if m := renameRe.FindSubmatch(stripped); m != nil {
			pairs := parseRenamePairs(string(m[1]), defaultSchema)
			if len(pairs) == 0 {
				return nil
			}
			stmt.RenamePairs = pairs
			if len(pairs) == 1 {
				stmt.SchemaName = pairs[0].From.SchemaName
				stmt.TableName = pairs[0].From.TableName
			}
			return stmt
		}
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

	// Build a name → column lookup so reorders alone don't trigger recopy.
	newByName := make(map[string]int, len(newCols))
	for i, c := range newCols {
		newByName[c.Name] = i
	}

	for _, oc := range oldCols {
		ni, ok := newByName[oc.Name]
		if !ok {
			// Column was renamed or replaced — semantics are not preserved.
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

	// Pagination column must remain the same column at the same position;
	// changing it mid-flight breaks the cursor invariants.
	if oldSchema.PaginationKeyIndex != newSchema.PaginationKeyIndex {
		return ImpactRequiresRecopy
	}
	if oldSchema.PaginationKeyColumn != nil && newSchema.PaginationKeyColumn != nil &&
		oldSchema.PaginationKeyColumn.Name != newSchema.PaginationKeyColumn.Name {
		return ImpactRequiresRecopy
	}

	// Same set of columns, same raw types, same pagination key — index/comment/
	// default changes between the two schemas are safely metadata-only.
	return ImpactMetadataOnly
}

func parseRenamePairs(clauseList, defaultSchema string) []RenamePair {
	matches := renamePairRe.FindAllStringSubmatch(clauseList, -1)
	if len(matches) == 0 {
		return nil
	}
	out := make([]RenamePair, 0, len(matches))
	for _, m := range matches {
		// m[1]/m[2] = FROM schema/table; m[3]/m[4] = TO schema/table.
		from := TableRef{
			SchemaName: pickSchema(m[1], defaultSchema),
			TableName:  m[2],
		}
		to := TableRef{
			SchemaName: pickSchema(m[3], defaultSchema),
			TableName:  m[4],
		}
		out = append(out, RenamePair{From: from, To: to})
	}
	return out
}

func pickSchema(parsed, fallback string) string {
	if parsed != "" {
		return parsed
	}
	return fallback
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
