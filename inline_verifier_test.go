package ghostferry

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompareDecompressedDataNoDifference(t *testing.T) {
	source := map[uint64]map[string][]byte{
		31: {"name": []byte("Leszek")},
	}
	target := map[uint64]map[string][]byte{
		31: {"name": []byte("Leszek")},
	}

	result := compareDecompressedData(source, target)

	assert.Equal(t, map[uint64]InlineVerifierMismatches{}, result)
}

func TestCompareDecompressedDataContentDifference(t *testing.T) {
	source := map[uint64]map[string][]byte{
		1: {"name": []byte("Leszek")},
	}
	target := map[uint64]map[string][]byte{
		1: {"name": []byte("Steve")},
	}

	result := compareDecompressedData(source, target)

	assert.Equal(t, map[uint64]InlineVerifierMismatches{
		1: {
			Pk:             1,
			MismatchType:   MismatchColumnValueDifference,
			MismatchColumn: "name",
			SourceChecksum: "e356a972989f87a1531252cfa2152797",
			TargetChecksum: "81b8a1b77068d06e1c8190825253066f",
		},
	}, result)
}

func TestCompareDecompressedDataMissingTarget(t *testing.T) {
	source := map[uint64]map[string][]byte{
		1: {"name": []byte("Leszek")},
	}
	target := map[uint64]map[string][]byte{}

	result := compareDecompressedData(source, target)

	assert.Equal(t, map[uint64]InlineVerifierMismatches{1: {Pk: 1, MismatchType: MismatchRowMissingOnTarget}}, result)
}

func TestCompareDecompressedDataMissingSource(t *testing.T) {
	source := map[uint64]map[string][]byte{}
	target := map[uint64]map[string][]byte{
		3: {"name": []byte("Leszek")},
	}

	result := compareDecompressedData(source, target)

	assert.Equal(t, map[uint64]InlineVerifierMismatches{3: {Pk: 3, MismatchType: MismatchRowMissingOnSource}}, result)
}

func TestFormatMismatch(t *testing.T) {
	mismatches := map[string]map[string][]InlineVerifierMismatches{
		"default": {
			"users": {
				InlineVerifierMismatches{
					Pk:           1,
					MismatchType: MismatchRowMissingOnSource,
				},
			},
		},
	}
	message, tables := formatMismatches(mismatches)

	assert.Equal(t, string("cutover verification failed for: default.users [PKs: 1 (type: row missing on source) ] "), message)
	assert.Equal(t, []string{string("default.users")}, tables)
}

func TestFormatMismatches(t *testing.T) {
	mismatches := map[string]map[string][]InlineVerifierMismatches{
		"default": {
			"users": {
				InlineVerifierMismatches{
					Pk:           1,
					MismatchType: MismatchRowMissingOnSource,
				},
				InlineVerifierMismatches{
					Pk:           5,
					MismatchType: MismatchRowMissingOnTarget,
				},
			},
			"posts": {
				InlineVerifierMismatches{
					Pk:             9,
					MismatchType:   MismatchColumnValueDifference,
					MismatchColumn: string("title"),
					SourceChecksum: "boo",
					TargetChecksum: "aaa",
				},
			},
			"attachments": {
				InlineVerifierMismatches{
					Pk:             7,
					MismatchType:   MismatchColumnValueDifference,
					MismatchColumn: string("name"),
					SourceChecksum: "boo",
					TargetChecksum: "aaa",
				},
			},
		},
	}
	message, tables := formatMismatches(mismatches)

	assert.Equal(t, string("cutover verification failed for: default.attachments [PKs: 7 (type: column value difference, source: boo, target: aaa, column: name) ] default.posts [PKs: 9 (type: column value difference, source: boo, target: aaa, column: title) ] default.users [PKs: 1 (type: row missing on source) 5 (type: row missing on target) ] "), message)
	assert.Equal(t, []string{string("default.attachments"), string("default.posts"), string("default.users")}, tables)
}
