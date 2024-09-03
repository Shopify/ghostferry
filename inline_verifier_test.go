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

	assert.Equal(t, map[uint64]mismatch{}, result)
}

func TestCompareDecompressedDataContentDifference(t *testing.T) {
	source := map[uint64]map[string][]byte{
		1: {"name": []byte("Leszek")},
	}
	target := map[uint64]map[string][]byte{
		1: {"name": []byte("Steve")},
	}

	result := compareDecompressedData(source, target)

	assert.Equal(t, map[uint64]mismatch{1: {mismatchType: MismatchContentDifference, column: "name"}}, result)
}

func TestCompareDecompressedDataMissingTarget(t *testing.T) {
	source := map[uint64]map[string][]byte{
		1: {"name": []byte("Leszek")},
	}
	target := map[uint64]map[string][]byte{}

	result := compareDecompressedData(source, target)

	assert.Equal(t, map[uint64]mismatch{1: {mismatchType: MismatchRowMissingOnTarget}}, result)
}

func TestCompareDecompressedDataMissingSource(t *testing.T) {
	source := map[uint64]map[string][]byte{}
	target := map[uint64]map[string][]byte{
		3: {"name": []byte("Leszek")},
	}

	result := compareDecompressedData(source, target)

	assert.Equal(t, map[uint64]mismatch{3: {mismatchType: MismatchRowMissingOnSource}}, result)
}

func TestFormatMismatch(t *testing.T) {
	mismatches := map[string]map[string][]InlineVerifierMismatches{
		"default": {
			"users": {
				InlineVerifierMismatches{
					Pk: 1,
					SourceChecksum: "",
					TargetChecksum: "bar",
					Mismatch: mismatch{
						mismatchType: MismatchRowMissingOnSource,
					},
				},
			},
		},
	}
	message, tables := formatMismatches(mismatches)

	assert.Equal(t, string("cutover verification failed for: default.users [paginationKeys: 1 (source: , target: bar, type: row missing on source) ] "), message)
	assert.Equal(t, []string{string("default.users")}, tables)
}

func TestFormatMismatches(t *testing.T) {
	mismatches := map[string]map[string][]InlineVerifierMismatches{
		"default": {
			"users": {
				InlineVerifierMismatches{
					Pk: 1,
					SourceChecksum: "",
					TargetChecksum: "bar",
					Mismatch: mismatch{
						mismatchType: MismatchRowMissingOnSource,
					},
				},
				InlineVerifierMismatches{
					Pk: 5,
					SourceChecksum: "baz",
					TargetChecksum: "",
					Mismatch: mismatch{
						mismatchType: MismatchRowMissingOnTarget,
					},
				},
			},
			"posts": {
				InlineVerifierMismatches{
					Pk: 9,
					SourceChecksum: "boo",
					TargetChecksum: "aaa",
					Mismatch: mismatch{
						mismatchType: MismatchContentDifference,
						column: string("title"),
					},
				},
			},
			"attachments": {
				InlineVerifierMismatches{
					Pk: 7,
					SourceChecksum: "boo",
					TargetChecksum: "aaa",
					Mismatch: mismatch{
						mismatchType: MismatchContentDifference,
						column: string("name"),
					},
				},
			},
		},
	}
	message, tables := formatMismatches(mismatches)

	assert.Equal(t, string("cutover verification failed for: default.attachments [paginationKeys: 7 (source: boo, target: aaa, type: content difference, column: name) ] default.posts [paginationKeys: 9 (source: boo, target: aaa, type: content difference, column: title) ] default.users [paginationKeys: 1 (source: , target: bar, type: row missing on source) 5 (source: baz, target: , type: row missing on target) ] "), message)
	assert.Equal(t, []string{string("default.attachments"), string("default.posts"), string("default.users")}, tables)
}
