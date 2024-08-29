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

	assert.Equal(t, map[uint64]struct{}{}, result)
}

func TestCompareDecompressedDataContentDifference(t *testing.T) {
	source := map[uint64]map[string][]byte{
		1: {"name": []byte("Leszek")},
	}
	target := map[uint64]map[string][]byte{
		1: {"name": []byte("Steve")},
	}

	result := compareDecompressedData(source, target)

	assert.Equal(t, map[uint64]struct{}{1: {}}, result)
}

func TestCompareDecompressedDataMissingTarget(t *testing.T) {
	source := map[uint64]map[string][]byte{
		1: {"name": []byte("Leszek")},
	}
	target := map[uint64]map[string][]byte{}

	result := compareDecompressedData(source, target)

	assert.Equal(t, map[uint64]struct{}{1: {}}, result)
}

func TestCompareDecompressedDataMissingSource(t *testing.T) {
	source := map[uint64]map[string][]byte{}
	target := map[uint64]map[string][]byte{
		3: {"name": []byte("Leszek")},
	}

	result := compareDecompressedData(source, target)

	assert.Equal(t, map[uint64]struct{}{3: {}}, result)
}
