package test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Shopify/ghostferry/v2"
)

func newMockBinlogVerifySerializedStore() ghostferry.BinlogVerifySerializedStore {
	s := make(ghostferry.BinlogVerifySerializedStore)
	s["db"] = map[string]map[uint64]int{
		"table1": map[uint64]int{
			3:  1,
			10: 2,
			30: 3,
		},
	}
	s["db2"] = map[string]map[uint64]int{
		"table2": map[uint64]int{
			4:  1,
			20: 2,
			40: 1,
		},
	}
	return s
}

func TestBinlogVerifySerializedStoreRowCount(t *testing.T) {
	r := require.New(t)

	s := newMockBinlogVerifySerializedStore()

	r.Equal(uint64(10), s.RowCount())
}

func TestBinlogVerifySerializedStoreCopy(t *testing.T) {
	r := require.New(t)

	s := newMockBinlogVerifySerializedStore()
	s2 := s.Copy()
	s2["db"]["table1"][3] += 1

	r.Equal(uint64(10), s.RowCount())
	r.Equal(uint64(11), s2.RowCount())
}
