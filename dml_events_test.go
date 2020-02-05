package ghostferry

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRowData_GetUint64(t *testing.T) {
	rowData := make(RowData, 1)
	testCases := []struct {
		desc   string
		value  interface{}
		expect interface{}
	}{
		{desc: "It should return uint64 given int", value: int(10), expect: uint64(10)},
		{desc: "It should return uint64 given []bytes", value: []byte("18446744073709551615"), expect: uint64(18446744073709551615)},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			rowData[0] = tc.value
			result, err := rowData.GetUint64(0)

			require.Nil(t, err)
			assert.Equal(t, result, tc.expect)
		})
	}

}
