package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompareServerVersions(t *testing.T) {
	tests := []struct {
		A      string
		B      string
		Expect int
	}{
		{A: "1.2.3", B: "1.2.3", Expect: 0},
		{A: "5.6-999", B: "8.0", Expect: -1},
		{A: "5.6.3-999", B: "5.6", Expect: 0},
		{A: "5.6.3-999", B: "5.5-tag", Expect: 1},
		{A: "8.0.32-0ubuntu0.20.04.2", B: "8.0.28", Expect: 1},
		{A: "a.b.c", B: "8.0", Expect: 2},
	}

	for _, test := range tests {
		got, err := CompareServerVersions(test.A, test.B)
		if test.Expect == 2 {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, test.Expect, got)
		}

		// test logic is commutative
		got, err = CompareServerVersions(test.B, test.A)
		if test.Expect == 2 {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, -test.Expect, got)
		}
	}
}
