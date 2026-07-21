package test

import (
	"encoding/json"
	"testing"

	"github.com/Shopify/ghostferry"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBinlogCoordinate_FilePositionBasics(t *testing.T) {
	pos := mysql.Position{Name: "mysql-bin.000123", Pos: 456}
	coord := ghostferry.NewFilePositionCoordinate(pos)

	assert.True(t, coord.IsFilePosition())
	assert.False(t, coord.IsZero())
	assert.Equal(t, pos, coord.Position())
	assert.Equal(t, "mysql-bin.000123:456", coord.String())
}

func TestBinlogCoordinate_ZeroValueIsFilePosition(t *testing.T) {
	var coord ghostferry.BinlogCoordinate

	assert.True(t, coord.IsFilePosition())
	assert.True(t, coord.IsZero())
	assert.Equal(t, mysql.Position{}, coord.Position())
}

func TestBinlogCoordinate_Compare(t *testing.T) {
	a := ghostferry.NewFilePositionCoordinate(mysql.Position{Name: "mysql-bin.000001", Pos: 10})
	b := ghostferry.NewFilePositionCoordinate(mysql.Position{Name: "mysql-bin.000001", Pos: 20})
	c := ghostferry.NewFilePositionCoordinate(mysql.Position{Name: "mysql-bin.000002", Pos: 5})

	assert.Equal(t, -1, a.Compare(b))
	assert.Equal(t, 1, b.Compare(a))
	assert.Equal(t, 0, a.Compare(a))
	assert.Equal(t, -1, b.Compare(c))
}

func TestBinlogCoordinate_JSONRoundTrip(t *testing.T) {
	coord := ghostferry.NewFilePositionCoordinate(mysql.Position{Name: "mysql-bin.000777", Pos: 999})

	data, err := json.Marshal(coord)
	require.NoError(t, err)

	var decoded ghostferry.BinlogCoordinate
	require.NoError(t, json.Unmarshal(data, &decoded))

	assert.True(t, decoded.IsFilePosition())
	assert.Equal(t, coord.Position(), decoded.Position())
}

// TestBinlogCoordinate_UnmarshalBareMysqlPosition guards the backwards
// compatible decoding path: a bare {"Name":...,"Pos":...} object (the shape an
// older mysql.Position produced) must decode into a file/position coordinate.
func TestBinlogCoordinate_UnmarshalBareMysqlPosition(t *testing.T) {
	raw := []byte(`{"Name":"mysql-bin.000042","Pos":314}`)

	var decoded ghostferry.BinlogCoordinate
	require.NoError(t, json.Unmarshal(raw, &decoded))

	assert.True(t, decoded.IsFilePosition())
	assert.Equal(t, "mysql-bin.000042", decoded.Position().Name)
	assert.Equal(t, uint32(314), decoded.Position().Pos)
}

func TestBinlogCoordinate_UnmarshalTypedForm(t *testing.T) {
	raw := []byte(`{"Type":"file_position","FilePosition":{"Name":"mysql-bin.000050","Pos":700}}`)

	var decoded ghostferry.BinlogCoordinate
	require.NoError(t, json.Unmarshal(raw, &decoded))

	assert.True(t, decoded.IsFilePosition())
	assert.Equal(t, "mysql-bin.000050", decoded.Position().Name)
	assert.Equal(t, uint32(700), decoded.Position().Pos)
}

const (
	testGTIDSetA = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-57"
	testGTIDSetB = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100"
)

func TestBinlogCoordinate_GTIDBasics(t *testing.T) {
	coord := ghostferry.NewGTIDCoordinate(testGTIDSetA)

	assert.True(t, coord.IsGTID())
	assert.False(t, coord.IsFilePosition())
	assert.False(t, coord.IsZero())
	assert.Equal(t, "gtid:"+testGTIDSetA, coord.String())

	set, err := coord.ParsedGTIDSet()
	require.NoError(t, err)
	assert.Equal(t, testGTIDSetA, set.String())
}

func TestBinlogCoordinate_GTIDEmptyIsZero(t *testing.T) {
	coord := ghostferry.NewGTIDCoordinate("")

	assert.True(t, coord.IsGTID())
	assert.True(t, coord.IsZero())
}

func TestBinlogCoordinate_GTIDContains(t *testing.T) {
	larger := ghostferry.NewGTIDCoordinate(testGTIDSetB)
	smaller := ghostferry.NewGTIDCoordinate(testGTIDSetA)

	contains, err := larger.Contains(smaller)
	require.NoError(t, err)
	assert.True(t, contains)

	contains, err = smaller.Contains(larger)
	require.NoError(t, err)
	assert.False(t, contains)
}

func TestBinlogCoordinate_ContainsRejectsFilePosition(t *testing.T) {
	gtid := ghostferry.NewGTIDCoordinate(testGTIDSetA)
	filePos := ghostferry.NewFilePositionCoordinate(mysql.Position{Name: "mysql-bin.000001", Pos: 4})

	_, err := gtid.Contains(filePos)
	assert.Error(t, err)
}

func TestBinlogCoordinate_ParsedGTIDSetRejectsFilePosition(t *testing.T) {
	filePos := ghostferry.NewFilePositionCoordinate(mysql.Position{Name: "mysql-bin.000001", Pos: 4})

	_, err := filePos.ParsedGTIDSet()
	assert.Error(t, err)
}

func TestBinlogCoordinate_GTIDJSONRoundTrip(t *testing.T) {
	coord := ghostferry.NewGTIDCoordinate(testGTIDSetA)

	data, err := json.Marshal(coord)
	require.NoError(t, err)

	var decoded ghostferry.BinlogCoordinate
	require.NoError(t, json.Unmarshal(data, &decoded))

	assert.True(t, decoded.IsGTID())
	assert.Equal(t, testGTIDSetA, decoded.GTIDSet)
}

func TestBinlogCoordinate_CompareRejectsMixedTypes(t *testing.T) {
	gtid := ghostferry.NewGTIDCoordinate(testGTIDSetA)
	filePos := ghostferry.NewFilePositionCoordinate(mysql.Position{Name: "mysql-bin.000001", Pos: 4})

	assert.Panics(t, func() {
		gtid.Compare(filePos)
	})
}

func TestBinlogCoordinate_CompareUnsupportedForGTID(t *testing.T) {
	a := ghostferry.NewGTIDCoordinate(testGTIDSetA)
	b := ghostferry.NewGTIDCoordinate(testGTIDSetB)

	assert.Panics(t, func() {
		a.Compare(b)
	})
}
