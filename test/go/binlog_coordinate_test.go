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
