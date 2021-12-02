/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package sqltypes implements interfaces and types that represent SQL values.
package sqltypes

import (
	"encoding/base64"
	"errors"
	"strconv"

	"github.com/blastrain/vitess-sqlparser/bytes2"
	"github.com/blastrain/vitess-sqlparser/hack"
)

var (
	// NULL represents the NULL value.
	NULL = Value{}
	// DontEscape tells you if a character should not be escaped.
	DontEscape = byte(255)
	nullstr    = []byte("null")
)

// BinWriter interface is used for encoding values.
// Types like bytes.Buffer conform to this interface.
// We expect the writer objects to be in-memory buffers.
// So, we don't expect the write operations to fail.
type BinWriter interface {
	Write([]byte) (int, error)
}

// Value can store any SQL value. If the value represents
// an integral type, the bytes are always stored as a cannonical
// representation that matches how MySQL returns such values.
type Value struct {
	typ Type
	val []byte
}

// MakeTrusted makes a new Value based on the type.
// If the value is an integral, then val must be in its cannonical
// form. This function should only be used if you know the value
// and type conform to the rules.  Every place this function is
// called, a comment is needed that explains why it's justified.
// Functions within this package are exempt.
func MakeTrusted(typ Type, val []byte) Value {
	if typ == Null {
		return NULL
	}
	return Value{typ: typ, val: val}
}

// MakeString makes a VarBinary Value.
func MakeString(val []byte) Value {
	return MakeTrusted(VarBinary, val)
}

// Type returns the type of Value.
func (v Value) Type() Type {
	return v.typ
}

// Raw returns the raw bytes. All types are currently implemented as []byte.
// You should avoid using this function. If you do, you should treat the
// bytes as read-only.
func (v Value) Raw() []byte {
	return v.val
}

// Bytes returns a copy of the raw data. All types are currently implemented as []byte.
// Use this function instead of Raw if you can't be sure about maintaining the read-only
// requirements of the bytes.
func (v Value) Bytes() []byte {
	out := make([]byte, len(v.val))
	copy(out, v.val)
	return out
}

// Len returns the length.
func (v Value) Len() int {
	return len(v.val)
}

// String returns the raw value as a string.
func (v Value) String() string {
	return hack.String(v.val)
}

// ToNative converts Value to a native go type.
// This does not work for sqltypes.Tuple. The function
// panics if there are inconsistencies.
func (v Value) ToNative() interface{} {
	var out interface{}
	var err error
	switch {
	case v.typ == Null:
		// no-op
	case IsSigned(v.typ):
		out, err = v.ParseInt64()
	case IsUnsigned(v.typ):
		out, err = v.ParseUint64()
	case IsFloat(v.typ):
		out, err = v.ParseFloat64()
	case v.typ == Tuple:
		err = errors.New("unexpected tuple")
	default:
		out = v.val
	}
	if err != nil {
		panic(err)
	}
	return out
}

// ParseInt64 will parse a Value into an int64. It does
// not check the type.
// TODO(sougou): deprecate this function in favor of a
// more type-aware implemention in arithmetic.
func (v Value) ParseInt64() (val int64, err error) {
	return strconv.ParseInt(v.String(), 10, 64)
}

// ParseUint64 will parse a Value into a uint64. It does
// not check the type.
// TODO(sougou): deprecate this function in favor of a
// more type-aware implemention in arithmetic.
func (v Value) ParseUint64() (val uint64, err error) {
	return strconv.ParseUint(v.String(), 10, 64)
}

// ParseFloat64 will parse a Value into an float64. It does
// not check the type.
// TODO(sougou): deprecate this function in favor of a
// more type-aware implemention in arithmetic.
func (v Value) ParseFloat64() (val float64, err error) {
	return strconv.ParseFloat(v.String(), 64)
}

// EncodeSQL encodes the value into an SQL statement. Can be binary.
func (v Value) EncodeSQL(b BinWriter) {
	// ToNative panics if v is invalid.
	_ = v.ToNative()
	switch {
	case v.typ == Null:
		b.Write(nullstr)
	case IsQuoted(v.typ):
		encodeBytesSQL(v.val, b)
	default:
		b.Write(v.val)
	}
}

// EncodeASCII encodes the value using 7-bit clean ascii bytes.
func (v Value) EncodeASCII(b BinWriter) {
	// ToNative panics if v is invalid.
	_ = v.ToNative()
	switch {
	case v.typ == Null:
		b.Write(nullstr)
	case IsQuoted(v.typ):
		encodeBytesASCII(v.val, b)
	default:
		b.Write(v.val)
	}
}

func encodeBytesSQL(val []byte, b BinWriter) {
	buf := &bytes2.Buffer{}
	buf.WriteByte('\'')
	for _, ch := range val {
		if encodedChar := SQLEncodeMap[ch]; encodedChar == DontEscape {
			buf.WriteByte(ch)
		} else {
			buf.WriteByte('\\')
			buf.WriteByte(encodedChar)
		}
	}
	buf.WriteByte('\'')
	b.Write(buf.Bytes())
}

func encodeBytesASCII(val []byte, b BinWriter) {
	buf := &bytes2.Buffer{}
	buf.WriteByte('\'')
	encoder := base64.NewEncoder(base64.StdEncoding, buf)
	encoder.Write(val)
	encoder.Close()
	buf.WriteByte('\'')
	b.Write(buf.Bytes())
}

// SQLEncodeMap specifies how to escape binary data with '\'.
// Complies to http://dev.mysql.com/doc/refman/5.1/en/string-syntax.html
var SQLEncodeMap [256]byte

// SQLDecodeMap is the reverse of SQLEncodeMap
var SQLDecodeMap [256]byte

var encodeRef = map[byte]byte{
	'\x00': '0',
	'\'':   '\'',
	'"':    '"',
	'\b':   'b',
	'\n':   'n',
	'\r':   'r',
	'\t':   't',
	26:     'Z', // ctl-Z
	'\\':   '\\',
}

func init() {
	for i := range SQLEncodeMap {
		SQLEncodeMap[i] = DontEscape
		SQLDecodeMap[i] = DontEscape
	}
	for i := range SQLEncodeMap {
		if to, ok := encodeRef[byte(i)]; ok {
			SQLEncodeMap[byte(i)] = to
			SQLDecodeMap[to] = byte(i)
		}
	}
}
