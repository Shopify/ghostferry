package testhelpers

import (
	"path/filepath"
	"runtime"
)

var (
	_, path, _, _ = runtime.Caller(0)
	basename      = filepath.Dir(path)
	fixturesPath  = filepath.Clean(basename + "/../test/fixtures")
)

func FixturePath(name string) string {
	return fixturesPath + "/" + name
}
