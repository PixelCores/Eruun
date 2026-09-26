package bcode

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func snapshotBcodeRegistry() (map[int32]*Bcode, error) {
	snapshot := make(map[int32]*Bcode, len(bcodeMap))
	for code, value := range bcodeMap {
		snapshot[code] = value
	}
	return snapshot, bcodeInitErr
}

func restoreBcodeRegistry(snapshot map[int32]*Bcode, initErr error) {
	bcodeMap = snapshot
	bcodeInitErr = initErr
}

func TestNewBcode_DuplicateDoesNotPanic(t *testing.T) {
	oldMap, oldErr := snapshotBcodeRegistry()
	bcodeMap = map[int32]*Bcode{}
	bcodeInitErr = nil
	defer restoreBcodeRegistry(oldMap, oldErr)

	first := NewBcode(400, 99001, "first")
	second := NewBcode(400, 99001, "second")

	require.Same(t, first, second)
	require.Error(t, Init())
}

func TestInit_NoErrorWhenRegistryClean(t *testing.T) {
	oldMap, oldErr := snapshotBcodeRegistry()
	bcodeMap = map[int32]*Bcode{}
	bcodeInitErr = nil
	defer restoreBcodeRegistry(oldMap, oldErr)

	NewBcode(400, 99002, "ok")
	require.NoError(t, Init())

	bcodeInitErr = errors.New("forced")
	require.Error(t, Init())
}
