package mysql

import (
	"testing"
	"time"

	mysqldsn "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func TestMatchedRowsDSNPreservesConnectionOptions(t *testing.T) {
	for _, raw := range []string{
		"user:placeholder@tcp(127.0.0.1:3306)/test?parseTime=true&loc=UTC&timeout=3s",
		"user:placeholder@tcp(127.0.0.1:3306)/test?parseTime=true&loc=UTC&timeout=3s&clientFoundRows=false",
	} {
		value, err := matchedRowsDSN(raw)
		require.NoError(t, err)
		parsed, err := mysqldsn.ParseDSN(value)
		require.NoError(t, err)
		require.True(t, parsed.ClientFoundRows)
		require.True(t, parsed.ParseTime)
		require.Equal(t, 3*time.Second, parsed.Timeout)
		require.Equal(t, "test", parsed.DBName)
		require.Equal(t, "placeholder", parsed.Passwd)
	}
	_, err := matchedRowsDSN("invalid")
	require.Error(t, err)
}
