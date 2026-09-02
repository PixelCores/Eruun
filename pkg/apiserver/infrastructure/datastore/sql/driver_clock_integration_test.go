//go:build integration

package sql

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	mysqlgorm "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestCurrentDatabaseTimeIntegrationAcrossDaylightSaving(t *testing.T) {
	dsn := os.Getenv("MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("MYSQL_TEST_DSN not set; skip MySQL clock integration test")
	}
	db, err := gorm.Open(mysqlgorm.New(mysqlgorm.Config{
		DSN:                       dsn,
		SkipInitializeWithVersion: true,
	}), &gorm.Config{
		DisableAutomaticPing: true,
		Logger:               logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })

	tests := []struct {
		name string
		utc  string
	}{
		{"before spring forward", "2026-03-08T06:59:59.015625Z"},
		{"after spring forward", "2026-03-08T07:00:00.015625Z"},
		{"first repeated hour", "2026-11-01T05:30:00.015625Z"},
		{"before fall back", "2026-11-01T05:59:59.015625Z"},
		{"after fall back", "2026-11-01T06:00:00.015625Z"},
		{"second repeated hour", "2026-11-01T06:30:00.015625Z"},
		{"after repeated hour", "2026-11-01T07:00:00.015625Z"},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// SET timestamp and SET time_zone are session-scoped, so pin the clock
	// reads to the same connection. No schema or application rows are changed.
	require.NoError(t, db.WithContext(ctx).Connection(func(conn *gorm.DB) error {
		store := &Driver{Client: *conn}
		for _, zone := range []string{"+00:00", "+08:00", "America/New_York"} {
			t.Run(zone, func(t *testing.T) {
				require.NoError(t, conn.Exec("SET time_zone = ?", zone).Error,
					"test MySQL must have the America/New_York time zone loaded")
				for _, tt := range tests {
					t.Run(tt.name, func(t *testing.T) {
						want, err := time.Parse(time.RFC3339Nano, tt.utc)
						require.NoError(t, err)
						timestamp := fmt.Sprintf("%d.%06d", want.Unix(), want.Nanosecond()/1000)
						require.NoError(t, conn.Exec("SET timestamp = ?", timestamp).Error)

						got, err := store.CurrentDatabaseTime(ctx)
						require.NoError(t, err)
						require.Equal(t, want, got)
					})
				}
			})
		}
		return nil
	}))
}
