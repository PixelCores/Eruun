package datastore

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func TestDBErrorPreservesCause(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded, ErrRecordNotExist} {
		t.Run(cause.Error(), func(t *testing.T) {
			err := fmt.Errorf("load record: %w", NewDBError(cause))
			require.ErrorIs(t, err, cause)
			var dbErr *DBError
			require.ErrorAs(t, err, &dbErr)
			require.Equal(t, cause.Error(), dbErr.Error())
		})
	}
}

func TestDBErrorPreservesDriverError(t *testing.T) {
	cause := &mysql.MySQLError{Number: 1205, Message: "lock wait timeout"}
	err := fmt.Errorf("write record: %w", NewDBError(cause))
	var driverErr *mysql.MySQLError
	require.True(t, errors.As(err, &driverErr))
	require.Same(t, cause, driverErr)
}
