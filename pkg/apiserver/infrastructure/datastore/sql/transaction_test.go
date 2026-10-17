package sql

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTransactionResultKeepsCancellationCause(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	err := transactionResult(cancelled, sql.ErrTxDone)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, err, sql.ErrTxDone)

	// Without cancellation, ErrTxDone is a real misuse and stays unchanged.
	require.Same(t, sql.ErrTxDone, transactionResult(context.Background(), sql.ErrTxDone))
	other := errors.New("commit failed")
	require.Same(t, other, transactionResult(cancelled, other))
}
