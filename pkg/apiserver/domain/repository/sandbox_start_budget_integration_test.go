//go:build integration

package repository

import (
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/stretchr/testify/require"
)

func TestSandboxStartMySQLConcurrentBudget(t *testing.T) {
	store := newMySQLJobSchedulerTestStore(t)
	require.NoError(t, store.Client.AutoMigrate(&model.JobSandbox{}))
	t.Cleanup(func() { require.NoError(t, store.Client.Migrator().DropTable(&model.JobSandbox{})) })
	testSandboxStartBudget(t, store)
}
