package model

import (
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type testBuiltinModel struct {
	name string
}

func (t *testBuiltinModel) TableName() string      { return t.name }
func (t *testBuiltinModel) ShortTableName() string { return t.name }

func TestBuiltinModelsReturnsOrderedIsolatedSnapshots(t *testing.T) {
	first, err := BuiltinModels()
	require.NoError(t, err)
	second, err := BuiltinModels()
	require.NoError(t, err)

	require.Equal(t, []string{
		"eruun_users", "eruun_identities", "eruun_sessions", "eruun_session_refresh_tokens", "eruun_workspaces", "eruun_workspace_members", "eruun_workspace_invitations",
		"eruun_applications",
		"eruun_app_components",
		"eruun_workflow",
		"eruun_workflow_queue",
		"eruun_job",
		"eruun_system_info",
		"eruun_system_setting",
		"eruun_programming_languages",
		"eruun_job_result_outbox",
		"eruun_workflow_schedule",
	}, modelTableNames(first))
	require.Equal(t, modelTableNames(first), modelTableNames(second))
	require.NotSame(t, first[0], second[0])
	first[0] = &testBuiltinModel{name: "changed"}
	require.Equal(t, "eruun_users", second[0].TableName())
}

func TestValidateModelSetReportsInvalidAndDuplicateEntries(t *testing.T) {
	var nilModel *testBuiltinModel
	_, err := validateModelSet([]Interface{
		nilModel,
		&testBuiltinModel{},
		&testBuiltinModel{name: "duplicate"},
		&testBuiltinModel{name: "duplicate"},
	})

	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "nil model"))
	require.True(t, strings.Contains(err.Error(), "empty table name"))
	require.True(t, strings.Contains(err.Error(), "model table name duplicate conflict"))
}

func TestBuiltinModelsSupportsConcurrentSnapshots(t *testing.T) {
	const callers = 32
	var wg sync.WaitGroup
	errors := make(chan error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			models, err := BuiltinModels()
			if err == nil && len(models) != 17 {
				err = &unexpectedBuiltinModelCount{count: len(models)}
			}
			errors <- err
		}()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
}

type unexpectedBuiltinModelCount struct{ count int }

func (e *unexpectedBuiltinModelCount) Error() string { return "unexpected built-in model count" }

func modelTableNames(models []Interface) []string {
	names := make([]string, 0, len(models))
	for _, item := range models {
		names = append(names, item.TableName())
	}
	return names
}

func builtinModelExists(t *testing.T, tableName string) bool {
	t.Helper()
	models, err := BuiltinModels()
	require.NoError(t, err)
	for _, item := range models {
		if item.TableName() == tableName {
			return true
		}
	}
	return false
}
