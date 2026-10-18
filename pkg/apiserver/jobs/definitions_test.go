package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/jobs/artifacts"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestDefinitionScheduleAcceptsWorkflowScheduleCronForms(t *testing.T) {
	service, _, ctx := testJobService(t)
	request := DefinitionRequest{JobSpec: commandRequest().JobSpec, Schedule: &ScheduleRequest{Cron: " 0 0 2 * * * ", Enabled: true}}
	created, err := service.CreateDefinition(ctx, request)
	require.NoError(t, err)
	require.Equal(t, "0 2 * * *", created.Schedule.Cron, "a zero seconds field is normalized away, as for WorkflowSchedule")
}

func TestDefinitionScheduleValidation(t *testing.T) {
	for _, tc := range []struct {
		name, message string
		schedule      *ScheduleRequest
	}{
		{"invalid cron", "schedule cron:", &ScheduleRequest{Cron: "61 * * * *"}},
		{"invalid policy", "concurrencyPolicy must", &ScheduleRequest{Cron: "* * * * *", ConcurrencyPolicy: "forbid"}},
		{"enabled without cron", "cron is required", &ScheduleRequest{Enabled: true}},
		{"non-zero seconds", "seconds field must be 0", &ScheduleRequest{Cron: "30 * * * * *"}},
		{"descriptor", "5 or 6 fields", &ScheduleRequest{Cron: "@hourly"}},
		{"timezone", "seconds field must be 0", &ScheduleRequest{Cron: "CRON_TZ=Asia/Shanghai * * * * *"}},
		{"oversized cron", "64 bytes", &ScheduleRequest{Cron: strings.Repeat("0,", 32) + "0 * * * *"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, raw, ctx := testJobService(t)
			request := DefinitionRequest{JobSpec: commandRequest().JobSpec}
			created, err := service.CreateDefinition(ctx, request)
			require.NoError(t, err)
			request.Schedule = tc.schedule
			_, err = service.CreateDefinition(ctx, request)
			require.ErrorIs(t, err, bcode.ErrJobInput)
			require.Contains(t, bcode.SafeClientMessage(err), tc.message)
			_, err = service.UpdateDefinition(ctx, created.ID, request)
			require.ErrorIs(t, err, bcode.ErrJobInput)
			require.Contains(t, bcode.SafeClientMessage(err), tc.message)
			count, err := raw.Count(ctx, &model.JobDefinition{}, nil)
			require.NoError(t, err)
			require.EqualValues(t, 1, count)
			stored, err := service.GetDefinition(ctx, created.ID)
			require.NoError(t, err)
			require.Nil(t, stored.Schedule)
			require.EqualValues(t, 1, stored.Revision)
		})
	}
}

func TestDefinitionScheduleReplacementAndNextRun(t *testing.T) {
	service, raw, ctx := testJobService(t)
	request := DefinitionRequest{JobSpec: commandRequest().JobSpec, Schedule: &ScheduleRequest{Cron: "  *  * * * *  ", Enabled: true}}
	before := time.Now().Unix()
	created, err := service.CreateDefinition(ctx, request)
	require.NoError(t, err)
	require.NotNil(t, created.Schedule)
	require.Equal(t, "* * * * *", created.Schedule.Cron)
	require.Equal(t, "Forbid", created.Schedule.ConcurrencyPolicy)
	require.True(t, created.Schedule.Enabled)
	require.Greater(t, created.Schedule.NextRun, before)
	require.LessOrEqual(t, created.Schedule.NextRun, time.Now().Unix()+60)

	due := time.Now().Add(-time.Minute).Unix()
	record := &model.JobDefinition{ID: created.ID}
	require.NoError(t, raw.Get(ctx, record))
	record.NextRun, record.LastRunTaskID, record.LastSkipReason = due, "previous-run", "previous skip"
	require.NoError(t, raw.Put(ctx, record))
	request.Name = "changed specification"
	updated, err := service.UpdateDefinition(ctx, created.ID, request)
	require.NoError(t, err)
	require.Equal(t, due, updated.Schedule.NextRun, "specification changes must preserve the overdue occurrence")
	require.Equal(t, "previous-run", updated.Schedule.LastRunTaskID)
	require.Equal(t, "previous skip", updated.Schedule.LastSkipReason)
	request.Schedule.ConcurrencyPolicy = "Allow"
	updated, err = service.UpdateDefinition(ctx, created.ID, request)
	require.NoError(t, err)
	require.Equal(t, due, updated.Schedule.NextRun, "policy changes must preserve the overdue occurrence")
	require.Equal(t, "Allow", updated.Schedule.ConcurrencyPolicy)

	request.Schedule.Cron = "0 2 * * *"
	before = time.Now().Unix()
	updated, err = service.UpdateDefinition(ctx, created.ID, request)
	require.NoError(t, err)
	require.Greater(t, updated.Schedule.NextRun, before)
	next := time.Unix(updated.Schedule.NextRun, 0).UTC()
	require.Equal(t, 2, next.Hour())
	require.Zero(t, next.Minute())
	request.Schedule.Enabled = false
	updated, err = service.UpdateDefinition(ctx, created.ID, request)
	require.NoError(t, err)
	require.Zero(t, updated.Schedule.NextRun)
	stored, err := service.GetDefinition(ctx, created.ID)
	require.NoError(t, err)
	require.False(t, stored.Schedule.Enabled)
	require.Zero(t, stored.Schedule.NextRun)
	request.Schedule.Enabled = true
	before = time.Now().Unix()
	updated, err = service.UpdateDefinition(ctx, created.ID, request)
	require.NoError(t, err)
	require.Greater(t, updated.Schedule.NextRun, before)

	request.Schedule = nil
	updated, err = service.UpdateDefinition(ctx, created.ID, request)
	require.NoError(t, err)
	require.Nil(t, updated.Schedule)
	require.NoError(t, raw.Get(ctx, record))
	require.Empty(t, record.Cron)
	require.Empty(t, record.ConcurrencyPolicy)
	require.False(t, record.ScheduleEnabled)
	require.Zero(t, record.NextRun)
	require.Equal(t, "previous-run", record.LastRunTaskID, "removing the schedule keeps the last run for later Forbid or Replace checks")
	require.Empty(t, record.LastSkipReason)

	request.Schedule = &ScheduleRequest{}
	updated, err = service.UpdateDefinition(ctx, created.ID, request)
	require.NoError(t, err)
	require.Equal(t, &Schedule{ScheduleRequest: ScheduleRequest{ConcurrencyPolicy: "Forbid"}, LastRunTaskID: "previous-run"}, updated.Schedule)
}

func TestDefinitionCRUDKeepsRunSnapshotsAfterUpdateAndDelete(t *testing.T) {
	service, raw, ctx := testJobService(t)
	declaration := commandRequest().JobSpec
	created, err := service.CreateDefinition(ctx, DefinitionRequest{JobSpec: declaration})
	require.NoError(t, err)
	require.Len(t, created.ID, 24)
	require.EqualValues(t, 1, created.Revision)
	require.Equal(t, "member", created.Creator)
	require.False(t, created.CreateTime.IsZero())
	stored := &model.JobDefinition{ID: created.ID}
	require.NoError(t, raw.Get(ctx, stored))
	originalSpec := stored.Spec
	count, err := raw.Count(ctx, &model.WorkflowQueue{}, nil)
	require.NoError(t, err)
	require.Zero(t, count, "saving a definition must not submit a run")
	read, err := service.GetDefinition(ctx, created.ID)
	require.NoError(t, err)
	require.WithinDuration(t, created.CreateTime, read.CreateTime, 0)
	require.WithinDuration(t, created.UpdateTime, read.UpdateTime, 0)
	require.Equal(t, created.Spec, read.Spec)
	require.Equal(t, created.ID, read.ID)
	require.Equal(t, created.Revision, read.Revision)
	created = read

	_, err = service.CreateDefinition(ctx, DefinitionRequest{JobSpec: commandRequest().JobSpec})
	require.ErrorIs(t, err, bcode.ErrJobDefinitionConflict)
	otherSpec := commandRequest().JobSpec
	otherSpec.Name = "another definition"
	other, err := service.CreateDefinition(ctx, DefinitionRequest{JobSpec: otherSpec})
	require.NoError(t, err)
	_, err = service.UpdateDefinition(ctx, created.ID, DefinitionRequest{JobSpec: otherSpec})
	require.ErrorIs(t, err, bcode.ErrJobDefinitionConflict)
	read, err = service.GetDefinition(ctx, created.ID)
	require.NoError(t, err)
	require.Equal(t, created, read, "a conflicting replacement must roll back its revision")

	accepted, err := service.Run(ctx, created.ID, time.Now().Add(time.Hour).Unix())
	require.NoError(t, err)
	run := &model.WorkflowQueue{TaskID: accepted.TaskID}
	require.NoError(t, raw.Get(ctx, run))
	require.Equal(t, created.ID, run.JobDefinitionID)
	require.EqualValues(t, 1, run.JobDefinitionRevision)
	require.Equal(t, originalSpec, run.JobSpec)

	replacement := commandRequest().JobSpec
	replacement.Name = "replaced definition"
	replacement.Stages = []spec.JobStage{{Name: "new stage", Steps: []spec.JobStep{{Name: "replacement", Type: "command", Spec: json.RawMessage(`{"image":"busybox:1.37.0","command":["true"]}`)}}}}
	editor := account.WithScope(ctx, account.Scope{WorkspaceID: "space", Namespace: "space-ns", Role: "member", UserID: "editor"})
	updated, err := service.UpdateDefinition(editor, created.ID, DefinitionRequest{JobSpec: replacement})
	require.NoError(t, err)
	require.Equal(t, created.ID, updated.ID)
	require.EqualValues(t, 2, updated.Revision)
	require.Equal(t, "member", updated.Creator)
	require.Equal(t, "replaced definition", updated.Name)
	require.Len(t, updated.Spec.Stages, 1)
	require.Equal(t, "replacement", updated.Spec.Stages[0].Steps[0].Name)
	require.NoError(t, raw.Get(ctx, run))
	require.Equal(t, originalSpec, run.JobSpec)
	require.EqualValues(t, 1, run.JobDefinitionRevision)
	next, err := service.Run(ctx, created.ID, 0)
	require.NoError(t, err)
	nextRun := &model.WorkflowQueue{TaskID: next.TaskID}
	require.NoError(t, raw.Get(ctx, nextRun))
	require.EqualValues(t, 2, nextRun.JobDefinitionRevision)
	require.NoError(t, raw.Get(ctx, stored))
	require.Equal(t, stored.Spec, nextRun.JobSpec)
	require.NotEqual(t, originalSpec, nextRun.JobSpec)

	require.NoError(t, service.DeleteDefinition(ctx, created.ID))
	_, err = service.GetDefinition(ctx, created.ID)
	require.ErrorIs(t, err, datastore.ErrRecordNotExist)
	_, err = service.Run(ctx, created.ID, 0)
	require.ErrorIs(t, err, datastore.ErrRecordNotExist)
	detail, err := service.Get(ctx, accepted.TaskID)
	require.NoError(t, err)
	require.Equal(t, created.Spec, detail.Job)
	require.NoError(t, raw.Get(ctx, run))
	require.Equal(t, created.ID, run.JobDefinitionID)
	runs, err := service.List(ctx, created.ID, 1, 20)
	require.NoError(t, err)
	require.EqualValues(t, 2, runs.Total)
	require.Len(t, runs.Items, 2)
	remaining, err := service.ListDefinitions(ctx, 1, 20)
	require.NoError(t, err)
	require.EqualValues(t, 1, remaining.Total)
	require.Equal(t, other.ID, remaining.Items[0].ID)
	require.Empty(t, service.Kube.(*fake.Clientset).Actions(), "command definition writes must not create Kubernetes resources")
}

func TestDefinitionsRequireWorkspaceScopeAndMemberWrites(t *testing.T) {
	service, raw, ctx := testJobService(t)
	created, err := service.CreateDefinition(ctx, DefinitionRequest{JobSpec: commandRequest().JobSpec})
	require.NoError(t, err)
	viewer := account.WithScope(ctx, account.Scope{WorkspaceID: "space", Namespace: "space-ns", Role: "viewer"})
	_, err = service.GetDefinition(viewer, created.ID)
	require.NoError(t, err)
	_, err = service.ListDefinitions(viewer, 1, 20)
	require.NoError(t, err)
	_, err = service.List(viewer, "", 1, 20)
	require.NoError(t, err)
	writes := map[string]func(context.Context) error{
		"create": func(ctx context.Context) error {
			_, err := service.CreateDefinition(ctx, DefinitionRequest{JobSpec: commandRequest().JobSpec})
			return err
		},
		"update": func(ctx context.Context) error {
			_, err := service.UpdateDefinition(ctx, created.ID, DefinitionRequest{JobSpec: commandRequest().JobSpec})
			return err
		},
		"delete": func(ctx context.Context) error { return service.DeleteDefinition(ctx, created.ID) },
		"run":    func(ctx context.Context) error { _, err := service.Run(ctx, created.ID, 0); return err },
	}
	for name, write := range writes {
		t.Run(name, func(t *testing.T) {
			require.ErrorIs(t, write(viewer), bcode.ErrForbidden)
			require.ErrorIs(t, write(context.Background()), bcode.ErrForbidden)
		})
	}
	foreign := account.WithScope(ctx, account.Scope{WorkspaceID: "other", Namespace: "other-ns", Role: "member"})
	require.NoError(t, raw.Add(ctx, &model.Workspace{ID: "other", Namespace: "other-ns"}))
	other, err := service.CreateDefinition(foreign, DefinitionRequest{JobSpec: commandRequest().JobSpec})
	require.NoError(t, err, "definition names are unique per workspace")
	require.NotEqual(t, created.ID, other.ID)
	for name, action := range map[string]func() error{
		"get": func() error { _, err := service.GetDefinition(foreign, created.ID); return err },
		"update": func() error {
			_, err := service.UpdateDefinition(foreign, created.ID, DefinitionRequest{JobSpec: commandRequest().JobSpec})
			return err
		},
		"delete": func() error { return service.DeleteDefinition(foreign, created.ID) },
		"run":    func() error { _, err := service.Run(foreign, created.ID, 0); return err },
	} {
		t.Run("cross workspace "+name, func(t *testing.T) {
			err := action()
			require.True(t, errors.Is(err, bcode.ErrNotFound) || errors.Is(err, bcode.ErrForbidden), "got %v", err)
		})
	}
	page, err := service.ListDefinitions(ctx, 1, 20)
	require.NoError(t, err)
	require.EqualValues(t, 1, page.Total)
	require.Equal(t, created.ID, page.Items[0].ID)
	page, err = service.ListDefinitions(foreign, 1, 20)
	require.NoError(t, err)
	require.EqualValues(t, 1, page.Total)
	require.Equal(t, other.ID, page.Items[0].ID)
	// The persistence wrapper also enforces scope when called directly.
	require.ErrorIs(t, service.Store.Get(foreign, &model.JobDefinition{ID: created.ID}), bcode.ErrForbidden)
	plain, err := New(raw, service.Kube, service.Config)
	require.NoError(t, err)
	_, err = plain.GetDefinition(foreign, created.ID)
	require.ErrorIs(t, err, bcode.ErrNotFound)
	plainPage, err := plain.ListDefinitions(ctx, 1, 20)
	require.NoError(t, err)
	require.EqualValues(t, 1, plainPage.Total)
	require.Equal(t, created.ID, plainPage.Items[0].ID)
}

func TestDefinitionUpdatesSerializeRevisionInTransaction(t *testing.T) {
	service, _, ctx := testJobService(t)
	created, err := service.CreateDefinition(ctx, DefinitionRequest{JobSpec: commandRequest().JobSpec})
	require.NoError(t, err)
	const writers = 6
	results := make(chan *Definition, writers)
	errors := make(chan error, writers)
	var wg sync.WaitGroup
	for i := range writers {
		wg.Go(func() {
			declaration := commandRequest().JobSpec
			declaration.Name = fmt.Sprintf("revision writer %d", i)
			updated, err := service.UpdateDefinition(ctx, created.ID, DefinitionRequest{JobSpec: declaration})
			results <- updated
			errors <- err
		})
	}
	wg.Wait()
	close(results)
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	revisions := make([]int, 0, writers)
	for updated := range results {
		revisions = append(revisions, int(updated.Revision))
	}
	sort.Ints(revisions)
	require.Equal(t, []int{2, 3, 4, 5, 6, 7}, revisions)
	read, err := service.GetDefinition(ctx, created.ID)
	require.NoError(t, err)
	require.EqualValues(t, writers+1, read.Revision)
}

func TestSubmitAndDefinitionRunNormalizeExecuteAt(t *testing.T) {
	service, raw, ctx := testJobService(t)
	definition, err := service.CreateDefinition(ctx, DefinitionRequest{JobSpec: commandRequest().JobSpec})
	require.NoError(t, err)
	now := time.Now().Unix()
	for _, entry := range []struct {
		name string
		run  func(int64) (*Accepted, error)
	}{
		{"submit", func(executeAt int64) (*Accepted, error) {
			request := commandRequest()
			request.ExecuteAt = executeAt
			return service.Submit(ctx, request)
		}},
		{"definition", func(executeAt int64) (*Accepted, error) { return service.Run(ctx, definition.ID, executeAt) }},
	} {
		for _, tc := range []struct {
			name            string
			input, expected int64
		}{
			{"negative", -1, 0}, {"immediate", 0, 0}, {"past", now - 10, 0}, {"future", now + 3600, now + 3600},
		} {
			t.Run(entry.name+"/"+tc.name, func(t *testing.T) {
				before, err := raw.Count(ctx, &model.WorkflowQueue{}, nil)
				require.NoError(t, err)
				accepted, err := entry.run(tc.input)
				if tc.input < 0 {
					require.ErrorIs(t, err, bcode.ErrJobInput)
					after, err := raw.Count(ctx, &model.WorkflowQueue{}, nil)
					require.NoError(t, err)
					require.Equal(t, before, after)
					return
				}
				require.NoError(t, err)
				task := &model.WorkflowQueue{TaskID: accepted.TaskID}
				require.NoError(t, raw.Get(ctx, task))
				require.Equal(t, tc.expected, task.ExecuteAt)
				require.Equal(t, config.StatusWaiting, task.Status)
				if entry.name == "submit" {
					require.Empty(t, task.JobDefinitionID)
					require.Zero(t, task.JobDefinitionRevision)
				}
			})
		}
	}
}

func TestDefinitionWritesReuseFullSubmissionValidation(t *testing.T) {
	service, raw, ctx := testJobService(t)
	created, err := service.CreateDefinition(ctx, DefinitionRequest{JobSpec: commandRequest().JobSpec})
	require.NoError(t, err)
	created, err = service.GetDefinition(ctx, created.ID)
	require.NoError(t, err)
	for _, tc := range []struct {
		name        string
		declaration func() spec.JobSpec
	}{
		{"normalize", func() spec.JobSpec { return spec.JobSpec{Name: "missing stages"} }},
		{"workspace preparation", func() spec.JobSpec {
			declaration := commandRequest().JobSpec
			declaration.Volume = &spec.JobVolume{Size: "1Gi", StorageClass: "not-allowed"}
			return declaration
		}},
		{"last evaluation step", func() spec.JobSpec {
			declaration := commandRequest().JobSpec
			declaration.Stages = append(declaration.Stages, evaluationDeclaration("missing dataset", "11111111-1111-1111-1111-111111111111", "oracle", "").Stages[0])
			declaration.Stages[1].Name = "evaluation"
			return declaration
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, submitErr := service.Submit(ctx, SubmitRequest{JobSpec: tc.declaration()})
			require.Error(t, submitErr)
			_, createErr := service.CreateDefinition(ctx, DefinitionRequest{JobSpec: tc.declaration()})
			require.EqualError(t, createErr, submitErr.Error())
			_, updateErr := service.UpdateDefinition(ctx, created.ID, DefinitionRequest{JobSpec: tc.declaration()})
			require.EqualError(t, updateErr, submitErr.Error())
			read, err := service.GetDefinition(ctx, created.ID)
			require.NoError(t, err)
			require.Equal(t, created, read)
		})
	}
	count, err := raw.Count(ctx, &model.JobDefinition{}, nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	count, err = raw.Count(ctx, &model.WorkflowQueue{}, nil)
	require.NoError(t, err)
	require.Zero(t, count)

	datasetID := "11111111-1111-1111-1111-111111111111"
	require.NoError(t, raw.Add(ctx, &model.JobArtifact{ID: datasetID, WorkspaceID: "space", Kind: artifacts.KindDataset}))
	evaluation, err := service.CreateDefinition(ctx, DefinitionRequest{JobSpec: evaluationDeclaration("valid evaluation", datasetID, "oracle", "")})
	require.NoError(t, err)
	require.Equal(t, spec.DefaultJobResultPolicy(), *evaluation.Spec.Stages[0].Steps[0].Traits.Evaluation.ResultPolicy)
}

func TestJobListingsFilterAndPaginateWithinWorkspace(t *testing.T) {
	service, raw, ctx := testJobService(t)
	definition, err := service.CreateDefinition(ctx, DefinitionRequest{JobSpec: commandRequest().JobSpec})
	require.NoError(t, err)
	otherSpec := commandRequest().JobSpec
	otherSpec.Name = "second definition"
	second, err := service.CreateDefinition(ctx, DefinitionRequest{JobSpec: otherSpec})
	require.NoError(t, err)
	base := time.Now().Add(-time.Hour)
	require.NoError(t, raw.Client.Model(&model.JobDefinition{}).Where("id = ?", definition.ID).Update("create_time", base).Error)
	require.NoError(t, raw.Client.Model(&model.JobDefinition{}).Where("id = ?", second.ID).Update("create_time", base.Add(time.Minute)).Error)
	definitions, err := service.ListDefinitions(ctx, 1, 1)
	require.NoError(t, err)
	require.EqualValues(t, 2, definitions.Total)
	require.Len(t, definitions.Items, 1)
	require.Equal(t, second.ID, definitions.Items[0].ID)
	definitions, err = service.ListDefinitions(ctx, 2, 1)
	require.NoError(t, err)
	require.Equal(t, definition.ID, definitions.Items[0].ID)

	ids := make([]string, 0, 4)
	for i := range 4 {
		var accepted *Accepted
		if i == 2 {
			accepted, err = service.Run(ctx, second.ID, 0)
		} else if i == 3 {
			accepted, err = service.Submit(ctx, commandRequest())
		} else {
			accepted, err = service.Run(ctx, definition.ID, 0)
		}
		require.NoError(t, err)
		ids = append(ids, accepted.TaskID)
		require.NoError(t, raw.Client.Model(&model.WorkflowQueue{}).Where("task_id = ?", accepted.TaskID).Update("create_time", base.Add(time.Duration(i)*time.Minute)).Error)
	}
	for _, task := range []*model.WorkflowQueue{
		{TaskID: "foreign-run", WorkspaceID: "other", Type: config.WorkflowTaskTypeJob, JobDefinitionID: definition.ID},
		{TaskID: "application-job", WorkspaceID: "space", AppID: "app", Type: config.WorkflowTaskTypeJob},
		{TaskID: "application-workflow", WorkspaceID: "space", AppID: "app", Type: config.WorkflowTaskTypeWorkflow},
	} {
		require.NoError(t, raw.Add(ctx, task))
	}
	plain, err := New(raw, service.Kube, service.Config)
	require.NoError(t, err)
	plainPage, err := plain.List(ctx, "", 1, 20)
	require.NoError(t, err)
	require.EqualValues(t, 4, plainPage.Total)
	for _, run := range plainPage.Items {
		require.Equal(t, "space", run.WorkspaceID)
	}
	page, err := service.List(ctx, "", 1, 2)
	require.NoError(t, err)
	require.EqualValues(t, 4, page.Total)
	require.Equal(t, 1, page.Page)
	require.Equal(t, 2, page.PageSize)
	require.Equal(t, []string{ids[3], ids[2]}, []string{page.Items[0].TaskID, page.Items[1].TaskID})
	page, err = service.List(ctx, "", 2, 2)
	require.NoError(t, err)
	require.Equal(t, []string{ids[1], ids[0]}, []string{page.Items[0].TaskID, page.Items[1].TaskID})
	page, err = service.List(ctx, definition.ID, 2, 1)
	require.NoError(t, err)
	require.EqualValues(t, 2, page.Total)
	require.Len(t, page.Items, 1)
	require.Equal(t, ids[0], page.Items[0].TaskID)
	require.Equal(t, definition.ID, page.Items[0].JobDefinitionID)
	require.EqualValues(t, 1, page.Items[0].JobDefinitionRevision)
	page, err = service.List(ctx, "missing-definition", 1, 20)
	require.NoError(t, err)
	require.Zero(t, page.Total)
	require.NotNil(t, page.Items)
	require.Empty(t, page.Items)
	for _, pagination := range [][2]int{{0, 20}, {1, 0}, {-1, 20}, {1, 101}, {10000001, 20}} {
		_, err := service.List(ctx, "", pagination[0], pagination[1])
		require.ErrorIs(t, err, bcode.ErrJobInput)
		_, err = service.ListDefinitions(ctx, pagination[0], pagination[1])
		require.ErrorIs(t, err, bcode.ErrJobInput)
	}
}
