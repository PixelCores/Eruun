package application

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

func TestLifecycleOperationRecordsPreserveResourceErrors(t *testing.T) {
	for _, operation := range []string{"restart", "stop", "start", "cleanup"} {
		t.Run(operation, func(t *testing.T) {
			var record func(string, bool, error)
			var records func() []operationJobRecord
			var resultError func() error
			var failures func() []string
			info := ""
			switch operation {
			case "restart":
				reporter := newRestartReporter()
				record = func(name string, skipped bool, err error) {
					reporter.record("Deployment", "default", name, skipped, err)
				}
				records = func() []operationJobRecord { return buildRestartJobRecords(reporter) }
				resultError = reporter.err
				failures = func() []string { return formatFailedResources(reporter.failedResources) }
				info = "restarted"
			case "stop", "start":
				reporter := newDeploymentScaleReporter()
				record = func(name string, skipped bool, err error) {
					reporter.record("Deployment", "default", name, skipped, err)
				}
				resultError = reporter.err
				failures = func() []string { return formatFailedResources(reporter.failedResources) }
				if operation == "stop" {
					records = func() []operationJobRecord { return buildStopJobRecords(reporter) }
					info = "stopped"
				} else {
					records = func() []operationJobRecord { return buildStartJobRecords(reporter) }
					info = "started"
				}
			case "cleanup":
				reporter := newCleanupReporter()
				record = func(name string, _ bool, err error) { reporter.record("Deployment", "default", name, err) }
				records = func() []operationJobRecord { return buildCleanupJobRecords(reporter) }
				resultError = reporter.err
				failures = func() []string { return formatFailedResources(reporter.failedResources) }
				info = "deleted"
			}

			nestedErr := errors.New("request failed (retry exhausted)")
			record("api", false, nestedErr)
			record("worker", false, errors.New("request denied"))
			record("empty-error", false, errors.New(""))
			record("whitespace-error", false, errors.New("  request denied (slow)  "))
			record("success", false, nil)
			record("", false, errors.New("ignored unnamed resource"))
			want := []operationJobRecord{{name: "Deployment:default/success", status: config.StatusCompleted, info: info}}
			if operation != "cleanup" {
				record("skip", true, nil)
				want = append(want, operationJobRecord{name: "Deployment:default/skip", status: config.StatusSkipped, info: "skipped"})
			}
			want = append(want,
				operationJobRecord{name: "Deployment:default/api", status: config.StatusFailed, info: "failed", errMsg: nestedErr.Error()},
				operationJobRecord{name: "Deployment:default/worker", status: config.StatusFailed, info: "failed", errMsg: "request denied"},
				operationJobRecord{name: "Deployment:default/empty-error", status: config.StatusFailed, info: "failed", errMsg: "operation failed"},
				operationJobRecord{name: "Deployment:default/whitespace-error", status: config.StatusFailed, info: "failed", errMsg: "request denied (slow)"},
			)
			jobs := records()
			require.Equal(t, want, jobs)
			require.ErrorIs(t, resultError(), nestedErr)
			require.Equal(t, []string{
				"Deployment:default/api (request failed (retry exhausted))",
				"Deployment:default/worker (request denied)",
				"Deployment:default/empty-error ()",
				"Deployment:default/whitespace-error (  request denied (slow)  )",
			}, failures())

			ctx := context.Background()
			store := newApplicationCallbackStore(t)
			app := &model.Applications{ID: "operation-errors", Name: "operation-errors"}
			service := &applicationsServiceImpl{Store: store}
			task, err := service.recordAppOperationTask(ctx, app, config.WorkflowTaskType(operation), operation, "", config.StatusFailed, 1, 2, jobs, nil)
			require.NoError(t, err)
			persisted, err := store.List(ctx, &model.JobInfo{TaskID: task.TaskID}, &datastore.ListOptions{SortBy: []datastore.SortOption{{Key: "id", Order: datastore.SortOrderAscending}}})
			require.NoError(t, err)
			require.Len(t, persisted, len(want))
			for i, entity := range persisted {
				job := entity.(*model.JobInfo)
				require.Equal(t, want[i].name, job.ServiceName)
				require.Equal(t, want[i].errMsg, job.Error)
				require.Equal(t, string(want[i].status), job.Status)
				require.Equal(t, want[i].info, job.Info)
			}
		})
	}
}
