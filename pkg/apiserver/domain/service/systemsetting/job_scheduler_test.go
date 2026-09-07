package systemsetting

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestJobSchedulerSystemSettingValidationAndDeleteProtection(t *testing.T) {
	repo := newMockSystemSettingRepo()
	svc := &systemSettingServiceImpl{SettingRepo: repo}
	ctx := context.Background()
	setting, err := svc.Create(ctx, apisv1.CreateSystemSettingRequest{Type: model.SystemSettingTypeWorkflowScheduler, Value: json.RawMessage(`{}`)})
	require.NoError(t, err)
	require.JSONEq(t, `{"strategy":"priority","maxConcurrentJobs":100,"maxConcurrentJobsPerWorkspace":10,"agingSeconds":60}`, string(setting.Value))
	_, err = svc.Update(ctx, model.SystemSettingTypeWorkflowScheduler, apisv1.UpdateSystemSettingRequest{Value: json.RawMessage(`{"strategy":"fifo","maxConcurrentJobs":3,"maxConcurrentJobsPerWorkspace":1}`)})
	require.NoError(t, err)
	_, err = svc.Update(ctx, model.SystemSettingTypeWorkflowScheduler, apisv1.UpdateSystemSettingRequest{Value: json.RawMessage(`{"unrecognized":true}`)})
	require.ErrorIs(t, err, bcode.ErrSystemSettingValueInvalid)
	require.ErrorIs(t, svc.Delete(ctx, model.SystemSettingTypeWorkflowScheduler), bcode.ErrSystemSettingTypeInvalid)
	_, err = svc.Get(ctx, model.SystemSettingTypeWorkflowScheduler)
	require.NoError(t, err, "policy row used to serialize admissions cannot be deleted")
}
