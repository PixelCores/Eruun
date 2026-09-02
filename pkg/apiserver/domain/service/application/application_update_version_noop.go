package application

import (
	"context"
	"fmt"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func (c *applicationsServiceImpl) commitNoopVersionUpdateWithCallbackTask(
	ctx context.Context,
	app *model.Applications,
	newVersion, description, workflowID string,
	startTime, endTime int64,
	jobs []operationJobRecord,
	callback *model.JSONStruct,
) (*model.WorkflowQueue, error) {
	txStore, ok := c.Store.(datastore.Transactional)
	if !ok {
		return nil, fmt.Errorf("%w: no-op version update callback requires transactional datastore", bcode.ErrVersionUpdateFailed)
	}
	updatedApp := *app
	updatedApp.Version = newVersion
	if description != "" {
		updatedApp.Description = description
	}
	var task *model.WorkflowQueue
	err := txStore.WithTransaction(ctx, func(tx datastore.DataStore) error {
		if err := tx.Put(ctx, &updatedApp); err != nil {
			return bcode.ErrVersionUpdateFailed
		}
		var err error
		task, err = recordAppOperationTaskInStore(ctx, tx, &updatedApp, config.WorkflowTaskTypeUpdate, operationTaskNameUpdateVersion, workflowID, config.StatusCompleted, startTime, endTime, jobs, callback)
		return err
	})
	if err != nil {
		return nil, err
	}
	*app = updatedApp
	return task, nil
}
