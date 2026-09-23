package repository

import (
	"context"
	"fmt"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

// ReserveSandboxStart bounds unconfirmed creation and Pending environments
// across replicas. It reserves no rate permit. Capacity remains durable until
// the lifecycle service confirms Ready or stops/cleans the intent.
func ReserveSandboxStart(ctx context.Context, store datastore.DataStore, id string) (bool, error) {
	transactional, ok := store.(datastore.ReadCommittedTransactional)
	if !ok || id == "" {
		return false, fmt.Errorf("sandbox start budget requires identity and read-committed transactions")
	}
	admitted := false
	err := transactional.WithReadCommittedTransaction(ctx, func(tx datastore.DataStore) error {
		locked, err := tx.CompareAndSwap(ctx, &model.SystemSetting{Type: model.SystemSettingTypeWorkflowScheduler}, "type", model.SystemSettingTypeWorkflowScheduler, nil)
		if err != nil {
			return err
		}
		if !locked {
			return datastore.ErrRecordNotExist
		}
		policy, err := LoadJobSchedulerPolicy(ctx, tx)
		if err != nil {
			return err
		}
		row := &model.JobSandbox{ID: id}
		locker, ok := tx.(datastore.RowLocker)
		if !ok {
			return fmt.Errorf("sandbox start budget requires row locking")
		}
		if err := locker.GetForUpdate(ctx, row); err != nil {
			return err
		}
		if !row.SlotReserved || row.ReleaseRequested || row.State == "released" || row.State == "failed" || row.PodUID != "" {
			return nil
		}
		if row.StartReserved {
			admitted = true
			return nil
		}
		var count int64
		if aggregate, ok := tx.(interface {
			CountStartingSandboxes(context.Context) (int64, error)
		}); ok {
			// The scoped store exposes only this aggregate across workspaces.
			// The intent itself was locked and authorized above.
			count, err = aggregate.CountStartingSandboxes(ctx)
		} else {
			count, err = tx.Count(ctx, &model.JobSandbox{StartReserved: true}, nil)
		}
		if err != nil {
			return err
		}
		if count >= int64(policy.MaxStartingSandboxes) {
			return nil
		}
		admitted, err = tx.CompareAndSwap(ctx, row, "id", id, map[string]interface{}{"start_reserved": true})
		return err
	})
	if err != nil {
		return false, fmt.Errorf("reserve sandbox start: %w", err)
	}
	return admitted, nil
}
