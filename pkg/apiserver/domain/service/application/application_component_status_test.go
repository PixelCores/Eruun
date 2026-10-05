package application

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
)

func TestMarkComponentsStatus(t *testing.T) {
	for _, status := range []config.ComponentStatus{config.ComponentStatusUpdating, config.ComponentStatusRestarting} {
		t.Run(string(status), func(t *testing.T) {
			t.Run("updates only selected non-cleaning components", func(t *testing.T) {
				selected := &model.ApplicationComponent{AppID: "app-1", Name: "API", Status: "Running", LastAbnormal: "old failure", ReadyReplicas: 2}
				unselected := &model.ApplicationComponent{AppID: "app-1", Name: "other", Status: "Running", LastAbnormal: "keep other failure"}
				cleaning := &model.ApplicationComponent{AppID: "app-1", Name: "cleaning", Status: "Cleaning", LastAbnormal: "keep cleaning failure"}
				padded := &model.ApplicationComponent{AppID: "app-1", Name: " padded ", Status: "Running", LastAbnormal: "keep padded failure"}
				beforeUnselected, beforeCleaning, beforePadded := *unselected, *cleaning, *padded
				components := &cleanupStore{components: []*model.ApplicationComponent{nil, selected, unselected, cleaning, padded}}
				store := &statusSyncStore{}
				svc := &applicationsServiceImpl{Store: store, ComponentRepo: &mockCleanupComponentRepo{store: components}}

				err := svc.markComponentsStatus(context.Background(), "app-1", []string{" API ", "api", "cleaning", "padded", " "}, status)

				require.NoError(t, err)
				require.Equal(t, string(status), selected.Status)
				require.Empty(t, selected.LastAbnormal)
				require.Equal(t, int32(2), selected.ReadyReplicas)
				require.Equal(t, beforeUnselected, *unselected)
				require.Equal(t, beforeCleaning, *cleaning)
				require.Equal(t, beforePadded, *padded)
				require.Equal(t, 1, store.casCalls)
				require.Equal(t, map[string]interface{}{"status": string(status), "last_abnormal": ""}, store.casUpdates)
			})

			t.Run("empty input skips repositories", func(t *testing.T) {
				for _, input := range []struct {
					name       string
					appID      string
					components []string
				}{
					{name: "empty app", components: []string{"api"}},
					{name: "no components", appID: "app-1"},
					{name: "blank components", appID: "app-1", components: []string{"", " ", "\t"}},
				} {
					t.Run(input.name, func(t *testing.T) {
						svc := &applicationsServiceImpl{}
						require.NoError(t, svc.markComponentsStatus(context.Background(), input.appID, input.components, status))
					})
				}
			})

			t.Run("query error is preserved", func(t *testing.T) {
				queryErr := errors.New("query unavailable")
				svc := &applicationsServiceImpl{ComponentRepo: &mockCleanupComponentRepo{store: &cleanupStore{}, findByAppIDErr: queryErr}}

				err := svc.markComponentsStatus(context.Background(), "app-1", []string{"api"}, status)

				require.ErrorIs(t, err, queryErr)
				require.EqualError(t, err, "list components for app app-1: query unavailable")
			})

			t.Run("write error stops later components", func(t *testing.T) {
				first := &model.ApplicationComponent{AppID: "app-1", Name: "first", Status: "Running", LastAbnormal: "first failure"}
				later := &model.ApplicationComponent{AppID: "app-1", Name: "later", Status: "Running", LastAbnormal: "later failure"}
				beforeLater := *later
				store := &cleanupStore{components: []*model.ApplicationComponent{first, later}, runtimeUpdateErr: errors.New("write unavailable")}
				svc := &applicationsServiceImpl{Store: store, ComponentRepo: &mockCleanupComponentRepo{store: store}}

				err := svc.markComponentsStatus(context.Background(), "app-1", []string{"first", "later"}, status)

				require.ErrorIs(t, err, store.runtimeUpdateErr)
				require.EqualError(t, err, fmt.Sprintf("update component first status to %s: write unavailable", status))
				require.Equal(t, beforeLater, *later)
			})
		})
	}
}
