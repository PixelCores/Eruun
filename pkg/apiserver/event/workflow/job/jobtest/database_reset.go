package jobtest

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	spec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

func CloneDatabaseResetJobInfo(jobInfo *model.JobInfo) *model.JobInfo {
	if jobInfo == nil {
		return nil
	}
	copy := *jobInfo
	return &copy
}

func DatabaseResetMatchesInFilter(opts *datastore.ListOptions, key, value string) bool {
	if opts == nil {
		return true
	}
	for _, filter := range opts.FilterOptions.In {
		if filter.Key != key {
			continue
		}
		for _, candidate := range filter.Values {
			if candidate == value {
				return true
			}
		}
		return false
	}
	return true
}

func DatabaseResetStoreComponent(t *testing.T, name string, storage []spec.StorageTraitSpec) *model.ApplicationComponent {
	t.Helper()
	return &model.ApplicationComponent{
		ID:              len(name),
		AppID:           "app-1",
		Name:            name,
		Namespace:       "default",
		Image:           "mysql:8.0",
		Replicas:        1,
		ComponentType:   config.StoreJob,
		ResourceAppName: "demo",
		Properties:      MustDatabaseResetJSON(t, &model.Properties{}),
		Traits: MustDatabaseResetJSON(t, &spec.Traits{
			Storage: storage,
		}),
	}
}

func DatabaseResetServerComponent(t *testing.T, name string) *model.ApplicationComponent {
	t.Helper()
	return &model.ApplicationComponent{
		ID:              len(name) + 100,
		AppID:           "app-1",
		Name:            name,
		Namespace:       "default",
		Image:           "nginx:latest",
		Replicas:        1,
		ComponentType:   config.ServerJob,
		ResourceAppName: "demo",
		Properties: MustDatabaseResetJSON(t, &model.Properties{
			Ports: []model.Ports{{Port: 8080}},
		}),
		Traits: MustDatabaseResetJSON(t, &spec.Traits{}),
	}
}

func MustDatabaseResetJSON(t *testing.T, value interface{}) *model.JSONStruct {
	t.Helper()
	result, err := model.NewJSONStructByStruct(value)
	require.NoError(t, err)
	return result
}
