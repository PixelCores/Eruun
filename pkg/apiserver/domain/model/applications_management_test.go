package model

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
)

func TestApplicationsEffectiveManagementMode(t *testing.T) {
	tests := []struct {
		name string
		app  *Applications
		want config.ManagementMode
	}{
		{name: "new native", app: NewApplications("id", "name", "default", "1.0.0", "", "", "", "", false), want: config.ManagementModeNative},
		{name: "legacy native", app: &Applications{}, want: config.ManagementModeNative},
		{name: "legacy imported", app: &Applications{Project: " imported ", Version: "IMPORTED"}, want: config.ManagementModeObserve},
		{name: "explicit native imported values", app: &Applications{Project: "imported", Version: "imported", ManagementMode: config.ManagementModeNative}, want: config.ManagementModeNative},
		{name: "observe", app: &Applications{ManagementMode: config.ManagementModeObserve}, want: config.ManagementModeObserve},
		{name: "adopted", app: &Applications{ManagementMode: config.ManagementModeAdopted}, want: config.ManagementModeAdopted},
		{name: "unknown fails closed", app: &Applications{ManagementMode: config.ManagementMode("future")}, want: config.ManagementModeObserve},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.app.EffectiveManagementMode())
		})
	}
}

func TestApplicationsIndexOnlyUsesExplicitManagementMode(t *testing.T) {
	require.NotContains(t, (&Applications{}).Index(), "management_mode")
	require.Equal(t, config.ManagementModeObserve, (&Applications{ManagementMode: config.ManagementModeObserve}).Index()["management_mode"])
}

func TestApplicationComponentSourceWorkloadRequiresCompleteIdentity(t *testing.T) {
	uid := "workload-uid"
	component := &ApplicationComponent{
		SourceWorkloadAPIVersion: "apps/v1",
		SourceWorkloadKind:       "Deployment",
		SourceWorkloadName:       "api",
		SourceWorkloadUID:        &uid,
	}
	require.True(t, component.HasSourceWorkload())

	component.SourceWorkloadName = ""
	require.False(t, component.HasSourceWorkload())
	require.False(t, (*ApplicationComponent)(nil).HasSourceWorkload())
}

func TestAdoptionPersistenceFieldsStayOutOfModelJSON(t *testing.T) {
	uid := "workload-uid"
	snapshot := JSONStruct{"secret": "snapshot-must-not-leak"}
	secretData := JSONStruct{"password": "ciphertext-must-not-leak"}
	selector := JSONStruct{"app": "api"}
	component := ApplicationComponent{
		SourceWorkloadAPIVersion: "apps/v1",
		SourceWorkloadKind:       "Deployment",
		SourceWorkloadName:       "api",
		SourceWorkloadUID:        &uid,
		SourcePodSelector:        &selector,
		AdoptedSecretData:        &secretData,
	}
	application := Applications{AdoptionSnapshot: &snapshot}

	componentJSON, err := json.Marshal(component)
	require.NoError(t, err)
	applicationJSON, err := json.Marshal(application)
	require.NoError(t, err)
	joined := string(componentJSON) + string(applicationJSON)
	for _, forbidden := range []string{"workload-uid", "snapshot-must-not-leak", "ciphertext-must-not-leak", "sourceWorkload"} {
		require.NotContains(t, joined, forbidden)
	}

	field, ok := reflect.TypeOf(ApplicationComponent{}).FieldByName("SourceWorkloadUID")
	require.True(t, ok)
	require.Equal(t, reflect.Pointer, field.Type.Kind())
	require.Equal(t, "-", field.Tag.Get("json"))
	require.True(t, strings.Contains(field.Tag.Get("gorm"), "uniqueIndex:uidx_component_source_workload_uid"))
}
