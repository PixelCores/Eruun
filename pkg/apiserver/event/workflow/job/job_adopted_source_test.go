package job

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"github.com/PixelCores/Eruun/pkg/apiserver/adoption"
	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/importsecret"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"testing"
	"time"
)

type adoptedSourceStore struct {
	noopStore
	component                  *model.ApplicationComponent
	components                 []*model.ApplicationComponent
	app                        *model.Applications
	putErr                     error
	componentCASCount          int
	componentCASConflicts      int
	applicationCASCount        int
	applicationCASAttempts     int
	applicationCASErrOnAttempt int
	applicationCASErr          error
	workloadCASConflicts       int
	beforeWorkloadComponentCAS func(*model.ApplicationComponent)
}

func (s *adoptedSourceStore) List(_ context.Context, query datastore.Entity, _ *datastore.ListOptions) ([]datastore.Entity, error) {
	if _, ok := query.(*model.ApplicationComponent); ok {
		result := make([]datastore.Entity, 0, len(s.components)+1)
		if s.component != nil {
			result = append(result, s.component)
		}
		for _, component := range s.components {
			if component != nil {
				result = append(result, component)
			}
		}
		return result, nil
	}
	return nil, nil
}

func (s *adoptedSourceStore) Get(_ context.Context, entity datastore.Entity) error {
	app, ok := entity.(*model.Applications)
	if !ok {
		return nil
	}
	if s.app == nil {
		return datastore.ErrRecordNotExist
	}
	*app = *s.app
	return nil
}

func (s *adoptedSourceStore) Put(_ context.Context, entity datastore.Entity) error {
	if s.putErr != nil {
		return s.putErr
	}
	switch value := entity.(type) {
	case *model.ApplicationComponent:
		copy := *value
		if value.SourceWorkloadUID != nil {
			uid := *value.SourceWorkloadUID
			copy.SourceWorkloadUID = &uid
		}
		s.component = &copy
	case *model.Applications:
		copy := *value
		s.app = &copy
	}
	return nil
}

func (s *adoptedSourceStore) WithTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	return fn(s)
}

func (s *adoptedSourceStore) CompareAndSwap(
	_ context.Context,
	entity datastore.Entity,
	_ string,
	_ interface{},
	updates map[string]interface{},
) (bool, error) {
	if s.putErr != nil {
		return false, s.putErr
	}
	switch value := entity.(type) {
	case *model.ApplicationComponent:
		if s.componentCASConflicts > 0 {
			s.componentCASConflicts--
			return false, nil
		}
		var component *model.ApplicationComponent
		candidates := append([]*model.ApplicationComponent{s.component}, s.components...)
		for _, candidate := range candidates {
			if candidate != nil &&
				value != nil &&
				candidate.AppID == value.AppID &&
				candidate.Name == value.Name {
				component = candidate
				break
			}
		}
		if component == nil {
			return false, nil
		}
		secretJSON, ok := updates["adopted_secret_data"].(string)
		if !ok || secretJSON == "" {
			return false, nil
		}
		secretData, err := model.NewJSONStructByString(secretJSON)
		if err != nil {
			return false, err
		}
		component.AdoptedSecretData = secretData
		s.componentCASCount++
		return true, nil
	case *model.Applications:
	default:
		return false, nil
	}
	s.applicationCASAttempts++
	if s.applicationCASErrOnAttempt > 0 &&
		s.applicationCASAttempts == s.applicationCASErrOnAttempt {
		return false, s.applicationCASErr
	}
	if s.app == nil {
		return false, nil
	}
	snapshotJSON, ok := updates["adoption_snapshot"].(string)
	if !ok || snapshotJSON == "" {
		return false, nil
	}
	snapshot, err := model.NewJSONStructByString(snapshotJSON)
	if err != nil {
		return false, err
	}
	s.app.AdoptionSnapshot = snapshot
	s.applicationCASCount++
	return true, nil
}

func (s *adoptedSourceStore) CompareAndSwapWithConditions(
	_ context.Context,
	entity datastore.Entity,
	conditions map[string]interface{},
	updates map[string]interface{},
) (bool, error) {
	if s.putErr != nil {
		return false, s.putErr
	}
	value, ok := entity.(*model.ApplicationComponent)
	if !ok || value == nil {
		return false, nil
	}
	var component *model.ApplicationComponent
	candidates := append([]*model.ApplicationComponent{s.component}, s.components...)
	for _, candidate := range candidates {
		if candidate == nil || candidate.AppID != value.AppID {
			continue
		}
		if id, ok := conditions["id"].(int); ok && candidate.ID != id {
			continue
		}
		if name, ok := conditions["name"].(string); ok && candidate.Name != name {
			continue
		}
		component = candidate
		break
	}
	if component == nil {
		return false, nil
	}
	if s.workloadCASConflicts > 0 {
		s.workloadCASConflicts--
		return false, nil
	}
	if s.beforeWorkloadComponentCAS != nil {
		before := s.beforeWorkloadComponentCAS
		s.beforeWorkloadComponentCAS = nil
		before(component)
	}
	if expected, ok := conditions["update_time"].(time.Time); ok && !component.UpdateTime.Equal(expected) {
		return false, nil
	}
	if expected, ok := conditions["source_workload_uid"].(string); ok {
		if component.SourceWorkloadUID == nil || *component.SourceWorkloadUID != expected {
			return false, nil
		}
	}
	newUID, ok := updates["source_workload_uid"].(string)
	if !ok || newUID == "" {
		return false, nil
	}
	component.SourceWorkloadUID = &newUID
	component.UpdateTime = time.Now()
	value.UpdateTime = component.UpdateTime
	return true, nil
}

func sourceComponent(appID, name, kind, sourceName string, uid types.UID) *model.ApplicationComponent {
	uidString := string(uid)
	return &model.ApplicationComponent{
		ID:                       7,
		AppID:                    appID,
		Name:                     name,
		Namespace:                "ops",
		SourceWorkloadAPIVersion: "apps/v1",
		SourceWorkloadKind:       kind,
		SourceWorkloadName:       sourceName,
		SourceWorkloadUID:        &uidString,
	}
}

func adoptedSnapshotResource(
	t *testing.T,
	object runtime.Object,
	componentName, role, ownership, disposition string,
) adoption.ResourceSnapshot {
	t.Helper()
	raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(object)
	require.NoError(t, err)
	snapshot, err := adoption.ResourceSnapshotFromObject(
		&unstructured.Unstructured{Object: raw},
		componentName,
		role,
		ownership,
		disposition,
	)
	require.NoError(t, err)
	return snapshot
}

func adoptedApplication(
	t *testing.T,
	appID, namespace string,
	resources ...adoption.ResourceSnapshot,
) *model.Applications {
	t.Helper()
	snapshot := adoption.NewSnapshot(namespace, resources)
	raw, err := model.NewJSONStructByStruct(snapshot)
	require.NoError(t, err)
	return &model.Applications{
		ID:               appID,
		Namespace:        namespace,
		ManagementMode:   config.ManagementModeAdopted,
		AdoptionSnapshot: raw,
	}
}

func decodeTestAdoptionSnapshot(t *testing.T, app *model.Applications) adoption.Snapshot {
	t.Helper()
	require.NotNil(t, app)
	require.NotNil(t, app.AdoptionSnapshot)
	payload, err := json.Marshal(app.AdoptionSnapshot)
	require.NoError(t, err)
	var snapshot adoption.Snapshot
	require.NoError(t, json.Unmarshal(payload, &snapshot))
	return snapshot
}

func testImportSecretKeyring(
	t *testing.T,
	activeKeyID string,
	keys map[string][]byte,
) *importsecret.Keyring {
	t.Helper()
	encoded := make(map[string]string, len(keys))
	for keyID, key := range keys {
		encoded[keyID] = base64.StdEncoding.EncodeToString(key)
	}
	payload, err := json.Marshal(map[string]interface{}{
		"activeKeyId": activeKeyID,
		"keys":        encoded,
	})
	require.NoError(t, err)
	keyring, err := importsecret.Parse(payload)
	require.NoError(t, err)
	return keyring
}

func encryptedAdoptedSecretData(
	t *testing.T,
	keyring *importsecret.Keyring,
	appID string,
	secret *corev1.Secret,
) *model.JSONStruct {
	t.Helper()
	envelopes := make(map[string]importsecret.Envelope, len(secret.Data))
	for key, value := range secret.Data {
		envelope, err := keyring.Encrypt(
			value,
			importsecret.ResourceAAD(appID, secret.Namespace, secret.APIVersion, secret.Kind, secret.Name, key),
		)
		require.NoError(t, err)
		envelopes[key] = envelope
	}
	encrypted, err := model.NewJSONStructByStruct(map[string]map[string]importsecret.Envelope{
		secret.Name: envelopes,
	})
	require.NoError(t, err)
	return encrypted
}

func adoptedSecretComponent(
	t *testing.T,
	keyring *importsecret.Keyring,
	appID, componentName string,
	secret *corev1.Secret,
) *model.ApplicationComponent {
	t.Helper()
	component := sourceComponent(appID, componentName, "Deployment", "legacy-workload", types.UID("workload-uid"))
	component.AdoptedSecretData = encryptedAdoptedSecretData(t, keyring, appID, secret)
	return component
}

func newTestAdoptedSecretController(
	jobTask *model.JobTask,
	client *fake.Clientset,
	store datastore.DataStore,
	keyring *importsecret.Keyring,
) *DeploySecretJobCtl {
	ctl := NewDeploySecretJobCtl(
		jobTask,
		client,
		store,
		func() {},
		locker.NewNoopLocker(shareLockerPrefix),
		nil,
	)
	ctl.setRuntime(newJobRuntime(nil, nil, nil, nil, nil, keyring))
	return ctl
}
