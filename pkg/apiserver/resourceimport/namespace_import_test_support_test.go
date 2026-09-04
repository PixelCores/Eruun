package resourceimport

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	applicationservice "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/application"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
)

func mustBuildPatch(t *testing.T, kind string, labels, selectorMatchLabels map[string]string) []byte {
	t.Helper()
	patch, err := buildLabelPatch(kind, labels, selectorMatchLabels)
	require.NoError(t, err)
	return patch
}

func decodePatchPayload(t *testing.T, patch []byte) map[string]interface{} {
	t.Helper()
	var payload map[string]interface{}
	require.NoError(t, json.Unmarshal(patch, &payload))
	return payload
}

func getStringMap(t *testing.T, payload map[string]interface{}, path ...string) map[string]string {
	t.Helper()
	var current interface{} = payload
	for _, key := range path {
		obj, ok := current.(map[string]interface{})
		require.Truef(t, ok, "path %v missing object at %q", path, key)
		next, exists := obj[key]
		require.Truef(t, exists, "path %v missing key %q", path, key)
		current = next
	}
	raw, ok := current.(map[string]interface{})
	require.Truef(t, ok, "path %v does not resolve to map", path)
	result := make(map[string]string, len(raw))
	for k, v := range raw {
		value, ok := v.(string)
		require.Truef(t, ok, "path %v has non-string value for key %q", path, k)
		result[k] = value
	}
	return result
}

func mustFindPlan(t *testing.T, plans []importAppPlan, appID string) *importAppPlan {
	t.Helper()
	for i := range plans {
		if plans[i].appID == appID {
			return &plans[i]
		}
	}
	require.FailNowf(t, "missing plan", "plan appID %q not found", appID)
	return nil
}

func mustFindPlanComponent(t *testing.T, plan *importAppPlan, componentName string) *apisv1.CreateComponentRequest {
	t.Helper()
	if plan == nil {
		require.FailNow(t, "missing plan", "plan is nil")
	}
	for i := range plan.components {
		if strings.EqualFold(strings.TrimSpace(plan.components[i].Name), strings.TrimSpace(componentName)) {
			return &plan.components[i]
		}
	}
	require.FailNowf(t, "missing component", "component %q not found in app %q", componentName, plan.appID)
	return nil
}

func mustFindResource(t *testing.T, resources []*importResource, name string) *importResource {
	t.Helper()
	for _, res := range resources {
		if res != nil && res.name == name {
			return res
		}
	}
	require.FailNowf(t, "missing resource", "resource %q not found", name)
	return nil
}

func newDeploymentResource(t *testing.T, name, namespace string, podLabels map[string]string, serviceAccount string, configMaps []string, secrets []string) *importResource {
	t.Helper()
	envFrom := make([]corev1.EnvFromSource, 0, len(configMaps)+len(secrets))
	for _, cm := range configMaps {
		envFrom = append(envFrom, corev1.EnvFromSource{
			ConfigMapRef: &corev1.ConfigMapEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: cm},
			},
		})
	}
	for _, secret := range secrets {
		envFrom = append(envFrom, corev1.EnvFromSource{
			SecretRef: &corev1.SecretEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: secret},
			},
		})
	}

	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: podLabels},
				Spec: corev1.PodSpec{
					ServiceAccountName: serviceAccount,
					Containers: []corev1.Container{
						{
							Name:    "app",
							Image:   "nginx:1.27",
							EnvFrom: envFrom,
						},
					},
				},
			},
		},
	}
	obj, err := toUnstructured(deploy, "Deployment", "apps/v1")
	require.NoError(t, err)
	return &importResource{
		kindKey:   importKindDeployments,
		kind:      "Deployment",
		namespace: namespace,
		name:      name,
		labels:    utils.CopyStringMap(deploy.Labels),
		object:    obj,
	}
}

func newDeploymentResourceWithPVC(t *testing.T, name, namespace string, podLabels map[string]string, volumeName, claimName string) *importResource {
	t.Helper()
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: podLabels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "app",
						Image: "nginx:1.27",
						VolumeMounts: []corev1.VolumeMount{{
							Name:      volumeName,
							MountPath: "/data",
						}},
					}},
					Volumes: []corev1.Volume{{
						Name: volumeName,
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claimName},
						},
					}},
				},
			},
		},
	}
	obj, err := toUnstructured(deploy, "Deployment", "apps/v1")
	require.NoError(t, err)
	return &importResource{
		kindKey:   importKindDeployments,
		kind:      "Deployment",
		namespace: namespace,
		name:      name,
		labels:    utils.CopyStringMap(deploy.Labels),
		object:    obj,
	}
}

func newStatefulSetResource(
	t *testing.T,
	name, namespace string,
	podLabels map[string]string,
	serviceAccount string,
	claimTemplates []string,
) *importResource {
	t.Helper()

	volumeClaims := make([]corev1.PersistentVolumeClaim, 0, len(claimTemplates))
	for _, claim := range claimTemplates {
		if strings.TrimSpace(claim) == "" {
			continue
		}
		volumeClaims = append(volumeClaims, corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: claim},
		})
	}

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: name + "-svc",
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: podLabels},
				Spec: corev1.PodSpec{
					ServiceAccountName: serviceAccount,
					Containers: []corev1.Container{
						{
							Name:  "app",
							Image: "nginx:1.27",
						},
					},
				},
			},
			VolumeClaimTemplates: volumeClaims,
		},
	}
	obj, err := toUnstructured(sts, "StatefulSet", "apps/v1")
	require.NoError(t, err)
	return &importResource{
		kindKey:   importKindStatefulSets,
		kind:      "StatefulSet",
		namespace: namespace,
		name:      name,
		labels:    utils.CopyStringMap(sts.Labels),
		object:    obj,
	}
}

func newConfigMapResource(t *testing.T, name, namespace string) *importResource {
	t.Helper()
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}}
	obj, err := toUnstructured(cm, "ConfigMap", "v1")
	require.NoError(t, err)
	return &importResource{
		kindKey:   importKindConfigMaps,
		kind:      "ConfigMap",
		namespace: namespace,
		name:      name,
		labels:    utils.CopyStringMap(cm.Labels),
		object:    obj,
	}
}

func newSecretResource(t *testing.T, name, namespace string) *importResource {
	t.Helper()
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}}
	obj, err := toUnstructured(secret, "Secret", "v1")
	require.NoError(t, err)
	return &importResource{
		kindKey:   importKindSecrets,
		kind:      "Secret",
		namespace: namespace,
		name:      name,
		labels:    utils.CopyStringMap(secret.Labels),
		object:    obj,
	}
}

func newServiceAccountResource(t *testing.T, name, namespace string) *importResource {
	t.Helper()
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}}
	obj, err := toUnstructured(sa, "ServiceAccount", "v1")
	require.NoError(t, err)
	return &importResource{
		kindKey:   importKindServiceAccounts,
		kind:      "ServiceAccount",
		namespace: namespace,
		name:      name,
		labels:    utils.CopyStringMap(sa.Labels),
		object:    obj,
	}
}

func newRoleResource(t *testing.T, name, namespace string) *importResource {
	t.Helper()
	role := &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}}
	obj, err := toUnstructured(role, "Role", "rbac.authorization.k8s.io/v1")
	require.NoError(t, err)
	return &importResource{
		kindKey:   importKindRoles,
		kind:      "Role",
		namespace: namespace,
		name:      name,
		labels:    utils.CopyStringMap(role.Labels),
		object:    obj,
	}
}

func newRoleResourceWithRules(t *testing.T, name, namespace string, rules []rbacv1.PolicyRule) *importResource {
	t.Helper()
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Rules:      rules,
	}
	obj, err := toUnstructured(role, "Role", "rbac.authorization.k8s.io/v1")
	require.NoError(t, err)
	return &importResource{
		kindKey:   importKindRoles,
		kind:      "Role",
		namespace: namespace,
		name:      name,
		labels:    utils.CopyStringMap(role.Labels),
		object:    obj,
	}
}

func newClusterRoleResource(t *testing.T, name string) *importResource {
	t.Helper()
	role := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: name}}
	obj, err := toUnstructured(role, "ClusterRole", "rbac.authorization.k8s.io/v1")
	require.NoError(t, err)
	return &importResource{
		kindKey: importKindClusterRoles,
		kind:    "ClusterRole",
		name:    name,
		labels:  utils.CopyStringMap(role.Labels),
		object:  obj,
	}
}

func newRoleBindingResource(t *testing.T, name, namespace string, roleRef rbacv1.RoleRef, subjects []rbacv1.Subject) *importResource {
	t.Helper()
	rb := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		RoleRef:    roleRef,
		Subjects:   subjects,
	}
	obj, err := toUnstructured(rb, "RoleBinding", "rbac.authorization.k8s.io/v1")
	require.NoError(t, err)
	return &importResource{
		kindKey:   importKindRoleBindings,
		kind:      "RoleBinding",
		namespace: namespace,
		name:      name,
		labels:    utils.CopyStringMap(rb.Labels),
		object:    obj,
	}
}

func newClusterRoleBindingResource(t *testing.T, name string, roleRef rbacv1.RoleRef, subjects []rbacv1.Subject) *importResource {
	t.Helper()
	rb := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		RoleRef:    roleRef,
		Subjects:   subjects,
	}
	obj, err := toUnstructured(rb, "ClusterRoleBinding", "rbac.authorization.k8s.io/v1")
	require.NoError(t, err)
	return &importResource{
		kindKey: importKindClusterRoleBindings,
		kind:    "ClusterRoleBinding",
		name:    name,
		labels:  utils.CopyStringMap(rb.Labels),
		object:  obj,
	}
}

func newServiceResource(t *testing.T, name, namespace string, selector map[string]string) *importResource {
	t.Helper()
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec:       corev1.ServiceSpec{Selector: selector, Ports: []corev1.ServicePort{{Port: 80}}},
	}
	obj, err := toUnstructured(svc, "Service", "v1")
	require.NoError(t, err)
	return &importResource{
		kindKey:   importKindServices,
		kind:      "Service",
		namespace: namespace,
		name:      name,
		labels:    utils.CopyStringMap(svc.Labels),
		object:    obj,
	}
}

func newPVCResource(t *testing.T, name, namespace string) *importResource {
	t.Helper()
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}
	obj, err := toUnstructured(pvc, "PersistentVolumeClaim", "v1")
	require.NoError(t, err)
	return &importResource{
		kindKey:   importKindPersistentVolumeClaims,
		kind:      "PersistentVolumeClaim",
		namespace: namespace,
		name:      name,
		labels:    utils.CopyStringMap(pvc.Labels),
		object:    obj,
	}
}

type namespaceImportWorkflowRepoStub struct {
	workflowsByAppID map[string][]*model.Workflow
	updateErr        error
	findByAppIDs     []string
}

func (s *namespaceImportWorkflowRepoStub) FindByID(context.Context, string) (*model.Workflow, error) {
	return nil, nil
}

func (s *namespaceImportWorkflowRepoStub) Create(context.Context, *model.Workflow) error {
	return nil
}

func (s *namespaceImportWorkflowRepoStub) Update(_ context.Context, workflow *model.Workflow) error {
	if s.updateErr != nil {
		return s.updateErr
	}
	return nil
}

func (s *namespaceImportWorkflowRepoStub) Delete(context.Context, *model.Workflow) error {
	return nil
}

func (s *namespaceImportWorkflowRepoStub) DeleteByAppID(context.Context, string) error {
	return nil
}

func (s *namespaceImportWorkflowRepoStub) FindByAppID(_ context.Context, appID string) ([]*model.Workflow, error) {
	s.findByAppIDs = append(s.findByAppIDs, appID)
	workflows := s.workflowsByAppID[appID]
	result := make([]*model.Workflow, 0, len(workflows))
	for _, wf := range workflows {
		if wf == nil {
			continue
		}
		cp := *wf
		result = append(result, &cp)
	}
	return result, nil
}

type namespaceImportAppServiceStub struct {
	createReqs      []apisv1.CreateApplicationsRequest
	createErr       error
	listApps        []*apisv1.ApplicationBase
	generatedID     string
	persistStore    *inMemoryAppStore
	componentIDSeed int
	beforeMutation  func()
}

func (s *namespaceImportAppServiceStub) CreateApplications(
	ctx context.Context,
	req apisv1.CreateApplicationsRequest,
) (*apisv1.ApplicationBase, error) {
	return s.createApplications(ctx, req, nil)
}

func (s *namespaceImportAppServiceStub) CreateApplicationsWithMutation(
	ctx context.Context,
	req apisv1.CreateApplicationsRequest,
	mutation applicationservice.ApplicationCreateMutation,
) (*apisv1.ApplicationBase, error) {
	return s.createApplications(ctx, req, mutation)
}

func (s *namespaceImportAppServiceStub) createApplications(
	ctx context.Context,
	req apisv1.CreateApplicationsRequest,
	mutation applicationservice.ApplicationCreateMutation,
) (*apisv1.ApplicationBase, error) {
	s.createReqs = append(s.createReqs, req)
	if s.createErr != nil {
		return nil, s.createErr
	}
	id := req.ID
	if strings.TrimSpace(id) == "" {
		id = strings.TrimSpace(s.generatedID)
		if id == "" {
			id = "generated-app-id"
		}
	}
	app := &model.Applications{
		ID:        id,
		Name:      req.Name,
		Namespace: req.Namespace,
		Alias:     req.Alias,
	}
	if s.persistStore != nil {
		if existing := s.persistStore.apps[id]; existing != nil {
			appCopy := *existing
			app = &appCopy
			app.Name = req.Name
			app.Namespace = req.Namespace
			app.Alias = req.Alias
		}
	}
	components := make([]*model.ApplicationComponent, 0, len(req.Component))
	nextComponentID := s.componentIDSeed
	for _, component := range req.Component {
		properties, err := model.NewJSONStructByStruct(component.Properties)
		if err != nil {
			return nil, err
		}
		traits, err := model.NewJSONStructByStruct(component.Traits)
		if err != nil {
			return nil, err
		}
		nextComponentID++
		components = append(components, &model.ApplicationComponent{
			ID:            nextComponentID,
			AppID:         id,
			Name:          component.Name,
			Namespace:     component.Namespace,
			Image:         component.Image,
			Replicas:      component.Replicas,
			ComponentType: component.ComponentType,
			Properties:    properties,
			Traits:        traits,
		})
	}
	if mutation != nil {
		if s.beforeMutation != nil {
			s.beforeMutation()
		}
		if err := mutation(ctx, s.persistStore, app, components); err != nil {
			return nil, err
		}
	}
	if s.persistStore != nil {
		s.persistStore.apps[id] = app
		for name, component := range s.persistStore.components {
			if component != nil && component.AppID == id {
				delete(s.persistStore.components, name)
			}
		}
		for _, component := range components {
			s.persistStore.components[component.Name] = component
		}
		s.componentIDSeed = nextComponentID
	}
	foundListApp := false
	for index := range s.listApps {
		if s.listApps[index] != nil && s.listApps[index].ID == id {
			s.listApps[index] = &apisv1.ApplicationBase{
				ID:             id,
				Name:           req.Name,
				Namespace:      req.Namespace,
				Alias:          req.Alias,
				ManagementMode: app.ManagementMode,
			}
			foundListApp = true
			break
		}
	}
	if !foundListApp {
		s.listApps = append(s.listApps, &apisv1.ApplicationBase{
			ID:             id,
			Name:           req.Name,
			Namespace:      req.Namespace,
			Alias:          req.Alias,
			ManagementMode: app.ManagementMode,
		})
	}
	return &apisv1.ApplicationBase{ID: id, Name: req.Name, Namespace: req.Namespace}, nil
}

var _ applicationCreator = (*namespaceImportAppServiceStub)(nil)
