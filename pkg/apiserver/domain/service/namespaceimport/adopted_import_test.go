package namespaceimport

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/adoption"
	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestCollectPodSpecReferencesIncludesEphemeralAndVolumeSecrets(t *testing.T) {
	configMaps := map[string]struct{}{}
	pvcs := map[string]struct{}{}
	secrets := map[string]struct{}{}
	spec := &corev1.PodSpec{
		Volumes: []corev1.Volume{
			{
				Name: "azure",
				VolumeSource: corev1.VolumeSource{
					AzureFile: &corev1.AzureFileVolumeSource{SecretName: "azure-secret"},
				},
			},
			{
				Name: "rbd",
				VolumeSource: corev1.VolumeSource{
					RBD: &corev1.RBDVolumeSource{
						SecretRef: &corev1.LocalObjectReference{Name: "rbd-secret"},
					},
				},
			},
			{
				Name: "pvc",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"},
				},
			},
		},
		EphemeralContainers: []corev1.EphemeralContainer{{
			EphemeralContainerCommon: corev1.EphemeralContainerCommon{
				EnvFrom: []corev1.EnvFromSource{{
					ConfigMapRef: &corev1.ConfigMapEnvSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: "debug-config"},
					},
				}},
				Env: []corev1.EnvVar{{
					Name: "TOKEN",
					ValueFrom: &corev1.EnvVarSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "debug-secret"},
							Key:                  "token",
						},
					},
				}},
			},
		}},
	}

	collectPodSpecReferences(spec, configMaps, pvcs, secrets)

	require.Equal(t, map[string]struct{}{"debug-config": {}}, configMaps)
	require.Equal(t, map[string]struct{}{"data": {}}, pvcs)
	require.Equal(t, map[string]struct{}{
		"azure-secret": {},
		"rbd-secret":   {},
		"debug-secret": {},
	}, secrets)
}

func TestEqualAdoptionReplaySnapshotsTreatsLegacyVersionAsEquivalent(t *testing.T) {
	resource := adoption.ResourceSnapshot{
		Source: adoption.ResourceIdentity{
			APIVersion: "v1",
			Kind:       "ConfigMap",
			Namespace:  "prod",
			Name:       "settings",
			UID:        "uid-1",
			SpecDigest: "digest",
		},
		DependencyRole: "config",
		Ownership:      adoption.OwnershipExclusive,
		Disposition:    adoption.DispositionManaged,
	}
	legacy := adoption.Snapshot{Version: 1, Namespace: "prod", Resources: []adoption.ResourceSnapshot{resource}}
	current := legacy
	current.Version = adoption.SnapshotVersion

	require.True(t, equalAdoptionReplaySnapshots(legacy, current))

	current.Resources = append([]adoption.ResourceSnapshot(nil), current.Resources...)
	current.Resources[0].PendingRecreation = &adoption.RecreationClaim{Token: "claim-1"}
	require.False(t, equalAdoptionReplaySnapshots(legacy, current))
}

func TestNormalizeImportManagementMode_DefaultsLegacyToObserveAndRejectsMixedAdoption(t *testing.T) {
	mode, err := normalizeImportManagementMode(apisv1.ImportNamespaceApplicationsRequest{})
	require.NoError(t, err)
	assert.Equal(t, config.ManagementModeObserve, mode)

	_, err = normalizeImportManagementMode(apisv1.ImportNamespaceApplicationsRequest{
		ManagementMode: config.ManagementModeAdopted,
		Applications: []apisv1.ImportNamespaceApplicationMapping{{
			Name:       "app",
			Components: []apisv1.ImportNamespaceComponentMapping{{Name: "api"}},
		}},
		IncludeKinds: []string{"deployments"},
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "cannot be combined with includeKinds")

	_, err = normalizeImportManagementMode(apisv1.ImportNamespaceApplicationsRequest{
		ManagementMode: config.ManagementModeAdopted,
		Applications: []apisv1.ImportNamespaceApplicationMapping{
			{Name: "app-a", Components: []apisv1.ImportNamespaceComponentMapping{{Name: "api"}}},
			{Name: "app-b", Components: []apisv1.ImportNamespaceComponentMapping{{Name: "api"}}},
		},
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "requires exactly one application mapping")
}

func TestBuildAdoptedCanonicalTargetStateIgnoresRuntimeOnlyComponentUpdates(t *testing.T) {
	uid := "deployment-uid"
	properties := model.JSONStruct{"labels": map[string]interface{}{"release": "v1"}}
	traits := model.JSONStruct{"targetWorkEnv": map[string]interface{}{"pool": "default"}}
	app := &model.Applications{
		ID:             "app-1",
		Name:           "api",
		Namespace:      "prod",
		ManagementMode: config.ManagementModeObserve,
		BaseModel:      model.BaseModel{UpdateTime: time.Unix(100, 0)},
	}
	component := &model.ApplicationComponent{
		ID:                       42,
		AppID:                    app.ID,
		Name:                     "backend",
		Namespace:                app.Namespace,
		Image:                    "api:v1",
		Replicas:                 2,
		ComponentType:            config.ServerJob,
		Properties:               &properties,
		Traits:                   &traits,
		SourceWorkloadAPIVersion: appsv1.SchemeGroupVersion.String(),
		SourceWorkloadKind:       "Deployment",
		SourceWorkloadName:       "legacy-backend",
		SourceWorkloadUID:        &uid,
		Status:                   string(config.ComponentStatusPending),
		ReadyReplicas:            1,
		BaseModel:                model.BaseModel{UpdateTime: time.Unix(100, 0)},
	}

	expected, err := buildAdoptedCanonicalTargetState(app, []*model.ApplicationComponent{component}, nil)
	require.NoError(t, err)

	component.Status = string(config.ComponentStatusRunning)
	component.ReadyReplicas = 2
	component.LastAbnormal = "recovered"
	component.UpdateTime = time.Unix(200, 0)
	actual, err := buildAdoptedCanonicalTargetState(app, []*model.ApplicationComponent{component}, nil)
	require.NoError(t, err)
	require.Equal(t, expected, actual)

	component.Image = "api:v2"
	configChanged, err := buildAdoptedCanonicalTargetState(app, []*model.ApplicationComponent{component}, nil)
	require.NoError(t, err)
	require.NotEqual(t, expected.ComponentStateDigest, configChanged.ComponentStateDigest)

	component.Image = "api:v1"
	replacementUID := "replacement-uid"
	component.SourceWorkloadUID = &replacementUID
	bindingChanged, err := buildAdoptedCanonicalTargetState(app, []*model.ApplicationComponent{component}, nil)
	require.NoError(t, err)
	require.NotEqual(t, expected.ComponentStateDigest, bindingChanged.ComponentStateDigest)
}

func TestAssignAdoptedResourceSemantics_SameAppMultiRootDependencyRemainsManaged(t *testing.T) {
	resource := &importResource{
		kindKey:   importKindConfigMaps,
		kind:      "ConfigMap",
		namespace: "prod",
		name:      "shared-by-two-components",
	}
	member := &adoptedMembership{
		resource: resource,
		appComponents: map[string]map[string]struct{}{
			"app-plan": {
				"backend":  {},
				"frontend": {},
			},
		},
	}

	assignAdoptedResourceSemantics(
		map[string]*adoptedMembership{resourceResultKey(resource): member},
		nil,
	)

	assert.Equal(t, adoption.OwnershipExclusive, resource.ownership)
	assert.Equal(t, adoption.DispositionManaged, resource.disposition)
	assert.Empty(t, resource.componentName, "an app-wide dependency need not be assigned to an arbitrary root component")
	assert.False(t, adoptedMembershipRequiresSharedPreservation(member))
}

func TestAssignAdoptedResourceSemantics_ClusterRBACIsAlwaysExternalPreserved(t *testing.T) {
	for _, kindKey := range []string{importKindClusterRoles, importKindClusterRoleBindings} {
		t.Run(kindKey, func(t *testing.T) {
			resource := &importResource{
				kindKey: kindKey,
				kind:    kindKey,
				name:    "global-reader",
			}
			member := &adoptedMembership{
				resource: resource,
				appComponents: map[string]map[string]struct{}{
					"app-plan": {"backend": {}},
				},
			}

			assignAdoptedResourceSemantics(
				map[string]*adoptedMembership{resourceResultKey(resource): member},
				nil,
			)

			assert.Equal(t, adoption.OwnershipExternal, resource.ownership)
			assert.Equal(t, adoption.DispositionSharedPreserved, resource.disposition)
		})
	}
}

func TestAdoptedOwnerReferenceConflictsBlocksManagedDependency(t *testing.T) {
	root := newDeploymentResource(t, "backend", "prod", map[string]string{"app": "backend"}, "", nil, nil)
	root.object.SetUID(types.UID("deployment-uid"))
	configMap := newConfigMapResource(t, "backend-config", "prod")
	configMap.object.SetOwnerReferences([]metav1.OwnerReference{{
		APIVersion: "apps/v1",
		Kind:       "Deployment",
		Name:       root.name,
		UID:        root.object.GetUID(),
	}})
	membership := map[string]*adoptedMembership{
		resourceResultKey(root): {
			resource: root,
			appComponents: map[string]map[string]struct{}{
				"app-plan": {"backend": {}},
			},
		},
		resourceResultKey(configMap): {
			resource: configMap,
			appComponents: map[string]map[string]struct{}{
				"app-plan": {"backend": {}},
			},
		},
	}

	conflicts := adoptedOwnerReferenceConflicts(membership)

	require.Len(t, conflicts["app-plan"], 1)
	assert.Contains(t, conflicts["app-plan"][0], "remove or rebind the ownerReference before import")
}

func TestAdoptedOwnerReferenceConflictsIncludesUIDChangingDependencies(t *testing.T) {
	serviceAccount := newServiceAccountResource(t, "backend-runtime", "prod")
	serviceAccount.object.SetUID(types.UID("service-account-uid"))
	configMap := newConfigMapResource(t, "backend-config", "prod")
	configMap.object.SetOwnerReferences([]metav1.OwnerReference{{
		APIVersion: "v1",
		Kind:       "ServiceAccount",
		Name:       serviceAccount.name,
		UID:        serviceAccount.object.GetUID(),
	}})
	membership := map[string]*adoptedMembership{
		resourceResultKey(serviceAccount): {
			resource: serviceAccount,
			appComponents: map[string]map[string]struct{}{
				"app-plan": {"backend": {}},
			},
		},
		resourceResultKey(configMap): {
			resource: configMap,
			appComponents: map[string]map[string]struct{}{
				"app-plan": {"backend": {}},
			},
		},
	}

	conflicts := adoptedOwnerReferenceConflicts(membership)

	require.Len(t, conflicts["app-plan"], 1)
	assert.Contains(t, conflicts["app-plan"][0], "ServiceAccount/backend-runtime")
}

func TestPropagateAdoptedRBACSharing_ClusterBindingPreservesNamespacedServiceAccount(t *testing.T) {
	const namespace = "prod"
	serviceAccount := newServiceAccountResource(t, "backend-sa", namespace)
	clusterRole := newClusterRoleResource(t, "global-reader")
	clusterBinding := newClusterRoleBindingResource(
		t,
		"global-reader-backend",
		rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "ClusterRole",
			Name:     clusterRole.name,
		},
		[]rbacv1.Subject{{
			Kind:      rbacv1.ServiceAccountKind,
			Name:      serviceAccount.name,
			Namespace: namespace,
		}},
	)
	resources := []*importResource{serviceAccount, clusterRole, clusterBinding}
	membership := make(map[string]*adoptedMembership, len(resources))
	for _, resource := range resources {
		membership[resourceResultKey(resource)] = &adoptedMembership{
			resource: resource,
			appComponents: map[string]map[string]struct{}{
				"app-plan": {"backend": {}},
			},
		}
	}

	propagateAdoptedRBACSharing(namespace, resources, indexAdoptedResources(resources), membership)
	assignAdoptedResourceSemantics(membership, nil)

	assert.Equal(t, adoption.OwnershipShared, serviceAccount.ownership)
	assert.Equal(t, adoption.DispositionSharedPreserved, serviceAccount.disposition)
	assert.Equal(t, adoption.OwnershipExternal, clusterRole.ownership)
	assert.Equal(t, adoption.DispositionSharedPreserved, clusterRole.disposition)
	assert.Equal(t, adoption.OwnershipExternal, clusterBinding.ownership)
	assert.Equal(t, adoption.DispositionSharedPreserved, clusterBinding.disposition)
}

func TestPropagateAdoptedSharedDependencies_ClusterBindingPreservesServiceAccountSecrets(t *testing.T) {
	const namespace = "prod"
	serviceAccount := newServiceAccountResource(t, "backend-sa", namespace)
	var serviceAccountObject corev1.ServiceAccount
	require.NoError(t, runtime.DefaultUnstructuredConverter.FromUnstructured(
		serviceAccount.object.Object,
		&serviceAccountObject,
	))
	serviceAccountObject.Secrets = []corev1.ObjectReference{{Name: "token-secret"}}
	serviceAccountObject.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "registry-secret"}}
	updatedObject, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&serviceAccountObject)
	require.NoError(t, err)
	serviceAccount.object.Object = updatedObject

	tokenSecret := newSecretResource(t, "token-secret", namespace)
	registrySecret := newSecretResource(t, "registry-secret", namespace)
	clusterRole := newClusterRoleResource(t, "global-reader")
	clusterBinding := newClusterRoleBindingResource(
		t,
		"global-reader-backend",
		rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "ClusterRole",
			Name:     clusterRole.name,
		},
		[]rbacv1.Subject{{
			Kind:      rbacv1.ServiceAccountKind,
			Name:      serviceAccount.name,
			Namespace: namespace,
		}},
	)
	resources := []*importResource{
		serviceAccount,
		tokenSecret,
		registrySecret,
		clusterRole,
		clusterBinding,
	}
	membership := make(map[string]*adoptedMembership, len(resources))
	for _, resource := range resources {
		membership[resourceResultKey(resource)] = &adoptedMembership{
			resource: resource,
			appComponents: map[string]map[string]struct{}{
				"app-plan": {"backend": {}},
			},
		}
	}

	propagateAdoptedSharedDependencies(namespace, resources, membership)
	assignAdoptedResourceSemantics(membership, nil)

	assert.Equal(t, adoption.DispositionSharedPreserved, serviceAccount.disposition)
	assert.Equal(t, adoption.DispositionSharedPreserved, tokenSecret.disposition)
	assert.Equal(t, adoption.DispositionSharedPreserved, registrySecret.disposition)
}

func TestPropagateAdoptedRBACSharing_ClusterBindingClassificationIsDeterministic(t *testing.T) {
	const namespace = "prod"
	serviceAccount := newServiceAccountResource(t, "backend-sa", namespace)
	role := newRoleResource(t, "backend-reader", namespace)
	roleBinding := newRoleBindingResource(
		t,
		"backend-reader",
		namespace,
		rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: role.name},
		[]rbacv1.Subject{{
			Kind:      rbacv1.ServiceAccountKind,
			Name:      serviceAccount.name,
			Namespace: namespace,
		}},
	)
	clusterRole := newClusterRoleResource(t, "global-reader")
	clusterBinding := newClusterRoleBindingResource(
		t,
		"global-reader-backend",
		rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: clusterRole.name},
		[]rbacv1.Subject{{
			Kind:      rbacv1.ServiceAccountKind,
			Name:      serviceAccount.name,
			Namespace: namespace,
		}},
	)
	resources := []*importResource{serviceAccount, role, roleBinding, clusterRole, clusterBinding}

	for range 64 {
		membership := make(map[string]*adoptedMembership, len(resources))
		for index := len(resources) - 1; index >= 0; index-- {
			resource := resources[index]
			membership[resourceResultKey(resource)] = &adoptedMembership{
				resource: resource,
				appComponents: map[string]map[string]struct{}{
					"app-plan": {"backend": {}},
				},
			}
		}

		propagateAdoptedRBACSharing(namespace, resources, indexAdoptedResources(resources), membership)
		assignAdoptedResourceSemantics(membership, nil)

		assert.Equal(t, adoption.OwnershipShared, serviceAccount.ownership)
		assert.Equal(t, adoption.DispositionSharedPreserved, serviceAccount.disposition)
		assert.Equal(t, adoption.OwnershipShared, roleBinding.ownership)
		assert.Equal(t, adoption.DispositionSharedPreserved, roleBinding.disposition)
		assert.Equal(t, adoption.OwnershipShared, role.ownership)
		assert.Equal(t, adoption.DispositionSharedPreserved, role.disposition)
	}
}

func TestAdoptedServicePortsDoesNotInventNumericTargetForNamedPort(t *testing.T) {
	ports := adoptedServicePorts([]corev1.ServicePort{{
		Name:       "http",
		Port:       80,
		TargetPort: intstr.FromString("backend-http"),
		Protocol:   corev1.ProtocolTCP,
	}})

	require.Len(t, ports, 1)
	assert.Zero(t, ports[0].TargetPort)
}

func TestImportNamespaceResources_AdoptedFiveExactRootsDependencyClosureAndSafeApply(t *testing.T) {
	const (
		namespace = "2506191710kp42v3"
		appName   = "lucky77pro-25062015279gan7p"
		appID     = "adopted-app-id"
	)
	names := map[string]string{
		"backend":  appName + "-backend",
		"frontend": appName + "-frontend",
		"socket":   appName + "-socket",
		"redis":    appName + "-redis",
		"mysql":    "m25062015279gan7p-mysql",
	}

	backend := adoptedTestDeployment(names["backend"], namespace, map[string]string{"app": "backend", "shared-edge": "true"})
	frontend := adoptedTestDeployment(names["frontend"], namespace, map[string]string{"app": "frontend"})
	socket := adoptedTestDeployment(names["socket"], namespace, map[string]string{"app": "socket"})
	redis := adoptedTestStatefulSet(names["redis"], namespace, map[string]string{"app": "redis"}, "", nil)
	mysql := adoptedTestStatefulSet(names["mysql"], namespace, map[string]string{"app": "mysql"}, names["mysql"], []string{"data"})
	mysql.Spec.Template.Spec.ServiceAccountName = "pod-labeler-sa"
	mysql.Spec.Template.Spec.Containers[0].EnvFrom = []corev1.EnvFromSource{{
		SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: names["mysql"]}},
	}}
	mysql.Spec.Template.Spec.Volumes = append(mysql.Spec.Template.Spec.Volumes, corev1.Volume{
		Name: "mysql-config",
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: names["mysql"]}},
		},
	})
	mysql.Spec.Template.Spec.Containers[0].VolumeMounts = append(
		mysql.Spec.Template.Spec.Containers[0].VolumeMounts,
		corev1.VolumeMount{Name: "mysql-config", MountPath: "/etc/mysql"},
	)

	proxy := adoptedTestDeployment("proxy-"+namespace, namespace, map[string]string{"app": "proxy", "shared-edge": "true"})
	proxy.Spec.Template.Spec.Containers[0].EnvFrom = []corev1.EnvFromSource{{
		SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: names["mysql"]}},
	}}
	mysqlPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: adoptedTestObjectMeta("data-"+names["mysql"]+"-0", namespace),
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			VolumeName:  "mysql-pv",
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			},
		},
	}
	mysqlSecret := &corev1.Secret{
		ObjectMeta: adoptedTestObjectMeta(names["mysql"], namespace),
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{"password": []byte("mysql-password")},
	}
	mysqlConfig := &corev1.ConfigMap{
		ObjectMeta: adoptedTestObjectMeta(names["mysql"], namespace),
		Data:       map[string]string{"my.cnf": "[mysqld]"},
	}
	backendService := adoptedTestService("backend-svc", namespace, map[string]string{"shared-edge": "true"}, false)
	mysqlService := adoptedTestService(names["mysql"], namespace, map[string]string{"app": "mysql"}, true)
	serviceAccount := &corev1.ServiceAccount{
		ObjectMeta:       adoptedTestObjectMeta("pod-labeler-sa", namespace),
		ImagePullSecrets: []corev1.LocalObjectReference{{Name: "registry-credentials"}},
	}
	registrySecret := &corev1.Secret{
		ObjectMeta: adoptedTestObjectMeta("registry-credentials", namespace),
		Type:       corev1.SecretTypeDockerConfigJson,
		Data:       map[string][]byte{corev1.DockerConfigJsonKey: []byte(`{"auths":{}}`)},
	}
	tlsSecret := &corev1.Secret{
		ObjectMeta: adoptedTestObjectMeta("lucky-tls", namespace),
		Type:       corev1.SecretTypeTLS,
		Data:       map[string][]byte{corev1.TLSCertKey: []byte("test-certificate")},
	}
	role := &rbacv1.Role{
		ObjectMeta: adoptedTestObjectMeta("pod-labeler", namespace),
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{""},
			Resources: []string{"pods"},
			Verbs:     []string{"get", "patch"},
		}},
	}
	roleBinding := &rbacv1.RoleBinding{
		ObjectMeta: adoptedTestObjectMeta("pod-labeler", namespace),
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "Role",
			Name:     role.Name,
		},
		Subjects: []rbacv1.Subject{{
			Kind:      rbacv1.ServiceAccountKind,
			Name:      serviceAccount.Name,
			Namespace: namespace,
		}},
	}
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: adoptedTestObjectMeta("mysql-pdb", namespace),
		Spec: policyv1.PodDisruptionBudgetSpec{
			MinAvailable: &intstr.IntOrString{Type: intstr.Int, IntVal: 1},
			Selector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": "mysql"}},
		},
	}
	networkPolicy := &networkingv1.NetworkPolicy{
		ObjectMeta: adoptedTestObjectMeta("mysql-network-policy", namespace),
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"app": "mysql"}},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
		},
	}
	persistentVolume := &corev1.PersistentVolume{
		ObjectMeta: adoptedTestObjectMeta("mysql-pv", ""),
		Spec: corev1.PersistentVolumeSpec{
			Capacity: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			ClaimRef: &corev1.ObjectReference{
				Namespace: namespace,
				Name:      mysqlPVC.Name,
			},
		},
	}
	pathType := networkingv1.PathTypePrefix
	ingress := &networkingv1.Ingress{
		ObjectMeta: adoptedTestObjectMeta("lucky77pro", namespace),
		Spec: networkingv1.IngressSpec{
			TLS: []networkingv1.IngressTLS{{SecretName: tlsSecret.Name, Hosts: []string{"lucky.example.test"}}},
			Rules: []networkingv1.IngressRule{{
				Host: "lucky.example.test",
				IngressRuleValue: networkingv1.IngressRuleValue{
					HTTP: &networkingv1.HTTPIngressRuleValue{Paths: []networkingv1.HTTPIngressPath{{
						Path:     "/",
						PathType: &pathType,
						Backend: networkingv1.IngressBackend{Service: &networkingv1.IngressServiceBackend{
							Name: "backend-svc",
							Port: networkingv1.ServiceBackendPort{Number: 80},
						}},
					}}},
				},
			}},
		},
	}

	client := fake.NewSimpleClientset(
		backend,
		frontend,
		socket,
		redis,
		mysql,
		proxy,
		mysqlPVC,
		mysqlSecret,
		mysqlConfig,
		backendService,
		mysqlService,
		serviceAccount,
		registrySecret,
		tlsSecret,
		role,
		roleBinding,
		pdb,
		networkPolicy,
		persistentVolume,
		ingress,
	)
	store := newInMemoryAppStore()
	appService := &namespaceImportAppServiceStub{
		generatedID:  appID,
		persistStore: store,
	}
	svc := &namespaceImportServiceImpl{
		Cfg:                 adoptedImportTestConfig(),
		KubeClient:          client,
		AdoptedImportLocker: locker.NewMemoryLocker("test-adopted-import"),
		ApplicationService:  appService,
		ValidationService:   NewValidationService(),
		AppRepo:             &mockAppRepo{store: store},
		WorkflowRepo:        &mockWorkflowRepo{store: store},
		ComponentRepo:       &mockComponentRepo{store: store},
	}
	request := adoptedImportRequest(namespace, appName, names)

	dryRun, err := svc.ImportNamespaceResources(context.Background(), request)
	require.NoError(t, err)
	require.NotNil(t, dryRun)
	assert.Equal(t, config.ManagementModeAdopted, dryRun.ManagementMode)
	assert.NotEmpty(t, dryRun.PlanFingerprint)
	require.Len(t, dryRun.Apps, 1)
	assert.ElementsMatch(t, []string{"backend", "frontend", "socket", "redis", "mysql"}, dryRun.Apps[0].Components)
	assert.Empty(t, appService.createReqs)
	assertKubeActionsReadOnly(t, client.Actions())

	mysqlResult := requireImportResourceResult(t, dryRun, "StatefulSet", names["mysql"])
	assert.Equal(t, "mysql", mysqlResult.ComponentName)
	assert.Equal(t, adoptedDependencyRoleWorkload, mysqlResult.DependencyRole)
	assert.Equal(t, adoption.OwnershipExclusive, mysqlResult.Ownership)
	assert.Equal(t, adoption.DispositionManaged, mysqlResult.Disposition)
	require.NotNil(t, mysqlResult.Source)
	assert.Equal(t, string(mysql.UID), mysqlResult.Source.UID)

	pvcResult := requireImportResourceResult(t, dryRun, "PersistentVolumeClaim", mysqlPVC.Name)
	assert.Equal(t, adoption.OwnershipDataProtected, pvcResult.Ownership)
	assert.Equal(t, adoption.DispositionDataProtected, pvcResult.Disposition)

	ingressResult := requireImportResourceResult(t, dryRun, "Ingress", ingress.Name)
	assert.Equal(t, "backend", ingressResult.ComponentName)
	assert.Equal(t, adoptedDependencyRoleIngress, ingressResult.DependencyRole)
	assert.Equal(t, adoption.OwnershipShared, ingressResult.Ownership)
	assert.Equal(t, adoption.DispositionSharedPreserved, ingressResult.Disposition)

	serviceResult := requireImportResourceResult(t, dryRun, "Service", backendService.Name)
	assert.Equal(t, adoption.OwnershipShared, serviceResult.Ownership)
	assert.Equal(t, adoption.DispositionSharedPreserved, serviceResult.Disposition)
	secretResult := requireImportResourceResult(t, dryRun, "Secret", mysqlSecret.Name)
	assert.Equal(t, adoption.OwnershipShared, secretResult.Ownership)
	assert.Equal(t, adoption.DispositionSharedPreserved, secretResult.Disposition)
	tlsSecretResult := requireImportResourceResult(t, dryRun, "Secret", tlsSecret.Name)
	assert.Equal(t, adoption.OwnershipShared, tlsSecretResult.Ownership)
	assert.Equal(t, adoption.DispositionSharedPreserved, tlsSecretResult.Disposition)
	registrySecretResult := requireImportResourceResult(t, dryRun, "Secret", registrySecret.Name)
	assert.Equal(t, adoption.OwnershipExclusive, registrySecretResult.Ownership)
	assert.Equal(t, adoption.DispositionManaged, registrySecretResult.Disposition)

	for _, resource := range []struct {
		kind string
		name string
		role string
	}{
		{kind: "ServiceAccount", name: serviceAccount.Name, role: adoptedDependencyRoleServiceAccount},
		{kind: "Role", name: role.Name, role: adoptedDependencyRoleRBAC},
		{kind: "RoleBinding", name: roleBinding.Name, role: adoptedDependencyRoleRBAC},
		{kind: "PodDisruptionBudget", name: pdb.Name, role: adoptedDependencyRolePDB},
		{kind: "NetworkPolicy", name: networkPolicy.Name, role: adoptedDependencyRoleNetworkPolicy},
	} {
		result := requireImportResourceResult(t, dryRun, resource.kind, resource.name)
		assert.Equal(t, resource.role, result.DependencyRole)
		assert.Equal(t, adoption.OwnershipExclusive, result.Ownership)
		assert.Equal(t, adoption.DispositionManaged, result.Disposition)
	}

	pvResult := requireImportResourceResult(t, dryRun, "PersistentVolume", persistentVolume.Name)
	assert.Equal(t, adoption.OwnershipExternal, pvResult.Ownership)
	assert.Equal(t, adoption.DispositionExcluded, pvResult.Disposition)

	proxyResult := requireImportResourceResult(t, dryRun, "Deployment", proxy.Name)
	assert.Equal(t, adoption.OwnershipExternal, proxyResult.Ownership)
	assert.Equal(t, adoption.DispositionExcluded, proxyResult.Disposition)
	assert.Equal(t, importResourceStatusSkipped, proxyResult.Status)

	originalMySQLTemplate := mysql.Spec.Template.DeepCopy()
	originalProxyTemplate := proxy.Spec.Template.DeepCopy()
	request.Mode = importModeApply
	request.PlanFingerprint = dryRun.PlanFingerprint
	applied, err := svc.ImportNamespaceResources(context.Background(), request)
	require.NoError(t, err)
	require.NotNil(t, applied)
	assert.Equal(t, 1, applied.Summary.AppsApplied)
	assert.False(t, applied.Apps[0].WorkflowDisabled)

	persistedApp := store.apps[appID]
	require.NotNil(t, persistedApp)
	assert.Equal(t, config.ManagementModeAdopted, persistedApp.ManagementMode)
	require.NotNil(t, persistedApp.AdoptionSnapshot)
	assert.NotContains(t, mustJSON(t, persistedApp.AdoptionSnapshot), "mysql-password")

	require.Len(t, store.components, 5)
	mysqlComponent := store.components["mysql"]
	require.NotNil(t, mysqlComponent)
	assert.Equal(t, appsv1.SchemeGroupVersion.String(), mysqlComponent.SourceWorkloadAPIVersion)
	assert.Equal(t, "StatefulSet", mysqlComponent.SourceWorkloadKind)
	assert.Equal(t, names["mysql"], mysqlComponent.SourceWorkloadName)
	require.NotNil(t, mysqlComponent.SourceWorkloadUID)
	assert.Equal(t, string(mysql.UID), *mysqlComponent.SourceWorkloadUID)
	assert.Nil(t, mysqlComponent.ResumeReplicas)
	require.NotNil(t, mysqlComponent.AdoptedSecretData)
	assert.NotContains(t, mustJSON(t, mysqlComponent.AdoptedSecretData), "mysql-password")
	assert.Contains(t, mustJSON(t, mysqlComponent.AdoptedSecretData), "ciphertext")
	require.NotNil(t, mysqlComponent.Properties)
	assert.NotContains(t, mustJSON(t, mysqlComponent.Properties), "mysql-password")

	liveMySQL, err := client.AppsV1().StatefulSets(namespace).Get(context.Background(), names["mysql"], metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, originalMySQLTemplate, liveMySQL.Spec.Template.DeepCopy())
	assert.Empty(t, liveMySQL.Labels[config.LabelAppID])

	liveProxy, err := client.AppsV1().Deployments(namespace).Get(context.Background(), proxy.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, originalProxyTemplate, liveProxy.Spec.Template.DeepCopy())
	assert.Empty(t, liveProxy.Labels[config.LabelAppID])

	livePVC, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(context.Background(), mysqlPVC.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Empty(t, livePVC.Labels[config.LabelAppID])

	componentIDs := make(map[string]int, len(store.components))
	for name, component := range store.components {
		require.NotNil(t, component)
		componentIDs[name] = component.ID
	}
	client.ClearActions()
	replayed, err := svc.ImportNamespaceResources(context.Background(), request)
	require.NoError(t, err)
	require.NotNil(t, replayed)
	require.Len(t, replayed.Apps, 1)
	assert.Equal(t, appID, replayed.Apps[0].AppID)
	assert.Equal(t, 1, replayed.Summary.AppsApplied)
	assert.Equal(t, 5, replayed.Summary.ComponentsApplied)
	assert.Contains(t, strings.Join(replayed.Warnings, "\n"), "already applied")
	assert.Len(t, appService.createReqs, 1, "replayed apply must not write the database")
	for name, expectedID := range componentIDs {
		require.NotNil(t, store.components[name])
		assert.Equal(t, expectedID, store.components[name].ID)
	}
	assertKubeActionsReadOnly(t, client.Actions())

	driftedProxy, err := client.AppsV1().Deployments(namespace).Get(context.Background(), proxy.Name, metav1.GetOptions{})
	require.NoError(t, err)
	driftedProxy.Spec.Template.Spec.Containers[0].EnvFrom = nil
	driftedProxy.ResourceVersion = "2"
	_, err = client.AppsV1().Deployments(namespace).Update(context.Background(), driftedProxy, metav1.UpdateOptions{})
	require.NoError(t, err)
	client.ClearActions()
	_, err = svc.ImportNamespaceResources(context.Background(), request)
	require.Error(t, err)
	assert.True(t, errors.Is(err, bcode.ErrNamespaceImportPlanDrift))
	assert.Len(t, appService.createReqs, 1)
	assertKubeActionsReadOnly(t, client.Actions())
}

func TestImportNamespaceResources_AdoptedPreservesExternalManagedByServiceSelector(t *testing.T) {
	const (
		namespace = "prod"
		appName   = "api-app"
		appID     = "adopted-api-app-id"
	)
	workloadLabels := map[string]string{"app": "api"}
	deployment := adoptedTestDeployment("legacy-api", namespace, workloadLabels)
	deployment.Spec.Template.Labels[config.LabelManagedBy] = "Helm"
	serviceSelector := map[string]string{
		"app":                 "api",
		config.LabelManagedBy: "Helm",
	}
	service := adoptedTestService("legacy-api", namespace, serviceSelector, false)
	client := fake.NewSimpleClientset(deployment, service)
	store := newInMemoryAppStore()
	appService := &namespaceImportAppServiceStub{generatedID: appID, persistStore: store}
	svc := &namespaceImportServiceImpl{
		Cfg:                 adoptedImportTestConfig(),
		KubeClient:          client,
		AdoptedImportLocker: locker.NewMemoryLocker("test-adopted-import"),
		ApplicationService:  appService,
		ValidationService:   NewValidationService(),
		AppRepo:             &mockAppRepo{store: store},
		WorkflowRepo:        &mockWorkflowRepo{store: store},
		ComponentRepo:       &mockComponentRepo{store: store},
	}
	request := adoptedSingleWorkloadRequest(namespace, appName, "api", "Deployment", deployment.Name)

	dryRun, err := svc.ImportNamespaceResources(context.Background(), request)
	require.NoError(t, err)
	require.NotEmpty(t, dryRun.PlanFingerprint)

	request.Mode = importModeApply
	request.PlanFingerprint = dryRun.PlanFingerprint
	_, err = svc.ImportNamespaceResources(context.Background(), request)
	require.NoError(t, err)
	require.Len(t, appService.createReqs, 1)
	require.Len(t, appService.createReqs[0].Component, 1)
	require.Len(t, appService.createReqs[0].Component[0].Traits.Service, 1)
	assert.Equal(t, serviceSelector, appService.createReqs[0].Component[0].Traits.Service[0].Selector)
	assertKubeActionsReadOnly(t, client.Actions())
}

func TestImportNamespaceResources_AdoptedPreviousKeyFingerprintApplyRemainsIdempotent(t *testing.T) {
	const (
		namespace = "prod"
		appName   = "rotated-app"
		appID     = "rotated-app-id"
	)
	oldKey := base64.StdEncoding.EncodeToString([]byte("old-key-0123456789abcdef01234567"))
	activeKey := base64.StdEncoding.EncodeToString([]byte("new-key-0123456789abcdef01234567"))
	deployment := adoptedTestDeployment("legacy-api", namespace, map[string]string{"app": "api"})
	client := fake.NewSimpleClientset(deployment)
	store := newInMemoryAppStore()
	appService := &namespaceImportAppServiceStub{generatedID: appID, persistStore: store}
	svc := &namespaceImportServiceImpl{
		Cfg: &config.Config{ImportSecretKeyring: fmt.Sprintf(
			`{"activeKeyId":"old","keys":{"old":%q}}`,
			oldKey,
		)},
		KubeClient:          client,
		AdoptedImportLocker: locker.NewMemoryLocker("test-adopted-import"),
		ApplicationService:  appService,
		ValidationService:   NewValidationService(),
		AppRepo:             &mockAppRepo{store: store},
		WorkflowRepo:        &mockWorkflowRepo{store: store},
		ComponentRepo:       &mockComponentRepo{store: store},
	}
	request := adoptedSingleWorkloadRequest(namespace, appName, "api", "Deployment", deployment.Name)

	dryRun, err := svc.ImportNamespaceResources(context.Background(), request)
	require.NoError(t, err)
	require.NotEmpty(t, dryRun.PlanFingerprint)
	require.Contains(t, dryRun.PlanFingerprint, "v1:old:")

	svc.Cfg = &config.Config{ImportSecretKeyring: fmt.Sprintf(
		`{"activeKeyId":"active","keys":{"old":%q,"active":%q}}`,
		oldKey,
		activeKey,
	)}
	request.Mode = importModeApply
	request.PlanFingerprint = dryRun.PlanFingerprint

	applied, err := svc.ImportNamespaceResources(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, dryRun.PlanFingerprint, applied.PlanFingerprint)
	require.Len(t, appService.createReqs, 1)
	require.NotNil(t, store.apps[appID])
	require.Contains(t, mustJSON(t, store.apps[appID].AdoptionSnapshot), dryRun.PlanFingerprint)

	client.ClearActions()
	replayed, err := svc.ImportNamespaceResources(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, dryRun.PlanFingerprint, replayed.PlanFingerprint)
	require.Contains(t, strings.Join(replayed.Warnings, "\n"), "already applied")
	require.Len(t, appService.createReqs, 1, "exact retry must not rewrite the database")
	assertKubeActionsReadOnly(t, client.Actions())
}

func TestImportNamespaceResources_AdoptedPlanDriftAndHPATargetWriteNothing(t *testing.T) {
	const namespace = "prod"

	t.Run("resource drift rejects apply before DB writes", func(t *testing.T) {
		deployment := adoptedTestDeployment("legacy-api", namespace, map[string]string{"app": "api"})
		client := fake.NewSimpleClientset(deployment)
		appService := &namespaceImportAppServiceStub{}
		store := newInMemoryAppStore()
		svc := &namespaceImportServiceImpl{
			Cfg:                 adoptedImportTestConfig(),
			KubeClient:          client,
			AdoptedImportLocker: locker.NewMemoryLocker("test-adopted-import"),
			ApplicationService:  appService,
			ValidationService:   NewValidationService(),
			AppRepo:             &mockAppRepo{store: store},
			WorkflowRepo:        &mockWorkflowRepo{store: store},
			ComponentRepo:       &mockComponentRepo{store: store},
		}
		request := adoptedSingleWorkloadRequest(namespace, "api-app", "api", "Deployment", deployment.Name)
		dryRun, err := svc.ImportNamespaceResources(context.Background(), request)
		require.NoError(t, err)
		require.NotEmpty(t, dryRun.PlanFingerprint)

		live, err := client.AppsV1().Deployments(namespace).Get(context.Background(), deployment.Name, metav1.GetOptions{})
		require.NoError(t, err)
		live.Spec.Template.Spec.Containers[0].Image = "nginx:drifted"
		live.ResourceVersion = "2"
		_, err = client.AppsV1().Deployments(namespace).Update(context.Background(), live, metav1.UpdateOptions{})
		require.NoError(t, err)

		request.Mode = importModeApply
		request.PlanFingerprint = dryRun.PlanFingerprint
		_, err = svc.ImportNamespaceResources(context.Background(), request)
		require.Error(t, err)
		assert.True(t, errors.Is(err, bcode.ErrNamespaceImportPlanDrift))
		assert.Empty(t, appService.createReqs)
		assert.Empty(t, store.apps)
	})

	t.Run("secret value drift rejects an exact apply replay", func(t *testing.T) {
		deployment := adoptedTestDeployment("legacy-api", namespace, map[string]string{"app": "api"})
		deployment.Spec.Template.Spec.Containers[0].EnvFrom = []corev1.EnvFromSource{{
			SecretRef: &corev1.SecretEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: "api-secret"},
			},
		}}
		secret := &corev1.Secret{
			ObjectMeta: adoptedTestObjectMeta("api-secret", namespace),
			Data:       map[string][]byte{"token": []byte("before")},
		}
		client := fake.NewSimpleClientset(deployment, secret)
		appService := &namespaceImportAppServiceStub{
			generatedID:  "api-app-id",
			persistStore: newInMemoryAppStore(),
		}
		store := appService.persistStore
		svc := &namespaceImportServiceImpl{
			Cfg:                 adoptedImportTestConfig(),
			KubeClient:          client,
			AdoptedImportLocker: locker.NewMemoryLocker("test-adopted-import"),
			ApplicationService:  appService,
			ValidationService:   NewValidationService(),
			AppRepo:             &mockAppRepo{store: store},
			WorkflowRepo:        &mockWorkflowRepo{store: store},
			ComponentRepo:       &mockComponentRepo{store: store},
		}
		request := adoptedSingleWorkloadRequest(namespace, "api-app", "api", "Deployment", deployment.Name)
		dryRun, err := svc.ImportNamespaceResources(context.Background(), request)
		require.NoError(t, err)

		request.Mode = importModeApply
		request.PlanFingerprint = dryRun.PlanFingerprint
		_, err = svc.ImportNamespaceResources(context.Background(), request)
		require.NoError(t, err)

		liveSecret, err := client.CoreV1().Secrets(namespace).Get(
			context.Background(),
			secret.Name,
			metav1.GetOptions{},
		)
		require.NoError(t, err)
		liveSecret.Data["token"] = []byte("after")
		liveSecret.ResourceVersion = "2"
		_, err = client.CoreV1().Secrets(namespace).Update(
			context.Background(),
			liveSecret,
			metav1.UpdateOptions{},
		)
		require.NoError(t, err)

		_, err = svc.ImportNamespaceResources(context.Background(), request)
		require.ErrorIs(t, err, bcode.ErrNamespaceImportPlanDrift)
		require.Len(t, appService.createReqs, 1, "drifted replay must not rewrite the database")
	})

	t.Run("HPA target blocks the entire adopted apply", func(t *testing.T) {
		deployment := adoptedTestDeployment("legacy-api", namespace, map[string]string{"app": "api"})
		hpa := &autoscalingv2.HorizontalPodAutoscaler{
			ObjectMeta: adoptedTestObjectMeta("legacy-api-hpa", namespace),
			Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
					APIVersion: appsv1.SchemeGroupVersion.String(),
					Kind:       "Deployment",
					Name:       deployment.Name,
				},
				MinReplicas: int32Ptr(1),
				MaxReplicas: 3,
			},
		}
		client := fake.NewSimpleClientset(deployment, hpa)
		appService := &namespaceImportAppServiceStub{}
		store := newInMemoryAppStore()
		svc := &namespaceImportServiceImpl{
			Cfg:                 adoptedImportTestConfig(),
			KubeClient:          client,
			AdoptedImportLocker: locker.NewMemoryLocker("test-adopted-import"),
			ApplicationService:  appService,
			ValidationService:   NewValidationService(),
			AppRepo:             &mockAppRepo{store: store},
			WorkflowRepo:        &mockWorkflowRepo{store: store},
			ComponentRepo:       &mockComponentRepo{store: store},
		}
		request := adoptedSingleWorkloadRequest(namespace, "api-app", "api", "Deployment", deployment.Name)
		dryRun, err := svc.ImportNamespaceResources(context.Background(), request)
		require.NoError(t, err)
		require.Len(t, dryRun.Apps, 1)
		assert.Contains(t, dryRun.Apps[0].Error, "HPA")
		result := requireImportResourceResult(t, dryRun, "Deployment", deployment.Name)
		assert.Equal(t, adoption.DispositionBlocked, result.Disposition)
		assert.Equal(t, importResourceStatusSkipped, result.Status)

		request.Mode = importModeApply
		request.PlanFingerprint = dryRun.PlanFingerprint
		_, err = svc.ImportNamespaceResources(context.Background(), request)
		require.Error(t, err)
		assert.True(t, errors.Is(err, bcode.ErrAdoptedResourceConflict))
		assert.Empty(t, appService.createReqs)
		assert.Empty(t, store.apps)
	})

	t.Run("partial conflicting labels on a target Pod block adoption", func(t *testing.T) {
		statefulSet := adoptedTestStatefulSet("legacy-mysql", namespace, map[string]string{"app": "mysql"}, "mysql", nil)
		controller := true
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name:      "legacy-mysql-0",
			Namespace: namespace,
			UID:       types.UID("legacy-mysql-pod-uid"),
			Labels: map[string]string{
				config.LabelAppID: "old-app",
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: appsv1.SchemeGroupVersion.String(),
				Kind:       "StatefulSet",
				Name:       statefulSet.Name,
				UID:        statefulSet.UID,
				Controller: &controller,
			}},
		}}
		client := fake.NewSimpleClientset(statefulSet, pod)
		appService := &namespaceImportAppServiceStub{}
		store := newInMemoryAppStore()
		svc := &namespaceImportServiceImpl{
			Cfg:                 adoptedImportTestConfig(),
			KubeClient:          client,
			AdoptedImportLocker: locker.NewMemoryLocker("test-adopted-import"),
			ApplicationService:  appService,
			ValidationService:   NewValidationService(),
			AppRepo:             &mockAppRepo{store: store},
			WorkflowRepo:        &mockWorkflowRepo{store: store},
			ComponentRepo:       &mockComponentRepo{store: store},
		}
		request := adoptedSingleWorkloadRequest(namespace, "mysql-app", "mysql", "StatefulSet", statefulSet.Name)
		dryRun, err := svc.ImportNamespaceResources(context.Background(), request)
		require.NoError(t, err)
		require.Len(t, dryRun.Apps, 1)
		assert.Contains(t, dryRun.Apps[0].Error, "Pod/legacy-mysql-0")
		assert.Contains(t, dryRun.Apps[0].Error, config.LabelAppID)
		result := requireImportResourceResult(t, dryRun, "StatefulSet", statefulSet.Name)
		assert.Equal(t, adoption.DispositionBlocked, result.Disposition)

		request.Mode = importModeApply
		request.PlanFingerprint = dryRun.PlanFingerprint
		_, err = svc.ImportNamespaceResources(context.Background(), request)
		require.ErrorIs(t, err, bcode.ErrAdoptedResourceConflict)
		assert.Empty(t, appService.createReqs)
		assert.Empty(t, store.apps)
	})

	t.Run("existing source UID ownership blocks the entire adopted apply", func(t *testing.T) {
		deployment := adoptedTestDeployment("legacy-api", namespace, map[string]string{"app": "api"})
		client := fake.NewSimpleClientset(deployment)
		appService := &namespaceImportAppServiceStub{}
		store := newInMemoryAppStore()
		store.apps["other-app"] = &model.Applications{
			ID:             "other-app",
			Name:           "other-app",
			Namespace:      namespace,
			ManagementMode: config.ManagementModeAdopted,
		}
		sourceUID := string(deployment.UID)
		store.components["other-api"] = &model.ApplicationComponent{
			ID:                91,
			AppID:             "other-app",
			Name:              "other-api",
			Namespace:         namespace,
			SourceWorkloadUID: &sourceUID,
		}
		svc := &namespaceImportServiceImpl{
			Cfg:                 adoptedImportTestConfig(),
			KubeClient:          client,
			AdoptedImportLocker: locker.NewMemoryLocker("test-adopted-import"),
			ApplicationService:  appService,
			ValidationService:   NewValidationService(),
			AppRepo:             &mockAppRepo{store: store},
			WorkflowRepo:        &mockWorkflowRepo{store: store},
			ComponentRepo:       &mockComponentRepo{store: store},
		}
		request := adoptedSingleWorkloadRequest(namespace, "api-app", "api", "Deployment", deployment.Name)
		dryRun, err := svc.ImportNamespaceResources(context.Background(), request)
		require.NoError(t, err)
		require.Len(t, dryRun.Apps, 1)
		assert.Contains(t, dryRun.Apps[0].Error, "already adopted")
		result := requireImportResourceResult(t, dryRun, "Deployment", deployment.Name)
		assert.Equal(t, adoption.DispositionBlocked, result.Disposition)

		request.Mode = importModeApply
		request.PlanFingerprint = dryRun.PlanFingerprint
		_, err = svc.ImportNamespaceResources(context.Background(), request)
		require.Error(t, err)
		assert.True(t, errors.Is(err, bcode.ErrAdoptedResourceConflict))
		assert.Empty(t, appService.createReqs)
		assert.Len(t, store.apps, 1)
	})

	t.Run("persisted exclusive dependency ownership survives a missing root workload", func(t *testing.T) {
		deployment := adoptedTestDeployment("legacy-api", namespace, map[string]string{"app": "api"})
		deployment.Spec.Template.Spec.Containers[0].EnvFrom = []corev1.EnvFromSource{{
			ConfigMapRef: &corev1.ConfigMapEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: "shared-config"},
			},
		}}
		configMap := &corev1.ConfigMap{ObjectMeta: adoptedTestObjectMeta("shared-config", namespace)}
		client := fake.NewSimpleClientset(deployment, configMap)
		appService := &namespaceImportAppServiceStub{}
		store := newInMemoryAppStore()
		snapshot := adoption.NewSnapshot(namespace, []adoption.ResourceSnapshot{
			{
				Source: adoption.ResourceIdentity{
					APIVersion:      appsv1.SchemeGroupVersion.String(),
					Kind:            "Deployment",
					Namespace:       namespace,
					Name:            "missing-owner-root",
					UID:             "missing-owner-root-uid",
					ResourceVersion: "1",
					SpecDigest:      "persisted-root-digest",
				},
				ComponentName:  "owner",
				DependencyRole: adoptedDependencyRoleWorkload,
				Ownership:      adoption.OwnershipExclusive,
				Disposition:    adoption.DispositionManaged,
			},
			{
				Source: adoption.ResourceIdentity{
					APIVersion:      corev1.SchemeGroupVersion.String(),
					Kind:            "ConfigMap",
					Namespace:       namespace,
					Name:            configMap.Name,
					UID:             "old-shared-config-uid",
					ResourceVersion: "1",
					SpecDigest:      "persisted-config-digest",
				},
				DependencyRole: adoptedDependencyRoleConfigMap,
				Ownership:      adoption.OwnershipExclusive,
				Disposition:    adoption.DispositionManaged,
			},
		})
		snapshotJSON, err := model.NewJSONStructByStruct(snapshot)
		require.NoError(t, err)
		store.apps["other-app"] = &model.Applications{
			ID:               "other-app",
			Name:             "other-app",
			Namespace:        namespace,
			ManagementMode:   config.ManagementModeAdopted,
			AdoptionSnapshot: snapshotJSON,
		}
		svc := &namespaceImportServiceImpl{
			Cfg:                 adoptedImportTestConfig(),
			KubeClient:          client,
			AdoptedImportLocker: locker.NewMemoryLocker("test-adopted-import"),
			ApplicationService:  appService,
			ValidationService:   NewValidationService(),
			AppRepo:             &mockAppRepo{store: store},
			WorkflowRepo:        &mockWorkflowRepo{store: store},
			ComponentRepo:       &mockComponentRepo{store: store},
		}
		request := adoptedSingleWorkloadRequest(namespace, "api-app", "api", "Deployment", deployment.Name)

		dryRun, err := svc.ImportNamespaceResources(context.Background(), request)
		require.NoError(t, err)
		require.Len(t, dryRun.Apps, 1)
		assert.Contains(t, dryRun.Apps[0].Error, "already managed exclusively by adopted app other-app")
		result := requireImportResourceResult(t, dryRun, "ConfigMap", configMap.Name)
		assert.Equal(t, adoption.DispositionBlocked, result.Disposition)

		request.Mode = importModeApply
		request.PlanFingerprint = dryRun.PlanFingerprint
		_, err = svc.ImportNamespaceResources(context.Background(), request)
		require.ErrorIs(t, err, bcode.ErrAdoptedResourceConflict)
		assert.Empty(t, appService.createReqs)
		assert.Len(t, store.apps, 1)
	})

	t.Run("persisted workload identity blocks a same-name replacement UID", func(t *testing.T) {
		deployment := adoptedTestDeployment("legacy-api", namespace, map[string]string{"app": "api"})
		client := fake.NewSimpleClientset(deployment)
		appService := &namespaceImportAppServiceStub{}
		store := newInMemoryAppStore()
		snapshot := adoption.NewSnapshot(namespace, []adoption.ResourceSnapshot{{
			Source: adoption.ResourceIdentity{
				APIVersion:      appsv1.SchemeGroupVersion.String(),
				Kind:            "Deployment",
				Namespace:       namespace,
				Name:            deployment.Name,
				UID:             "replaced-workload-uid",
				ResourceVersion: "1",
				SpecDigest:      "persisted-workload-digest",
			},
			ComponentName:  "owner",
			DependencyRole: adoptedDependencyRoleWorkload,
			Ownership:      adoption.OwnershipExclusive,
			Disposition:    adoption.DispositionManaged,
		}})
		snapshotJSON, err := model.NewJSONStructByStruct(snapshot)
		require.NoError(t, err)
		store.apps["other-app"] = &model.Applications{
			ID:               "other-app",
			Name:             "other-app",
			Namespace:        namespace,
			ManagementMode:   config.ManagementModeAdopted,
			AdoptionSnapshot: snapshotJSON,
		}
		svc := &namespaceImportServiceImpl{
			Cfg:                 adoptedImportTestConfig(),
			KubeClient:          client,
			AdoptedImportLocker: locker.NewMemoryLocker("test-adopted-import"),
			ApplicationService:  appService,
			ValidationService:   NewValidationService(),
			AppRepo:             &mockAppRepo{store: store},
			WorkflowRepo:        &mockWorkflowRepo{store: store},
			ComponentRepo:       &mockComponentRepo{store: store},
		}
		request := adoptedSingleWorkloadRequest(namespace, "api-app", "api", "Deployment", deployment.Name)

		dryRun, err := svc.ImportNamespaceResources(context.Background(), request)
		require.NoError(t, err)
		require.Len(t, dryRun.Apps, 1)
		assert.Contains(t, dryRun.Apps[0].Error, "already managed exclusively by adopted app other-app")
		result := requireImportResourceResult(t, dryRun, "Deployment", deployment.Name)
		assert.Equal(t, adoption.DispositionBlocked, result.Disposition)

		request.Mode = importModeApply
		request.PlanFingerprint = dryRun.PlanFingerprint
		_, err = svc.ImportNamespaceResources(context.Background(), request)
		require.ErrorIs(t, err, bcode.ErrAdoptedResourceConflict)
		assert.Empty(t, appService.createReqs)
	})

	t.Run("legacy snapshot resource namespace falls back to snapshot namespace", func(t *testing.T) {
		deployment := adoptedTestDeployment("legacy-api", namespace, map[string]string{"app": "api"})
		deployment.Spec.Template.Spec.Containers[0].EnvFrom = []corev1.EnvFromSource{{
			ConfigMapRef: &corev1.ConfigMapEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: "shared-config"},
			},
		}}
		configMap := &corev1.ConfigMap{ObjectMeta: adoptedTestObjectMeta("shared-config", namespace)}
		client := fake.NewSimpleClientset(deployment, configMap)
		store := newInMemoryAppStore()
		snapshot := adoption.NewSnapshot(namespace, []adoption.ResourceSnapshot{{
			Source: adoption.ResourceIdentity{
				APIVersion:      corev1.SchemeGroupVersion.String(),
				Kind:            "ConfigMap",
				Name:            configMap.Name,
				UID:             "old-config-uid",
				ResourceVersion: "1",
				SpecDigest:      "persisted-config-digest",
			},
			DependencyRole: adoptedDependencyRoleConfigMap,
			Ownership:      adoption.OwnershipExclusive,
			Disposition:    adoption.DispositionManaged,
		}})
		snapshotJSON, err := model.NewJSONStructByStruct(snapshot)
		require.NoError(t, err)
		store.apps["other-app"] = &model.Applications{
			ID:               "other-app",
			Name:             "other-app",
			Namespace:        namespace,
			ManagementMode:   config.ManagementModeAdopted,
			AdoptionSnapshot: snapshotJSON,
		}
		svc := &namespaceImportServiceImpl{
			Cfg:                adoptedImportTestConfig(),
			KubeClient:         client,
			ApplicationService: &namespaceImportAppServiceStub{},
			ValidationService:  NewValidationService(),
			AppRepo:            &mockAppRepo{store: store},
			WorkflowRepo:       &mockWorkflowRepo{store: store},
			ComponentRepo:      &mockComponentRepo{store: store},
		}
		request := adoptedSingleWorkloadRequest(namespace, "api-app", "api", "Deployment", deployment.Name)

		dryRun, err := svc.ImportNamespaceResources(context.Background(), request)
		require.NoError(t, err)
		require.Len(t, dryRun.Apps, 1)
		assert.Contains(t, dryRun.Apps[0].Error, "already managed exclusively by adopted app other-app")
		result := requireImportResourceResult(t, dryRun, "ConfigMap", configMap.Name)
		assert.Equal(t, adoption.DispositionBlocked, result.Disposition)
	})

	t.Run("unverifiable adopted ownership fails closed", func(t *testing.T) {
		deployment := adoptedTestDeployment("legacy-api", namespace, map[string]string{"app": "api"})
		client := fake.NewSimpleClientset(deployment)
		store := newInMemoryAppStore()
		store.apps["other-app"] = &model.Applications{
			ID:             "other-app",
			Name:           "other-app",
			Namespace:      namespace,
			ManagementMode: config.ManagementModeAdopted,
		}
		svc := &namespaceImportServiceImpl{
			Cfg:                adoptedImportTestConfig(),
			KubeClient:         client,
			ApplicationService: &namespaceImportAppServiceStub{},
			ValidationService:  NewValidationService(),
			AppRepo:            &mockAppRepo{store: store},
			WorkflowRepo:       &mockWorkflowRepo{store: store},
			ComponentRepo:      &mockComponentRepo{store: store},
		}
		request := adoptedSingleWorkloadRequest(namespace, "api-app", "api", "Deployment", deployment.Name)

		dryRun, err := svc.ImportNamespaceResources(context.Background(), request)

		require.NoError(t, err)
		require.Len(t, dryRun.Apps, 1)
		assert.Contains(t, dryRun.Apps[0].Error, "cannot verify adopted ownership for app other-app")
		result := requireImportResourceResult(t, dryRun, "Deployment", deployment.Name)
		assert.Equal(t, adoption.DispositionBlocked, result.Disposition)
	})

	t.Run("unverifiable adopted ownership in another namespace does not block", func(t *testing.T) {
		deployment := adoptedTestDeployment("legacy-api", namespace, map[string]string{"app": "api"})
		client := fake.NewSimpleClientset(deployment)
		store := newInMemoryAppStore()
		store.apps["other-app"] = &model.Applications{
			ID:             "other-app",
			Name:           "other-app",
			Namespace:      "tenant-b",
			ManagementMode: config.ManagementModeAdopted,
		}
		svc := &namespaceImportServiceImpl{
			Cfg:                adoptedImportTestConfig(),
			KubeClient:         client,
			ApplicationService: &namespaceImportAppServiceStub{},
			ValidationService:  NewValidationService(),
			AppRepo:            &mockAppRepo{store: store},
			WorkflowRepo:       &mockWorkflowRepo{store: store},
			ComponentRepo:      &mockComponentRepo{store: store},
		}
		request := adoptedSingleWorkloadRequest(namespace, "api-app", "api", "Deployment", deployment.Name)

		dryRun, err := svc.ImportNamespaceResources(context.Background(), request)

		require.NoError(t, err)
		require.Len(t, dryRun.Apps, 1)
		assert.Empty(t, dryRun.Apps[0].Error)
		result := requireImportResourceResult(t, dryRun, "Deployment", deployment.Name)
		assert.Equal(t, adoption.DispositionManaged, result.Disposition)
	})
}

func TestImportNamespaceResources_AdoptedTargetAppPreservesMatchingComponentID(t *testing.T) {
	const (
		namespace = "prod"
		targetID  = "existing-adopted-target"
	)
	deployment := adoptedTestDeployment("legacy-api", namespace, map[string]string{"app": "api"})
	client := fake.NewSimpleClientset(deployment)
	store := newInMemoryAppStore()
	store.apps[targetID] = &model.Applications{
		ID:             targetID,
		Name:           "api-app",
		Namespace:      namespace,
		ManagementMode: config.ManagementModeObserve,
	}
	store.components["api"] = &model.ApplicationComponent{
		ID:        77,
		AppID:     targetID,
		Name:      "api",
		Namespace: namespace,
	}
	appService := &namespaceImportAppServiceStub{
		listApps: []*apisv1.ApplicationBase{{
			ID:             targetID,
			Name:           "api-app",
			Namespace:      namespace,
			ManagementMode: config.ManagementModeObserve,
		}},
		persistStore:    store,
		componentIDSeed: 100,
	}
	svc := &namespaceImportServiceImpl{
		Cfg:                 adoptedImportTestConfig(),
		KubeClient:          client,
		AdoptedImportLocker: locker.NewMemoryLocker("test-adopted-import"),
		ApplicationService:  appService,
		ValidationService:   NewValidationService(),
		AppRepo:             &mockAppRepo{store: store},
		WorkflowRepo:        &mockWorkflowRepo{store: store},
		ComponentRepo:       &mockComponentRepo{store: store},
	}
	request := adoptedSingleWorkloadRequest(namespace, "api-app", "api", "Deployment", deployment.Name)
	request.Applications[0].TargetAppID = targetID

	dryRun, err := svc.ImportNamespaceResources(context.Background(), request)
	require.NoError(t, err)
	require.NotEmpty(t, dryRun.PlanFingerprint)
	require.Len(t, dryRun.Apps, 1)
	assert.Equal(t, targetID, dryRun.Apps[0].AppID)

	request.Mode = importModeApply
	request.PlanFingerprint = dryRun.PlanFingerprint
	applied, err := svc.ImportNamespaceResources(context.Background(), request)
	require.NoError(t, err)
	require.Len(t, applied.Apps, 1)
	assert.Equal(t, targetID, applied.Apps[0].AppID)
	require.NotNil(t, store.apps[targetID])
	assert.Equal(t, config.ManagementModeAdopted, store.apps[targetID].ManagementMode)
	require.NotNil(t, store.components["api"])
	assert.Equal(t, 77, store.components["api"].ID)
	assert.Equal(t, targetID, store.components["api"].AppID)
	assertKubeActionsReadOnly(t, client.Actions())

	client.ClearActions()
	replayed, err := svc.ImportNamespaceResources(context.Background(), request)
	require.NoError(t, err)
	require.Len(t, replayed.Apps, 1)
	assert.Equal(t, targetID, replayed.Apps[0].AppID)
	assert.Len(t, appService.createReqs, 1)
	assert.Equal(t, 77, store.components["api"].ID)
	assertKubeActionsReadOnly(t, client.Actions())

	live, err := client.AppsV1().Deployments(namespace).Get(context.Background(), deployment.Name, metav1.GetOptions{})
	require.NoError(t, err)
	live.Spec.Template.Spec.Containers[0].Image = "nginx:1.28"
	live.ResourceVersion = "2"
	_, err = client.AppsV1().Deployments(namespace).Update(context.Background(), live, metav1.UpdateOptions{})
	require.NoError(t, err)

	request.Mode = importModeDryRun
	request.PlanFingerprint = ""
	updatedDryRun, err := svc.ImportNamespaceResources(context.Background(), request)
	require.NoError(t, err)
	require.NotEmpty(t, updatedDryRun.PlanFingerprint)
	assert.NotEqual(t, dryRun.PlanFingerprint, updatedDryRun.PlanFingerprint)

	request.Mode = importModeApply
	request.PlanFingerprint = updatedDryRun.PlanFingerprint
	_, err = svc.ImportNamespaceResources(context.Background(), request)
	require.NoError(t, err)
	assert.Len(t, appService.createReqs, 2, "a newly approved target re-adoption must execute atomically")
	assert.Equal(t, 77, store.components["api"].ID)
}

func TestImportNamespaceResources_AdoptedApplyRejectsWorkflowDriftInsideTransaction(t *testing.T) {
	const (
		namespace = "prod"
		targetID  = "existing-observe-target"
	)
	deployment := adoptedTestDeployment("legacy-api", namespace, map[string]string{"app": "api"})
	client := fake.NewSimpleClientset(deployment)
	store := newInMemoryAppStore()
	store.apps[targetID] = &model.Applications{
		ID:             targetID,
		Name:           "api-app",
		Namespace:      namespace,
		ManagementMode: config.ManagementModeObserve,
	}
	store.components["api"] = &model.ApplicationComponent{
		ID:        77,
		AppID:     targetID,
		Name:      "api",
		Namespace: namespace,
	}
	store.workflows["workflow-1"] = &model.Workflow{
		ID:        "workflow-1",
		AppID:     targetID,
		Name:      "api-workflow",
		Alias:     "approved",
		Namespace: namespace,
	}
	appService := &namespaceImportAppServiceStub{
		listApps: []*apisv1.ApplicationBase{{
			ID:             targetID,
			Name:           "api-app",
			Namespace:      namespace,
			ManagementMode: config.ManagementModeObserve,
		}},
		persistStore: store,
	}
	svc := &namespaceImportServiceImpl{
		Cfg:                 adoptedImportTestConfig(),
		KubeClient:          client,
		AdoptedImportLocker: locker.NewMemoryLocker("test-adopted-import"),
		ApplicationService:  appService,
		ValidationService:   NewValidationService(),
		AppRepo:             &mockAppRepo{store: store},
		WorkflowRepo:        &mockWorkflowRepo{store: store},
		ComponentRepo:       &mockComponentRepo{store: store},
	}
	request := adoptedSingleWorkloadRequest(namespace, "api-app", "api", "Deployment", deployment.Name)
	request.Applications[0].TargetAppID = targetID

	dryRun, err := svc.ImportNamespaceResources(context.Background(), request)
	require.NoError(t, err)
	require.NotEmpty(t, dryRun.PlanFingerprint)

	appService.beforeMutation = func() {
		store.workflows["workflow-1"].Alias = "changed-after-plan"
	}
	request.Mode = importModeApply
	request.PlanFingerprint = dryRun.PlanFingerprint
	_, err = svc.ImportNamespaceResources(context.Background(), request)

	require.ErrorIs(t, err, bcode.ErrNamespaceImportPlanDrift)
	require.Equal(t, config.ManagementModeObserve, store.apps[targetID].ManagementMode)
	require.Equal(t, "changed-after-plan", store.workflows["workflow-1"].Alias)
	assertKubeActionsReadOnly(t, client.Actions())
}

func TestImportNamespaceResources_LegacyObserveNeverLabelsSkippedVCTStatefulSetOrPVC(t *testing.T) {
	const (
		namespace = "tenant-a"
		appID     = "26022513312d88jw"
	)
	statefulSetName := "lucky-" + appID + "-mysql"
	statefulSet := adoptedTestStatefulSet(statefulSetName, namespace, map[string]string{"app": "mysql"}, statefulSetName, []string{"data"})
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: adoptedTestObjectMeta("data-"+statefulSetName+"-0", namespace),
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			},
		},
	}
	configMap := &corev1.ConfigMap{
		ObjectMeta: adoptedTestObjectMeta("lucky-"+appID+"-config", namespace),
		Data:       map[string]string{"key": "value"},
	}
	client := fake.NewSimpleClientset(statefulSet, pvc, configMap)
	appService := &namespaceImportAppServiceStub{}
	store := newInMemoryAppStore()
	svc := &namespaceImportServiceImpl{
		KubeClient:         client,
		ApplicationService: appService,
		ValidationService:  NewValidationService(),
		AppRepo:            &mockAppRepo{store: store},
		WorkflowRepo:       &mockWorkflowRepo{store: store},
		ComponentRepo:      &mockComponentRepo{store: store},
	}

	resp, err := svc.ImportNamespaceResources(context.Background(), apisv1.ImportNamespaceApplicationsRequest{
		Namespace: namespace,
		Mode:      importModeApply,
		IncludeKinds: []string{
			importKindStatefulSets,
			importKindPersistentVolumeClaims,
			importKindConfigMaps,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, config.ManagementModeObserve, resp.ManagementMode)
	require.Len(t, appService.createReqs, 1)

	liveStatefulSet, err := client.AppsV1().StatefulSets(namespace).Get(context.Background(), statefulSetName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Empty(t, liveStatefulSet.Labels[config.LabelAppID])
	assert.Empty(t, liveStatefulSet.Spec.Template.Labels[config.LabelAppID])
	livePVC, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(context.Background(), pvc.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Empty(t, livePVC.Labels[config.LabelAppID])

	stsResult := requireImportResourceResult(t, resp, "StatefulSet", statefulSetName)
	assert.Equal(t, adoption.DispositionBlocked, stsResult.Disposition)
	assert.Equal(t, importResourceStatusSkipped, stsResult.Status)
	pvcResult := requireImportResourceResult(t, resp, "PersistentVolumeClaim", pvc.Name)
	assert.Equal(t, adoption.DispositionDataProtected, pvcResult.Disposition)
	assert.Equal(t, importResourceStatusSkipped, pvcResult.Status)
}

func adoptedImportRequest(namespace, appName string, names map[string]string) apisv1.ImportNamespaceApplicationsRequest {
	components := make([]apisv1.ImportNamespaceComponentMapping, 0, 5)
	for _, name := range []string{"backend", "frontend", "socket", "redis", "mysql"} {
		kind := "Deployment"
		if name == "redis" || name == "mysql" {
			kind = "StatefulSet"
		}
		components = append(components, apisv1.ImportNamespaceComponentMapping{
			Name: name,
			Workload: apisv1.ImportNamespaceWorkloadReference{
				APIVersion: appsv1.SchemeGroupVersion.String(),
				Kind:       kind,
				Name:       names[name],
			},
		})
	}
	return apisv1.ImportNamespaceApplicationsRequest{
		Namespace:      namespace,
		Mode:           importModeDryRun,
		ManagementMode: config.ManagementModeAdopted,
		Applications: []apisv1.ImportNamespaceApplicationMapping{{
			Name:       appName,
			Components: components,
		}},
	}
}

func adoptedSingleWorkloadRequest(namespace, appName, componentName, kind, workloadName string) apisv1.ImportNamespaceApplicationsRequest {
	return apisv1.ImportNamespaceApplicationsRequest{
		Namespace:      namespace,
		Mode:           importModeDryRun,
		ManagementMode: config.ManagementModeAdopted,
		Applications: []apisv1.ImportNamespaceApplicationMapping{{
			Name: appName,
			Components: []apisv1.ImportNamespaceComponentMapping{{
				Name: componentName,
				Workload: apisv1.ImportNamespaceWorkloadReference{
					APIVersion: appsv1.SchemeGroupVersion.String(),
					Kind:       kind,
					Name:       workloadName,
				},
			}},
		}},
	}
}

func adoptedTestDeployment(name, namespace string, labels map[string]string) *appsv1.Deployment {
	replicas := int32(1)
	return &appsv1.Deployment{
		ObjectMeta: adoptedTestObjectMeta(name, namespace),
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: copyStringMap(labels)},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: copyStringMap(labels)},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{
					Name:  "app",
					Image: "nginx:1.27",
				}}},
			},
		},
	}
}

func adoptedTestStatefulSet(
	name, namespace string,
	labels map[string]string,
	serviceName string,
	claimTemplates []string,
) *appsv1.StatefulSet {
	replicas := int32(1)
	templates := make([]corev1.PersistentVolumeClaim, 0, len(claimTemplates))
	mounts := make([]corev1.VolumeMount, 0, len(claimTemplates))
	for _, claimName := range claimTemplates {
		templates = append(templates, corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: claimName},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
				},
			},
		})
		mounts = append(mounts, corev1.VolumeMount{Name: claimName, MountPath: "/" + claimName})
	}
	return &appsv1.StatefulSet{
		ObjectMeta: adoptedTestObjectMeta(name, namespace),
		Spec: appsv1.StatefulSetSpec{
			Replicas:             &replicas,
			ServiceName:          serviceName,
			Selector:             &metav1.LabelSelector{MatchLabels: copyStringMap(labels)},
			VolumeClaimTemplates: templates,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: copyStringMap(labels)},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{
					Name:         "app",
					Image:        "mysql:8.0",
					VolumeMounts: mounts,
				}}},
			},
		},
	}
}

func adoptedTestService(name, namespace string, selector map[string]string, headless bool) *corev1.Service {
	clusterIP := ""
	if headless {
		clusterIP = corev1.ClusterIPNone
	}
	return &corev1.Service{
		ObjectMeta: adoptedTestObjectMeta(name, namespace),
		Spec: corev1.ServiceSpec{
			ClusterIP: clusterIP,
			Selector:  copyStringMap(selector),
			Ports: []corev1.ServicePort{{
				Name:       "http",
				Port:       80,
				TargetPort: intstr.FromInt32(80),
			}},
		},
	}
}

func adoptedTestObjectMeta(name, namespace string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:            name,
		Namespace:       namespace,
		UID:             types.UID("uid-" + name),
		ResourceVersion: "1",
	}
}

func adoptedImportTestConfig() *config.Config {
	key := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))
	return &config.Config{
		ImportSecretKeyring: fmt.Sprintf(`{"activeKeyId":"test","keys":{"test":%q}}`, key),
	}
}

func requireImportResourceResult(
	t *testing.T,
	response *apisv1.ImportNamespaceApplicationsResponse,
	kind, name string,
) *apisv1.ImportNamespaceResourceResult {
	t.Helper()
	for index := range response.ResourceResults {
		result := &response.ResourceResults[index]
		if result.Kind == kind && result.Name == name {
			return result
		}
	}
	require.FailNowf(t, "missing import resource result", "%s/%s not found", kind, name)
	return nil
}

func assertKubeActionsReadOnly(t *testing.T, actions []k8stesting.Action) {
	t.Helper()
	for _, action := range actions {
		switch strings.ToLower(action.GetVerb()) {
		case "get", "list", "watch":
		default:
			require.Failf(t, "unexpected Kubernetes write", "verb=%s resource=%s", action.GetVerb(), action.GetResource().Resource)
		}
	}
}

func int32Ptr(value int32) *int32 {
	return &value
}
