package workflow

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/adoption"
	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	workflowjob "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
	traitsPlu "github.com/PixelCores/Eruun/pkg/apiserver/workflow/traits"
)

func TestGenerateJobTasks_ImportedAdoptionSnapshotProducesManagedDependencyClosure(t *testing.T) {
	traitsPlu.RegisterAllProcessors()

	const (
		appID      = "adopted-app"
		appName    = "lucky77pro-25062015279gan7p"
		namespace  = "2506191710kp42v3"
		workflowID = "adopted-workflow"
	)

	backendSource := &appsv1.Deployment{
		ObjectMeta: adoptedWorkflowObjectMeta("legacy-backend", namespace),
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "backend"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "backend"}},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{
					Name:  "backend",
					Image: "backend:v1",
				}}},
			},
		},
	}
	frontendSource := backendSource.DeepCopy()
	frontendSource.ObjectMeta = adoptedWorkflowObjectMeta("legacy-frontend", namespace)
	frontendSource.Spec.Selector.MatchLabels = map[string]string{"app": "frontend"}
	frontendSource.Spec.Template.Labels = map[string]string{"app": "frontend"}
	frontendSource.Spec.Template.Spec.Containers[0].Name = "frontend"
	frontendSource.Spec.Template.Spec.Containers[0].Image = "frontend:v1"
	socketSource := backendSource.DeepCopy()
	socketSource.ObjectMeta = adoptedWorkflowObjectMeta("legacy-socket", namespace)
	socketSource.Spec.Selector.MatchLabels = map[string]string{"app": "socket"}
	socketSource.Spec.Template.Labels = map[string]string{"app": "socket"}
	socketSource.Spec.Template.Spec.Containers[0].Name = "socket"
	socketSource.Spec.Template.Spec.Containers[0].Image = "socket:v1"
	mysqlSource := &appsv1.StatefulSet{
		ObjectMeta: adoptedWorkflowObjectMeta("legacy-mysql", namespace),
		Spec: appsv1.StatefulSetSpec{
			ServiceName: "mysql-headless",
			Selector:    &metav1.LabelSelector{MatchLabels: map[string]string{"app": "mysql"}},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
				ObjectMeta: metav1.ObjectMeta{Name: "data"},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("10Gi"),
					}},
				},
			}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "mysql"}},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{
					Name:  "mysql",
					Image: "mysql:8",
				}}},
			},
		},
	}
	redisSource := mysqlSource.DeepCopy()
	redisSource.ObjectMeta = adoptedWorkflowObjectMeta("legacy-redis", namespace)
	redisSource.Spec.ServiceName = "redis-headless"
	redisSource.Spec.Selector.MatchLabels = map[string]string{"app": "redis"}
	redisSource.Spec.Template.Labels = map[string]string{"app": "redis"}
	redisSource.Spec.Template.Spec.Containers[0].Name = "redis"
	redisSource.Spec.Template.Spec.Containers[0].Image = "redis:7"
	mysqlService := &corev1.Service{
		ObjectMeta: adoptedWorkflowObjectMeta("mysql-headless", namespace),
		Spec: corev1.ServiceSpec{
			ClusterIP: corev1.ClusterIPNone,
			Selector:  map[string]string{"app": "mysql"},
			Ports: []corev1.ServicePort{{
				Name:       "mysql",
				Port:       3306,
				TargetPort: intstr.FromInt32(3306),
			}},
		},
	}
	sharedBackendService := &corev1.Service{
		ObjectMeta: adoptedWorkflowObjectMeta("backend-svc", namespace),
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "backend"},
			Ports:    []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromInt32(8080)}},
		},
	}
	appWideService := &corev1.Service{
		ObjectMeta: adoptedWorkflowObjectMeta("inside-app-shared-svc", namespace),
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app.kubernetes.io/part-of": "lucky77pro"},
			Ports:    []corev1.ServicePort{{Name: "metrics", Port: 9090}},
		},
	}
	pathType := networkingv1.PathTypePrefix
	sharedIngress := &networkingv1.Ingress{
		ObjectMeta: adoptedWorkflowObjectMeta("lucky77pro", namespace),
		Spec: networkingv1.IngressSpec{Rules: []networkingv1.IngressRule{{
			Host: "lucky.example.test",
			IngressRuleValue: networkingv1.IngressRuleValue{
				HTTP: &networkingv1.HTTPIngressRuleValue{Paths: []networkingv1.HTTPIngressPath{{
					Path:     "/",
					PathType: &pathType,
					Backend: networkingv1.IngressBackend{Service: &networkingv1.IngressServiceBackend{
						Name: sharedBackendService.Name,
						Port: networkingv1.ServiceBackendPort{Number: 80},
					}},
				}}},
			},
		}}},
	}
	mysqlConfig := &corev1.ConfigMap{
		ObjectMeta: adoptedWorkflowObjectMeta("mysql-config", namespace),
		Data:       map[string]string{"my.cnf": "[mysqld]"},
	}
	registrySecret := &corev1.Secret{
		ObjectMeta: adoptedWorkflowObjectMeta("registry-credentials", namespace),
		Type:       corev1.SecretTypeDockerConfigJson,
		Data:       map[string][]byte{corev1.DockerConfigJsonKey: []byte(`{"auths":{}}`)},
	}
	sharedSecret := &corev1.Secret{
		ObjectMeta: adoptedWorkflowObjectMeta("mysql-shared-secret", namespace),
		Data:       map[string][]byte{"password": []byte("must-never-enter-job-payload")},
	}
	storageClassName := "expandable-rwx"
	mysqlPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: adoptedWorkflowObjectMeta("mysql-data", namespace),
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
			StorageClassName: &storageClassName,
			VolumeName:       "mysql-pv",
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("10Gi"),
			}},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
	mysqlVCTPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: adoptedWorkflowObjectMeta("data-"+mysqlSource.Name+"-0", namespace),
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("10Gi"),
			}},
		},
	}
	serviceAccount := &corev1.ServiceAccount{
		ObjectMeta: adoptedWorkflowObjectMeta("mysql-sa", namespace),
	}
	role := &rbacv1.Role{
		ObjectMeta: adoptedWorkflowObjectMeta("mysql-role", namespace),
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{""},
			Resources: []string{"pods"},
			Verbs:     []string{"get"},
		}},
	}
	roleBinding := &rbacv1.RoleBinding{
		ObjectMeta: adoptedWorkflowObjectMeta("mysql-binding", namespace),
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
		ObjectMeta: adoptedWorkflowObjectMeta("mysql-pdb", namespace),
		Spec: policyv1.PodDisruptionBudgetSpec{
			MinAvailable: &intstr.IntOrString{Type: intstr.Int, IntVal: 1},
			Selector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": "mysql"}},
		},
	}
	networkPolicy := &networkingv1.NetworkPolicy{
		ObjectMeta: adoptedWorkflowObjectMeta("mysql-network-policy", namespace),
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"app": "mysql"}},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
		},
	}
	clusterRole := &rbacv1.ClusterRole{
		ObjectMeta: adoptedWorkflowObjectMeta("global-reader", ""),
		Rules:      role.Rules,
	}

	snapshot := adoption.NewSnapshot(namespace, []adoption.ResourceSnapshot{
		adoptedWorkflowSnapshotResource(t, backendSource, "apps/v1", "Deployment", "backend", "workload", adoption.OwnershipExclusive, adoption.DispositionManaged),
		adoptedWorkflowSnapshotResource(t, frontendSource, "apps/v1", "Deployment", "frontend", "workload", adoption.OwnershipExclusive, adoption.DispositionManaged),
		adoptedWorkflowSnapshotResource(t, socketSource, "apps/v1", "Deployment", "socket", "workload", adoption.OwnershipExclusive, adoption.DispositionManaged),
		adoptedWorkflowSnapshotResource(t, mysqlSource, "apps/v1", "StatefulSet", "mysql", "workload", adoption.OwnershipExclusive, adoption.DispositionManaged),
		adoptedWorkflowSnapshotResource(t, redisSource, "apps/v1", "StatefulSet", "redis", "workload", adoption.OwnershipExclusive, adoption.DispositionManaged),
		adoptedWorkflowSnapshotResource(t, mysqlService, "v1", "Service", "mysql", "service", adoption.OwnershipExclusive, adoption.DispositionManaged),
		adoptedWorkflowSnapshotResource(t, appWideService, "v1", "Service", "", "service", adoption.OwnershipExclusive, adoption.DispositionManaged),
		adoptedWorkflowSnapshotResource(t, sharedBackendService, "v1", "Service", "backend", "service", adoption.OwnershipShared, adoption.DispositionSharedPreserved),
		adoptedWorkflowSnapshotResource(t, sharedIngress, networkingv1.SchemeGroupVersion.String(), "Ingress", "backend", "ingress", adoption.OwnershipShared, adoption.DispositionSharedPreserved),
		adoptedWorkflowSnapshotResource(t, mysqlConfig, "v1", "ConfigMap", "mysql", "configmap", adoption.OwnershipExclusive, adoption.DispositionManaged),
		adoptedWorkflowSnapshotResource(t, registrySecret, "v1", "Secret", "", "secret", adoption.OwnershipExclusive, adoption.DispositionManaged),
		adoptedWorkflowSnapshotResource(t, sharedSecret, "v1", "Secret", "", "secret", adoption.OwnershipShared, adoption.DispositionSharedPreserved),
		adoptedWorkflowSnapshotResource(t, mysqlPVC, "v1", "PersistentVolumeClaim", "mysql", "pvc", adoption.OwnershipDataProtected, adoption.DispositionDataProtected),
		adoptedWorkflowSnapshotResource(t, mysqlVCTPVC, "v1", "PersistentVolumeClaim", "mysql", "pvc", adoption.OwnershipDataProtected, adoption.DispositionDataProtected),
		adoptedWorkflowSnapshotResource(t, serviceAccount, "v1", "ServiceAccount", "mysql", "serviceaccount", adoption.OwnershipExclusive, adoption.DispositionManaged),
		adoptedWorkflowSnapshotResource(t, role, rbacv1.SchemeGroupVersion.String(), "Role", "mysql", "rbac", adoption.OwnershipExclusive, adoption.DispositionManaged),
		adoptedWorkflowSnapshotResource(t, roleBinding, rbacv1.SchemeGroupVersion.String(), "RoleBinding", "mysql", "rbac", adoption.OwnershipExclusive, adoption.DispositionManaged),
		adoptedWorkflowSnapshotResource(t, pdb, policyv1.SchemeGroupVersion.String(), "PodDisruptionBudget", "mysql", "pdb", adoption.OwnershipExclusive, adoption.DispositionManaged),
		adoptedWorkflowSnapshotResource(t, networkPolicy, networkingv1.SchemeGroupVersion.String(), "NetworkPolicy", "mysql", "networkpolicy", adoption.OwnershipExclusive, adoption.DispositionManaged),
		adoptedWorkflowSnapshotResource(t, clusterRole, rbacv1.SchemeGroupVersion.String(), "ClusterRole", "", "rbac", adoption.OwnershipExternal, adoption.DispositionSharedPreserved),
	})
	snapshotJSON, err := model.NewJSONStructByStruct(snapshot)
	require.NoError(t, err)

	emptyProperties, err := model.NewJSONStructByStruct(model.Properties{})
	require.NoError(t, err)
	emptyTraits, err := model.NewJSONStructByStruct(spec.Traits{})
	require.NoError(t, err)
	backendTraits, err := model.NewJSONStructByStruct(spec.Traits{
		Service: []spec.ServiceTraitSpec{{
			Name:     sharedBackendService.Name,
			Selector: sharedBackendService.Spec.Selector,
			Ports: []spec.ServicePortTraitSpec{{
				Name:       "http",
				Port:       80,
				TargetPort: 8080,
			}},
		}},
		Ingress: []spec.IngressTraitsSpec{{
			Name:      sharedIngress.Name,
			Namespace: namespace,
			Routes: []spec.IngressRoutes{{
				Path:     "/",
				PathType: string(networkingv1.PathTypePrefix),
				Host:     "lucky.example.test",
				Backend: spec.IngressRoute{
					ServiceName: sharedBackendService.Name,
					ServicePort: 80,
				},
			}},
		}},
	})
	require.NoError(t, err)
	mysqlTraits, err := model.NewJSONStructByStruct(spec.Traits{
		Storage: []spec.StorageTraitSpec{{
			Name:         "mysql-data",
			Type:         "persistent",
			MountPath:    "/var/lib/mysql",
			ClaimName:    mysqlPVC.Name,
			Size:         "20Gi",
			StorageClass: storageClassName,
		}},
		Service: []spec.ServiceTraitSpec{{
			Name:     mysqlService.Name,
			Headless: true,
			Selector: mysqlService.Spec.Selector,
			Ports: []spec.ServicePortTraitSpec{{
				Name:       "mysql",
				Port:       3306,
				TargetPort: 3306,
			}},
		}},
	})
	require.NoError(t, err)

	components := []*model.ApplicationComponent{
		adoptedWorkflowComponent("backend", config.ServerJob, namespace, backendSource.Name, string(backendSource.UID), emptyProperties, backendTraits),
		adoptedWorkflowComponent("frontend", config.ServerJob, namespace, frontendSource.Name, string(frontendSource.UID), emptyProperties, emptyTraits),
		adoptedWorkflowComponent("socket", config.ServerJob, namespace, socketSource.Name, string(socketSource.UID), emptyProperties, emptyTraits),
		adoptedWorkflowComponent("mysql", config.StoreJob, namespace, mysqlSource.Name, string(mysqlSource.UID), emptyProperties, mysqlTraits),
		adoptedWorkflowComponent("redis", config.StoreJob, namespace, redisSource.Name, string(redisSource.UID), emptyProperties, emptyTraits),
	}
	stepsJSON, err := model.NewJSONStructByStruct(model.WorkflowSteps{Steps: []*model.WorkflowStep{{
		Name: "adopt-all",
		Mode: config.WorkflowModeDAG,
		Properties: []model.Policies{{
			Policies: []string{"backend", "frontend", "socket", "mysql", "redis"},
		}},
	}}})
	require.NoError(t, err)
	store := &fakeDataStore{
		application: &model.Applications{
			ID:               appID,
			Name:             appName,
			Namespace:        namespace,
			ManagementMode:   config.ManagementModeAdopted,
			AdoptionSnapshot: snapshotJSON,
		},
		workflow: &model.Workflow{
			ID:        workflowID,
			Name:      workflowID,
			Namespace: namespace,
			AppID:     appID,
			Steps:     stepsJSON,
		},
		components: components,
	}

	executions := mustGenerateJobTasks(t, context.Background(), &model.WorkflowQueue{
		TaskID:       "adopted-task",
		WorkflowID:   workflowID,
		WorkflowName: workflowID,
		AppID:        appID,
	}, store, int64(config.DefaultJobTaskTimeout))

	jobsByType := make(map[config.JobType][]*model.JobTask)
	totalJobs := 0
	for _, execution := range executions {
		for _, jobs := range execution.Jobs {
			for _, jobTask := range jobs {
				require.NotNil(t, jobTask)
				jobsByType[config.JobType(jobTask.JobType)] = append(jobsByType[config.JobType(jobTask.JobType)], jobTask)
				totalJobs++
			}
		}
	}
	require.Equal(t, 15, totalJobs)
	require.Len(t, jobsByType[config.JobDeploy], 3)
	require.Len(t, jobsByType[config.JobDeployStore], 2)
	requireJobNames(t, jobsByType[config.JobDeployService], mysqlService.Name, appWideService.Name)
	requireJobNames(t, jobsByType[config.JobDeployConfigMap], mysqlConfig.Name)
	requireJobNames(t, jobsByType[config.JobDeploySecret], registrySecret.Name)
	requireJobNames(t, jobsByType[config.JobDeployServiceAccount], serviceAccount.Name)
	requireJobNames(t, jobsByType[config.JobDeployRole], role.Name)
	requireJobNames(t, jobsByType[config.JobDeployRoleBinding], roleBinding.Name)
	requireJobNames(t, jobsByType[config.JobDeployPodDisruptionBudget], pdb.Name)
	requireJobNames(t, jobsByType[config.JobDeployNetworkPolicy], networkPolicy.Name)
	requireJobNames(t, jobsByType[config.JobDeployPVC], mysqlPVC.Name)
	require.Empty(t, jobsByType[config.JobDeployIngress])
	require.Empty(t, jobsByType[config.JobDeployClusterRole])
	require.Empty(t, jobsByType[config.JobDeployClusterRoleBinding])

	require.Len(t, jobsByType[config.JobDeploySecret], 1)
	secretJobInfo, ok := jobsByType[config.JobDeploySecret][0].JobInfo.(*corev1.Secret)
	require.True(t, ok)
	require.Nil(t, secretJobInfo.Data)
	require.Nil(t, secretJobInfo.StringData)

	require.Len(t, jobsByType[config.JobDeployPVC], 1)
	pvcJob := jobsByType[config.JobDeployPVC][0]
	pvcJobInfo, ok := pvcJob.JobInfo.(*corev1.PersistentVolumeClaim)
	require.True(t, ok)
	require.Equal(t, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}, pvcJobInfo.Spec.AccessModes)
	require.Equal(t, resource.MustParse("20Gi"), pvcJobInfo.Spec.Resources.Requests[corev1.ResourceStorage])

	allowExpansion := true
	kubeClient := fake.NewSimpleClientset(
		mysqlPVC.DeepCopy(),
		&storagev1.StorageClass{
			ObjectMeta:           metav1.ObjectMeta{Name: storageClassName},
			AllowVolumeExpansion: &allowExpansion,
		},
	)
	pvcController := workflowjob.NewDeployPVCJobCtl(pvcJob, kubeClient, store, func() {}, nil)
	require.NotNil(t, pvcController)
	require.NoError(t, pvcController.Run(context.Background()))
	expandedPVC, err := kubeClient.CoreV1().PersistentVolumeClaims(namespace).Get(
		context.Background(),
		mysqlPVC.Name,
		metav1.GetOptions{},
	)
	require.NoError(t, err)
	require.Equal(t, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}, expandedPVC.Spec.AccessModes)
	require.Equal(t, resource.MustParse("20Gi"), expandedPVC.Spec.Resources.Requests[corev1.ResourceStorage])
	require.Equal(t, "mysql-pv", expandedPVC.Spec.VolumeName)
}

func TestAdoptedProtectedPVCJobRejectsExplicitStorageClassChange(t *testing.T) {
	const namespace = "ops"
	currentClass := "expandable-rwx"
	source := &corev1.PersistentVolumeClaim{
		ObjectMeta: adoptedWorkflowObjectMeta("mysql-data", namespace),
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
			StorageClassName: &currentClass,
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("10Gi"),
			}},
		},
	}
	snapshot := adoption.NewSnapshot(namespace, []adoption.ResourceSnapshot{
		adoptedWorkflowSnapshotResource(
			t,
			source,
			"v1",
			"PersistentVolumeClaim",
			"mysql",
			"pvc",
			adoption.OwnershipDataProtected,
			adoption.DispositionDataProtected,
		),
	})
	otherClass := "replacement"
	generated := source.DeepCopy()
	generated.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	generated.Spec.StorageClassName = &otherClass
	generated.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("20Gi")

	_, err := adoptedProtectedPVCJob(
		&snapshot.Resources[0],
		&snapshot,
		&model.WorkflowQueue{WorkflowID: "workflow", AppID: "app", TaskID: "task"},
		"app",
		int64(config.DefaultJobTaskTimeout),
		&model.JobTask{JobInfo: generated},
	)
	require.ErrorContains(t, err, "cannot change storageClassName")
}

func TestAugmentAdoptedDependencyJobsPreservesComponentAndApprovalScope(t *testing.T) {
	const (
		appID      = "adopted-app"
		namespace  = "ops"
		workflowID = "workflow"
	)
	mysqlConfig := &corev1.ConfigMap{
		ObjectMeta: adoptedWorkflowObjectMeta("mysql-config", namespace),
		Data:       map[string]string{"my.cnf": "[mysqld]"},
	}
	globalSecret := &corev1.Secret{
		ObjectMeta: adoptedWorkflowObjectMeta("registry-credentials", namespace),
		Data:       map[string][]byte{".dockerconfigjson": []byte(`{"auths":{}}`)},
	}
	snapshot := adoption.NewSnapshot(namespace, []adoption.ResourceSnapshot{
		adoptedWorkflowSnapshotResource(t, mysqlConfig, "v1", "ConfigMap", "mysql", "configmap", adoption.OwnershipExclusive, adoption.DispositionManaged),
		adoptedWorkflowSnapshotResource(t, globalSecret, "v1", "Secret", "", "secret", adoption.OwnershipExclusive, adoption.DispositionManaged),
	})
	snapshotJSON, err := model.NewJSONStructByStruct(snapshot)
	require.NoError(t, err)
	store := &fakeDataStore{application: &model.Applications{
		ID:               appID,
		Name:             "legacy",
		Namespace:        namespace,
		ManagementMode:   config.ManagementModeAdopted,
		AdoptionSnapshot: snapshotJSON,
	}}
	task := &model.WorkflowQueue{TaskID: "task", WorkflowID: workflowID, AppID: appID}

	newWorkloadJob := func(name string, jobType config.JobType) *model.JobTask {
		jobTask := NewJobTask(name, namespace, workflowID, "", appID, task.TaskID, int64(config.DefaultJobTaskTimeout), "legacy")
		jobTask.JobType = string(jobType)
		return jobTask
	}
	newStepGroups := func(includeMySQL bool) [][]StepExecution {
		groups := [][]StepExecution{
			{{
				Name:     "backend",
				Mode:     config.WorkflowModeStepByStep,
				StepType: config.WorkflowStepTypeComponent,
				Jobs: map[int][]*model.JobTask{
					config.JobPriorityNormal: {newWorkloadJob("backend", config.JobDeploy)},
				},
			}},
			{{
				Name:     "manual-approval",
				Mode:     config.WorkflowModeStepByStep,
				StepType: config.WorkflowStepTypeApproval,
				Approval: &ApprovalExecution{NotifyURL: "https://example.test/approval"},
			}},
		}
		if includeMySQL {
			groups = append(groups, []StepExecution{{
				Name:     "mysql",
				Mode:     config.WorkflowModeStepByStep,
				StepType: config.WorkflowStepTypeComponent,
				Jobs: map[int][]*model.JobTask{
					config.JobPriorityNormal: {newWorkloadJob("mysql", config.JobDeployStore)},
				},
			}})
		}
		return groups
	}

	t.Run("component dependency remains after approval", func(t *testing.T) {
		groups, err := augmentAdoptedDependencyJobs(
			context.Background(),
			newStepGroups(true),
			task,
			store,
			int64(config.DefaultJobTaskTimeout),
		)
		require.NoError(t, err)
		requireJobNames(t, groups[0][0].Jobs[config.JobPriorityMaxHigh], globalSecret.Name)
		require.Empty(t, groups[1][0].Jobs)
		requireJobNames(t, groups[2][0].Jobs[config.JobPriorityMaxHigh], mysqlConfig.Name)
	})

	t.Run("out of scope component dependency is omitted", func(t *testing.T) {
		groups, err := augmentAdoptedDependencyJobs(
			context.Background(),
			newStepGroups(false),
			task,
			store,
			int64(config.DefaultJobTaskTimeout),
		)
		require.NoError(t, err)
		requireJobNames(t, groups[0][0].Jobs[config.JobPriorityMaxHigh], globalSecret.Name)
		for _, jobs := range groups[0][0].Jobs {
			for _, jobTask := range jobs {
				require.NotEqual(t, mysqlConfig.Name, jobTask.Name)
			}
		}
	})
}

func adoptedWorkflowObjectMeta(name, namespace string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:            name,
		Namespace:       namespace,
		UID:             types.UID("uid-" + name),
		ResourceVersion: "1",
	}
}

func adoptedWorkflowSnapshotResource(
	t *testing.T,
	object interface{},
	apiVersion, kind, componentName, dependencyRole, ownership, disposition string,
) adoption.ResourceSnapshot {
	t.Helper()
	raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(object)
	require.NoError(t, err)
	source := &unstructured.Unstructured{Object: raw}
	source.SetAPIVersion(apiVersion)
	source.SetKind(kind)
	resource, err := adoption.ResourceSnapshotFromObject(
		source,
		componentName,
		dependencyRole,
		ownership,
		disposition,
	)
	require.NoError(t, err)
	return resource
}

func adoptedWorkflowComponent(
	name string,
	componentType config.JobType,
	namespace, sourceName, sourceUID string,
	properties, traits *model.JSONStruct,
) *model.ApplicationComponent {
	apiVersion := appsv1.SchemeGroupVersion.String()
	kind := "Deployment"
	if componentType == config.StoreJob {
		kind = "StatefulSet"
	}
	return &model.ApplicationComponent{
		AppID:                    "adopted-app",
		Name:                     name,
		Namespace:                namespace,
		Image:                    name + ":v1",
		Replicas:                 1,
		ComponentType:            componentType,
		Properties:               properties,
		Traits:                   traits,
		SourceWorkloadAPIVersion: apiVersion,
		SourceWorkloadKind:       kind,
		SourceWorkloadName:       sourceName,
		SourceWorkloadUID:        &sourceUID,
	}
}

func requireJobNames(t *testing.T, jobs []*model.JobTask, expected ...string) {
	t.Helper()
	names := make([]string, 0, len(jobs))
	for _, jobTask := range jobs {
		if jobTask != nil {
			names = append(names, jobTask.Name)
		}
	}
	require.ElementsMatch(t, expected, names)
}
