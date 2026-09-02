package config

// ResourceKind identifies the category of Kubernetes resources managed by jobs.
type ResourceKind string

const (
	ResourceDeployment          ResourceKind = "deployment"
	ResourceStatefulSet         ResourceKind = "statefulset"
	ResourceService             ResourceKind = "service"
	ResourcePVC                 ResourceKind = "pvc"
	ResourceConfigMap           ResourceKind = "configmap"
	ResourceSecret              ResourceKind = "secret"
	ResourceIngress             ResourceKind = "ingress"
	ResourceJob                 ResourceKind = "job"
	ResourceCronJob             ResourceKind = "cronjob"
	ResourceCloudJob            ResourceKind = "cloudjob"
	ResourceServiceAccount      ResourceKind = "serviceaccount"
	ResourceRole                ResourceKind = "role"
	ResourceRoleBinding         ResourceKind = "rolebinding"
	ResourceClusterRole         ResourceKind = "clusterrole"
	ResourceClusterRoleBinding  ResourceKind = "clusterrolebinding"
	ResourcePodDisruptionBudget ResourceKind = "poddisruptionbudget"
	ResourceNetworkPolicy       ResourceKind = "networkpolicy"
)

type KubeKind string

const (
	KubeKindStatefulSet           KubeKind = "StatefulSet"
	KubeKindDeployment            KubeKind = "Deployment"
	KubeKindDaemonSet             KubeKind = "DaemonSet"
	KubeKindService               KubeKind = "Service"
	KubeKindConfigMap             KubeKind = "ConfigMap"
	KubeKindSecret                KubeKind = "Secret"
	KubeKindIngress               KubeKind = "Ingress"
	KubeKindJob                   KubeKind = "Job"
	KubeKindCronJob               KubeKind = "CronJob"
	KubeKindServiceAccount        KubeKind = "ServiceAccount"
	KubeKindRole                  KubeKind = "Role"
	KubeKindRoleBinding           KubeKind = "RoleBinding"
	KubeKindClusterRole           KubeKind = "ClusterRole"
	KubeKindClusterRoleBinding    KubeKind = "ClusterRoleBinding"
	KubeKindPersistentVolumeClaim KubeKind = "PersistentVolumeClaim"
	KubeKindList                  KubeKind = "List"
)

const (
	ResourceNvidiaGPU = "nvidia.com/gpu"
)
