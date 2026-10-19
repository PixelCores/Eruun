package spec

// ResourceKind identifies the category of Kubernetes resources managed by jobs.
type ResourceKind string

const (
	ResourceDeployment         ResourceKind = "deployment"
	ResourceStatefulSet        ResourceKind = "statefulset"
	ResourceService            ResourceKind = "service"
	ResourcePVC                ResourceKind = "pvc"
	ResourceConfigMap          ResourceKind = "configmap"
	ResourceSecret             ResourceKind = "secret"
	ResourceIngress            ResourceKind = "ingress"
	ResourceJob                ResourceKind = "job"
	ResourceServiceAccount     ResourceKind = "serviceaccount"
	ResourceRole               ResourceKind = "role"
	ResourceRoleBinding        ResourceKind = "rolebinding"
	ResourceClusterRole        ResourceKind = "clusterrole"
	ResourceClusterRoleBinding ResourceKind = "clusterrolebinding"
)

type KubeKind string

const (
	KubeKindStatefulSet KubeKind = "StatefulSet"
	KubeKindJob         KubeKind = "Job"
)

const (
	ResourceNvidiaGPU = "nvidia.com/gpu"
)
