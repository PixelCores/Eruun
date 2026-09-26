package spec

// 用户侧声明的存储类型（API 入参）
const (
	StorageTypePersistent  = "persistent"
	StorageTypeEphemeral   = "ephemeral"
	StorageTypeHostMounted = "host-mounted"
	StorageTypeConfig      = "config"
	StorageTypeSecret      = "secret"
)

// Kubernetes 中内部映射的 Volume 类型
const (
	VolumeTypePVC       = "pvc"
	VolumeTypeEmptyDir  = "emptyDir"
	VolumeTypeHostPath  = "hostPath"
	VolumeTypeConfigMap = "configMap"
	VolumeTypeSecret    = "secret"
)

var StorageTypeMapping = map[string]string{
	StorageTypePersistent:  VolumeTypePVC,
	StorageTypeEphemeral:   VolumeTypeEmptyDir,
	StorageTypeHostMounted: VolumeTypeHostPath,
	StorageTypeConfig:      VolumeTypeConfigMap,
	StorageTypeSecret:      VolumeTypeSecret,
}
