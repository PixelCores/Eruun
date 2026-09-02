package aliyun

import "time"

const (
	ProviderName = "aliyun"

	ActionNasEnsureFilesystem    = "aliyun.nas.ensure_filesystem"
	ActionNasEnsureMountTarget   = "aliyun.nas.ensure_mount_target"
	ActionK8sEnsureStorageClass  = "aliyun.k8s.ensure_storage_class"
	ActionNasDescribeMountTarget = "aliyun.nas.describe_mount_target"

	StateStepKey                         = "step"
	StateStepFilesystemTagPending        = "filesystem-tag-pending"
	StateStepFilesystemReady             = "filesystem-ready"
	StateStepMountTargetCreated          = "mount-target-created"
	StateStepMountTargetPending          = "mount-target-pending"
	StateStepMountTargetReady            = "mount-target-ready"
	StateStepStorageClassWaitMountTarget = "storageclass-wait-mount-target"
	StateStepStorageClassReady           = "storageclass-ready"

	StateFileSystemIDKey            = "fileSystemId"
	StateFileSystemTagRetryCountKey = "fileSystemTagRetryCount"
	StateMountDomainKey             = "mountTargetDomain"
	StateMountStatusKey             = "mountTargetStatus"
	StateMountConfirmInfoKey        = "mountTargetConfirmInfo"
	StateStorageClassNameKey        = "storageClassName"
	StateMountStatusActive          = "active"

	ParamTenantID          = "tenantId"
	ParamRegionID          = "regionId"
	ParamZoneID            = "zoneId"
	ParamCapacityGiB       = "capacityGiB"
	ParamStorageType       = "storageType"
	ParamProtocolType      = "protocolType"
	ParamFileSystemType    = "fileSystemType"
	ParamDescription       = "description"
	ParamVpcID             = "vpcId"
	ParamVSwitchID         = "vswId"
	ParamAccessGroupName   = "accessGroupName"
	ParamSecurityGroupID   = "securityGroupId"
	ParamPollIntervalSec   = "pollIntervalSeconds"
	ParamStorageClassName  = "storageClassName"
	ParamReclaimPolicy     = "reclaimPolicy"
	ParamVolumeBindingMode = "volumeBindingMode"
	ParamServerPath        = "serverPath"

	AliyunTenantTagKey               = ParamTenantID
	AliyunNASResourceTypeFileSystem  = "FileSystem"
	AliyunNASNetworkTypeVpc          = "Vpc"
	AliyunNASDefaultAccessGroupName  = "DEFAULT_VPC_GROUP_NAME"
	AliyunNASStorageProvisioner      = "nasplugin.csi.alibabacloud.com"
	StorageClassParamServer          = "server"
	StorageClassParamVolumeAs        = "volumeAs"
	StorageClassParamVolumeAsSubpath = "subpath"
	DefaultStorageClassServerPath    = "/"

	DefaultPollInterval                   = 15 * time.Second
	DefaultFileSystemTagPendingMaxRetries = int64(3)
)
