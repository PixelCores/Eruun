package spec

const (
	AliyunCloudSecretMaskedValue = "******"
)

// AliyunCloudSettingSpec defines credentials and topology defaults for aliyun cloudjob.
type AliyunCloudSettingSpec struct {
	AccessKeyID     string `json:"accessKeyId,omitempty"`
	AccessKeySecret string `json:"accessKeySecret,omitempty"`
	Endpoint        string `json:"endpoint,omitempty"`
	RegionID        string `json:"regionId,omitempty"`
	ZoneID          string `json:"zoneId,omitempty"`
	VpcID           string `json:"vpcId,omitempty"`
	VSwitchID       string `json:"vswId,omitempty"`
}
