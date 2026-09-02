package v1

import "github.com/PixelCores/Eruun/pkg/apiserver/config"

type ListApplicationComponentsResponse struct {
	Components []*ApplicationComponent `json:"components"`
}

type BatchApplicationComponentStatusRequest struct {
	AppIDs []string `json:"appIds"`
}

type BatchApplicationComponentStatusResponse struct {
	Results []BatchApplicationComponentStatusResult `json:"results"`
}

type BatchApplicationComponentStatusResult struct {
	AppID  string `json:"appId"`
	Status string `json:"status,omitempty"`
	Error  string `json:"error,omitempty"`
}

type ApplicationStatusResponse struct {
	AppID  string `json:"appId"`
	Status string `json:"status"`
}

// ApplicationComponentStatusResponse reports runtime status for components under an app.
type ApplicationComponentStatusResponse struct {
	AppID      string                       `json:"appId"`
	Components []ApplicationComponentStatus `json:"components"`
}

// ApplicationComponentStatus is the runtime status snapshot for a component.
type ApplicationComponentStatus struct {
	Name          string         `json:"name"`
	Namespace     string         `json:"namespace,omitempty"`
	Type          config.JobType `json:"type,omitempty"`
	Status        string         `json:"status,omitempty"`
	Replicas      int32          `json:"replicas,omitempty"`
	ReadyReplicas int32          `json:"readyReplicas,omitempty"`
	LastAbnormal  string         `json:"lastAbnormal,omitempty"`
}
