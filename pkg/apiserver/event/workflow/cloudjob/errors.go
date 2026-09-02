package cloudjob

import "errors"

var (
	ErrCloudProviderNotFound = errors.New("cloud provider is not registered")
	ErrCloudActionNotFound   = errors.New("cloud action is not registered")
)
