package application

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	corev1 "k8s.io/api/core/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/kube"
)

type ComponentLogStream struct {
	Reader        io.ReadCloser
	Namespace     string
	PodName       string
	ContainerName string
}

func (s *ComponentLogStream) Close() error {
	if s == nil || s.Reader == nil {
		return nil
	}
	return s.Reader.Close()
}

func (c *applicationsServiceImpl) StreamComponentLogs(ctx context.Context, appID, componentName, requestedContainer string) (*ComponentLogStream, error) {
	appID = strings.TrimSpace(appID)
	componentName = strings.TrimSpace(componentName)
	requestedContainer = strings.TrimSpace(requestedContainer)
	if appID == "" || componentName == "" {
		return nil, bcode.ErrComponentNotFound
	}
	if c.KubeClient == nil {
		return nil, fmt.Errorf("kube client is nil")
	}
	if c.ComponentRepo == nil {
		return nil, fmt.Errorf("component repository is nil")
	}
	component, err := c.ComponentRepo.FindByName(ctx, appID, componentName)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, bcode.ErrComponentNotFound
		}
		return nil, err
	}
	namespace := strings.TrimSpace(component.Namespace)
	if namespace == "" {
		namespace = config.DefaultNamespace
	}
	pods, err := c.listComponentPods(ctx, appID, component)
	if err != nil {
		return nil, err
	}
	pod, state := kube.SelectComponentLogPod(pods.Items)
	switch state {
	case kube.ComponentLogPodPending:
		return nil, bcode.ErrComponentPendingScheduling
	case kube.ComponentLogPodUnavailable:
		return nil, bcode.ErrComponentLogUnavailable
	}
	if pod == nil {
		return nil, bcode.ErrComponentLogUnavailable
	}
	if requestedContainer != "" && !kube.HasContainerName(pod, requestedContainer) {
		return nil, bcode.ErrComponentLogContainerInvalid
	}
	containerPreference := component.Name
	if requestedContainer != "" {
		containerPreference = requestedContainer
	}
	containerName := kube.SelectContainerName(pod, containerPreference)
	if containerName == "" {
		return nil, bcode.ErrComponentLogUnavailable
	}
	tailLines := config.DefaultComponentLogTailLines
	follow := state == kube.ComponentLogPodRunning
	options := &corev1.PodLogOptions{
		Container: containerName,
		Follow:    follow,
	}
	if tailLines > 0 {
		options.TailLines = &tailLines
	}
	stream, err := c.KubeClient.CoreV1().Pods(namespace).GetLogs(pod.Name, options).Stream(ctx)
	if err != nil {
		return nil, err
	}
	return &ComponentLogStream{
		Reader:        stream,
		Namespace:     namespace,
		PodName:       pod.Name,
		ContainerName: containerName,
	}, nil
}
