package application

import (
	"context"
	"io"

	corev1 "k8s.io/api/core/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
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
	target, err := c.resolveComponentPodTarget(ctx, appID, componentName, requestedContainer, bcode.ErrComponentLogContainerInvalid, bcode.ErrComponentLogUnavailable, true)
	if err != nil {
		return nil, err
	}

	tailLines := config.DefaultComponentLogTailLines
	options := &corev1.PodLogOptions{
		Container: target.ContainerName,
		Follow:    target.PodState == kube.ComponentLogPodRunning,
	}
	if tailLines > 0 {
		options.TailLines = &tailLines
	}
	stream, err := c.KubeClient.CoreV1().Pods(target.Namespace).GetLogs(target.PodName, options).Stream(ctx)
	if err != nil {
		return nil, err
	}
	return &ComponentLogStream{
		Reader:        stream,
		Namespace:     target.Namespace,
		PodName:       target.PodName,
		ContainerName: target.ContainerName,
	}, nil
}
