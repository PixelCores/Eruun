package application

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/kube"
)

var archiveComponentPodPathAsZip = kube.ArchivePodPathAsZip
var execComponentPodShellScript = kube.ExecPodShellScript
var streamComponentPodShellScript = kube.StreamPodShellScript

type componentPodTarget struct {
	Namespace     string
	PodName       string
	ContainerName string
}

// ComponentFileArchiveStream represents an archive stream from a component Pod.
type ComponentFileArchiveStream struct {
	Reader        io.ReadCloser
	Namespace     string
	PodName       string
	ContainerName string
	FileName      string
	ContentType   string
}

// ComponentShellScriptStream represents a shell execution event stream from a component Pod.
type ComponentShellScriptStream struct {
	Namespace     string
	PodName       string
	ContainerName string
	Events        <-chan kube.PodShellStreamEvent
}

func (s *ComponentFileArchiveStream) Close() error {
	if s == nil || s.Reader == nil {
		return nil
	}
	return s.Reader.Close()
}

func (c *applicationsServiceImpl) ExportComponentFilesZip(ctx context.Context, appID, componentName string, req apisv1.ExportComponentFilesRequest) (*ComponentFileArchiveStream, error) {
	req.Path = strings.TrimSpace(req.Path)
	if req.Path == "" {
		return nil, bcode.ErrComponentFilePathInvalid
	}
	if c.KubeConfig == nil {
		return nil, fmt.Errorf("kube config is nil")
	}

	target, err := c.resolveComponentPodTarget(ctx, appID, componentName, req.Container, bcode.ErrComponentContainerInvalid, bcode.ErrComponentPodUnavailable, false)
	if err != nil {
		return nil, err
	}
	archive, err := archiveComponentPodPathAsZip(ctx, c.KubeClient, c.KubeConfig, target.Namespace, target.PodName, target.ContainerName, req.Path)
	if err != nil {
		if kube.IsArchivePathInvalidError(err) || kube.IsArchivePathLookupError(err) {
			return nil, bcode.ErrComponentFilePathInvalid
		}
		return nil, err
	}
	if archive == nil || archive.Reader == nil {
		return nil, fmt.Errorf("archive stream is empty")
	}
	return &ComponentFileArchiveStream{
		Reader:        archive.Reader,
		Namespace:     target.Namespace,
		PodName:       target.PodName,
		ContainerName: target.ContainerName,
		FileName:      archive.FileName,
		ContentType:   archive.ContentType,
	}, nil
}

func (c *applicationsServiceImpl) ExecComponentShellScript(ctx context.Context, appID, componentName string, req apisv1.ExecComponentShellScriptRequest) (*apisv1.ExecComponentShellScriptResponse, error) {
	req.Script = strings.TrimSpace(req.Script)
	if req.Script == "" {
		return nil, bcode.ErrComponentShellScriptInvalid
	}
	var response *apisv1.ExecComponentShellScriptResponse
	_, err := c.withWritableApplicationLock(ctx, appID, "exec-component-shell-script", func(lockCtx context.Context, _ *model.Applications) error {
		var execErr error
		response, execErr = c.execComponentShellScriptLocked(lockCtx, appID, componentName, req)
		return execErr
	})
	if err != nil {
		return response, err
	}
	return response, nil
}

func (c *applicationsServiceImpl) execComponentShellScriptLocked(ctx context.Context, appID, componentName string, req apisv1.ExecComponentShellScriptRequest) (*apisv1.ExecComponentShellScriptResponse, error) {
	if c.KubeConfig == nil {
		return nil, fmt.Errorf("kube config is nil")
	}

	target, err := c.resolveComponentPodTarget(ctx, appID, componentName, req.Container, bcode.ErrComponentContainerInvalid, bcode.ErrComponentPodUnavailable, false)
	if err != nil {
		return nil, err
	}
	result, err := execComponentPodShellScript(ctx, c.KubeClient, c.KubeConfig, target.Namespace, target.PodName, target.ContainerName, req.Script)
	if err != nil {
		return nil, err
	}
	return &apisv1.ExecComponentShellScriptResponse{
		Namespace:     target.Namespace,
		PodName:       target.PodName,
		ContainerName: target.ContainerName,
		Stdout:        result.Stdout,
		Stderr:        result.Stderr,
		ExitCode:      result.ExitCode,
		Succeeded:     result.Succeeded(),
	}, nil
}

func (c *applicationsServiceImpl) StreamComponentShellScript(ctx context.Context, appID, componentName string, req apisv1.ExecComponentShellScriptRequest) (*ComponentShellScriptStream, error) {
	req.Script = strings.TrimSpace(req.Script)
	if req.Script == "" {
		return nil, bcode.ErrComponentShellScriptInvalid
	}
	var stream *ComponentShellScriptStream
	_, err := c.withWritableApplicationLock(ctx, appID, "stream-component-shell-script", func(lockCtx context.Context, _ *model.Applications) error {
		var streamErr error
		stream, streamErr = c.streamComponentShellScriptLocked(lockCtx, appID, componentName, req)
		return streamErr
	})
	if err != nil {
		return stream, err
	}
	return stream, nil
}

func (c *applicationsServiceImpl) streamComponentShellScriptLocked(ctx context.Context, appID, componentName string, req apisv1.ExecComponentShellScriptRequest) (*ComponentShellScriptStream, error) {
	if c.KubeConfig == nil {
		return nil, fmt.Errorf("kube config is nil")
	}

	target, err := c.resolveComponentPodTarget(ctx, appID, componentName, req.Container, bcode.ErrComponentContainerInvalid, bcode.ErrComponentPodUnavailable, false)
	if err != nil {
		return nil, err
	}
	events, err := streamComponentPodShellScript(ctx, c.KubeClient, c.KubeConfig, target.Namespace, target.PodName, target.ContainerName, req.Script)
	if err != nil {
		return nil, err
	}
	return &ComponentShellScriptStream{
		Namespace:     target.Namespace,
		PodName:       target.PodName,
		ContainerName: target.ContainerName,
		Events:        events,
	}, nil
}

func (c *applicationsServiceImpl) resolveComponentPodTarget(ctx context.Context, appID, componentName, requestedContainer string, invalidContainerErr, unavailableErr error, allowCompleted bool) (*componentPodTarget, error) {
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
	if component == nil {
		return nil, bcode.ErrComponentNotFound
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
	case kube.ComponentLogPodCompleted:
		if !allowCompleted {
			return nil, unavailableErr
		}
	case kube.ComponentLogPodUnavailable:
		return nil, unavailableErr
	}
	if pod == nil {
		return nil, unavailableErr
	}
	if requestedContainer != "" && !kube.HasContainerName(pod, requestedContainer) {
		return nil, invalidContainerErr
	}
	containerPreference := component.Name
	if requestedContainer != "" {
		containerPreference = requestedContainer
	}
	containerName := kube.SelectContainerName(pod, containerPreference)
	if containerName == "" {
		return nil, unavailableErr
	}
	return &componentPodTarget{
		Namespace:     namespace,
		PodName:       pod.Name,
		ContainerName: containerName,
	}, nil
}
