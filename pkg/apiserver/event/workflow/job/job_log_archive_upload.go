package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/kube"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

var ErrArchiveUploaderNotConfigured = errors.New("archive uploader is not configured")

// ArchiveUploader uploads a zip archive produced from a Pod path.
type ArchiveUploader interface {
	UploadArchive(ctx context.Context, input ArchiveUploadInput) (*ArchiveUploadResult, error)
}

type ArchiveUploadInput struct {
	Reader        io.Reader
	FileName      string
	ContentType   string
	AppID         string
	WorkflowID    string
	TaskID        string
	ComponentName string
	Namespace     string
	PodName       string
	ContainerName string
	Path          string
}

type ArchiveUploadResult struct {
	URL       string
	FileName  string
	SizeBytes int64
}

var (
	archiveUploaderMu       sync.RWMutex
	defaultArchiveUploader  ArchiveUploader
	archivePodPathForUpload archivePodPathFunc = kube.ArchivePodPathAsZip
)

type archivePodPathFunc func(context.Context, kubernetes.Interface, *rest.Config, string, string, string, string) (*kube.PodPathArchiveStream, error)

// SetArchiveUploader configures the default uploader used by workflow archive jobs.
func SetArchiveUploader(uploader ArchiveUploader) {
	archiveUploaderMu.Lock()
	defer archiveUploaderMu.Unlock()
	defaultArchiveUploader = uploader
}

func currentArchiveUploader() ArchiveUploader {
	archiveUploaderMu.RLock()
	defer archiveUploaderMu.RUnlock()
	return defaultArchiveUploader
}

type LogArchiveUploadJobInfo struct {
	Component *model.ApplicationComponent `json:"component"`
	Path      string                      `json:"path"`
	Container string                      `json:"container,omitempty"`
}

type LogArchiveUploadJobResult struct {
	ArchiveURL    string `json:"archiveUrl"`
	ComponentName string `json:"componentName"`
	Namespace     string `json:"namespace"`
	PodName       string `json:"podName"`
	ContainerName string `json:"containerName"`
	Path          string `json:"path"`
	FileName      string `json:"fileName"`
	SizeBytes     int64  `json:"sizeBytes,omitempty"`
}

type LogArchiveUploadJobCtl struct {
	deployNamespacedResourceJobBase
	runtime *jobRuntime
}

type logArchiveUploadTarget struct {
	Namespace     string
	PodName       string
	ContainerName string
}

func NewLogArchiveUploadJobCtl(job *model.JobTask, client kubernetes.Interface, store datastore.DataStore, ack func()) *LogArchiveUploadJobCtl {
	base, ok := newDeployNamespacedResourceJobBase("LogArchiveUploadJobCtl", job, client, store, ack, nil)
	if !ok {
		return nil
	}
	return &LogArchiveUploadJobCtl{deployNamespacedResourceJobBase: base}
}

func (c *LogArchiveUploadJobCtl) setRuntime(runtime *jobRuntime) {
	if c == nil {
		return
	}
	c.runtime = runtime
	c.deployNamespacedResourceJobBase.setRuntime(runtime)
}

func (c *LogArchiveUploadJobCtl) Clean(context.Context) {}

func (c *LogArchiveUploadJobCtl) Run(ctx context.Context) error {
	return c.runWithStatus(ctx, c.run, "log archive upload job run error")
}

func (c *LogArchiveUploadJobCtl) run(ctx context.Context) error {
	if c.client == nil {
		return fmt.Errorf("client is nil")
	}
	if c.store == nil {
		return fmt.Errorf("store is nil")
	}
	info, err := requiredJobInfo[*LogArchiveUploadJobInfo](c.job)
	if err != nil {
		return err
	}
	targetPath := strings.TrimSpace(info.Path)
	if targetPath == "" {
		return fmt.Errorf("log archive upload path is required")
	}
	if c.runtime == nil || c.runtime.kubeConfig == nil {
		return fmt.Errorf("kube config is nil")
	}
	uploader := c.runtime.archiveUploader
	if uploader == nil {
		return ErrArchiveUploaderNotConfigured
	}

	component := normalizeLogArchiveUploadComponent(info.Component)
	if strings.TrimSpace(component.AppID) == "" || strings.TrimSpace(component.Name) == "" {
		return fmt.Errorf("log archive upload component identity is incomplete")
	}
	target, err := c.resolveLogArchiveUploadTarget(ctx, component, info.Container)
	if err != nil {
		return err
	}

	archive, err := archivePodPathForUpload(ctx, c.client, c.runtime.kubeConfig, target.Namespace, target.PodName, target.ContainerName, targetPath)
	if err != nil {
		return err
	}
	if archive == nil || archive.Reader == nil {
		return fmt.Errorf("log archive stream is empty")
	}
	defer archive.Close()
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(archive.ContentType)), "application/zip") {
		return fmt.Errorf("log archive upload requires zip archive, got content type %q", archive.ContentType)
	}
	fileName := strings.TrimSpace(archive.FileName)
	if fileName == "" {
		fileName = fmt.Sprintf("%s.zip", component.Name)
	}

	result, err := uploader.UploadArchive(ctx, ArchiveUploadInput{
		Reader:        archive.Reader,
		FileName:      fileName,
		ContentType:   archive.ContentType,
		AppID:         c.job.AppID,
		WorkflowID:    c.job.WorkflowID,
		TaskID:        c.job.TaskID,
		ComponentName: component.Name,
		Namespace:     target.Namespace,
		PodName:       target.PodName,
		ContainerName: target.ContainerName,
		Path:          targetPath,
	})
	if err != nil {
		return err
	}
	if result == nil || strings.TrimSpace(result.URL) == "" {
		return fmt.Errorf("archive uploader returned empty result")
	}
	resultFileName := strings.TrimSpace(result.FileName)
	if resultFileName == "" {
		resultFileName = fileName
	}

	payload := LogArchiveUploadJobResult{
		ArchiveURL:    strings.TrimSpace(result.URL),
		ComponentName: component.Name,
		Namespace:     target.Namespace,
		PodName:       target.PodName,
		ContainerName: target.ContainerName,
		Path:          targetPath,
		FileName:      resultFileName,
		SizeBytes:     result.SizeBytes,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal log archive upload result: %w", err)
	}
	c.job.Info = string(data)
	return nil
}

func (c *LogArchiveUploadJobCtl) resolveLogArchiveUploadTarget(ctx context.Context, component *model.ApplicationComponent, requestedContainer string) (*logArchiveUploadTarget, error) {
	namespace := strings.TrimSpace(component.Namespace)
	if namespace == "" {
		namespace = config.DefaultNamespace
	}
	pods, err := kube.ListPodsByLabels(ctx, c.client, namespace, labels.Set{
		config.LabelAppID:         component.AppID,
		config.LabelComponentName: naming.BoundedLabelValue(component.Name),
	})
	if err != nil {
		return nil, err
	}
	if pods == nil {
		return nil, fmt.Errorf("component %q pod is unavailable", component.Name)
	}
	pod, state := kube.SelectComponentLogPod(pods.Items)
	switch state {
	case kube.ComponentLogPodPending:
		return nil, fmt.Errorf("component %q pod is pending scheduling", component.Name)
	case kube.ComponentLogPodCompleted:
		return nil, fmt.Errorf("component %q pod is completed", component.Name)
	case kube.ComponentLogPodUnavailable:
		return nil, fmt.Errorf("component %q pod is unavailable", component.Name)
	}
	if pod == nil {
		return nil, fmt.Errorf("component %q pod is unavailable", component.Name)
	}

	requestedContainer = strings.TrimSpace(requestedContainer)
	if requestedContainer != "" && !kube.HasContainerName(pod, requestedContainer) {
		return nil, fmt.Errorf("container %q not found in pod %s/%s", requestedContainer, namespace, pod.Name)
	}
	containerPreference := component.Name
	if requestedContainer != "" {
		containerPreference = requestedContainer
	}
	containerName := kube.SelectContainerName(pod, containerPreference)
	if containerName == "" {
		return nil, fmt.Errorf("component %q pod has no selectable container", component.Name)
	}
	return &logArchiveUploadTarget{
		Namespace:     namespace,
		PodName:       pod.Name,
		ContainerName: containerName,
	}, nil
}

func normalizeLogArchiveUploadComponent(component *model.ApplicationComponent) *model.ApplicationComponent {
	if component == nil {
		return &model.ApplicationComponent{Namespace: config.DefaultNamespace}
	}
	cp := *component
	if strings.TrimSpace(cp.Namespace) == "" {
		cp.Namespace = config.DefaultNamespace
	}
	return &cp
}
