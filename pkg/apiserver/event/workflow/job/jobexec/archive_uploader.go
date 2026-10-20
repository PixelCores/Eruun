package jobexec

import (
	"context"
	"io"
)

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

// ArchiveUploader uploads a zip archive produced from a Pod path.
type ArchiveUploader interface {
	UploadArchive(ctx context.Context, input ArchiveUploadInput) (*ArchiveUploadResult, error)
}
