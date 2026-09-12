package artifacts

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

type objectStore interface {
	Put(context.Context, string, io.Reader, int64, string) (string, error)
	Get(context.Context, string, io.Writer) error
}

type minIOStore struct {
	client           *minio.Client
	bucket, location string
}
type objectReference struct {
	Location string `json:"location"`
	Bucket   string `json:"bucket"`
	Key      string `json:"key"`
	Version  string `json:"version,omitempty"`
}

func newMinIO(cfg *spec.MinIOConfig) (*minIOStore, error) {
	if cfg.Endpoint == "" || cfg.Bucket == "" || cfg.AccessKey == "" || cfg.SecretKey == "" {
		return nil, fmt.Errorf("MinIO configuration is incomplete")
	}
	transport, err := minio.DefaultTransport(cfg.Secure)
	if err != nil {
		return nil, fmt.Errorf("initialize MinIO transport")
	}
	transport.ResponseHeaderTimeout = 30 * time.Second
	client, err := minio.New(cfg.Endpoint, &minio.Options{Creds: credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""), Secure: cfg.Secure, Region: cfg.Region, Transport: transport})
	if err != nil {
		return nil, fmt.Errorf("invalid MinIO connection configuration")
	}
	return &minIOStore{client: client, bucket: cfg.Bucket, location: stableID(cfg.Endpoint, cfg.Bucket, fmt.Sprint(cfg.Secure))}, nil
}
func (m *minIOStore) Put(ctx context.Context, key string, r io.Reader, size int64, digest string) (string, error) {
	info, err := m.client.PutObject(ctx, m.bucket, key, r, size, minio.PutObjectOptions{ContentType: "application/gzip", UserMetadata: map[string]string{"sha256": digest}})
	if err != nil {
		return "", fmt.Errorf("%w: MinIO write failed; verify destination availability and permissions", ErrDestinationUnavailable)
	}
	data, err := json.Marshal(objectReference{Location: m.location, Bucket: m.bucket, Key: key, Version: info.VersionID})
	if err != nil {
		return "", err
	}
	return string(data), nil
}
func (m *minIOStore) Get(ctx context.Context, reference string, w io.Writer) error {
	var ref objectReference
	if err := json.Unmarshal([]byte(reference), &ref); err != nil || ref.Location != m.location || ref.Bucket != m.bucket || ref.Key == "" {
		return fmt.Errorf("%w: saved MinIO location differs from configured destination", ErrDestinationUnavailable)
	}
	object, err := m.client.GetObject(ctx, ref.Bucket, ref.Key, minio.GetObjectOptions{VersionID: ref.Version})
	if err != nil {
		return fmt.Errorf("%w: MinIO read failed; verify destination availability and permissions", ErrDestinationUnavailable)
	}
	defer object.Close()
	if _, err = io.Copy(w, object); err != nil {
		return fmt.Errorf("%w: MinIO read failed; verify destination availability and permissions", ErrDestinationUnavailable)
	}
	return nil
}
