//go:build integration

package artifacts

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
	mysqlgorm "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	sqldriver "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
)

func TestMySQLConcurrentArtifactPublicationAndDelivery(t *testing.T) {
	dsn := os.Getenv("MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("MYSQL_TEST_DSN not set; requires an isolated MySQL test database")
	}
	db, err := gorm.Open(mysqlgorm.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&model.Workspace{}, &model.WorkflowQueue{}, &model.JobArtifact{}, &model.ArtifactChunk{}, &model.JobDelivery{}))
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(8)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	driver := &sqldriver.Driver{Client: *db}
	s, err := New(driver, nil)
	require.NoError(t, err)
	s.objects = &fakeObjects{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	workspace, task := uuid.NewString(), uuid.NewString()
	require.NoError(t, driver.Add(ctx, &model.Workspace{ID: workspace, Namespace: "test-" + workspace}))
	require.NoError(t, driver.Add(ctx, &model.WorkflowQueue{TaskID: task, WorkspaceID: workspace}))
	t.Cleanup(func() {
		for _, entity := range []datastore.Entity{&model.ArtifactChunk{WorkspaceID: workspace}, &model.JobArtifact{WorkspaceID: workspace}, &model.JobDelivery{WorkspaceID: workspace}, &model.WorkflowQueue{WorkspaceID: workspace}, &model.Workspace{ID: workspace}} {
			require.NoError(t, driver.DeleteByFilter(context.Background(), entity, nil))
		}
	})
	p := policy(spec.JobResultTarget{Type: "minio", Mode: "full"}, spec.JobResultTarget{Type: "database", Mode: "full"})
	data := resultBytes(t)
	const workers = 4
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.PutResult(ctx, workspace, task, p, bytes.NewReader(data))
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	count, err := driver.Count(ctx, &model.JobArtifact{WorkspaceID: workspace, Kind: KindSource}, nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	deliveries, err := s.Deliveries(ctx, workspace, task)
	require.NoError(t, err)
	require.Len(t, deliveries, 2)
	errs = make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- s.ReconcilePending(ctx, 10) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	deliveries, err = s.Deliveries(ctx, workspace, task)
	require.NoError(t, err)
	for _, delivery := range deliveries {
		require.Equal(t, DeliverySucceeded, delivery.State)
		require.Equal(t, 1, delivery.Attempts)
		var out bytes.Buffer
		require.NoError(t, s.DownloadDelivery(ctx, workspace, task, delivery.Target, &out))
		require.Equal(t, data, out.Bytes())
	}
	source, err := s.Get(ctx, workspace, sourceID(workspace, task))
	require.NoError(t, err)
	now, err := clock(ctx, driver)
	require.NoError(t, err)
	updated, err := driver.CompareAndSwap(ctx, source, "expired", false, map[string]interface{}{"expires_at": now.Add(-time.Second)})
	require.NoError(t, err)
	require.True(t, updated)
	require.NoError(t, s.CleanupExpired(ctx, 10))
	require.ErrorIs(t, s.Download(ctx, workspace, source.ID, io.Discard), ErrSourceExpired)
	for _, target := range []string{"database", "minio"} {
		require.NoError(t, s.DownloadDelivery(ctx, workspace, task, target, io.Discard), fmt.Sprintf("saved %s survives source expiry", target))
	}
}

// MINIO_TEST_CONFIG points to a mode-0600 JSON file containing a MinIOConfig.
// The test creates and removes its own bucket; it never uses an existing bucket.
func TestMySQLAndMinIOPreserveFullResultsAfterSourceExpiry(t *testing.T) {
	dsn, configPath := os.Getenv("MYSQL_TEST_DSN"), os.Getenv("MINIO_TEST_CONFIG")
	if dsn == "" || configPath == "" {
		t.Skip("MYSQL_TEST_DSN and MINIO_TEST_CONFIG not set; requires isolated MySQL and MinIO")
	}
	raw, err := os.ReadFile(configPath)
	require.NoError(t, err)
	var cfg spec.MinIOConfig
	require.NoError(t, json.Unmarshal(raw, &cfg))
	cfg.Bucket = "eruun-test-" + uuid.NewString()
	db, err := gorm.Open(mysqlgorm.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&model.Workspace{}, &model.WorkflowQueue{}, &model.JobArtifact{}, &model.ArtifactChunk{}, &model.JobDelivery{}))
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	driver := &sqldriver.Driver{Client: *db}
	s, err := New(driver, &cfg)
	require.NoError(t, err)
	objects := s.objects.(*minIOStore)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	workspace, task := uuid.NewString(), uuid.NewString()
	require.NoError(t, driver.Add(ctx, &model.Workspace{ID: workspace, Namespace: "test-" + workspace}))
	require.NoError(t, driver.Add(ctx, &model.WorkflowQueue{TaskID: task, WorkspaceID: workspace}))
	t.Cleanup(func() {
		for _, entity := range []datastore.Entity{&model.ArtifactChunk{WorkspaceID: workspace}, &model.JobArtifact{WorkspaceID: workspace}, &model.JobDelivery{WorkspaceID: workspace}, &model.WorkflowQueue{WorkspaceID: workspace}, &model.Workspace{ID: workspace}} {
			require.NoError(t, driver.DeleteByFilter(context.Background(), entity, nil))
		}
	})
	payload := make([]byte, 3*ChunkSize+77)
	_, err = rand.New(rand.NewSource(2)).Read(payload)
	require.NoError(t, err)
	data := archiveBytes(t, testEntry{name: "result.json", content: `{"collectionComplete":true}`}, testEntry{name: "outputs/artifact.bin", content: string(payload)})
	source, err := s.PutResult(ctx, workspace, task, policy(spec.JobResultTarget{Type: "database", Mode: "full"}, spec.JobResultTarget{Type: "minio", Mode: "full"}), bytes.NewReader(data))
	require.NoError(t, err)
	require.Greater(t, source.Chunks, 3)
	require.NoError(t, s.ReconcilePending(ctx, 10))
	deliveries, err := s.Deliveries(ctx, workspace, task)
	require.NoError(t, err)
	for _, delivery := range deliveries {
		if delivery.Target == "minio" {
			require.Equal(t, DeliveryFailed, delivery.State)
			require.NotEmpty(t, delivery.LastError)
		} else {
			require.Equal(t, DeliverySucceeded, delivery.State)
		}
	}
	require.NoError(t, objects.client.MakeBucket(ctx, cfg.Bucket, minio.MakeBucketOptions{}))
	t.Cleanup(func() {
		for object := range objects.client.ListObjects(context.Background(), cfg.Bucket, minio.ListObjectsOptions{Recursive: true}) {
			require.NoError(t, object.Err)
			require.NoError(t, objects.client.RemoveObject(context.Background(), cfg.Bucket, object.Key, minio.RemoveObjectOptions{}))
		}
		require.NoError(t, objects.client.RemoveBucket(context.Background(), cfg.Bucket))
	})
	require.NoError(t, s.Retry(ctx, workspace, task, "minio"))
	require.NoError(t, s.ReconcilePending(ctx, 10))
	deliveries, err = s.Deliveries(ctx, workspace, task)
	require.NoError(t, err)
	for _, delivery := range deliveries {
		require.Equal(t, DeliverySucceeded, delivery.State, delivery.LastError)
		if delivery.Target == "minio" {
			require.Equal(t, 2, delivery.Attempts)
		} else {
			require.Equal(t, 1, delivery.Attempts)
		}
	}
	now, err := clock(ctx, driver)
	require.NoError(t, err)
	updated, err := driver.CompareAndSwap(ctx, source, "expired", false, map[string]interface{}{"expires_at": now.Add(-time.Second)})
	require.NoError(t, err)
	require.True(t, updated)
	require.NoError(t, s.CleanupExpired(ctx, 10))
	require.ErrorIs(t, s.Download(ctx, workspace, source.ID, io.Discard), ErrSourceExpired)
	for _, target := range []string{"minio", "database"} {
		var out bytes.Buffer
		require.NoError(t, s.DownloadDelivery(ctx, workspace, task, target, &out))
		require.Equal(t, data, out.Bytes())
	}
	require.ErrorIs(t, s.DownloadDelivery(ctx, strings.ToUpper(workspace), task, "minio", io.Discard), datastore.ErrRecordNotExist)
	// A separate metadata-only destination retains its MinIO reference too.
	metadataTask := uuid.NewString()
	require.NoError(t, driver.Add(ctx, &model.WorkflowQueue{TaskID: metadataTask, WorkspaceID: workspace}))
	metadataData := resultBytes(t)
	metadataSource, err := s.PutResult(ctx, workspace, metadataTask, policy(spec.JobResultTarget{Type: "database", Mode: "metadata"}, spec.JobResultTarget{Type: "minio", Mode: "full"}), bytes.NewReader(metadataData))
	require.NoError(t, err)
	require.NoError(t, s.ReconcilePending(ctx, 10))
	_, err = driver.CompareAndSwap(ctx, metadataSource, "expired", false, map[string]interface{}{"expires_at": now.Add(-time.Second)})
	require.NoError(t, err)
	require.NoError(t, s.CleanupExpired(ctx, 10))
	var metadataOut bytes.Buffer
	require.NoError(t, s.DownloadDelivery(ctx, workspace, metadataTask, "database", &metadataOut))
	require.Equal(t, metadataData, metadataOut.Bytes())
}
