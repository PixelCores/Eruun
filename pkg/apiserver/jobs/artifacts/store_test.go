package artifacts

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	sqldriver "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
)

func testStore(t *testing.T) (*Store, *sqldriver.Driver) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&model.Workspace{}, &model.WorkflowQueue{}, &model.JobArtifact{}, &model.ArtifactChunk{}, &model.JobDelivery{}))
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	driver := &sqldriver.Driver{Client: *db}
	store, err := New(driver, nil)
	require.NoError(t, err)
	for _, id := range []string{"space-a", "space-b"} {
		require.NoError(t, driver.Add(context.Background(), &model.Workspace{ID: id, Namespace: id}))
	}
	require.NoError(t, driver.Add(context.Background(), &model.WorkflowQueue{TaskID: "task-a", WorkspaceID: "space-a"}))
	return store, driver
}

func resultBytes(t *testing.T) []byte {
	return archiveBytes(t, testEntry{name: "result.json", content: `{"collectionComplete":true,"evaluationStatus":"succeeded"}`}, testEntry{name: "outputs/trial/trajectory.json", content: `[{"reward":1}]`})
}
func policy(targets ...spec.JobResultTarget) spec.JobResultPolicy {
	return spec.JobResultPolicy{RetentionDays: 90, Targets: targets}
}
func byTarget(t *testing.T, s *Store) map[string]*model.JobDelivery {
	t.Helper()
	records, err := s.Deliveries(context.Background(), "space-a", "task-a")
	require.NoError(t, err)
	result := map[string]*model.JobDelivery{}
	for _, r := range records {
		result[r.Target] = r
	}
	return result
}

type fakeObjects struct {
	mu     sync.Mutex
	fail   bool
	writes int
	data   map[string][]byte
	onPut  func()
}

func (m *fakeObjects) Put(ctx context.Context, key string, r io.Reader, _ int64, _ string) (string, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return "", err
	}
	m.mu.Lock()
	m.writes++
	failed := m.fail
	if !failed {
		if m.data == nil {
			m.data = map[string][]byte{}
		}
		m.data[key] = b
	}
	m.mu.Unlock()
	if m.onPut != nil {
		m.onPut()
	}
	if failed {
		return "", errors.New("test destination unavailable")
	}
	return key, nil
}
func (m *fakeObjects) Get(_ context.Context, key string, w io.Writer) error {
	m.mu.Lock()
	b, ok := m.data[key]
	m.mu.Unlock()
	if !ok {
		return datastore.ErrRecordNotExist
	}
	_, err := w.Write(b)
	return err
}

func TestDatasetAndResultIsolationImmutablePublication(t *testing.T) {
	s, db := testStore(t)
	ctx := context.Background()
	data := archiveBytes(t, taskEntries("")...)
	dataset, err := s.UploadDataset(ctx, "space-a", "demo", bytes.NewReader(data))
	require.NoError(t, err)
	require.Nil(t, dataset.ExpiresAt)
	var download bytes.Buffer
	require.NoError(t, s.Download(ctx, "space-a", dataset.ID, &download))
	require.Equal(t, data, download.Bytes())
	_, err = s.Get(ctx, "space-b", dataset.ID)
	require.ErrorIs(t, err, datastore.ErrRecordNotExist)
	err = s.Download(ctx, "space-b", dataset.ID, io.Discard)
	require.ErrorIs(t, err, datastore.ErrRecordNotExist)
	_, err = s.UploadDataset(ctx, "missing", "demo", bytes.NewReader(data))
	require.ErrorIs(t, err, datastore.ErrRecordNotExist)
	result := resultBytes(t)
	p := spec.DefaultJobResultPolicy()
	source, err := s.PutResult(ctx, "space-a", "task-a", p, bytes.NewReader(result))
	require.NoError(t, err)
	require.WithinDuration(t, time.Now().Add(90*24*time.Hour), *source.ExpiresAt, time.Second)
	again, err := s.PutResult(ctx, "space-a", "task-a", p, bytes.NewReader(result))
	require.NoError(t, err)
	require.Equal(t, source.ID, again.ID)
	_, err = s.PutResult(ctx, "space-a", "task-a", p, bytes.NewReader(archiveBytes(t, testEntry{name: "different", content: "x"})))
	require.ErrorIs(t, err, ErrConflict)
	_, err = s.PutResult(ctx, "space-b", "task-a", p, bytes.NewReader(result))
	require.ErrorIs(t, err, datastore.ErrRecordNotExist)
	count, err := db.Count(ctx, &model.JobDelivery{WorkspaceID: "space-a", TaskID: "task-a"}, nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	require.NoError(t, s.SetRetention(ctx, "space-a", "task-a", 30))
	source, err = s.Get(ctx, "space-a", source.ID)
	require.NoError(t, err)
	require.WithinDuration(t, time.Now().Add(30*24*time.Hour), *source.ExpiresAt, time.Second)
}

func TestPartialFailureRetryAndSourceExpiryPreserveCopies(t *testing.T) {
	s, db := testStore(t)
	ctx := context.Background()
	objects := &fakeObjects{fail: true}
	s.objects = objects
	data := resultBytes(t)
	p := policy(spec.JobResultTarget{Type: "minio", Mode: "full"}, spec.JobResultTarget{Type: "database", Mode: "full"})
	source, err := s.PutResult(ctx, "space-a", "task-a", p, bytes.NewReader(data))
	require.NoError(t, err)
	require.NoError(t, s.ReconcilePending(ctx, 10))
	statuses := byTarget(t, s)
	require.Equal(t, DeliveryFailed, statuses["minio"].State)
	require.Equal(t, DeliverySucceeded, statuses["database"].State)
	require.NotEmpty(t, statuses["minio"].LastError)
	require.NoError(t, s.ReconcilePending(ctx, 10))
	require.Equal(t, 1, objects.writes, "failed deliveries need an explicit retry")
	objects.fail = false
	require.NoError(t, s.Retry(ctx, "space-a", "task-a", "minio"))
	require.NoError(t, s.Retry(ctx, "space-a", "task-a", "minio"))
	require.NoError(t, s.ReconcilePending(ctx, 10))
	statuses = byTarget(t, s)
	require.Equal(t, DeliverySucceeded, statuses["minio"].State)
	require.Equal(t, 2, statuses["minio"].Attempts)
	require.Equal(t, 1, statuses["database"].Attempts)
	require.NoError(t, s.Retry(ctx, "space-a", "task-a", "database"))
	require.NoError(t, s.ReconcilePending(ctx, 10))
	require.Equal(t, 2, objects.writes)
	_, err = db.CompareAndSwap(ctx, source, "expired", false, map[string]interface{}{"expires_at": time.Now().UTC().Add(-time.Hour)})
	require.NoError(t, err)
	require.NoError(t, s.CleanupExpired(ctx, 10))
	require.ErrorIs(t, s.Download(ctx, "space-a", source.ID, io.Discard), ErrSourceExpired)
	for _, target := range []string{"minio", "database"} {
		var out bytes.Buffer
		require.NoError(t, s.DownloadDelivery(ctx, "space-a", "task-a", target, &out))
		require.Equal(t, data, out.Bytes())
		require.ErrorIs(t, s.DownloadDelivery(ctx, "space-b", "task-a", target, io.Discard), datastore.ErrRecordNotExist)
	}
	source, err = s.Get(ctx, "space-a", source.ID)
	require.NoError(t, err)
	require.True(t, source.Expired)
	require.NotEmpty(t, source.Summary)
	copies, err := s.List(ctx, "space-a", KindDatabase, "task-a")
	require.NoError(t, err)
	require.Len(t, copies, 1)
	require.Nil(t, copies[0].ExpiresAt)
}

func TestMetadataCopyRequiresSuccessfulFullMinIO(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	objects := &fakeObjects{fail: true}
	s.objects = objects
	p := policy(spec.JobResultTarget{Type: "database", Mode: "metadata"}, spec.JobResultTarget{Type: "minio", Mode: "full"})
	_, err := s.PutResult(ctx, "space-a", "task-a", p, bytes.NewReader(resultBytes(t)))
	require.NoError(t, err)
	require.NoError(t, s.ReconcilePending(ctx, 10))
	statuses := byTarget(t, s)
	require.Equal(t, DeliveryFailed, statuses["database"].State)
	require.Contains(t, statuses["database"].LastError, "successful MinIO")
	objects.fail = false
	require.NoError(t, s.Retry(ctx, "space-a", "task-a", "database"))
	require.NoError(t, s.Retry(ctx, "space-a", "task-a", "minio"))
	require.NoError(t, s.ReconcilePending(ctx, 10))
	statuses = byTarget(t, s)
	require.Equal(t, DeliverySucceeded, statuses["database"].State)
	copy, err := s.Get(ctx, "space-a", statuses["database"].Reference)
	require.NoError(t, err)
	require.Zero(t, copy.Chunks)
	require.NotEmpty(t, copy.Reference)
	var out bytes.Buffer
	require.NoError(t, s.DownloadDelivery(ctx, "space-a", "task-a", "database", &out))
	require.Equal(t, resultBytes(t), out.Bytes())
}

func TestExpiredSourceRejectsRetryAndGuardRollsBack(t *testing.T) {
	s, db := testStore(t)
	ctx := context.Background()
	p := policy(spec.JobResultTarget{Type: "minio", Mode: "full"})
	rejected := errors.New("stale execution token")
	_, err := s.PutResultGuarded(ctx, "space-a", "task-a", p, bytes.NewReader(resultBytes(t)), func(datastore.DataStore) error { return rejected })
	require.ErrorIs(t, err, rejected)
	count, err := db.Count(ctx, &model.JobArtifact{WorkspaceID: "space-a", Kind: KindSource}, nil)
	require.NoError(t, err)
	require.Zero(t, count)
	source, err := s.PutResult(ctx, "space-a", "task-a", p, bytes.NewReader(resultBytes(t)))
	require.NoError(t, err)
	require.NoError(t, s.ReconcilePending(ctx, 10))
	require.Equal(t, DeliveryFailed, byTarget(t, s)["minio"].State)
	_, err = db.CompareAndSwap(ctx, source, "expired", false, map[string]interface{}{"expires_at": time.Now().UTC().Add(-time.Hour)})
	require.NoError(t, err)
	require.ErrorIs(t, s.Retry(ctx, "space-a", "task-a", "minio"), ErrSourceExpired)
	require.ErrorIs(t, s.SetRetention(ctx, "space-a", "task-a", 90), ErrSourceExpired)
}

func TestDeliveryRecoveryFencesStaleCompletion(t *testing.T) {
	s, db := testStore(t)
	ctx := context.Background()
	objects := &fakeObjects{}
	s.objects = objects
	_, err := s.PutResult(ctx, "space-a", "task-a", policy(spec.JobResultTarget{Type: "minio", Mode: "full"}), bytes.NewReader(resultBytes(t)))
	require.NoError(t, err)
	d := byTarget(t, s)["minio"]
	objects.onPut = func() {
		_, err := db.CompareAndSwap(ctx, d, "state", DeliveryRunning, map[string]interface{}{"lease_token": "new-worker", "lease_until": time.Now().UTC().Add(time.Minute)})
		require.NoError(t, err)
	}
	require.NoError(t, s.ReconcilePending(ctx, 10))
	current := byTarget(t, s)["minio"]
	require.Equal(t, DeliveryRunning, current.State)
	require.Equal(t, "new-worker", current.LeaseToken)
	require.Empty(t, current.Reference)
	objects.onPut = nil
	_, err = db.CompareAndSwap(ctx, d, "state", DeliveryRunning, map[string]interface{}{"lease_until": time.Now().UTC().Add(-time.Minute)})
	require.NoError(t, err)
	require.NoError(t, s.ReconcilePending(ctx, 10))
	current = byTarget(t, s)["minio"]
	require.Equal(t, DeliverySucceeded, current.State)
	require.Equal(t, 2, current.Attempts)
}

func TestDatabaseChunkedCopyRetainsExactArchive(t *testing.T) {
	s, db := testStore(t)
	ctx := context.Background()
	payload := make([]byte, 3*ChunkSize+77)
	_, err := rand.New(rand.NewSource(1)).Read(payload)
	require.NoError(t, err)
	data := archiveBytes(t, testEntry{name: "outputs/artifact.bin", content: string(payload)})
	source, err := s.PutResult(ctx, "space-a", "task-a", spec.DefaultJobResultPolicy(), bytes.NewReader(data))
	require.NoError(t, err)
	require.Greater(t, source.Chunks, 3)
	chunks, err := db.List(ctx, &model.ArtifactChunk{ArtifactID: source.ID}, nil)
	require.NoError(t, err)
	for _, chunk := range chunks {
		require.LessOrEqual(t, len(chunk.(*model.ArtifactChunk).Data), ChunkSize)
	}
	require.NoError(t, s.ReconcilePending(ctx, 10))
	var out bytes.Buffer
	require.NoError(t, s.DownloadDelivery(ctx, "space-a", "task-a", "database", &out))
	require.Equal(t, data, out.Bytes())
	// Persisted chunk corruption must not be silently accepted as a valid archive.
	first := chunks[0].(*model.ArtifactChunk)
	_, err = db.CompareAndSwap(ctx, first, "artifact_id", source.ID, map[string]interface{}{"data": []byte("broken")})
	require.NoError(t, err)
	require.ErrorContains(t, s.Download(ctx, "space-a", source.ID, io.Discard), "integrity mismatch")
}

func TestStorageRejectsMissingScopeAndInvalidPolicy(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	_, err := s.List(ctx, "", KindDataset, "")
	require.Error(t, err)
	_, err = s.Deliveries(ctx, "space-a", "")
	require.Error(t, err)
	_, err = s.PutResult(ctx, "space-a", "task-a", policy(spec.JobResultTarget{Type: "database", Mode: "metadata"}), bytes.NewReader(nil))
	require.Error(t, err)
	for _, limit := range []int{0, 1001} {
		require.Error(t, s.CleanupExpired(ctx, limit))
		require.Error(t, s.ReconcilePending(ctx, limit))
	}
	_, err = New(nil, nil)
	require.Error(t, err)
	for _, cfg := range []*spec.MinIOConfig{{}, {Endpoint: "https://bad.example", Bucket: "results", AccessKey: "example", SecretKey: "example"}} {
		_, err = New(s.db, cfg)
		require.Error(t, err)
		require.NotContains(t, fmt.Sprint(err), "bad.example")
	}
}

func TestDatasetPaginationIsStableAndScoped(t *testing.T) {
	s, db := testStore(t)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		require.NoError(t, db.Add(ctx, &model.JobArtifact{ID: fmt.Sprintf("dataset-%d", i), WorkspaceID: "space-a", Kind: KindDataset}))
	}
	require.NoError(t, db.Add(ctx, &model.JobArtifact{ID: "other-dataset", WorkspaceID: "space-b", Kind: KindDataset}))
	var ids []string
	for page := 1; page <= 3; page++ {
		rows, err := s.ListPage(ctx, "space-a", KindDataset, "", page, 2)
		require.NoError(t, err)
		for _, row := range rows {
			ids = append(ids, row.ID)
		}
	}
	require.Equal(t, []string{"dataset-4", "dataset-3", "dataset-2", "dataset-1", "dataset-0"}, ids)
	for _, args := range [][2]int{{0, 1}, {1, 0}, {1, 101}} {
		_, err := s.ListPage(ctx, "space-a", KindDataset, "", args[0], args[1])
		require.ErrorIs(t, err, ErrInvalidInput)
	}
	_, err := s.UploadDataset(ctx, "space-a", "", bytes.NewReader(nil))
	require.ErrorIs(t, err, ErrInvalidInput)
}
