package job

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
)

type DatabaseResetJobInfo struct {
	DatabaseComponents []*model.ApplicationComponent
	// RestartComponents is retained for compatibility with legacy job payloads.
	// Database reset execution intentionally ignores this field.
	RestartComponents []*model.ApplicationComponent
	InitSQLURL        string
	ExecutionKey      string
}

const databaseResetReplicaCheckpointVersion = 2

type databaseResetReplicaCheckpoint struct {
	Version          int              `json:"version"`
	ExecutionKey     string           `json:"executionKey"`
	Prepared         bool             `json:"prepared"`
	OriginalReplicas map[string]int32 `json:"originalReplicas,omitempty"`
}

func preservePreparedDatabaseResetCheckpointForSave(job *model.JobTask, existing, next *model.JobInfo) error {
	executionKey := databaseResetExecutionKeyFromJob(job)
	if executionKey == "" {
		return fmt.Errorf("database reset execution key is missing")
	}
	if existing == nil || next == nil {
		return fmt.Errorf("database reset job info record is missing")
	}
	existingCheckpoint, err := decodeDatabaseResetReplicaCheckpoint(existing.InternalInfo)
	if err != nil {
		return fmt.Errorf("decode existing database reset replica checkpoint: %w", err)
	}
	if existingCheckpoint.ExecutionKey != executionKey {
		return fmt.Errorf("existing database reset replica checkpoint execution key %q does not match job execution key %q", existingCheckpoint.ExecutionKey, executionKey)
	}
	nextCheckpoint, err := decodeDatabaseResetReplicaCheckpoint(next.InternalInfo)
	if err != nil {
		return fmt.Errorf("decode next database reset replica checkpoint: %w", err)
	}
	if nextCheckpoint.ExecutionKey != executionKey {
		return fmt.Errorf("next database reset replica checkpoint execution key %q does not match job execution key %q", nextCheckpoint.ExecutionKey, executionKey)
	}
	if existingCheckpoint.Prepared && !nextCheckpoint.Prepared {
		next.InternalInfo = existing.InternalInfo
		job.InternalInfo = existing.InternalInfo
	}
	return nil
}

func decodeDatabaseResetReplicaCheckpoint(raw string) (*databaseResetReplicaCheckpoint, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("database reset replica checkpoint is empty")
	}
	var checkpoint databaseResetReplicaCheckpoint
	if err := json.Unmarshal([]byte(raw), &checkpoint); err != nil {
		return nil, fmt.Errorf("decode database reset replica checkpoint: %w", err)
	}
	if checkpoint.Version != databaseResetReplicaCheckpointVersion {
		return nil, fmt.Errorf("database reset replica checkpoint version %d is unsupported", checkpoint.Version)
	}
	checkpoint.ExecutionKey = strings.TrimSpace(checkpoint.ExecutionKey)
	if checkpoint.ExecutionKey == "" {
		return nil, fmt.Errorf("database reset replica checkpoint execution key is missing")
	}
	if checkpoint.Prepared && checkpoint.OriginalReplicas == nil {
		return nil, fmt.Errorf("database reset replica checkpoint original replicas are missing")
	}
	if !checkpoint.Prepared && checkpoint.OriginalReplicas != nil {
		return nil, fmt.Errorf("database reset replica checkpoint is not prepared but contains original replicas")
	}
	return &checkpoint, nil
}

func selectDatabaseResetCheckpointJobInfo(job *model.JobTask, candidates []*model.JobInfo) (*model.JobInfo, error) {
	executionKey := databaseResetExecutionKeyFromJob(job)
	if executionKey == "" {
		return nil, fmt.Errorf("database reset execution key is missing")
	}
	var matched *model.JobInfo
	for _, candidate := range candidates {
		if candidate == nil {
			continue
		}
		checkpoint, err := decodeDatabaseResetReplicaCheckpoint(candidate.InternalInfo)
		if err != nil {
			return nil, fmt.Errorf("inspect database reset replica checkpoint record %d: %w", candidate.ID, err)
		}
		if checkpoint.ExecutionKey != executionKey {
			continue
		}
		if matched != nil {
			return nil, fmt.Errorf("multiple database reset replica checkpoints found for execution key %q", executionKey)
		}
		matched = candidate
	}
	return matched, nil
}

func databaseResetExecutionKey(info *DatabaseResetJobInfo) string {
	if info == nil {
		return ""
	}
	return strings.TrimSpace(info.ExecutionKey)
}

func databaseResetExecutionKeyFromJob(job *model.JobTask) string {
	if job == nil {
		return ""
	}
	info, ok := job.JobInfo.(*DatabaseResetJobInfo)
	if !ok {
		return ""
	}
	return databaseResetExecutionKey(info)
}
