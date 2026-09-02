package approvaltimeout

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
)

type timerEntry struct {
	id     uint64
	cancel context.CancelFunc
}

var (
	timerSeq atomic.Uint64
	timers   sync.Map
)

// Register records a cancel function for a task approval timeout timer.
// If a timer already exists for the same task, the old one is cancelled.
func Register(taskID string, cancel context.CancelFunc) uint64 {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" || cancel == nil {
		return 0
	}
	id := timerSeq.Add(1)
	next := timerEntry{id: id, cancel: cancel}
	previous, loaded := timers.Swap(taskID, next)
	if !loaded {
		return id
	}
	entry, ok := previous.(timerEntry)
	if !ok || entry.cancel == nil {
		return id
	}
	entry.cancel()
	return id
}

// Unregister removes a timer only when the task still points to the same timer id.
func Unregister(taskID string, timerID uint64) {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" || timerID == 0 {
		return
	}
	current, ok := timers.Load(taskID)
	if !ok {
		return
	}
	entry, ok := current.(timerEntry)
	if !ok {
		timers.Delete(taskID)
		return
	}
	if entry.id == timerID {
		timers.Delete(taskID)
	}
}

// Cancel stops and removes the registered timer for a task.
func Cancel(taskID string) bool {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return false
	}
	current, ok := timers.LoadAndDelete(taskID)
	if !ok {
		return false
	}
	entry, ok := current.(timerEntry)
	if !ok || entry.cancel == nil {
		return false
	}
	entry.cancel()
	return true
}
