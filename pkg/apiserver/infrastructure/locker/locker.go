/*
Copyright 2024 The Eruun Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package locker

import (
	"context"
	"errors"
)

// Common errors for distributed lock operations.
var (
	// ErrLockNotHeld is returned when trying to unlock or extend a lock that is not held.
	ErrLockNotHeld = errors.New("lock not held")

	// ErrLockAlreadyHeld is returned when trying to acquire a lock that is already held by this instance.
	ErrLockAlreadyHeld = errors.New("lock already held")

	// ErrLockAcquireFailed is returned when lock acquisition fails after retries.
	ErrLockAcquireFailed = errors.New("failed to acquire lock")

	// ErrLockExtendFailed is returned when lock extension/renewal fails.
	ErrLockExtendFailed = errors.New("failed to extend lock")

	// ErrLockerClosed is returned when operations are attempted on a closed locker.
	ErrLockerClosed = errors.New("locker is closed")
)

// Locker creates distributed mutex locks.
// Production uses Redis; in-memory and no-op implementations support tests.
type Locker interface {
	// NewMutex creates a new distributed mutex for the given key.
	// Options can be used to customize lock behavior (TTL, retry settings, etc.).
	NewMutex(key string, opts ...Option) Mutex

	// Close releases any underlying resources held by the locker.
	// After Close is called, NewMutex may return errors or degraded locks.
	Close() error
}

// Mutex represents a distributed mutual exclusion lock.
// Implementations must be safe for concurrent use by multiple goroutines.
type Mutex interface {
	// Lock acquires the lock, blocking until it's available or ctx is cancelled.
	// Returns nil on success, or an error if:
	//   - ctx is cancelled or times out
	//   - lock acquisition fails after configured retries
	//   - the underlying backend is unavailable
	Lock(ctx context.Context) error

	// TryLock attempts to acquire the lock without blocking.
	// Returns nil if the lock was acquired, ErrLockAcquireFailed if it's held by another,
	// or another error if the operation failed.
	TryLock(ctx context.Context) error

	// Unlock releases the lock.
	// Returns ErrLockNotHeld if this instance doesn't hold the lock.
	Unlock(ctx context.Context) error

	// Extend extends the lock's TTL, preventing it from expiring.
	// This is useful for long-running operations that need to hold the lock
	// beyond the initial TTL.
	// Returns ErrLockNotHeld if this instance doesn't hold the lock.
	Extend(ctx context.Context) error

	// Key returns the key/name of this mutex.
	Key() string
}
