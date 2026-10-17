package informer

import "time"

// WithSyncTimeout bounds the initial informer snapshot wait.
func WithSyncTimeout(d time.Duration) ManagerOption {
	return func(m *Manager) {
		m.syncTimeout = d
	}
}
