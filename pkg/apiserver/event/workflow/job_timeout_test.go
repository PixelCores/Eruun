package workflow

import (
	"math"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/stretchr/testify/require"
)

const testJobTimeoutSeconds int64 = 1200

func TestDefaultJobTimeoutUsesBoundedWholeSeconds(t *testing.T) {
	for _, tt := range []struct {
		name     string
		duration time.Duration
		want     int64
	}{
		{"subsecond", 500 * time.Millisecond, 1},
		{"fractional", 1500 * time.Millisecond, 2},
		{"exact", time.Minute, 60},
		{"maximum duration", time.Duration(math.MaxInt64), math.MaxInt64 / int64(time.Second)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.Workflow.DefaultJobTimeout = tt.duration
			seconds := resolveDefaultJobTimeout(cfg)
			require.Equal(t, tt.want, seconds)
			require.Positive(t, time.Duration(seconds)*time.Second)
			task := NewJobTask("job", "namespace", "workflow", "project", "app", "task", seconds)
			require.Equal(t, tt.want, task.Timeout)
		})
	}
}
