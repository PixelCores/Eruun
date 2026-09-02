package config

import (
	"encoding/base64"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

func TestNewConfigHasSequentialConcurrencyDefault(t *testing.T) {
	cfg := NewConfig()
	require.Equal(t, "127.0.0.1:8000", cfg.BindAddr)
	require.Equal(t, 1, cfg.Workflow.SequentialMaxConcurrency)
	require.Equal(t, DefaultWorkflowCallbackTimeoutMax, cfg.Workflow.CallbackTimeoutMax)
	require.Equal(t, 1, cfg.Messaging.KafkaTopicPartitions)
	require.Equal(t, 1, cfg.Messaging.KafkaTopicReplicationFactor)
	require.Equal(t, 15*time.Second, cfg.LeaderConfig.Duration)
	require.Equal(t, RuntimeRoleAPI, cfg.Role)
	require.Equal(t, 100, cfg.Workflow.MaxConcurrentWorkflows)
	require.Equal(t, 10*time.Second, cfg.Workflow.HeartbeatInterval)
	require.Equal(t, 30*time.Second, cfg.Workflow.LeaseDuration)
	require.Equal(t, 60*time.Second, cfg.Workflow.WorkerDrainTimeout)
	require.Zero(t, cfg.APIRateLimitQPS)
	require.Zero(t, cfg.APIRateLimitBurst)
}

func TestWorkflowLeaseFencingHasNoDisableFlag(t *testing.T) {
	cfg := NewConfig()
	flags := pflag.NewFlagSet("workflow-fencing", pflag.ContinueOnError)
	cfg.AddFlags(flags, cfg)

	require.Nil(t, flags.Lookup("workflow-lease-fencing-enabled"))
}

func TestRuntimeRoleCapabilities(t *testing.T) {
	for _, tc := range []struct {
		role                               RuntimeRole
		api, controller, scheduler, worker bool
	}{
		{RuntimeRoleAPI, true, false, false, false},
		{RuntimeRoleController, false, true, false, false},
		{RuntimeRoleScheduler, false, false, true, false},
		{RuntimeRoleWorker, false, false, false, true},
	} {
		cfg := NewConfig()
		cfg.Role = tc.role
		require.Equal(t, tc.api, cfg.RunsAPI())
		require.Equal(t, tc.controller, cfg.RunsController())
		require.Equal(t, tc.scheduler, cfg.RunsScheduler())
		require.Equal(t, tc.worker, cfg.RunsWorker())
	}
}

func TestApplyEnvOverridesParsesRuntimeRole(t *testing.T) {
	cfg := NewConfig()
	flags := pflag.NewFlagSet("runtime-role", pflag.ContinueOnError)
	cfg.AddFlags(flags, cfg)
	require.NoError(t, os.Setenv("ERUUN_ROLE", "worker"))
	t.Cleanup(func() { require.NoError(t, os.Unsetenv("ERUUN_ROLE")) })

	require.NoError(t, ApplyEnvOverrides(flags, EnvPrefix))
	require.Equal(t, RuntimeRoleWorker, cfg.Role)
	require.True(t, cfg.RunsWorker())
	require.False(t, cfg.RunsAPI())
}

func TestApplyEnvOverridesRejectsInvalidRuntimeRole(t *testing.T) {
	cfg := NewConfig()
	flags := pflag.NewFlagSet("runtime-role", pflag.ContinueOnError)
	cfg.AddFlags(flags, cfg)
	require.NoError(t, os.Setenv("ERUUN_ROLE", "unknown"))
	t.Cleanup(func() { require.NoError(t, os.Unsetenv("ERUUN_ROLE")) })

	err := ApplyEnvOverrides(flags, EnvPrefix)
	require.ErrorContains(t, err, "invalid runtime role")
}

func TestValidateRuntimeRoleAndLeaseWindow(t *testing.T) {
	cfg := NewConfig()
	cfg.Datastore.URL = "root:strong-pass@tcp(127.0.0.1:3306)/eruun?charset=utf8&parseTime=true"
	cfg.Role = "invalid"
	cfg.Workflow.LeaseDuration = cfg.Workflow.HeartbeatInterval
	errs := errorsJoin(cfg.Validate())
	require.Contains(t, errs, "runtime role must be one of")
	require.Contains(t, errs, "lease duration must be greater than heartbeat interval")
}

func TestValidateRuntimeLeaderLockNames(t *testing.T) {
	tests := []struct {
		name       string
		controller string
		scheduler  string
		want       string
	}{
		{name: "valid DNS subdomains", controller: "controller.runtime.example", scheduler: "scheduler.runtime.example"},
		{name: "same name", controller: "shared", scheduler: "shared", want: "lock names must be distinct"},
		{name: "empty controller", controller: "", scheduler: "scheduler", want: "controller leader election lock name cannot be empty"},
		{name: "invalid controller syntax", controller: "Controller_BAD", scheduler: "scheduler", want: "controller leader election lock name must be a valid DNS-1123 subdomain"},
		{name: "scheduler surrounding whitespace", controller: "controller", scheduler: " scheduler", want: "scheduler leader election lock name must not contain leading or trailing whitespace"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := NewConfig()
			cfg.Datastore.URL = "root:strong-pass@tcp(127.0.0.1:3306)/eruun?charset=utf8&parseTime=true"
			cfg.LeaderConfig.ControllerLockName = tc.controller
			cfg.LeaderConfig.SchedulerLockName = tc.scheduler

			errs := errorsJoin(cfg.Validate())
			if tc.want == "" {
				require.Empty(t, errs)
				return
			}
			require.Contains(t, errs, tc.want)
		})
	}
}

func TestNewConfigHasNoDefaultDatastoreURL(t *testing.T) {
	cfg := NewConfig()
	require.Equal(t, MYSQL, cfg.Datastore.Type)
	require.Empty(t, strings.TrimSpace(cfg.Datastore.URL))
}

func TestValidateImportSecretKeyring(t *testing.T) {
	validKey := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))
	document := fmt.Sprintf(`{"activeKeyId":"active","keys":{"active":%q}}`, validKey)

	t.Run("valid inline", func(t *testing.T) {
		cfg := NewConfig()
		cfg.Datastore.URL = "root:strong-pass@tcp(127.0.0.1:3306)/eruun?charset=utf8&parseTime=true"
		cfg.ImportSecretKeyring = document
		require.Empty(t, cfg.Validate())
	})

	t.Run("inline and file are mutually exclusive", func(t *testing.T) {
		cfg := NewConfig()
		cfg.Datastore.URL = "root:strong-pass@tcp(127.0.0.1:3306)/eruun?charset=utf8&parseTime=true"
		cfg.ImportSecretKeyring = document
		cfg.ImportSecretKeyringFile = "/mounted/keyring.json"
		require.Contains(t, errorsJoin(cfg.Validate()), "mutually exclusive")
	})

	t.Run("invalid key length fails startup validation", func(t *testing.T) {
		cfg := NewConfig()
		cfg.Datastore.URL = "root:strong-pass@tcp(127.0.0.1:3306)/eruun?charset=utf8&parseTime=true"
		cfg.ImportSecretKeyring = `{"activeKeyId":"active","keys":{"active":"YWJj"}}`
		require.Contains(t, errorsJoin(cfg.Validate()), "must decode to 32 bytes")
	})
}

func TestValidateSequentialConcurrency(t *testing.T) {
	t.Run("invalid_zero", func(t *testing.T) {
		cfg := NewConfig()
		cfg.Datastore.URL = "root:strong-pass@tcp(127.0.0.1:3306)/eruun?charset=utf8&parseTime=true"
		cfg.Workflow.SequentialMaxConcurrency = 0
		errs := cfg.Validate()
		require.NotEmpty(t, errs)
	})

	t.Run("valid_positive", func(t *testing.T) {
		cfg := NewConfig()
		cfg.Datastore.URL = "root:strong-pass@tcp(127.0.0.1:3306)/eruun?charset=utf8&parseTime=true"
		cfg.Workflow.SequentialMaxConcurrency = 4
		errs := cfg.Validate()
		require.Empty(t, errs)
	})
}

func TestValidateWorkflowCallbackTimeoutMax(t *testing.T) {
	t.Run("invalid_zero", func(t *testing.T) {
		cfg := NewConfig()
		cfg.Datastore.URL = "root:strong-pass@tcp(127.0.0.1:3306)/eruun?charset=utf8&parseTime=true"
		cfg.Workflow.CallbackTimeoutMax = 0
		errs := cfg.Validate()
		require.Contains(t, errorsJoin(errs), "workflow callback timeout max must be > 0")
	})

	t.Run("valid_positive", func(t *testing.T) {
		cfg := NewConfig()
		cfg.Datastore.URL = "root:strong-pass@tcp(127.0.0.1:3306)/eruun?charset=utf8&parseTime=true"
		cfg.Workflow.CallbackTimeoutMax = 72 * time.Hour
		errs := cfg.Validate()
		require.Empty(t, errs)
	})
}

func TestValidateDatastoreURLForMySQL(t *testing.T) {
	t.Run("mysql_empty_url", func(t *testing.T) {
		cfg := NewConfig()
		cfg.Datastore.URL = ""

		errs := cfg.Validate()
		require.Contains(t, errorsJoin(errs), "mysql url cannot be empty")
	})

	t.Run("mysql_placeholder_url", func(t *testing.T) {
		cfg := NewConfig()
		cfg.Datastore.URL = "__REPLACE_WITH_SECURE_DATASTORE_URL__"

		errs := cfg.Validate()
		require.Contains(t, errorsJoin(errs), "mysql url contains placeholder value")
	})

	t.Run("mysql_valid_url", func(t *testing.T) {
		cfg := NewConfig()
		cfg.Datastore.URL = "root:strong-pass@tcp(127.0.0.1:3306)/eruun?charset=utf8&parseTime=true"

		errs := cfg.Validate()
		require.Empty(t, errs)
	})
}

func TestValidateWorkflowTaskRunLockerRequiresRedisCacheType(t *testing.T) {
	cfg := NewConfig()
	cfg.Datastore.URL = "root:strong-pass@tcp(127.0.0.1:3306)/eruun?charset=utf8&parseTime=true"
	cfg.Cache.CacheType = "memory"

	errs := cfg.Validate()

	require.Contains(t, errorsJoin(errs), "workflow task run locker requires cache-type=redis")
}

func TestValidateWorkflowTaskRunLockerTrimsRedisCacheType(t *testing.T) {
	cfg := NewConfig()
	cfg.Datastore.URL = "root:strong-pass@tcp(127.0.0.1:3306)/eruun?charset=utf8&parseTime=true"
	cfg.Cache.CacheType = " redis "

	errs := cfg.Validate()

	require.Empty(t, errs)
}

func TestValidateAPIRateLimit(t *testing.T) {
	base := NewConfig()
	base.Datastore.URL = "root:strong-pass@tcp(127.0.0.1:3306)/eruun?charset=utf8&parseTime=true"

	t.Run("disabled_default", func(t *testing.T) {
		cfg := *base
		errs := cfg.Validate()
		require.Empty(t, errs)
	})

	t.Run("invalid_negative_qps", func(t *testing.T) {
		cfg := *base
		cfg.APIRateLimitQPS = -1
		errs := cfg.Validate()
		require.Contains(t, errorsJoin(errs), "api rate limit qps must be finite and >= 0; use 0 to disable")
	})

	t.Run("disabled_ignores_burst", func(t *testing.T) {
		cfg := *base
		cfg.APIRateLimitBurst = 1
		errs := cfg.Validate()
		require.Empty(t, errs)
	})

	t.Run("invalid_enabled_without_burst", func(t *testing.T) {
		cfg := *base
		cfg.APIRateLimitQPS = 10
		errs := cfg.Validate()
		require.Contains(t, errorsJoin(errs), "api rate limit burst must be > 0 when api rate limit qps is enabled")
	})

	t.Run("valid_enabled", func(t *testing.T) {
		cfg := *base
		cfg.APIRateLimitQPS = 10
		cfg.APIRateLimitBurst = 20
		errs := cfg.Validate()
		require.Empty(t, errs)
	})
}

func TestValidateLeaderLeaseDuration(t *testing.T) {
	base := NewConfig()
	base.Datastore.URL = "root:strong-pass@tcp(127.0.0.1:3306)/eruun?charset=utf8&parseTime=true"

	t.Run("rejects_below_minimum", func(t *testing.T) {
		cfg := *base
		cfg.LeaderConfig.Duration = 3 * time.Second
		errs := cfg.Validate()
		require.Contains(t, errorsJoin(errs), "leader election lease duration must be >= 4s")
	})

	t.Run("allows_minimum", func(t *testing.T) {
		cfg := *base
		cfg.LeaderConfig.Duration = minLeaderLeaseDuration
		errs := cfg.Validate()
		require.Empty(t, errs)
	})

	t.Run("allows_default", func(t *testing.T) {
		cfg := *base
		errs := cfg.Validate()
		require.Empty(t, errs)
	})

	t.Run("allows_explicit_long_duration", func(t *testing.T) {
		cfg := *base
		cfg.LeaderConfig.Duration = 5 * time.Minute
		errs := cfg.Validate()
		require.Empty(t, errs)
	})
}

func TestValidateKafkaTopicAutoCreateConfig(t *testing.T) {
	base := NewConfig()
	base.Datastore.URL = "root:strong-pass@tcp(127.0.0.1:3306)/eruun?charset=utf8&parseTime=true"
	base.Messaging.Type = "kafka"
	base.Messaging.KafkaBrokers = []string{"127.0.0.1:9092"}

	t.Run("invalid_partitions", func(t *testing.T) {
		cfg := *base
		cfg.Messaging.KafkaTopicPartitions = 0
		errs := cfg.Validate()
		require.Contains(t, errorsJoin(errs), "kafka topic partitions must be > 0")
	})

	t.Run("invalid_replication_factor", func(t *testing.T) {
		cfg := *base
		cfg.Messaging.KafkaTopicReplicationFactor = 0
		errs := cfg.Validate()
		require.Contains(t, errorsJoin(errs), "kafka topic replication factor must be > 0")
	})

	t.Run("valid_kafka_topic_auto_create_config", func(t *testing.T) {
		cfg := *base
		cfg.Messaging.KafkaTopicPartitions = 3
		cfg.Messaging.KafkaTopicReplicationFactor = 2
		errs := cfg.Validate()
		require.Empty(t, errs)
	})
}

func errorsJoin(errs []error) string {
	var b strings.Builder
	for i, err := range errs {
		if i > 0 {
			b.WriteString("; ")
		}
		b.WriteString(err.Error())
	}
	return b.String()
}
