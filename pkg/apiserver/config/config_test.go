package config

import (
	"fmt"
	"strings"
	"testing"
	"time"

	mysqldsn "github.com/go-sql-driver/mysql"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"

	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

func TestNewConfigHasSequentialConcurrencyDefault(t *testing.T) {
	cfg := NewConfig()
	require.Equal(t, "127.0.0.1:8001", cfg.BindAddr)
	require.Equal(t, "127.0.0.1:9001", cfg.GRPCBindAddr)
	require.Equal(t, 1, cfg.Workflow.SequentialMaxConcurrency)
	require.Equal(t, workflowconfig.DefaultWorkflowCallbackTimeoutMax, cfg.Workflow.CallbackTimeoutMax)
	require.Equal(t, 15*time.Second, cfg.LeaderConfig.Duration)
	require.Equal(t, "eruun-runtime", cfg.LeaderConfig.LockName)
	require.Empty(t, cfg.LeaderConfig.ServiceName)
	require.Empty(t, cfg.LeaderConfig.PodName)
	require.NotEqual(t, cfg.LeaderConfig.ID, NewConfig().LeaderConfig.ID)
	require.Equal(t, DatastoreSchemaModeMigrate, cfg.DatastoreSchemaMode)
	require.Equal(t, 100, cfg.Workflow.MaxConcurrentWorkflows)
	require.Equal(t, 10*time.Second, cfg.Workflow.HeartbeatInterval)
	require.Equal(t, 30*time.Second, cfg.Workflow.LeaseDuration)
	require.Equal(t, 100, cfg.Workflow.LeaseReaperBatchSize)
	require.Equal(t, 60*time.Second, cfg.Workflow.WorkerDrainTimeout)
	require.Zero(t, cfg.APIRateLimitQPS)
	require.Zero(t, cfg.APIRateLimitBurst)
}

func TestGRPCBindAddrFlagEnvironmentAndValidation(t *testing.T) {
	cfg := NewConfig()
	flags := pflag.NewFlagSet("grpc-address", pflag.ContinueOnError)
	cfg.AddFlags(flags, cfg)
	t.Setenv("ERUUN_GRPC_BIND_ADDR", "127.0.0.1:9500")
	require.NoError(t, flags.Parse(nil))
	require.NoError(t, ApplyEnvOverrides(flags, EnvPrefix))
	require.Equal(t, "127.0.0.1:9500", cfg.GRPCBindAddr)

	cfg.GRPCBindAddr = cfg.BindAddr
	require.Contains(t, errorsJoin(cfg.Validate()), "grpc bind address must differ from http")
	cfg.GRPCBindAddr = ""
	require.Contains(t, errorsJoin(cfg.Validate()), "grpc bind address cannot be empty")
}

func TestTracingFlagEnvironmentAndExporter(t *testing.T) {
	for _, tc := range []struct {
		name    string
		env     string
		args    []string
		want    bool
		invalid bool
	}{
		{name: "default enabled", want: true},
		{name: "environment enables", env: "true", want: true},
		{name: "environment disables", env: "false"},
		{name: "CLI disables over environment", env: "true", args: []string{"--enable-tracing=false"}},
		{name: "CLI enables over environment", env: "false", args: []string{"--enable-tracing=true"}, want: true},
		{name: "invalid environment", env: "invalid", invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, endpoint := range []string{"", "http://127.0.0.1:14268/api/traces"} {
				cfg := NewConfig()
				flags := pflag.NewFlagSet("tracing", pflag.ContinueOnError)
				cfg.AddFlags(flags, cfg)
				if tc.env != "" {
					t.Setenv("ERUUN_ENABLE_TRACING", tc.env)
				}
				t.Setenv("ERUUN_JAEGER_ENDPOINT", endpoint)
				require.NoError(t, flags.Parse(tc.args))
				err := ApplyEnvOverrides(flags, EnvPrefix)
				if tc.invalid {
					require.ErrorContains(t, err, "ERUUN_ENABLE_TRACING")
					continue
				}
				require.NoError(t, err)
				require.Equal(t, tc.want, cfg.EnableTracing)
				require.Equal(t, endpoint, cfg.JaegerEndpoint)
				cfg.Datastore.URL = "root:strong-pass@tcp(127.0.0.1:3306)/eruun?charset=utf8&parseTime=true"
				require.Empty(t, cfg.Validate())
				require.Equal(t, tc.want, cfg.EnableTracing)
			}
		})
	}
}

func TestRemovedAutoTracingInputsAreRejected(t *testing.T) {
	cfg := NewConfig()
	flags := pflag.NewFlagSet("removed-tracing-flag", pflag.ContinueOnError)
	cfg.AddFlags(flags, cfg)
	require.ErrorContains(t, flags.Parse([]string{"--auto-tracing=true"}), "unknown flag: --auto-tracing")
	for _, value := range []string{"", "false", "true"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("ERUUN_AUTO_TRACING", value)
			require.NoError(t, flags.Parse([]string{"--enable-tracing=false"}))
			err := ApplyEnvOverrides(flags, EnvPrefix)
			require.ErrorContains(t, err, "ERUUN_AUTO_TRACING is no longer supported")
			require.ErrorContains(t, err, "--enable-tracing or ERUUN_ENABLE_TRACING")
		})
	}
}

func TestWorkflowCallbackTimeoutConfigBoundary(t *testing.T) {
	tests := []struct {
		name string
		cfg  *Config
		want time.Duration
	}{
		{name: "nil config", want: 72 * time.Hour},
		{name: "zero config", cfg: &Config{}, want: 72 * time.Hour},
		{name: "negative max", cfg: &Config{Workflow: workflowconfig.RuntimeConfig{CallbackTimeoutMax: -time.Second}}, want: 72 * time.Hour},
		{name: "custom max", cfg: &Config{Workflow: workflowconfig.RuntimeConfig{CallbackTimeoutMax: 500 * time.Millisecond}}, want: 500 * time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, workflowconfig.ResolveWorkflowCallbackTimeoutMax(tt.cfg.WorkflowRuntime()))
		})
	}
}

func TestWorkflowConfigFlagAndEnvironmentOverrides(t *testing.T) {
	cfg := NewConfig()
	flags := pflag.NewFlagSet("workflow-config", pflag.ContinueOnError)
	cfg.AddFlags(flags, cfg)
	t.Setenv("ERUUN_WORKFLOW_MAX_CONCURRENT", "25")
	t.Setenv("ERUUN_WORKFLOW_CALLBACK_TIMEOUT_MAX", "45m")
	t.Setenv("ERUUN_WORKFLOW_LEASE_DURATION", "20s")
	t.Setenv("ERUUN_WORKFLOW_LEASE_REAPER_BATCH_SIZE", "2000")
	require.NoError(t, flags.Parse([]string{"--workflow-max-concurrent=7"}))
	require.NoError(t, ApplyEnvOverrides(flags, EnvPrefix))

	require.Equal(t, 7, cfg.Workflow.MaxConcurrentWorkflows)
	require.Equal(t, 45*time.Minute, cfg.Workflow.CallbackTimeoutMax)
	require.Equal(t, 20*time.Second, cfg.Workflow.LeaseDuration)
	require.Equal(t, 2000, cfg.Workflow.LeaseReaperBatchSize)
	require.Equal(t, time.Minute, cfg.Workflow.DefaultJobTimeout)
	require.Empty(t, cfg.Workflow.Validate())
}

func TestValidateDatastoreSchemaMode(t *testing.T) {
	for _, mode := range []string{DatastoreSchemaModeMigrate, DatastoreSchemaModeValidate, DatastoreSchemaModeMigrateOnly, " migrate-only "} {
		t.Run(mode, func(t *testing.T) {
			cfg := NewConfig()
			cfg.Datastore.URL = "root:strong-pass@tcp(127.0.0.1:3306)/eruun?charset=utf8&parseTime=true"
			cfg.DatastoreSchemaMode = mode
			require.Empty(t, cfg.Validate())
		})
	}

	cfg := NewConfig()
	cfg.Datastore.URL = "root:strong-pass@tcp(127.0.0.1:3306)/eruun?charset=utf8&parseTime=true"
	cfg.DatastoreSchemaMode = "unsafe"
	require.Contains(t, errorsJoin(cfg.Validate()), "datastore schema mode must be one of")
}

func TestMigrateOnlyValidatesOnlyDatastoreInputs(t *testing.T) {
	cfg := NewConfig()
	cfg.DatastoreSchemaMode = DatastoreSchemaModeMigrateOnly
	cfg.Datastore.URL = "root:strong-pass@tcp(127.0.0.1:3306)/eruun?charset=utf8&parseTime=true"
	cfg.BindAddr = ""
	cfg.Cache.CacheHost = ""
	cfg.Workflow = workflowconfig.RuntimeConfig{}

	require.Empty(t, cfg.Validate())
	require.True(t, cfg.MigrateSchemaOnly())
}

func TestWorkflowLeaseFencingHasNoDisableFlag(t *testing.T) {
	cfg := NewConfig()
	flags := pflag.NewFlagSet("workflow-fencing", pflag.ContinueOnError)
	cfg.AddFlags(flags, cfg)

	require.Nil(t, flags.Lookup("workflow-lease-fencing-enabled"))
}

func TestRemovedRuntimeConfigurationRejected(t *testing.T) {
	for _, removed := range []string{"role", "controller-lock-name", "scheduler-lock-name", "exit-on-lost-leader"} {
		t.Run(removed, func(t *testing.T) {
			cfg := NewConfig()
			flags := pflag.NewFlagSet("removed-runtime", pflag.ContinueOnError)
			cfg.AddFlags(flags, cfg)
			require.ErrorContains(t, flags.Parse([]string{"--" + removed + "=value"}), "unknown flag")
			key := buildEnvKey(EnvPrefix, removed)
			for _, value := range []string{"", "value"} {
				t.Setenv(key, value)
				require.ErrorContains(t, ApplyEnvOverrides(flags, EnvPrefix), key+" is no longer supported")
			}
		})
	}
}

func TestLeaderConfigurationOverridesAndValidation(t *testing.T) {
	cfg := NewConfig()
	cfg.Datastore.URL = "root:strong-pass@tcp(127.0.0.1:3306)/eruun?parseTime=true"
	flags := pflag.NewFlagSet("leader-config", pflag.ContinueOnError)
	cfg.AddFlags(flags, cfg)
	t.Setenv("ERUUN_LEADER_LOCK_NAME", "runtime-env")
	t.Setenv("ERUUN_LEADER_SERVICE_NAME", "eruun")
	require.NoError(t, flags.Parse([]string{"--leader-lock-name=runtime-cli", "--pod-name=eruun-runtime-abc"}))
	require.NoError(t, ApplyEnvOverrides(flags, EnvPrefix))
	require.Equal(t, "runtime-cli", cfg.LeaderConfig.LockName)
	require.Equal(t, "eruun", cfg.LeaderConfig.ServiceName)
	require.Empty(t, cfg.Validate())
	for _, tc := range []struct {
		name   string
		change func(*Config)
		want   string
	}{
		{"empty lock", func(c *Config) { c.LeaderConfig.LockName = "" }, "lock name cannot be empty"},
		{"invalid lock", func(c *Config) { c.LeaderConfig.LockName = "Bad_Name" }, "lock name must be a valid DNS-1123 subdomain"},
		{"whitespace lock", func(c *Config) { c.LeaderConfig.LockName = " runtime" }, "lock name must not contain leading or trailing whitespace"},
		{"invalid service", func(c *Config) { c.LeaderConfig.ServiceName = "bad.service" }, "service name must be a valid DNS-1035 label"},
		{"empty identity", func(c *Config) { c.LeaderConfig.ID = "" }, "identity cannot be empty"},
		{"empty Pod name", func(c *Config) { c.LeaderConfig.PodName = "" }, "pod name must be a valid DNS-1123 subdomain"},
		{"invalid Pod name", func(c *Config) { c.LeaderConfig.PodName = "Invalid_Pod" }, "pod name must be a valid DNS-1123 subdomain"},
		{"empty namespace", func(c *Config) { c.LeaderConfig.Namespace = "" }, "namespace must be a valid DNS-1123 label"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			copy := *cfg
			tc.change(&copy)
			require.Contains(t, errorsJoin(copy.Validate()), tc.want)
		})
	}
}

func TestValidateWorkflowLeaseWindow(t *testing.T) {
	cfg := NewConfig()
	cfg.Workflow.LeaseDuration = cfg.Workflow.HeartbeatInterval
	require.Contains(t, errorsJoin(cfg.Validate()), "lease duration must be greater than heartbeat interval")
}

func TestNewConfigHasMySQLDefaults(t *testing.T) {
	cfg := NewConfig()
	require.Equal(t, "eruun:__REPLACE_WITH_MYSQL_PASSWORD__@tcp(127.0.0.1:3306)/eruun?charset=utf8mb4&parseTime=true", cfg.Datastore.URL)

	flags := pflag.NewFlagSet("connection-defaults", pflag.ContinueOnError)
	cfg.AddFlags(flags, cfg)
	require.Equal(t, cfg.Datastore.URL, flags.Lookup("datastore-url").DefValue)

	for _, mode := range []string{DatastoreSchemaModeMigrate, DatastoreSchemaModeValidate, DatastoreSchemaModeMigrateOnly} {
		t.Run(mode, func(t *testing.T) {
			cfg.DatastoreSchemaMode = mode
			require.Contains(t, errorsJoin(cfg.Validate()), "mysql url contains placeholder value")
		})
	}
}

func TestConnectionConfigFlagAndEnvironmentOverrides(t *testing.T) {
	const envDSN = "eruun:test-env@tcp(mysql-env.example:3306)/from-env?parseTime=true"
	const cliDSN = "eruun:test-cli@tcp(mysql-cli.example:3307)/from-flag?parseTime=true"
	for _, tc := range []struct {
		name     string
		args     []string
		dsn      string
		database string
	}{
		{name: "environment overrides defaults", dsn: envDSN, database: "from-env"},
		{name: "flags override environment", args: []string{"--datastore-url=" + cliDSN}, dsn: cliDSN, database: "from-flag"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := NewConfig()
			flags := pflag.NewFlagSet("connection-overrides", pflag.ContinueOnError)
			cfg.AddFlags(flags, cfg)
			t.Setenv("ERUUN_DATASTORE_URL", envDSN)
			require.NoError(t, flags.Parse(tc.args))
			require.NoError(t, ApplyEnvOverrides(flags, EnvPrefix))

			require.Equal(t, tc.dsn, cfg.Datastore.URL)
			dsn, err := mysqldsn.ParseDSN(cfg.Datastore.URL)
			require.NoError(t, err)
			require.Equal(t, tc.database, dsn.DBName)
			require.Empty(t, cfg.Validate())
		})
	}
}

func TestRemovedDatastoreDatabaseInputsAreRejected(t *testing.T) {
	t.Run("flag", func(t *testing.T) {
		cfg := NewConfig()
		flags := pflag.NewFlagSet("removed-database-flag", pflag.ContinueOnError)
		cfg.AddFlags(flags, cfg)
		err := flags.Parse([]string{"--datastore-database=unused"})
		require.ErrorContains(t, err, "unknown flag: --datastore-database")
	})

	for _, value := range []string{"", "unused-database-value"} {
		for _, explicitDSN := range []bool{false, true} {
			t.Run(fmt.Sprintf("environment/value=%q/explicitDSN=%t", value, explicitDSN), func(t *testing.T) {
				cfg := NewConfig()
				flags := pflag.NewFlagSet("removed-database-env", pflag.ContinueOnError)
				cfg.AddFlags(flags, cfg)
				if explicitDSN {
					require.NoError(t, flags.Parse([]string{"--datastore-url=eruun:test-only@tcp(localhost:3306)/from-dsn"}))
				}
				t.Setenv("ERUUN_DATASTORE_DATABASE", value)
				err := ApplyEnvOverrides(flags, EnvPrefix)
				require.ErrorContains(t, err, "ERUUN_DATASTORE_DATABASE is no longer supported")
				require.ErrorContains(t, err, "remove it and set the database name in --datastore-url or ERUUN_DATASTORE_URL")
				require.NotContains(t, err.Error(), "unused-database-value")
				require.NotContains(t, err.Error(), "test-only")
			})
		}
	}
}

func TestValidateDatastoreInputsInEverySchemaMode(t *testing.T) {
	for _, mode := range []string{DatastoreSchemaModeMigrate, DatastoreSchemaModeValidate, DatastoreSchemaModeMigrateOnly} {
		for _, tc := range []struct {
			name string
			dsn  string
			want string
		}{
			{name: "mysql", dsn: "eruun:test-only@tcp(localhost:3306)/custom-db"},
			{name: "empty DSN", want: "mysql url cannot be empty"},
			{name: "placeholder DSN", dsn: NewConfig().Datastore.URL, want: "mysql url contains placeholder value"},
		} {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				cfg := NewConfig()
				cfg.DatastoreSchemaMode = mode
				cfg.Datastore.URL = tc.dsn
				errs := errorsJoin(cfg.Validate())
				if tc.want == "" {
					require.Empty(t, errs)
				} else {
					require.Contains(t, errs, tc.want)
				}
			})
		}
	}
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

func TestRemovedBackendSelectorsRejected(t *testing.T) {
	for _, selector := range []string{
		"datastore-type", "cache-type", "msg-type",
		"import-secret-keyring", "import-secret-keyring-file",
		"msg-kafka-brokers", "msg-kafka-group-id", "msg-kafka-offset-reset",
		"msg-kafka-topic-partitions", "msg-kafka-topic-replication-factor",
	} {
		t.Run(selector, func(t *testing.T) {
			cfg := NewConfig()
			flags := pflag.NewFlagSet("removed-backends", pflag.ContinueOnError)
			cfg.AddFlags(flags, cfg)
			require.ErrorContains(t, flags.Parse([]string{"--" + selector + "=value"}), "unknown flag")
			for _, value := range []string{"", "mysql", "redis"} {
				t.Setenv(buildEnvKey(EnvPrefix, selector), value)
				require.ErrorContains(t, ApplyEnvOverrides(flags, EnvPrefix), "is no longer supported")
			}
		})
	}
}
