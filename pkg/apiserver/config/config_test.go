package config

import (
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
	"time"

	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	mysqldsn "github.com/go-sql-driver/mysql"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

func TestNewConfigHasSequentialConcurrencyDefault(t *testing.T) {
	cfg := NewConfig()
	require.Equal(t, "127.0.0.1:8001", cfg.BindAddr)
	require.Equal(t, "127.0.0.1:9001", cfg.GRPCBindAddr)
	require.Equal(t, 1, cfg.Workflow.SequentialMaxConcurrency)
	require.Equal(t, workflowconfig.DefaultWorkflowCallbackTimeoutMax, cfg.Workflow.CallbackTimeoutMax)
	require.Equal(t, 1, cfg.Messaging.KafkaTopicPartitions)
	require.Equal(t, 1, cfg.Messaging.KafkaTopicReplicationFactor)
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
				for _, backend := range []string{REDIS, KAFKA} {
					cfg.Messaging.Type = backend
					require.Empty(t, cfg.Validate())
					require.Equal(t, tc.want, cfg.EnableTracing)
				}
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
	require.Zero(t, cfg.Workflow.WorkerMaxReadFailures)
	require.Zero(t, cfg.Workflow.WorkerMaxClaimFailures)
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
	cfg.Messaging.Type = "invalid"
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

func TestRuntimeMessagingTopics(t *testing.T) {
	cfg := NewConfig()
	cfg.Messaging.ChannelPrefix = "tenant"
	require.Equal(t, []string{"tenant.workflow.dispatch", "tenant.job.delay"}, cfg.RuntimeMessagingTopics())
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

func TestNewConfigHasMySQLAndKafkaDefaults(t *testing.T) {
	cfg := NewConfig()
	require.Equal(t, "eruun:__REPLACE_WITH_MYSQL_PASSWORD__@tcp(127.0.0.1:3306)/eruun?charset=utf8mb4&parseTime=true", cfg.Datastore.URL)
	require.Equal(t, REDIS, cfg.Messaging.Type)
	require.Equal(t, []string{"localhost:9092"}, cfg.Messaging.KafkaBrokers)
	require.Equal(t, "eruun-workflow-workers", cfg.Messaging.KafkaGroupID)
	require.Equal(t, "earliest", cfg.Messaging.KafkaAutoOffsetReset)

	flags := pflag.NewFlagSet("connection-defaults", pflag.ContinueOnError)
	cfg.AddFlags(flags, cfg)
	require.Equal(t, cfg.Datastore.URL, flags.Lookup("datastore-url").DefValue)
	require.Equal(t, "[localhost:9092]", flags.Lookup("msg-kafka-brokers").DefValue)
	require.Equal(t, cfg.Messaging.KafkaGroupID, flags.Lookup("msg-kafka-group-id").DefValue)
	require.Equal(t, cfg.Messaging.KafkaAutoOffsetReset, flags.Lookup("msg-kafka-offset-reset").DefValue)

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
		brokers  []string
		group    string
		offset   string
	}{
		{
			name: "environment overrides defaults", dsn: envDSN, database: "from-env",
			brokers: []string{"kafka-env-1.example:9092", "kafka-env-2.example:9092"},
			group:   "env-workers", offset: "latest",
		},
		{
			name: "flags override environment",
			args: []string{
				"--datastore-url=" + cliDSN,
				"--msg-kafka-brokers=kafka-cli.example:9093",
				"--msg-kafka-group-id=cli-workers",
				"--msg-kafka-offset-reset=earliest",
			},
			dsn: cliDSN, database: "from-flag", brokers: []string{"kafka-cli.example:9093"},
			group: "cli-workers", offset: "earliest",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := NewConfig()
			flags := pflag.NewFlagSet("connection-overrides", pflag.ContinueOnError)
			cfg.AddFlags(flags, cfg)
			t.Setenv("ERUUN_DATASTORE_URL", envDSN)
			t.Setenv("ERUUN_MSG_KAFKA_BROKERS", "kafka-env-1.example:9092,kafka-env-2.example:9092")
			t.Setenv("ERUUN_MSG_KAFKA_GROUP_ID", "env-workers")
			t.Setenv("ERUUN_MSG_KAFKA_OFFSET_RESET", "latest")
			require.NoError(t, flags.Parse(tc.args))
			require.NoError(t, ApplyEnvOverrides(flags, EnvPrefix))

			require.Equal(t, tc.dsn, cfg.Datastore.URL)
			dsn, err := mysqldsn.ParseDSN(cfg.Datastore.URL)
			require.NoError(t, err)
			require.Equal(t, tc.database, dsn.DBName)
			require.Equal(t, tc.brokers, cfg.Messaging.KafkaBrokers)
			require.Equal(t, tc.group, cfg.Messaging.KafkaGroupID)
			require.Equal(t, tc.offset, cfg.Messaging.KafkaAutoOffsetReset)
			cfg.Messaging.Type = KAFKA
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

func TestFixedBackendsDoNotExposeTypeFlags(t *testing.T) {
	for _, name := range []string{"datastore-type", "cache-type"} {
		t.Run(name, func(t *testing.T) {
			cfg := NewConfig()
			flags := pflag.NewFlagSet("fixed-backends", pflag.ContinueOnError)
			cfg.AddFlags(flags, cfg)
			require.Nil(t, flags.Lookup(name))
			require.ErrorContains(t, flags.Parse([]string{"--" + name + "=unused"}), "unknown flag: --"+name)
		})
	}
}

func TestValidateRedisConnectionForEveryMessagingBackend(t *testing.T) {
	for _, backend := range []string{REDIS, KAFKA} {
		for _, tc := range []struct {
			name string
			host string
			port int
		}{
			{name: "empty host", port: 6379},
			{name: "blank host", host: " ", port: 6379},
			{name: "zero port", host: "localhost"},
			{name: "negative port", host: "localhost", port: -1},
		} {
			t.Run(backend+"/"+tc.name, func(t *testing.T) {
				cfg := NewConfig()
				cfg.Datastore.URL = "eruun:test-only@tcp(localhost:3306)/eruun?parseTime=true"
				cfg.Messaging.Type = backend
				cfg.Cache.CacheHost, cfg.Cache.CacheProt = tc.host, tc.port
				require.Contains(t, errorsJoin(cfg.Validate()), "redis cache host/port is invalid")
			})
		}
	}
}

func TestFixedBackendsKeepConnectionEnvironmentOverrides(t *testing.T) {
	cfg := NewConfig()
	flags := pflag.NewFlagSet("fixed-backend-connections", pflag.ContinueOnError)
	cfg.AddFlags(flags, cfg)
	t.Setenv("ERUUN_DATASTORE_URL", "eruun:test-only@tcp(database.example:3306)/custom?parseTime=true")
	t.Setenv("ERUUN_DATASTORE_SCHEMA_MODE", DatastoreSchemaModeValidate)
	t.Setenv("ERUUN_CACHE_HOST", "redis.example")
	t.Setenv("ERUUN_CACHE_PORT", "6380")
	t.Setenv("ERUUN_CACHE_DB", "3")
	require.NoError(t, ApplyEnvOverrides(flags, EnvPrefix))
	require.Equal(t, "eruun:test-only@tcp(database.example:3306)/custom?parseTime=true", cfg.Datastore.URL)
	require.Equal(t, DatastoreSchemaModeValidate, cfg.DatastoreSchemaMode)
	require.Equal(t, "redis.example", cfg.Cache.CacheHost)
	require.Equal(t, 6380, cfg.Cache.CacheProt)
	require.EqualValues(t, 3, cfg.Cache.CacheDB)
	require.Empty(t, cfg.Validate())
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

	t.Run("valid_defaults", func(t *testing.T) {
		require.Empty(t, base.Validate())
	})

	t.Run("explicit_empty_brokers", func(t *testing.T) {
		cfg := *base
		flags := pflag.NewFlagSet("empty-kafka-brokers", pflag.ContinueOnError)
		cfg.AddFlags(flags, &cfg)
		require.NoError(t, flags.Parse([]string{"--msg-kafka-brokers="}))
		require.Empty(t, cfg.Messaging.KafkaBrokers)
		require.Contains(t, errorsJoin(cfg.Validate()), "kafka brokers cannot be empty")
	})

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
