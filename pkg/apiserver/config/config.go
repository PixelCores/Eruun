package config

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/pflag"
	k8svalidation "k8s.io/apimachinery/pkg/util/validation"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/security/importsecret"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/profiling"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

type leaderConfig struct {
	ID                 string
	ControllerLockName string
	SchedulerLockName  string
	Duration           time.Duration
	Namespace          string
}

const (
	defaultLeaderLeaseDuration     = 15 * time.Second
	minLeaderLeaseDuration         = 4 * time.Second
	DatastoreSchemaModeMigrate     = "migrate"
	DatastoreSchemaModeValidate    = "validate"
	DatastoreSchemaModeMigrateOnly = "migrate-only"
)

type Config struct {
	AuthConfigFile string
	Accounts       *spec.AccountConfig
	// Role selects the explicit runtime responsibility for this process.
	Role RuntimeRole

	// api server bind address
	BindAddr string

	// APIRateLimitQPS limits expensive API operations. Set 0 to disable it.
	APIRateLimitQPS float64

	// APIRateLimitBurst is the expensive-operation burst size when APIRateLimitQPS is enabled.
	APIRateLimitBurst int

	// AllowPrivateURLTargets controls whether outbound URL targets that resolve to
	// private/loopback/link-local addresses are allowed.
	AllowPrivateURLTargets bool

	//DTM Distributed transaction management
	DTMAddr string

	Datastore datastore.Config
	// DatastoreSchemaMode controls whether this process migrates, validates, or
	// migrates and exits before starting the runtime.
	DatastoreSchemaMode string

	Cache RedisCacheConfig

	// Istio Enable
	IstioEnable bool

	// EnableTracing enables distributed tracing
	EnableTracing bool

	// AutoTracing, when true and EnableTracing is false, auto-enables tracing
	// if a supported exporter is configured or a distributed queue is used.
	AutoTracing bool

	// JaegerEndpoint is the endpoint of the Jaeger collector
	JaegerEndpoint string

	// AddonCacheTime is how long between two cache operations
	AddonCacheTime time.Duration

	// LeaderConfig for leader election
	LeaderConfig leaderConfig

	// KubeBurst the burst of kube client
	KubeBurst int

	// KubeQPS the QPS of kube client
	KubeQPS float64

	// ExitOnLostLeader exits the process after leader election is lost.
	ExitOnLostLeader bool

	// Messaging configuration (pub/sub)
	Messaging MessagingConfig

	// Workflow configures workflow scheduling behaviour.
	Workflow workflowconfig.RuntimeConfig

	// ImportSecretKeyring is an inline JSON AES-256-GCM keyring used only for
	// adopted Kubernetes Secret payloads and import plan fingerprints.
	ImportSecretKeyring string
	// ImportSecretKeyringFile points at a mounted file containing the same JSON
	// document. It is mutually exclusive with ImportSecretKeyring.
	ImportSecretKeyringFile string
}

type RedisCacheConfig struct {
	CacheHost string
	CacheProt int
	CacheType string
	CacheDB   int64
	UserName  string
	Password  string
	// CacheTTL sets default ttl for ICache entries
	CacheTTL time.Duration
	// KeyPrefix applied to cache keys in redis
	KeyPrefix string
}

// MessagingConfig holds pub/sub configuration
type MessagingConfig struct {
	Type          string // redis|kafka
	ChannelPrefix string

	// Redis-specific configuration
	// RedisStreamMaxLen sets XADD MAXLEN to cap stream length (<=0 disables).
	RedisStreamMaxLen int64

	// Kafka-specific configuration
	// KafkaBrokers is a list of Kafka broker addresses (e.g., "localhost:9092").
	KafkaBrokers []string
	// KafkaGroupID is the consumer group ID for Kafka consumers.
	// Defaults to "eruun-workflow-workers" if not set.
	KafkaGroupID string
	// KafkaAutoOffsetReset determines where to start consuming when no offset exists.
	// Valid values: "earliest" (default) or "latest".
	KafkaAutoOffsetReset string
	// KafkaTopicPartitions is the default partition count when auto-creating topics.
	KafkaTopicPartitions int
	// KafkaTopicReplicationFactor is the default replication factor when auto-creating topics.
	KafkaTopicReplicationFactor int
}

// WorkflowRuntime returns the workflow settings, or zero settings for a nil server config.
func (c *Config) WorkflowRuntime() workflowconfig.RuntimeConfig {
	if c == nil {
		return workflowconfig.RuntimeConfig{}
	}
	return c.Workflow
}

func NewConfig() *Config {
	return &Config{
		Role:              RuntimeRoleAPI,
		BindAddr:          "127.0.0.1:8000",
		APIRateLimitQPS:   0,
		APIRateLimitBurst: 0,
		LeaderConfig: leaderConfig{
			ID:                 uuid.New().String(),
			ControllerLockName: "eruun-controller",
			SchedulerLockName:  "eruun-scheduler",
			Duration:           defaultLeaderLeaseDuration,
			Namespace:          NAMESPACE,
		},
		Datastore: datastore.Config{
			Type:     MYSQL,
			Database: DBNAME_ERUUN,
			// Must be provided via --datastore-url or ERUUN_DATASTORE_URL.
			URL:             "",
			MaxIdleConns:    10,
			MaxOpenConns:    100,
			ConnMaxLifetime: 30 * time.Minute,
			ConnMaxIdleTime: 10 * time.Minute,
		},
		DatastoreSchemaMode: DatastoreSchemaModeMigrate,
		Cache: RedisCacheConfig{
			CacheHost: "localhost",
			CacheProt: 6379,
			CacheType: REDIS,
			UserName:  "",
			Password:  "",
			CacheDB:   0,
			CacheTTL:  24 * time.Hour,
			KeyPrefix: "eruun:cache:",
		},
		KubeQPS:                100,
		KubeBurst:              300,
		AddonCacheTime:         time.Minute * 10,
		IstioEnable:            false,
		ExitOnLostLeader:       true,
		DTMAddr:                "",
		EnableTracing:          true,
		AutoTracing:            false,
		JaegerEndpoint:         "",
		AllowPrivateURLTargets: false,
		//JaegerEndpoint:   "http://localhost:14268/api/traces",
		Messaging: MessagingConfig{
			Type:                        REDIS,
			RedisStreamMaxLen:           50000,
			KafkaTopicPartitions:        1,
			KafkaTopicReplicationFactor: 1,
		},
		Workflow: workflowconfig.DefaultRuntimeConfig(),
	}
}

func (c *Config) Validate() []error {
	var errs []error
	schemaMode, schemaModeValid := normalizeDatastoreSchemaMode(c.DatastoreSchemaMode)
	if !schemaModeValid {
		errs = append(errs, fmt.Errorf("datastore schema mode must be one of migrate, validate, migrate-only; got %q", c.DatastoreSchemaMode))
	}
	if schemaMode == DatastoreSchemaModeMigrateOnly {
		if c.Datastore.Type != MYSQL {
			errs = append(errs, fmt.Errorf("unsupported datastore type: %s", c.Datastore.Type))
		}
		if strings.TrimSpace(c.Datastore.URL) == "" {
			errs = append(errs, fmt.Errorf("mysql url cannot be empty"))
		} else if strings.Contains(c.Datastore.URL, "__REPLACE_") {
			errs = append(errs, fmt.Errorf("mysql url contains placeholder value, please replace it with real credentials"))
		}
		return errs
	}
	_, roleValid := NormalizeRuntimeRole(string(c.Role))
	if !roleValid {
		errs = append(errs, fmt.Errorf("runtime role must be one of api, controller, scheduler, worker; got %q", c.Role))
	}
	if strings.TrimSpace(c.BindAddr) == "" {
		errs = append(errs, fmt.Errorf("bind address cannot be empty"))
	}
	apiRateLimitQPSValid := !math.IsNaN(c.APIRateLimitQPS) && !math.IsInf(c.APIRateLimitQPS, 0) && c.APIRateLimitQPS >= 0
	if !apiRateLimitQPSValid {
		errs = append(errs, fmt.Errorf("api rate limit qps must be finite and >= 0; use 0 to disable"))
	} else if c.APIRateLimitQPS > 0 && c.APIRateLimitBurst <= 0 {
		errs = append(errs, fmt.Errorf("api rate limit burst must be > 0 when api rate limit qps is enabled"))
	}
	if c.LeaderConfig.Duration < minLeaderLeaseDuration {
		errs = append(errs, fmt.Errorf("leader election lease duration must be >= 4s, got %s", c.LeaderConfig.Duration))
	}
	controllerLockName := strings.TrimSpace(c.LeaderConfig.ControllerLockName)
	schedulerLockName := strings.TrimSpace(c.LeaderConfig.SchedulerLockName)
	for _, lock := range []struct {
		scope string
		raw   string
		name  string
	}{
		{scope: "controller", raw: c.LeaderConfig.ControllerLockName, name: controllerLockName},
		{scope: "scheduler", raw: c.LeaderConfig.SchedulerLockName, name: schedulerLockName},
	} {
		switch {
		case lock.name == "":
			errs = append(errs, fmt.Errorf("%s leader election lock name cannot be empty", lock.scope))
		case lock.raw != lock.name:
			errs = append(errs, fmt.Errorf("%s leader election lock name must not contain leading or trailing whitespace", lock.scope))
		case len(k8svalidation.IsDNS1123Subdomain(lock.name)) > 0:
			errs = append(errs, fmt.Errorf("%s leader election lock name must be a valid DNS-1123 subdomain", lock.scope))
		}
	}
	if controllerLockName != "" && controllerLockName == schedulerLockName {
		errs = append(errs, fmt.Errorf("controller and scheduler leader election lock names must be distinct"))
	}
	if c.Datastore.Type == MYSQL && strings.TrimSpace(c.Datastore.URL) == "" {
		errs = append(errs, fmt.Errorf("mysql url cannot be empty"))
	}
	if c.Datastore.Type == MYSQL && strings.Contains(c.Datastore.URL, "__REPLACE_") {
		errs = append(errs, fmt.Errorf("mysql url contains placeholder value, please replace it with real credentials"))
	}
	cacheType := strings.ToLower(strings.TrimSpace(c.Cache.CacheType))
	if cacheType != REDIS {
		errs = append(errs, fmt.Errorf("distributed application mutation locking requires cache-type=redis"))
	} else if strings.TrimSpace(c.Cache.CacheHost) == "" || c.Cache.CacheProt <= 0 {
		errs = append(errs, fmt.Errorf("redis cache host/port is invalid"))
	}
	errs = append(errs, c.Workflow.Validate()...)
	if strings.TrimSpace(c.ImportSecretKeyring) != "" || strings.TrimSpace(c.ImportSecretKeyringFile) != "" {
		if _, err := importsecret.Load(c.ImportSecretKeyring, c.ImportSecretKeyringFile); err != nil {
			errs = append(errs, err)
		}
	}
	// messaging basic checks
	msgType := strings.ToLower(strings.TrimSpace(c.Messaging.Type))
	switch msgType {
	case "":
		errs = append(errs, fmt.Errorf("messaging type cannot be empty"))
	case REDIS:
		// Redis mode: reuses RedisCacheConfig for connection settings
		if strings.TrimSpace(c.Cache.CacheHost) == "" {
			errs = append(errs, fmt.Errorf("redis cache host cannot be empty when messaging type is redis"))
		}
		if c.Cache.CacheProt <= 0 {
			errs = append(errs, fmt.Errorf("redis cache port must be > 0 when messaging type is redis"))
		}
	case KAFKA:
		// Kafka mode: requires Kafka-specific configuration
		if len(c.Messaging.KafkaBrokers) == 0 {
			errs = append(errs, fmt.Errorf("kafka brokers cannot be empty when messaging type is kafka"))
		}
		offsetReset := strings.ToLower(strings.TrimSpace(c.Messaging.KafkaAutoOffsetReset))
		if offsetReset != "" && offsetReset != "earliest" && offsetReset != "latest" {
			errs = append(errs, fmt.Errorf("kafka auto offset reset must be 'earliest' or 'latest', got: %s", c.Messaging.KafkaAutoOffsetReset))
		}
		if c.Messaging.KafkaTopicPartitions <= 0 {
			errs = append(errs, fmt.Errorf("kafka topic partitions must be > 0 when messaging type is kafka"))
		}
		if c.Messaging.KafkaTopicReplicationFactor <= 0 {
			errs = append(errs, fmt.Errorf("kafka topic replication factor must be > 0 when messaging type is kafka"))
		}
	default:
		errs = append(errs, fmt.Errorf("unsupported messaging type: %s", c.Messaging.Type))
	}
	return errs
}

// AddFlags adds flags to the specified FlagSet
func (c *Config) AddFlags(fs *pflag.FlagSet, configParameter *Config) {
	fs.StringVar(&c.AuthConfigFile, "auth-config-file", c.AuthConfigFile, "Mounted Secret JSON containing account and workspace configuration (required)")
	c.Role = configParameter.Role
	fs.Var((*runtimeRoleValue)(&c.Role), "role", "runtime role: api|controller|scheduler|worker")
	fs.StringVar(&c.BindAddr, "bind-addr", configParameter.BindAddr, "The bind address used to serve the http APIs.")
	fs.Float64Var(&c.APIRateLimitQPS, "api-rate-limit-qps", configParameter.APIRateLimitQPS, "API rate limit for expensive operations in requests per second (0 disables)")
	fs.IntVar(&c.APIRateLimitBurst, "api-rate-limit-burst", configParameter.APIRateLimitBurst, "API rate limit burst size for expensive operations (required when api-rate-limit-qps > 0)")
	fs.BoolVar(&c.AllowPrivateURLTargets, "allow-private-url-targets", configParameter.AllowPrivateURLTargets, "allow outbound URL targets that resolve to private/loopback/link-local addresses")
	fs.StringVar(&c.LeaderConfig.ID, "id", configParameter.LeaderConfig.ID, "the holder identity name")
	fs.StringVar(&c.LeaderConfig.ControllerLockName, "controller-lock-name", configParameter.LeaderConfig.ControllerLockName, "the controller leader-election lease name")
	fs.StringVar(&c.LeaderConfig.SchedulerLockName, "scheduler-lock-name", configParameter.LeaderConfig.SchedulerLockName, "the scheduler leader-election lease name")
	fs.DurationVar(&c.LeaderConfig.Duration, "duration", configParameter.LeaderConfig.Duration, "leader election lease duration (e.g.15s)")
	fs.StringVar(&c.LeaderConfig.Namespace, "leader-namespace", configParameter.LeaderConfig.Namespace, "namespace for leader election lease")
	fs.Float64Var(&c.KubeQPS, "kube-api-qps", configParameter.KubeQPS, "the qps for kube clients. Low qps may lead to low throughput. High qps may give stress to api-server.")
	fs.IntVar(&c.KubeBurst, "kube-api-burst", configParameter.KubeBurst, "the burst for kube clients. Recommend setting it qps*3.")
	fs.BoolVar(&c.ExitOnLostLeader, "exit-on-lost-leader", configParameter.ExitOnLostLeader, "exit the process if this server lost the leader election")
	fs.StringVar(&c.Datastore.Type, "datastore-type", configParameter.Datastore.Type, "datastore backend type (e.g., mysql, tidb)")
	fs.StringVar(&c.Datastore.URL, "datastore-url", configParameter.Datastore.URL, "datastore connection URL / DSN")
	fs.StringVar(&c.Datastore.Database, "datastore-database", configParameter.Datastore.Database, "datastore database/schema name")
	fs.StringVar(&c.DatastoreSchemaMode, "datastore-schema-mode", configParameter.DatastoreSchemaMode, "datastore schema handling: migrate|validate|migrate-only")
	fs.IntVar(&c.Datastore.MaxIdleConns, "mysql-max-idle-conns", configParameter.Datastore.MaxIdleConns, "maximum number of idle MySQL connections to retain in the pool")
	fs.IntVar(&c.Datastore.MaxOpenConns, "mysql-max-open-conns", configParameter.Datastore.MaxOpenConns, "maximum number of open MySQL connections (<=0 means unlimited)")
	fs.DurationVar(&c.Datastore.ConnMaxLifetime, "mysql-conn-max-lifetime", configParameter.Datastore.ConnMaxLifetime, "maximum amount of time a MySQL connection may be reused (<=0 disables)")
	fs.DurationVar(&c.Datastore.ConnMaxIdleTime, "mysql-conn-max-idle-time", configParameter.Datastore.ConnMaxIdleTime, "maximum amount of time a MySQL connection may remain idle (<=0 disables)")
	fs.BoolVar(&c.EnableTracing, "enable-tracing", configParameter.EnableTracing, "Enable distributed tracing.")
	fs.BoolVar(&c.AutoTracing, "auto-tracing", configParameter.AutoTracing, "Auto-enable tracing when Jaeger is configured or messaging is redis (effective only if --enable-tracing=false).")
	fs.StringVar(&c.JaegerEndpoint, "jaeger-endpoint", configParameter.JaegerEndpoint, "The endpoint of the Jaeger collector.")
	// messaging basic flags (broker type & channel prefix). Redis connection will reuse RedisCacheConfig.
	fs.StringVar(&c.Messaging.Type, "msg-type", configParameter.Messaging.Type, "messaging broker type: redis|kafka")
	fs.StringVar(&c.Messaging.ChannelPrefix, "msg-channel-prefix", configParameter.Messaging.ChannelPrefix, "messaging channel prefix for topics")
	fs.Int64Var(&c.Messaging.RedisStreamMaxLen, "msg-redis-maxlen", configParameter.Messaging.RedisStreamMaxLen, "redis streams XADD MAXLEN cap (<=0 to disable)")
	// kafka-specific flags
	fs.StringSliceVar(&c.Messaging.KafkaBrokers, "msg-kafka-brokers", configParameter.Messaging.KafkaBrokers, "kafka broker addresses (e.g., localhost:9092)")
	fs.StringVar(&c.Messaging.KafkaGroupID, "msg-kafka-group-id", configParameter.Messaging.KafkaGroupID, "kafka consumer group ID (default: eruun-workflow-workers)")
	fs.StringVar(&c.Messaging.KafkaAutoOffsetReset, "msg-kafka-offset-reset", configParameter.Messaging.KafkaAutoOffsetReset, "kafka auto offset reset strategy: earliest|latest (default: earliest)")
	fs.IntVar(&c.Messaging.KafkaTopicPartitions, "msg-kafka-topic-partitions", configParameter.Messaging.KafkaTopicPartitions, "kafka topic partitions for auto-created topics (must be > 0)")
	fs.IntVar(&c.Messaging.KafkaTopicReplicationFactor, "msg-kafka-topic-replication-factor", configParameter.Messaging.KafkaTopicReplicationFactor, "kafka topic replication factor for auto-created topics (must be > 0)")
	// cache-specific flags
	fs.StringVar(&c.Cache.CacheType, "cache-type", configParameter.Cache.CacheType, "cache backend type (redis|memory)")
	fs.StringVar(&c.Cache.CacheHost, "cache-host", configParameter.Cache.CacheHost, "cache host for redis backend")
	fs.IntVar(&c.Cache.CacheProt, "cache-port", configParameter.Cache.CacheProt, "cache port for redis backend")
	fs.Int64Var(&c.Cache.CacheDB, "cache-db", configParameter.Cache.CacheDB, "cache database index for redis backend")
	fs.StringVar(&c.Cache.UserName, "cache-username", configParameter.Cache.UserName, "cache username for redis backend")
	fs.StringVar(&c.Cache.Password, "cache-password", configParameter.Cache.Password, "cache password for redis backend")
	fs.DurationVar(&c.Cache.CacheTTL, "cache-ttl", configParameter.Cache.CacheTTL, "default TTL for redis cache entries (e.g. 24h)")
	fs.StringVar(&c.Cache.KeyPrefix, "cache-prefix", configParameter.Cache.KeyPrefix, "key prefix for redis cache entries")
	fs.IntVar(&c.Workflow.SequentialMaxConcurrency, "workflow-sequential-max-concurrency", configParameter.Workflow.SequentialMaxConcurrency, "maximum number of jobs that may run concurrently inside sequential workflow steps (>=1)")
	fs.DurationVar(&c.Workflow.DispatchPollInterval, "workflow-dispatch-poll-interval", configParameter.Workflow.DispatchPollInterval, "interval for dispatcher waiting-task scans")
	fs.DurationVar(&c.Workflow.WorkerStaleInterval, "workflow-worker-stale-interval", configParameter.Workflow.WorkerStaleInterval, "interval between workflow worker stale-claim passes")
	fs.DurationVar(&c.Workflow.WorkerAutoClaimMinIdle, "workflow-worker-autoclaim-idle", configParameter.Workflow.WorkerAutoClaimMinIdle, "minimum idle duration before workflow workers auto-claim messages")
	fs.IntVar(&c.Workflow.WorkerAutoClaimCount, "workflow-worker-autoclaim-count", configParameter.Workflow.WorkerAutoClaimCount, "workflow worker auto-claim batch size")
	fs.IntVar(&c.Workflow.WorkerReadCount, "workflow-worker-read-count", configParameter.Workflow.WorkerReadCount, "workflow worker stream read batch size")
	fs.DurationVar(&c.Workflow.WorkerReadBlock, "workflow-worker-read-block", configParameter.Workflow.WorkerReadBlock, "workflow worker stream read block duration")
	fs.DurationVar(&c.Workflow.DefaultJobTimeout, "workflow-default-job-timeout", configParameter.Workflow.DefaultJobTimeout, "default workflow job timeout")
	fs.DurationVar(&c.Workflow.CallbackTimeoutMax, "workflow-callback-timeout-max", configParameter.Workflow.CallbackTimeoutMax, "maximum timeout for workflow callbacks")
	fs.IntVar(&c.Workflow.MaxConcurrentWorkflows, "workflow-max-concurrent", configParameter.Workflow.MaxConcurrentWorkflows, "maximum number of workflow controllers running concurrently per worker process")
	fs.DurationVar(&c.Workflow.HeartbeatInterval, "workflow-heartbeat-interval", configParameter.Workflow.HeartbeatInterval, "interval for renewing running workflow database leases")
	fs.DurationVar(&c.Workflow.LeaseDuration, "workflow-lease-duration", configParameter.Workflow.LeaseDuration, "database lease duration for queued and running workflows")
	fs.DurationVar(&c.Workflow.LeaseReaperInterval, "workflow-lease-reaper-interval", configParameter.Workflow.LeaseReaperInterval, "interval for recovering expired workflow leases")
	fs.DurationVar(&c.Workflow.WorkerDrainTimeout, "workflow-worker-drain-timeout", configParameter.Workflow.WorkerDrainTimeout, "maximum graceful worker drain duration")
	fs.StringVar(&c.ImportSecretKeyring, "import-secret-keyring", configParameter.ImportSecretKeyring, "inline JSON keyring for adopted Secret encryption and import plan fingerprints")
	fs.StringVar(&c.ImportSecretKeyringFile, "import-secret-keyring-file", configParameter.ImportSecretKeyringFile, "mounted JSON keyring file for adopted Secret encryption and import plan fingerprints")
	// profiling flags live in the profiling package; wire them here for convenience
	profiling.AddFlags(fs)
}

type runtimeRoleValue RuntimeRole

func (r *runtimeRoleValue) String() string {
	if r == nil || *r == "" {
		return string(RuntimeRoleAPI)
	}
	return string(*r)
}

func (r *runtimeRoleValue) Set(value string) error {
	role, ok := NormalizeRuntimeRole(value)
	if !ok {
		return fmt.Errorf("invalid runtime role %q", value)
	}
	*r = runtimeRoleValue(role)
	return nil
}

func (r *runtimeRoleValue) Type() string { return "runtime-role" }

func normalizeDatastoreSchemaMode(value string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", DatastoreSchemaModeMigrate:
		return DatastoreSchemaModeMigrate, true
	case DatastoreSchemaModeValidate:
		return DatastoreSchemaModeValidate, true
	case DatastoreSchemaModeMigrateOnly:
		return DatastoreSchemaModeMigrateOnly, true
	default:
		return "", false
	}
}

func (c Config) NormalizedDatastoreSchemaMode() string {
	mode, ok := normalizeDatastoreSchemaMode(c.DatastoreSchemaMode)
	if !ok {
		return DatastoreSchemaModeMigrate
	}
	return mode
}

func (c Config) MigrateSchemaOnly() bool {
	return c.NormalizedDatastoreSchemaMode() == DatastoreSchemaModeMigrateOnly
}

func NormalizeRuntimeRole(value string) (RuntimeRole, bool) {
	switch RuntimeRole(strings.ToLower(strings.TrimSpace(value))) {
	case RuntimeRoleAPI:
		return RuntimeRoleAPI, true
	case RuntimeRoleController:
		return RuntimeRoleController, true
	case RuntimeRoleScheduler:
		return RuntimeRoleScheduler, true
	case RuntimeRoleWorker:
		return RuntimeRoleWorker, true
	case "":
		return RuntimeRoleAPI, true
	default:
		return "", false
	}
}

func (c Config) NormalizedRole() RuntimeRole {
	role, ok := NormalizeRuntimeRole(string(c.Role))
	if !ok {
		return RuntimeRoleAPI
	}
	return role
}

func (c Config) RunsAPI() bool {
	return c.NormalizedRole() == RuntimeRoleAPI
}

func (c Config) RunsController() bool {
	return c.NormalizedRole() == RuntimeRoleController
}

func (c Config) RunsScheduler() bool {
	return c.NormalizedRole() == RuntimeRoleScheduler
}

func (c Config) RunsWorker() bool {
	return c.NormalizedRole() == RuntimeRoleWorker
}

func (c Config) RequiresDispatchQueue() bool {
	return c.RunsScheduler() || c.RunsWorker()
}

func (c Config) RequiresDelayQueue() bool {
	return c.RunsController() || c.RunsWorker()
}

func (c Config) RequiresResultQueue() bool {
	return c.RunsController()
}

func (c Config) RuntimeMessagingTopics() []string {
	topics := make([]string, 0, 3)
	if c.RequiresDispatchQueue() {
		topics = append(topics, workflowconfig.DispatchTopic(c.Messaging.ChannelPrefix))
	}
	if c.RequiresDelayQueue() {
		topics = append(topics, workflowconfig.DelayTopic(c.Messaging.ChannelPrefix))
	}
	if c.RequiresResultQueue() {
		topics = append(topics, workflowconfig.ResultTopic(c.Messaging.ChannelPrefix))
	}
	return topics
}

// HasExternalQueue returns true if a supported distributed queue backend is configured.
func (c Config) HasExternalQueue() bool {
	t := strings.ToLower(strings.TrimSpace(c.Messaging.Type))
	return t == REDIS || t == KAFKA
}
