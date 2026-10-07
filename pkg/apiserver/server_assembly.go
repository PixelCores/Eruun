package apiserver

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	urlpolicy "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/systemsetting"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/validation"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	workflowevent "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/cache"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/clients"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/mysql"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/identity"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	msg "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/messaging"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/workspace"
	"github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api"
	grpcapi "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/grpc"
	"github.com/PixelCores/Eruun/pkg/apiserver/jobs"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

func (s *restServer) buildIoCContainer(ctx context.Context) error {
	accountsConfig, err := spec.LoadAccountConfig(s.cfg.AuthConfigFile)
	if err != nil {
		return fmt.Errorf("load authentication configuration: %w", err)
	}
	s.cfg.Accounts = accountsConfig
	s.cfg.Jobs, err = spec.LoadJobsRuntimeConfig(s.cfg.JobsConfigFile)
	if err != nil {
		return fmt.Errorf("load Jobs configuration: %w", err)
	}
	builtinModels, err := model.BuiltinModels()
	if err != nil {
		return fmt.Errorf("build model set: %w", err)
	}
	// infrastructure
	if err := s.beanContainer.ProvideWithName("RestServer", s); err != nil {
		return fmt.Errorf("fail to provides the RestServer bean to the container: %w", err)
	}
	if err := s.beanContainer.ProvideWithName("runtimeReadiness", api.RuntimeReadiness(s)); err != nil {
		return fmt.Errorf("fail to provide runtime readiness bean: %w", err)
	}
	// 设置KubeConfig
	err = clients.SetKubeConfig(s.cfg)
	if err != nil {
		return err
	}
	// 获取k8s的配置文件
	kubeConfig, err := clients.GetKubeConfig()
	if err != nil {
		return err
	}
	// Sandbox lifecycle uses the platform identity after its own Runner claim
	// checks. Preserve the shared limiter before APIClient adds tenant transport.
	sandboxKubeConfig := rest.CopyConfig(kubeConfig)
	// 获取k8s的连接
	kubeClient, err := clients.GetKubeClient()
	if err != nil {
		return err
	}

	schemaMode := mysql.SchemaModeMigrate
	if s.cfg.NormalizedDatastoreSchemaMode() == config.DatastoreSchemaModeValidate {
		schemaMode = mysql.SchemaModeValidate
	}
	ds, err := mysql.NewWithSchemaMode(ctx, s.cfg.Datastore, builtinModels, schemaMode)
	if err != nil {
		return fmt.Errorf("create mysql datastore instance failure %w", err)
	}
	s.dataStore = account.NewStore(ds)
	if err := s.runBootstrapStep(ctx, s.ensureDefaultURLSecurityPolicySetting); err != nil {
		return err
	}
	if err := s.runBootstrapStep(ctx, s.ensureDefaultPodRestartMonitorSetting); err != nil {
		return err
	}
	if err := s.runBootstrapStep(ctx, func(ctx context.Context) error {
		return repository.EnsureJobSchedulerPolicy(ctx, s.dataStore)
	}); err != nil {
		return err
	}

	s.urlSecurityPolicyProvider = urlpolicy.NewProvider(s.dataStore, time.Minute)
	if err := s.beanContainer.Provides(s.urlSecurityPolicyProvider); err != nil {
		return fmt.Errorf("fail to provide url security policy provider bean: %w", err)
	}

	redisClient, err := s.initRedisClientForConfiguredBackends()
	if err != nil {
		return err
	}
	s.accounts = account.New(ds, accountsConfig, redisClient, &identity.Delivery{Config: accountsConfig})
	if err := s.runBootstrapStep(ctx, s.accounts.Bootstrap); err != nil {
		return err
	}
	s.workspaceManager = &workspace.Manager{Client: kubeClient, RESTConfig: kubeConfig, Config: accountsConfig.Workspace}
	if err := s.beanContainer.Provides(s.accounts, s.workspaceManager); err != nil {
		return err
	}

	iCache, err := cache.NewRedisICache(redisClient, false, s.cfg.Cache.CacheTTL, s.cfg.Cache.KeyPrefix)
	if err != nil {
		return fmt.Errorf("initialize redis cache: %w", err)
	}
	s.cache = iCache
	if err := s.beanContainer.ProvideWithName("redisClient", redisClient); err != nil {
		return fmt.Errorf("provide Redis coordination client: %w", err)
	}
	for name, prefix := range map[string]string{
		"appScheduleLocker": "eruun-app-schedule",
		"managementLocker":  "eruun-adopted-import",
	} {
		lockProvider, err := locker.NewRedisLocker(redisClient, prefix)
		if err != nil {
			return fmt.Errorf("initialize %s: %w", name, err)
		}
		if err := s.beanContainer.ProvideWithName(name, lockProvider); err != nil {
			return fmt.Errorf("provide %s: %w", name, err)
		}
	}

	// 将db 注入到IOC中
	if err := s.beanContainer.ProvideWithName("datastore", s.dataStore); err != nil {
		return fmt.Errorf("fail to provides the datastore bean to the container: %w", err)
	}

	if err := s.beanContainer.ProvideWithName("cache", iCache); err != nil {
		return fmt.Errorf("fail to provides the cache bean to the container: %w", err)
	}

	// Every node can become Leader and requires all runtime queues.
	if err := s.ensureKafkaMessagingReady(); err != nil {
		return err
	}
	runtimeQueues, err := s.buildRuntimeQueues(redisClient)
	if err != nil {
		return err
	}
	s.runtimeQueues = runtimeQueues
	s.Queue = runtimeQueues.Dispatch
	if err := s.beanContainer.Provides(runtimeQueues); err != nil {
		return fmt.Errorf("fail to provide runtime queues bean to the container: %w", err)
	}

	// Tenant-scoped API requests use impersonation; trusted background contexts keep the platform identity.
	kubeClient, kubeConfig, err = workspace.APIClient(kubeConfig, accountsConfig.Workspace)
	if err != nil {
		return fmt.Errorf("create scoped API Kubernetes client: %w", err)
	}
	if err := s.beanContainer.ProvideWithName("kubeClient", kubeClient); err != nil {
		return fmt.Errorf("fail to provides the kubeClient bean to the container: %w", err)
	}

	if err := s.beanContainer.ProvideWithName("kubeConfig", kubeConfig); err != nil {
		return fmt.Errorf("fail to provides the kubeConfig bean to the container: %w", err)
	}

	s.initRuntimeObservers(kubeClient)

	// provide config for downstream components that need it (inject by type)
	if err := s.beanContainer.Provides(&s.cfg); err != nil {
		return fmt.Errorf("fail to provides the config bean to the container: %w", err)
	}

	s.jobs, err = jobs.New(ds, kubeClient, &s.cfg)
	if err != nil {
		return fmt.Errorf("initialize workspace Jobs: %w", err)
	}
	if err := s.initSandboxObserver(kubeClient, sandboxKubeConfig); err != nil {
		return err
	}
	if err = s.beanContainer.Provides(s.jobs); err != nil {
		return err
	}

	return s.provideDomainAndEventBeans(runtimeQueues)
}

func (s *restServer) provideDomainAndEventBeans(runtimeQueues *msg.RuntimeQueues) error {
	programmingLanguageRepository, err := repository.NewProgrammingLanguageRepositoryWithStore(s.dataStore)
	if err != nil {
		return err
	}
	programmingLanguageService, err := service.NewProgrammingLanguageServiceWithRepository(programmingLanguageRepository)
	if err != nil {
		return err
	}

	appRepo := repository.NewApplicationRepository(s.dataStore)
	componentRepo := repository.NewComponentRepository(s.dataStore)
	validationService := validation.NewValidationService(&s.cfg, s.urlSecurityPolicyProvider, appRepo, componentRepo)

	// domain - repository (注入 Repository，依赖 datastore)
	repositories := append(repository.InitRepositoryBean(appRepo, componentRepo), programmingLanguageRepository)
	if err := s.beanContainer.Provides(repositories...); err != nil {
		return fmt.Errorf("fail to provides the repository bean to the container: %w", err)
	}

	// domain - service (注入 Service，可依赖 Repository)
	services := append(service.InitServiceBean(validationService), programmingLanguageService)
	for _, svc := range services {
		if err := s.beanContainer.Provides(svc); err != nil {
			return fmt.Errorf("fail to provides the service bean to the container: %w", err)
		}
	}

	if err := s.provideInterfaceBeans(); err != nil {
		return err
	}

	if err := s.provideWorkflowRuntime(runtimeQueues); err != nil {
		return err
	}

	if err := s.beanContainer.Populate(); err != nil {
		return fmt.Errorf("fail to populate the bean container: %w", err)
	}
	return nil
}

func (s *restServer) provideInterfaceBeans() error {
	for _, handler := range s.apiHandlers {
		if err := s.beanContainer.Provides(handler); err != nil {
			return fmt.Errorf("provide api handler: %w", err)
		}
	}

	s.grpcAdministration = &grpcapi.AdministrationServer{}
	s.grpcJobs = &grpcapi.JobsServer{}
	s.grpcApplications = &grpcapi.ApplicationsServer{}
	if err := s.beanContainer.Provides(s.grpcAdministration, s.grpcJobs, s.grpcApplications); err != nil {
		return fmt.Errorf("provide grpc business adapters: %w", err)
	}
	return nil
}

func (s *restServer) dispatchTopic() string {
	return workflowconfig.DispatchTopic(s.cfg.Messaging.ChannelPrefix)
}

func (s *restServer) delayTopic() string {
	return workflowconfig.DelayTopic(s.cfg.Messaging.ChannelPrefix)
}

func (s *restServer) ensureKafkaMessagingReady() error {
	if !strings.EqualFold(strings.TrimSpace(s.cfg.Messaging.Type), config.KAFKA) {
		return nil
	}
	topics := s.cfg.RuntimeMessagingTopics()

	err := ensureKafkaMessaging(clients.KafkaConfig{
		Brokers:                s.cfg.Messaging.KafkaBrokers,
		Topics:                 topics,
		TopicPartitions:        s.cfg.Messaging.KafkaTopicPartitions,
		TopicReplicationFactor: s.cfg.Messaging.KafkaTopicReplicationFactor,
	})
	if err != nil {
		return fmt.Errorf("init kafka client failed: %w", err)
	}
	return nil
}

func (s *restServer) buildRuntimeQueues(redisClient *redis.Client) (*msg.RuntimeQueues, error) {
	queues := &msg.RuntimeQueues{}
	var err error
	queues.Dispatch, err = s.buildQueue(s.dispatchTopic(), redisClient)
	if err != nil {
		return nil, fmt.Errorf("initialize dispatch queue: %w", err)
	}
	queues.Delay, err = s.buildQueue(s.delayTopic(), redisClient)
	if err != nil {
		return nil, fmt.Errorf("initialize delay queue: %w", err)
	}
	return queues, nil
}

func (s *restServer) initRuntimeObservers(kubeClient kubernetes.Interface) {
	// The manager writes observed state only while this node holds leadership.
	s.InformerManager = informer.NewManager(kubeClient, informer.WithResyncPeriod(30*time.Second), informer.WithLabelSelector(config.LabelAppID))
	waiter := s.InformerManager.GetWaiter()
	waiter.SetStatusSyncFunc(s.syncComponentStatus)
	waiter.SetPodRestartMonitorConfigFunc(s.loadPodRestartMonitorConfig)
	waiter.SetDeploymentPodRestartTriggerFunc(s.handleDeploymentPodRestartThresholdExceeded)
	s.resourceObserver = informer.NewKubernetesWorkloadObserver(kubeClient)
}

func (s *restServer) initSandboxObserver(kubeClient kubernetes.Interface, platformConfig *rest.Config) error {
	if s.cfg.Jobs == nil {
		return nil
	}
	if s.jobs == nil || platformConfig == nil {
		return fmt.Errorf("Sandbox runtime requires Jobs service and Kubernetes REST config")
	}
	sandboxClient, err := dynamic.NewForConfig(platformConfig)
	if err != nil {
		return fmt.Errorf("initialize Sandbox Kubernetes client: %w", err)
	}
	observer, err := informer.NewKubernetesSandboxObserver(sandboxClient, kubeClient)
	if err != nil {
		return err
	}
	s.sandboxObserver = observer
	s.jobs.SandboxClient = sandboxClient
	s.jobs.SandboxObserver = observer
	return nil
}

func (s *restServer) initRedisClientForConfiguredBackends() (*redis.Client, error) {
	// Authentication always requires Redis for single-use challenges and limits.
	redisClient, err := newRedisClient(s.cfg.Cache)
	if err != nil {
		return nil, fmt.Errorf("init redis client for configured redis backend: %w", err)
	}
	if redisClient == nil {
		return nil, fmt.Errorf("init redis client for configured redis backend: redis client is not initialized")
	}
	return redisClient, nil
}

func (s *restServer) buildQueue(streamKey string, redisClient *redis.Client) (msg.Queue, error) {
	msgType := strings.ToLower(strings.TrimSpace(s.cfg.Messaging.Type))
	switch msgType {
	case config.REDIS:
		return s.buildRedisQueue(streamKey, redisClient)
	case config.KAFKA:
		return s.buildKafkaQueue(streamKey)
	default:
		return nil, fmt.Errorf("unsupported messaging type: %s", s.cfg.Messaging.Type)
	}
}

func (s *restServer) buildRedisQueue(streamKey string, redisClient *redis.Client) (msg.Queue, error) {
	if redisClient == nil {
		return nil, fmt.Errorf("redis client is not initialized")
	}
	rq, err := msg.NewRedisStreamsWithClient(redisClient, streamKey, s.cfg.Messaging.RedisStreamMaxLen)
	if err != nil {
		return nil, fmt.Errorf("init redis streams with client failed: %w", err)
	}
	return rq, nil
}

func (s *restServer) buildKafkaQueue(topic string) (msg.Queue, error) {
	queueCfg := msg.KafkaConfig{
		Brokers:         s.cfg.Messaging.KafkaBrokers,
		Topic:           topic,
		GroupID:         s.cfg.Messaging.KafkaGroupID,
		AutoOffsetReset: s.cfg.Messaging.KafkaAutoOffsetReset,
	}

	kq, err := msg.NewKafkaQueue(queueCfg)
	if err != nil {
		return nil, fmt.Errorf("init kafka queue failed: %w", err)
	}
	return kq, nil
}

// provideWorkflowRuntime creates the one instance shared by Leader and Worker duties.
func (s *restServer) provideWorkflowRuntime(queues *msg.RuntimeQueues) error {
	workflow := &workflowevent.Workflow{
		Queue:          queues.Dispatch,
		DelayQueue:     queues.Delay,
		ResourceWaiter: s.resourceObserver,
	}
	if err := s.beanContainer.Provides(workflow); err != nil {
		return fmt.Errorf("provide workflow runtime: %w", err)
	}
	s.workflow = workflow
	return nil
}
