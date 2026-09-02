package apiserver

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service"
	"github.com/PixelCores/Eruun/pkg/apiserver/event"
	workflowevent "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/clients"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/mysql"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
	msg "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/messaging"
	"github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api"
	"github.com/PixelCores/Eruun/pkg/apiserver/security/urlpolicy"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/cache"
)

func (s *restServer) buildIoCContainer(ctx context.Context) error {
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
	// 获取k8s的连接
	kubeClient, err := clients.GetKubeClient()
	if err != nil {
		return err
	}

	var ds datastore.DataStore
	switch s.cfg.Datastore.Type {
	case "mysql":
		schemaMode := mysql.SchemaModeMigrate
		if s.cfg.NormalizedDatastoreSchemaMode() == config.DatastoreSchemaModeValidate {
			schemaMode = mysql.SchemaModeValidate
		}
		ds, err = mysql.NewWithSchemaMode(ctx, s.cfg.Datastore, builtinModels, schemaMode)
		if err != nil {
			return fmt.Errorf("create mysql datastore instance failure %w", err)
		}
	default:
		return fmt.Errorf("not support datastore type %s", s.cfg.Datastore.Type)
	}
	s.dataStore = ds

	if err := s.runBootstrapStep(ctx, s.ensureDefaultAPIAuthSetting); err != nil {
		return err
	}
	if err := s.runBootstrapStep(ctx, s.ensureDefaultURLSecurityPolicySetting); err != nil {
		return err
	}
	if err := s.runBootstrapStep(ctx, s.ensureDefaultPodRestartMonitorSetting); err != nil {
		return err
	}

	s.urlSecurityPolicyProvider = urlpolicy.NewProvider(s.dataStore, time.Minute)
	if err := s.beanContainer.Provides(s.urlSecurityPolicyProvider); err != nil {
		return fmt.Errorf("fail to provide url security policy provider bean: %w", err)
	}

	cacheType := strings.ToLower(strings.TrimSpace(s.cfg.Cache.CacheType))
	redisClient, err := s.initRedisClientForConfiguredBackends()
	if err != nil {
		return err
	}

	// Initialize cache implementation from explicit cache configuration.
	var iCache cache.ICache
	switch cacheType {
	case string(cache.CacheTypeRedis):
		if redisClient == nil {
			return fmt.Errorf("redis cache requested but redis client is not initialized")
		}
		iCache = cache.NewRedisICache(redisClient, false, s.cfg.Cache.CacheTTL, s.cfg.Cache.KeyPrefix)
	default:
		iCache = cache.NewMemCacheWithClient(false, redisClient)
	}
	s.cache = iCache

	// 将db 注入到IOC中
	if err := s.beanContainer.ProvideWithName("datastore", s.dataStore); err != nil {
		return fmt.Errorf("fail to provides the datastore bean to the container: %w", err)
	}

	if err := s.beanContainer.ProvideWithName("cache", iCache); err != nil {
		return fmt.Errorf("fail to provides the cache bean to the container: %w", err)
	}

	// Initialize only the queues used by this process role.
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

	// 将操作k8s的权限全都注入到IOC中
	if err := s.beanContainer.ProvideWithName("kubeClient", kubeClient); err != nil {
		return fmt.Errorf("fail to provides the kubeClient bean to the container: %w", err)
	}

	if err := s.beanContainer.ProvideWithName("kubeConfig", kubeConfig); err != nil {
		return fmt.Errorf("fail to provides the kubeConfig bean to the container: %w", err)
	}

	s.initRoleObservers(kubeClient)

	// provide config for downstream components that need it (inject by type)
	if err := s.beanContainer.Provides(&s.cfg); err != nil {
		return fmt.Errorf("fail to provides the config bean to the container: %w", err)
	}

	programmingLanguageRepository, err := repository.NewProgrammingLanguageRepositoryWithStore(s.dataStore)
	if err != nil {
		return err
	}
	programmingLanguageService, err := service.NewProgrammingLanguageServiceWithRepository(programmingLanguageRepository)
	if err != nil {
		return err
	}

	// domain - repository (注入 Repository，依赖 datastore)
	if err := s.beanContainer.Provides(repository.InitRepositoryBean(programmingLanguageRepository)...); err != nil {
		return fmt.Errorf("fail to provides the repository bean to the container: %w", err)
	}

	// domain - service (注入 Service，可依赖 Repository)
	services := service.InitServiceBean(programmingLanguageService)
	for _, svc := range services {
		if err := s.beanContainer.Provides(svc); err != nil {
			return fmt.Errorf("fail to provides the service bean to the container: %w", err)
		}
	}

	// interfaces
	if err := s.beanContainer.Provides(api.InitAPIBean()...); err != nil {
		return fmt.Errorf("fail to provides the api bean to the container: %w", err)
	}

	// event
	eventWorkers := event.InitEvent()
	configureWorkflowEventWorkers(eventWorkers, runtimeQueues, s.resourceObserver)
	s.eventWorkers = append([]event.Worker(nil), eventWorkers...)
	eventBeans := make([]interface{}, 0, len(eventWorkers))
	for _, worker := range eventWorkers {
		eventBeans = append(eventBeans, worker)
	}
	if err := s.beanContainer.Provides(eventBeans...); err != nil {
		return fmt.Errorf("fail to provides the event bean to the container: %w", err)
	}

	if err := s.beanContainer.Populate(); err != nil {
		return fmt.Errorf("fail to populate the bean container: %w", err)
	}
	return nil
}

func (s *restServer) dispatchTopic() string {
	return config.DispatchTopic(s.cfg.Messaging.ChannelPrefix)
}

func (s *restServer) delayTopic() string {
	return config.DelayTopic(s.cfg.Messaging.ChannelPrefix)
}

func (s *restServer) resultTopic() string {
	return config.ResultTopic(s.cfg.Messaging.ChannelPrefix)
}

func (s *restServer) ensureKafkaMessagingReady() error {
	if !strings.EqualFold(strings.TrimSpace(s.cfg.Messaging.Type), config.KAFKA) {
		return nil
	}
	topics := s.cfg.RuntimeMessagingTopics()
	if len(topics) == 0 {
		return nil
	}

	_, err := ensureKafkaMessaging(clients.KafkaConfig{
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
	if s.cfg.RequiresDispatchQueue() {
		queues.Dispatch, err = s.buildQueue(s.dispatchTopic(), redisClient)
		if err != nil {
			return nil, fmt.Errorf("initialize dispatch queue: %w", err)
		}
	}
	if s.cfg.RequiresDelayQueue() {
		queues.Delay, err = s.buildQueue(s.delayTopic(), redisClient)
		if err != nil {
			return nil, fmt.Errorf("initialize delay queue: %w", err)
		}
	}
	if s.cfg.RequiresResultQueue() {
		queues.Result, err = s.buildQueue(s.resultTopic(), redisClient)
		if err != nil {
			return nil, fmt.Errorf("initialize result queue: %w", err)
		}
	}
	return queues, nil
}

func (s *restServer) initRoleObservers(kubeClient kubernetes.Interface) {
	if s.cfg.RunsController() {
		// The controller alone owns cluster state observation and database sync.
		s.InformerManager = informer.NewManager(
			kubeClient,
			informer.WithResyncPeriod(30*time.Second),
			informer.WithLabelSelector(config.LabelAppID),
		)
		waiter := s.InformerManager.GetWaiter()
		waiter.SetStatusSyncFunc(s.syncComponentStatus)
		waiter.SetPodRestartMonitorConfigFunc(s.loadPodRestartMonitorConfig)
		waiter.SetDeploymentPodRestartTriggerFunc(s.handleDeploymentPodRestartThresholdExceeded)
		klog.Info("controller informer manager initialized with Eruun label selector")
	}
	if s.cfg.RunsWorker() {
		s.resourceObserver = informer.NewKubernetesWorkloadObserver(kubeClient)
	}
}

func configureWorkflowEventWorkers(workers []event.Worker, queues *msg.RuntimeQueues, observer informer.ComponentReadyObserver) {
	if queues == nil {
		queues = &msg.RuntimeQueues{}
	}
	for _, worker := range workers {
		workflowWorker, ok := worker.(*workflowevent.Workflow)
		if !ok {
			continue
		}
		workflowWorker.Queue = queues.Dispatch
		workflowWorker.DelayQueue = queues.Delay
		workflowWorker.ResultQueue = queues.Result
		workflowWorker.ResourceWaiter = observer
	}
}

func (s *restServer) initRedisClientForConfiguredBackends() (*redis.Client, error) {
	useRedisCache := strings.EqualFold(strings.TrimSpace(s.cfg.Cache.CacheType), string(cache.CacheTypeRedis))
	useRedisMessaging := strings.EqualFold(strings.TrimSpace(s.cfg.Messaging.Type), config.REDIS)
	if !useRedisCache && !useRedisMessaging {
		return nil, nil
	}
	redisClient, err := newRedisClient(s.cfg.Cache)
	if err != nil {
		return nil, fmt.Errorf("init redis client for configured redis backend: %w", err)
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
