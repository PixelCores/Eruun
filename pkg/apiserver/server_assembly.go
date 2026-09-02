package apiserver

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
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
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
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
		ds, err = mysql.New(ctx, s.cfg.Datastore, builtinModels)
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
	workflowTaskRunLocker, err := s.buildWorkflowTaskRunLocker(cacheType, iCache)
	if err != nil {
		return err
	}

	// 将db 注入到IOC中
	if err := s.beanContainer.ProvideWithName("datastore", s.dataStore); err != nil {
		return fmt.Errorf("fail to provides the datastore bean to the container: %w", err)
	}

	if err := s.beanContainer.ProvideWithName("cache", iCache); err != nil {
		return fmt.Errorf("fail to provides the cache bean to the container: %w", err)
	}
	if err := s.beanContainer.ProvideWithName("workflowTaskRunLocker", workflowTaskRunLocker); err != nil {
		return fmt.Errorf("fail to provides the workflow task run locker bean to the container: %w", err)
	}

	// Initialize work queue. Messaging backends are required once configured.
	if err := s.ensureKafkaMessagingReady(); err != nil {
		return err
	}

	streamKey := s.dispatchTopic()
	q, err := s.buildQueue(streamKey, redisClient)
	if err != nil {
		return fmt.Errorf("initialize dispatch queue: %w", err)
	}
	delayQueue, err := s.buildQueue(s.delayTopic(), redisClient)
	if err != nil {
		return fmt.Errorf("initialize delay queue: %w", err)
	}
	resultQueue, err := s.buildQueue(s.resultTopic(), redisClient)
	if err != nil {
		return fmt.Errorf("initialize result queue: %w", err)
	}

	// 注入消息队列
	if err := s.beanContainer.ProvideWithName("queue", q); err != nil {
		return fmt.Errorf("fail to provides the queue bean to the container: %w", err)
	}
	if err := s.beanContainer.ProvideWithName("delayQueue", delayQueue); err != nil {
		return fmt.Errorf("fail to provides the delay queue bean to the container: %w", err)
	}
	if err := s.beanContainer.ProvideWithName("resultQueue", resultQueue); err != nil {
		return fmt.Errorf("fail to provides the result queue bean to the container: %w", err)
	}

	// 将操作k8s的权限全都注入到IOC中
	if err := s.beanContainer.ProvideWithName("kubeClient", kubeClient); err != nil {
		return fmt.Errorf("fail to provides the kubeClient bean to the container: %w", err)
	}

	if err := s.beanContainer.ProvideWithName("kubeConfig", kubeConfig); err != nil {
		return fmt.Errorf("fail to provides the kubeConfig bean to the container: %w", err)
	}

	// 初始化 Informer Manager（只监听 Eruun 管理的资源以减少内存消耗）
	s.InformerManager = informer.NewManager(
		kubeClient,
		informer.WithResyncPeriod(30*time.Second),
		informer.WithLabelSelector(config.LabelAppID), // 只监听带有 eruun.io/app-id 标签的资源
	)

	// 设置状态同步回调，将组件运行状态同步到数据库
	waiter := s.InformerManager.GetWaiter()
	observer := informer.NewKubernetesWorkloadObserver(kubeClient)
	s.resourceObserver = observer
	if err := s.beanContainer.ProvideWithName("resourceObserver", observer); err != nil {
		return fmt.Errorf("fail to provide the resource observer bean to the container: %w", err)
	}
	waiter.SetStatusSyncFunc(s.syncComponentStatus)
	waiter.SetPodRestartMonitorConfigFunc(s.loadPodRestartMonitorConfig)
	waiter.SetDeploymentPodRestartTriggerFunc(s.handleDeploymentPodRestartThresholdExceeded)
	klog.Info("Informer manager initialized with label selector filter and status sync")

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

func (s *restServer) kafkaTopics() []string {
	return config.KafkaTopics(s.cfg.Messaging.ChannelPrefix)
}

func (s *restServer) ensureKafkaMessagingReady() error {
	if !strings.EqualFold(strings.TrimSpace(s.cfg.Messaging.Type), config.KAFKA) {
		return nil
	}

	_, err := ensureKafkaMessaging(clients.KafkaConfig{
		Brokers:                s.cfg.Messaging.KafkaBrokers,
		Topics:                 s.kafkaTopics(),
		TopicPartitions:        s.cfg.Messaging.KafkaTopicPartitions,
		TopicReplicationFactor: s.cfg.Messaging.KafkaTopicReplicationFactor,
	})
	if err != nil {
		return fmt.Errorf("init kafka client failed: %w", err)
	}
	return nil
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

func (s *restServer) buildWorkflowTaskRunLocker(cacheType string, iCache cache.ICache) (locker.Locker, error) {
	cacheType = strings.ToLower(strings.TrimSpace(cacheType))
	if cacheType != string(cache.CacheTypeRedis) {
		return nil, fmt.Errorf("workflow task run locker requires cache-type=redis")
	}
	if iCache == nil {
		return nil, fmt.Errorf("workflow task run locker requires redis cache")
	}
	redisClient := iCache.GetRedisClient()
	if redisClient == nil {
		return nil, fmt.Errorf("workflow task run locker requires redis client")
	}
	return workflowevent.NewTaskRunRedisLocker(redisClient)
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
