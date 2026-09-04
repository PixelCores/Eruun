package apiserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/clients"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api"
	"github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/middleware"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

func (s *restServer) runBootstrapStep(parent context.Context, step func(context.Context) error) error {
	if step == nil {
		return nil
	}
	bootstrapCtx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	return step(bootstrapCtx)
}

func (s *restServer) RegisterAPIRoute() {
	s.registerAPIRoutes(false)
}

func (s *restServer) RegisterHealthRoute() {
	s.registerAPIRoutes(true)
}

func (s *restServer) registerAPIRoutes(healthOnly bool) {
	// 初始化中间件
	s.webContainer.Use(gin.Recovery())

	// Exact configured origins are required for credentialed browser clients.
	var origins []string
	if s.cfg.Accounts != nil {
		origins = s.cfg.Accounts.Origins
	}
	var trustedProxies []string
	if s.cfg.Accounts != nil {
		trustedProxies = s.cfg.Accounts.TrustedProxyCIDRs
	}
	// CIDRs are validated during startup; forwarding headers are ignored by default.
	_ = s.webContainer.SetTrustedProxies(trustedProxies)
	s.webContainer.Use(middleware.CORS(middleware.CORSOptions{
		AllowOrigins:     origins,
		AllowMethods:     []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Content-Type", "Authorization", "Accept", "Origin", "X-Requested-With", "X-Eruun-Workspace-ID"},
		ExposeHeaders:    []string{},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
	}))
	if s.cfg.APIRateLimitQPS > 0 {
		s.webContainer.Use(middleware.RateLimit(middleware.RateLimitOptions{
			QPS:       s.cfg.APIRateLimitQPS,
			Burst:     s.cfg.APIRateLimitBurst,
			SkipPaths: middleware.DefaultRateLimitSkipPaths(),
		}))
	}
	s.webContainer.Use(middleware.RequestBodyLimit(config.DefaultRequestBodyLimitBytes))
	s.webContainer.Use(middleware.Auth(middleware.AuthOptions{
		Accounts: s.accounts,
	}))

	// Enable tracing middleware if configured
	if s.cfg.EnableTracing {
		s.webContainer.Use(otelgin.Middleware("eruun-server"))
	}

	// Always enable request logging (after tracing so trace/span IDs are populated)
	s.webContainer.Use(middleware.Logging())

	// Enable gzip compression for responses
	s.webContainer.Use(middleware.Gzip())

	// 获取所有注册的API
	apis := api.GetRegisteredAPI()
	// 为每个API前缀创建路由组
	for _, prefix := range api.GetAPIPrefix() {
		group := s.webContainer.Group(prefix)
		for _, handler := range apis {
			if healthOnly {
				named, ok := handler.(interface{ GetName() string })
				if !ok || named.GetName() != "health" {
					continue
				}
			}
			handler.RegisterRoutes(group)
		}
	}

}

func (s *restServer) ensureDefaultURLSecurityPolicySetting(ctx context.Context) error {
	return s.ensureDefaultSystemSetting(ctx, model.SystemSettingTypeURLSecurityPolicy, "urlSecurityPolicy", func() ([]byte, error) {
		policy := spec.DefaultURLSecurityPolicy()
		policy.AllowPrivateByDefault = s.cfg.AllowPrivateURLTargets
		return json.Marshal(policy)
	})
}

func (s *restServer) ensureDefaultPodRestartMonitorSetting(ctx context.Context) error {
	return s.ensureDefaultSystemSetting(ctx, model.SystemSettingTypePodRestartMonitor, "podRestartMonitor", func() ([]byte, error) {
		return json.Marshal(spec.DefaultPodRestartMonitorSetting())
	})
}

func (s *restServer) ensureDefaultSystemSetting(ctx context.Context, settingType, settingName string, defaultValue func() ([]byte, error)) error {
	if s.dataStore == nil {
		return fmt.Errorf("datastore is not initialized")
	}
	setting := &model.SystemSetting{Type: settingType}
	if err := s.dataStore.Get(ctx, setting); err != nil {
		if !errors.Is(err, datastore.ErrRecordNotExist) {
			return fmt.Errorf("load %s setting: %w", settingName, err)
		}
		payload, marshalErr := defaultValue()
		if marshalErr != nil {
			return fmt.Errorf("marshal default %s setting: %w", settingName, marshalErr)
		}
		setting.Value = payload
		if addErr := s.dataStore.Add(ctx, setting); addErr != nil {
			if errors.Is(addErr, datastore.ErrRecordExist) {
				return nil
			}
			return fmt.Errorf("create default %s setting: %w", settingName, addErr)
		}
		klog.Infof("bootstrapped default %s setting", settingName)
	}
	return nil
}

func (s *restServer) startHTTP(ctx context.Context) error {
	// Start HTTP appserver
	klog.Infof("HTTP APIs are being served on: %s, ctx: %s", s.cfg.BindAddr, ctx)
	server := &http.Server{
		Addr:              s.cfg.BindAddr,
		Handler:           s,
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// Graceful shutdown handler
	shutdownComplete := make(chan struct{})
	stopCh := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			klog.Info("HTTP server shutdown initiated")
			shutdownCtx, cancel := context.WithTimeout(context.Background(), config.DefaultHTTPShutdownTimeout)
			defer cancel()
			if err := server.Shutdown(shutdownCtx); err != nil {
				klog.Errorf("HTTP server graceful shutdown error: %v", err)
				// Force close if graceful shutdown fails
				if closeErr := server.Close(); closeErr != nil {
					klog.Errorf("HTTP server force close error: %v", closeErr)
				}
			} else {
				klog.Info("HTTP server graceful shutdown completed")
			}
			close(shutdownComplete)
		case <-stopCh:
			return
		}
	}()

	err := server.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		klog.Errorf("HTTP server failed to start on %s: %v", s.cfg.BindAddr, err)
		close(stopCh)
		return err
	}
	<-shutdownComplete

	// Ignore normal shutdown error
	if err == http.ErrServerClosed {
		klog.Info("HTTP server closed normally")
		return nil
	}
	return err
}

func newRuntimeLifecycleContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithCancel(context.WithoutCancel(parent))
}

func watchRuntimeShutdown(parent, runtimeCtx context.Context, shutdown func()) {
	go func() {
		select {
		case <-parent.Done():
			shutdown()
		case <-runtimeCtx.Done():
		}
	}()
}

func (s *restServer) Run(ctx context.Context, errChan chan error) error {
	if strings.EqualFold(strings.TrimSpace(s.cfg.Messaging.Type), config.KAFKA) {
		defer clients.CloseKafkaConnections()
	}

	// build the Ioc Container
	if err := s.buildIoCContainer(ctx); err != nil {
		return err
	}

	if s.cfg.RunsAPI() {
		s.RegisterAPIRoute()
	} else {
		s.RegisterHealthRoute()
	}

	// Keep already-started runtime work alive while the caller's context begins
	// graceful shutdown. shutdown cancels runCtx only after workers have drained.
	runCtx, runCancel := newRuntimeLifecycleContext(ctx)
	defer runCancel()
	var shutdownOnce sync.Once
	var runtimeLifecycleMu sync.Mutex
	shutdown := func() {
		shutdownOnce.Do(func() {
			runtimeLifecycleMu.Lock()
			defer runtimeLifecycleMu.Unlock()

			drainTimeout := s.cfg.Workflow.WorkerDrainTimeout
			if drainTimeout <= 0 {
				drainTimeout = workflowconfig.DefaultWorkerDrainTimeout
			}
			drainCtx, cancelDrain := context.WithTimeout(context.Background(), drainTimeout)
			defer cancelDrain()
			s.stopWorkers(drainCtx)
			s.stopControllerRun()
			s.stopSchedulerRun()
			runCancel()
		})
	}
	defer shutdown()
	watchRuntimeShutdown(ctx, runCtx, shutdown)
	if s.cfg.RunsAPI() {
		go s.accounts.RunSessionCleanup(runCtx)
	}

	elections, err := s.setupRuntimeLeaderElections(runCtx, errChan)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	if s.cfg.RunsWorker() {
		if s.resourceObserver == nil {
			return fmt.Errorf("worker resource observer is not configured")
		}
		if err := s.resourceObserver.Start(runCtx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("start worker resource observer: %w", err)
		}
		runtimeLifecycleMu.Lock()
		if runCtx.Err() == nil {
			s.startWorkers(runCtx, errChan)
		}
		runtimeLifecycleMu.Unlock()
	}
	for _, election := range elections {
		election := election
		go s.runRuntimeLeaderElection(runCtx, election)
	}
	klog.InfoS("Eruun runtime started", "role", s.cfg.NormalizedRole(), "leaderElections", len(elections))

	return s.startHTTP(ctx)
}
