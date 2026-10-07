package apiserver

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	urlpolicy "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/systemsetting"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/cache"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/clients"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
	msg "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/messaging"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/workspace"
	"github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api"
	grpcapi "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/grpc"
	"github.com/PixelCores/Eruun/pkg/apiserver/interfaces/ratelimit"
	"github.com/PixelCores/Eruun/pkg/apiserver/jobs"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/container"
)

type APIServer interface {
	Run(context.Context, chan error) error
}

// workflowRuntime is the single IoC-populated workflow instance. Its interface
// permits lifecycle tests to control startup and shutdown.
type workflowRuntime interface {
	StartLeader(context.Context, func())
	StartWorker(context.Context, context.Context, func(), func())
	RuntimeStats() workflow.RuntimeStats
}

type restServer struct {
	accounts                  *account.Service
	workspaceManager          *workspace.Manager
	grpcAdministration        *grpcapi.AdministrationServer
	grpcJobs                  *grpcapi.JobsServer
	grpcApplications          *grpcapi.ApplicationsServer
	apiRateLimiter            *ratelimit.Limiter
	jobs                      *jobs.Service
	webContainer              *gin.Engine
	apiHandlers               []api.Interface
	beanContainer             *container.Container
	cfg                       config.Config
	dataStore                 datastore.DataStore
	cache                     cache.ICache
	KubeClient                kubernetes.Interface `inject:"kubeClient"` //inject 是注入IOC的name，如果tag中包含inject 那么必须有对应的容器注入服务,必须大写，小写会无法访问
	KubeConfig                *rest.Config         `inject:"kubeConfig"`
	Queue                     msg.Queue
	runtimeQueues             *msg.RuntimeQueues
	InformerManager           *informer.Manager // Informer 管理器，用于 List-Watch 机制
	resourceObserver          *informer.KubernetesWorkloadObserver
	sandboxObserver           *informer.KubernetesSandboxObserver
	workflow                  workflowRuntime
	workersMu                 sync.Mutex
	workersReady              bool
	workersRun                *workerRun
	drainingWorkerRuns        map[*workerRun]struct{}
	urlSecurityPolicyProvider *urlpolicy.Provider
	ensureQueueGroupFailures  atomic.Int64
	leading                   atomic.Bool
	leaderMu                  sync.RWMutex
	leaderCtx                 context.Context
	leaderRun                 *workerRun
	leaderPodUID              string
}

func (s *restServer) RuntimeRole() string {
	if s.leading.Load() {
		return "leader"
	}
	return "worker"
}

func (s *restServer) RuntimeReady() (bool, string) {
	if s.leading.Load() {
		s.leaderMu.RLock()
		ready := s.leaderCtx != nil && s.leaderCtx.Err() == nil
		s.leaderMu.RUnlock()
		if !ready {
			return false, "leader is initializing or stopping"
		}
		return true, ""
	}
	s.workersMu.Lock()
	ready := s.workersRun != nil && s.workersReady
	s.workersMu.Unlock()
	if !ready {
		return false, "worker subscriber is not running"
	}
	return true, ""
}

var (
	ensureKafkaMessaging = clients.EnsureKafka
	newRedisClient       = clients.NewRedisClient
	runLeaderElector     = leaderelection.RunOrDie
	releaseLeaderLock    = bestEffortReleaseLeaderLock
)

const leaderElectionRetryPeriod = 2 * time.Second
const leaderElectionReleaseTimeout = 5 * time.Second

var leaderElectionRetryDelay = leaderElectionRetryPeriod

func New(cfg config.Config) (a APIServer) {
	handlers := api.NewHandlers()
	s := &restServer{
		webContainer:  gin.New(),
		apiHandlers:   handlers,
		beanContainer: container.NewContainer(),
		cfg:           cfg,
	}
	return s
}

func (s *restServer) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	for _, pre := range api.GetAPIPrefix() {
		if strings.HasPrefix(req.URL.Path, pre) {
			s.webContainer.ServeHTTP(res, req)
			return
		}
	}
	req.URL.Path = "/"
	s.webContainer.ServeHTTP(res, req)
}
