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
	"github.com/PixelCores/Eruun/pkg/apiserver/event"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/clients"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
	msg "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/messaging"
	"github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api"
	"github.com/PixelCores/Eruun/pkg/apiserver/security/urlpolicy"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/cache"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/container"
)

type APIServer interface {
	Run(context.Context, chan error) error
}

type restServer struct {
	webContainer              *gin.Engine
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
	eventWorkers              []event.Worker
	workersMu                 sync.Mutex
	workersStarted            bool
	workersReady              bool
	workersCancel             context.CancelFunc
	workersRun                *workerRun
	drainingWorkerRuns        map[*workerRun]struct{}
	controllerRun             *workerRun
	schedulerRun              *workerRun
	urlSecurityPolicyProvider *urlpolicy.Provider
	ensureQueueGroupFailures  atomic.Int64
	controllerLeading         atomic.Bool
	controllerReady           atomic.Bool
	schedulerLeading          atomic.Bool
	schedulerReady            atomic.Bool
}

func (s *restServer) RuntimeReady() (bool, string) {
	if s.cfg.RunsController() && s.controllerLeading.Load() && !s.controllerReady.Load() {
		return false, "controller leader is still initializing"
	}
	if s.cfg.RunsScheduler() && s.schedulerLeading.Load() && !s.schedulerReady.Load() {
		return false, "scheduler leader is still initializing"
	}
	if s.cfg.RunsWorker() {
		s.workersMu.Lock()
		ready := s.workersStarted && s.workersReady
		s.workersMu.Unlock()
		if !ready {
			return false, "worker subscriber is not running"
		}
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
	s := &restServer{
		webContainer:  gin.New(),
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
