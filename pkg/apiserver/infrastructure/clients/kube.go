package clients

import (
	"fmt"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	apiConfig "github.com/PixelCores/Eruun/pkg/apiserver/config"
)

// NewKubeClient loads the platform REST config with the configured request
// budget and builds a client that shares its rate limiter.
func NewKubeClient(c apiConfig.Config) (kubernetes.Interface, *rest.Config, error) {
	conf, err := config.GetConfig()
	if err != nil {
		return nil, nil, err
	}
	conf.Burst = c.KubeBurst
	conf.QPS = float32(c.KubeQPS)
	return newKubeClient(conf)
}

func newKubeClient(conf *rest.Config) (kubernetes.Interface, *rest.Config, error) {
	conf, err := withSharedRateLimiter(conf)
	if err != nil {
		return nil, nil, err
	}
	client, err := kubernetes.NewForConfig(conf)
	if err != nil {
		return nil, nil, err
	}
	return client, conf, nil
}

// withSharedRateLimiter copies conf and installs one token bucket. Config
// copies used by tenant, typed and dynamic clients retain this limiter, so
// creating a client does not create another request budget.
func withSharedRateLimiter(conf *rest.Config) (*rest.Config, error) {
	conf = rest.CopyConfig(conf)
	if conf.RateLimiter == nil && conf.QPS >= 0 {
		qps, burst := conf.QPS, conf.Burst
		if qps == 0 {
			qps = rest.DefaultQPS
			if burst == 0 {
				burst = rest.DefaultBurst
			}
		}
		if burst <= 0 {
			return nil, fmt.Errorf("configure Kubernetes client rate limiter: burst must be greater than 0 when QPS is positive")
		}
		conf.RateLimiter = flowcontrol.NewTokenBucketRateLimiter(qps, burst)
	}
	return conf, nil
}
