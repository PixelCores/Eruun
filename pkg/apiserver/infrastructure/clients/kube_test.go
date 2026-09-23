package clients

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"
)

func TestSetKubeConfigRateLimiter(t *testing.T) {
	previous := kubeConfig
	t.Cleanup(func() { kubeConfig = previous })
	for _, tc := range []struct {
		name    string
		config  rest.Config
		wantQPS float32
		wantNil bool
		wantErr bool
	}{
		{name: "configured budget", config: rest.Config{QPS: 100, Burst: 300}, wantQPS: 100},
		{name: "client-go defaults", wantQPS: rest.DefaultQPS},
		{name: "default QPS with configured burst", config: rest.Config{Burst: 1}, wantQPS: rest.DefaultQPS},
		{name: "disabled", config: rest.Config{QPS: -1}, wantNil: true},
		{name: "missing burst", config: rest.Config{QPS: 10}, wantErr: true},
		{name: "negative burst", config: rest.Config{QPS: 10, Burst: -1}, wantErr: true},
		{name: "negative burst with default QPS", config: rest.Config{Burst: -1}, wantErr: true},
		{name: "injected limiter wins", config: rest.Config{QPS: -1, Burst: -1, RateLimiter: flowcontrol.NewTokenBucketRateLimiter(7, 1)}, wantQPS: 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := kubeConfig
			err := setKubeConfig(&tc.config)
			if tc.wantErr {
				require.ErrorContains(t, err, "burst must be greater than 0")
				require.Same(t, before, kubeConfig)
				return
			}
			require.NoError(t, err)
			configured, err := GetKubeConfig()
			require.NoError(t, err)
			require.NotSame(t, &tc.config, configured)
			require.Equal(t, tc.config.QPS, configured.QPS)
			require.Equal(t, tc.config.Burst, configured.Burst)
			if tc.wantNil {
				require.Nil(t, configured.RateLimiter)
				return
			}
			require.Equal(t, tc.wantQPS, configured.RateLimiter.QPS())
			if tc.config.RateLimiter != nil {
				require.Same(t, tc.config.RateLimiter, configured.RateLimiter)
			} else {
				require.Nil(t, tc.config.RateLimiter, "initialization must not mutate the caller's config")
			}
		})
	}
}

func TestKubeClientsShareRequestBudget(t *testing.T) {
	previousConfig, previousClient := kubeConfig, kubeClient
	t.Cleanup(func() { kubeConfig, kubeClient = previousConfig, previousClient })
	kubeClient = nil
	var calls atomic.Int32
	err := setKubeConfig(&rest.Config{
		Host: "https://kubernetes.example", QPS: 0.000001, Burst: 8,
		Transport: kubeRoundTripperFunc(func(req *http.Request) (*http.Response, error) {
			calls.Add(1)
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(`{"apiVersion":"v1","kind":"Pod","metadata":{"name":"runner","namespace":"own"}}`)),
				Request:    req,
			}, nil
		}),
	})
	require.NoError(t, err)
	base, err := GetKubeConfig()
	require.NoError(t, err)
	primary, err := GetKubeClient()
	require.NoError(t, err)
	typed, err := kubernetes.NewForConfig(rest.CopyConfig(base))
	require.NoError(t, err)
	dynamicClient, err := dynamic.NewForConfig(rest.CopyConfig(base))
	require.NoError(t, err)
	require.Same(t, base.RateLimiter, primary.CoreV1().RESTClient().GetRateLimiter())
	require.Same(t, base.RateLimiter, typed.BatchV1().RESTClient().GetRateLimiter())

	// A negligible refill rate makes the result deterministic without sleeps:
	// eight requests share the initial burst, and the others cannot meet their
	// deadline. Requests through both client types consume that same budget.
	requests := []func(context.Context) error{
		func(ctx context.Context) error {
			_, err := primary.CoreV1().Pods("own").Get(ctx, "runner", metav1.GetOptions{})
			return err
		},
		func(ctx context.Context) error {
			_, err := typed.CoreV1().Pods("own").Get(ctx, "runner", metav1.GetOptions{})
			return err
		},
		func(ctx context.Context) error {
			_, err := dynamicClient.Resource(schema.GroupVersionResource{Version: "v1", Resource: "pods"}).Namespace("own").Get(ctx, "runner", metav1.GetOptions{})
			return err
		},
	}
	for _, request := range requests {
		require.NoError(t, request(context.Background()))
	}
	var successes atomic.Int32
	var wg sync.WaitGroup
	for i := range 12 {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if requests[i%len(requests)](ctx) == nil {
				successes.Add(1)
			}
		})
	}
	wg.Wait()
	require.EqualValues(t, 5, successes.Load())
	require.EqualValues(t, 8, calls.Load())
}

type kubeRoundTripperFunc func(*http.Request) (*http.Response, error)

func (f kubeRoundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
