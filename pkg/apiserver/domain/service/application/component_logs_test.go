package application

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	restfake "k8s.io/client-go/rest/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestStreamComponentLogsRequestOptions(t *testing.T) {
	for _, phase := range []corev1.PodPhase{corev1.PodRunning, corev1.PodSucceeded, corev1.PodFailed} {
		for _, container := range []string{"", "sidecar"} {
			t.Run(string(phase)+"/container="+container, func(t *testing.T) {
				store := newInMemoryAppStore()
				require.NoError(t, store.Add(context.Background(), &model.ApplicationComponent{
					AppID: "app-1", Name: "api", Namespace: " ",
				}))
				pod := newComponentLogPod("pod-api", config.DefaultNamespace, "app-1", "api", []corev1.Container{
					{Name: "sidecar"}, {Name: "api"},
				})
				pod.Status.Phase = phase
				podsJSON, err := json.Marshal(&corev1.PodList{
					TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PodList"},
					Items:    []corev1.Pod{*pod},
				})
				require.NoError(t, err)
				wantContainer := container
				if wantContainer == "" {
					wantContainer = "api"
				}
				logRequests := 0
				client := restfake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
					require.Equal(t, http.MethodGet, req.Method)
					response := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header)}
					switch req.URL.Path {
					case "/api/v1/namespaces/" + config.DefaultNamespace + "/pods":
						require.Equal(t, config.LabelAppID+"=app-1,"+config.LabelComponentName+"=api", req.URL.Query().Get("labelSelector"))
						response.Header.Set("Content-Type", "application/json")
						response.Body = io.NopCloser(bytes.NewReader(podsJSON))
					case "/api/v1/namespaces/" + config.DefaultNamespace + "/pods/pod-api/log":
						logRequests++
						query := req.URL.Query()
						require.Equal(t, wantContainer, query.Get("container"))
						if phase == corev1.PodRunning {
							require.Equal(t, "true", query.Get("follow"))
						} else {
							require.Empty(t, query.Get("follow"))
						}
						require.Equal(t, strconv.FormatInt(config.DefaultComponentLogTailLines, 10), query.Get("tailLines"))
						response.Body = io.NopCloser(strings.NewReader("log line\n"))
					default:
						return nil, fmt.Errorf("unexpected request: %s", req.URL)
					}
					return response, nil
				})
				svc := newMockServiceWithStore(store)
				svc.KubeClient, err = kubernetes.NewForConfigAndClient(&rest.Config{Host: "https://example.test"}, client)
				require.NoError(t, err)

				stream, err := svc.StreamComponentLogs(context.Background(), " app-1 ", " api ", " "+container+" ")
				require.NoError(t, err)
				defer stream.Close()
				require.Equal(t, 1, logRequests)
				require.Equal(t, config.DefaultNamespace, stream.Namespace)
				require.Equal(t, pod.Name, stream.PodName)
				require.Equal(t, wantContainer, stream.ContainerName)
				logs, err := io.ReadAll(stream.Reader)
				require.NoError(t, err)
				require.Equal(t, "log line\n", string(logs))
			})
		}
	}
}

func TestStreamComponentLogsInvalidRequestedContainer(t *testing.T) {
	store := newInMemoryAppStore()
	require.NoError(t, store.Add(context.Background(), &model.ApplicationComponent{
		AppID:     "app-1",
		Name:      "api",
		Namespace: config.DefaultNamespace,
	}))

	pod := newComponentLogPod("pod-api", config.DefaultNamespace, "app-1", "api", []corev1.Container{
		{Name: "sidecar"},
		{Name: "api"},
	})
	svc := newMockServiceWithStore(store)
	svc.KubeClient = k8sfake.NewSimpleClientset(pod)

	stream, err := svc.StreamComponentLogs(context.Background(), "app-1", "api", "missing")
	require.Nil(t, stream)
	require.ErrorIs(t, err, bcode.ErrComponentLogContainerInvalid)
}

func TestStreamComponentLogsNoNamedContainerReturnsUnavailable(t *testing.T) {
	store := newInMemoryAppStore()
	require.NoError(t, store.Add(context.Background(), &model.ApplicationComponent{
		AppID:     "app-1",
		Name:      "api",
		Namespace: config.DefaultNamespace,
	}))

	pod := newComponentLogPod("pod-api", config.DefaultNamespace, "app-1", "api", []corev1.Container{
		{Name: ""},
	})
	svc := newMockServiceWithStore(store)
	svc.KubeClient = k8sfake.NewSimpleClientset(pod)

	stream, err := svc.StreamComponentLogs(context.Background(), "app-1", "api", "")
	require.Nil(t, stream)
	require.ErrorIs(t, err, bcode.ErrComponentLogUnavailable)
}

func newComponentLogPod(name, namespace, appID, componentName string, containers []corev1.Container) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				config.LabelAppID:         appID,
				config.LabelComponentName: componentName,
			},
			CreationTimestamp: metav1.NewTime(time.Now()),
		},
		Spec: corev1.PodSpec{
			Containers: containers,
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}
}
