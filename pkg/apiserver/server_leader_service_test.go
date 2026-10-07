package apiserver

import (
	"context"
	"fmt"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/stretchr/testify/require"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
)

func leaderServiceFixture() (*restServer, *fake.Clientset) {
	cfg := config.NewConfig()
	cfg.LeaderConfig.ID = "process-a"
	cfg.LeaderConfig.PodName = "node-a"
	cfg.LeaderConfig.Namespace = "runtime"
	cfg.LeaderConfig.ServiceName = "eruun"
	client := fake.NewClientset(
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "node-a", Namespace: "runtime", UID: "pod-a", ResourceVersion: "1", Labels: map[string]string{"app": "eruun"}}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "eruun", Namespace: "runtime", ResourceVersion: "7"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "eruun", runtimeIdentityLabel: unassignedRuntimeIdentity}}},
		&coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: cfg.LeaderConfig.LockName, Namespace: "runtime"}, Spec: coordinationv1.LeaseSpec{HolderIdentity: ptr.To("process-a")}},
	)
	return &restServer{cfg: *cfg, KubeClient: client}, client
}

func TestLeaderServiceSelectsPodAndWithdrawsWithoutChangingOtherSelectors(t *testing.T) {
	s, c := leaderServiceFixture()
	ctx := context.Background()
	require.NoError(t, s.publishLeaderService(ctx))
	pod, err := c.CoreV1().Pods("runtime").Get(ctx, "node-a", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "pod-a", pod.Labels[runtimeIdentityLabel])
	svc, err := c.CoreV1().Services("runtime").Get(ctx, "eruun", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, map[string]string{"app": "eruun", runtimeIdentityLabel: "pod-a"}, svc.Spec.Selector)
	c.ClearActions()
	require.NoError(t, s.publishLeaderService(ctx))
	for _, a := range c.Actions() {
		require.NotEqual(t, "update", a.GetVerb(), "steady state must not rewrite Service")
	}
	require.NoError(t, s.withdrawLeaderService(ctx))
	svc, err = c.CoreV1().Services("runtime").Get(ctx, "eruun", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, unassignedRuntimeIdentity, svc.Spec.Selector[runtimeIdentityLabel])
	require.Equal(t, "eruun", svc.Spec.Selector["app"])
}

func TestLeaderServiceRejectsOldOwner(t *testing.T) {
	s, c := leaderServiceFixture()
	ctx := context.Background()
	lease, err := c.CoordinationV1().Leases("runtime").Get(ctx, s.cfg.LeaderConfig.LockName, metav1.GetOptions{})
	require.NoError(t, err)
	lease.Spec.HolderIdentity = ptr.To("node-b")
	_, err = c.CoordinationV1().Leases("runtime").Update(ctx, lease, metav1.UpdateOptions{})
	require.NoError(t, err)
	c.ClearActions()
	require.ErrorContains(t, s.publishLeaderService(ctx), "another node")
	for _, a := range c.Actions() {
		require.False(t, a.GetVerb() == "update" && a.GetResource().Resource == "services")
	}
}

func TestLeaderServiceCASDoesNotOverwriteSuccessor(t *testing.T) {
	for _, withdraw := range []bool{false, true} {
		t.Run(fmt.Sprint("withdraw=", withdraw), func(t *testing.T) {
			s, c := leaderServiceFixture()
			ctx := context.Background()
			require.NoError(t, s.publishLeaderService(ctx))
			svc, err := c.CoreV1().Services("runtime").Get(ctx, "eruun", metav1.GetOptions{})
			require.NoError(t, err)
			if !withdraw {
				svc.Spec.Selector[runtimeIdentityLabel] = unassignedRuntimeIdentity
				_, err = c.CoreV1().Services("runtime").Update(ctx, svc, metav1.UpdateOptions{})
				require.NoError(t, err)
			}
			attempts := 0
			c.PrependReactor("update", "services", func(action ktesting.Action) (bool, runtime.Object, error) {
				attempts++
				stale := action.(ktesting.UpdateAction).GetObject().(*corev1.Service)
				require.Equal(t, "7", stale.ResourceVersion)
				successor := svc.DeepCopy()
				successor.ResourceVersion = "8"
				successor.Spec.Selector[runtimeIdentityLabel] = "pod-b"
				require.NoError(t, c.Tracker().Update(corev1.SchemeGroupVersion.WithResource("services"), successor, "runtime"))
				return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "services"}, "eruun", fmt.Errorf("successor published"))
			})
			if withdraw {
				require.NoError(t, s.withdrawLeaderService(ctx))
			} else {
				require.Error(t, s.publishLeaderService(ctx))
			}
			require.Equal(t, 1, attempts, "must not retry a stale write against successor version")
			svc, err = c.CoreV1().Services("runtime").Get(ctx, "eruun", metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, "pod-b", svc.Spec.Selector[runtimeIdentityLabel])
		})
	}
}

func TestOldLeaderDoesNotWithdrawSuccessor(t *testing.T) {
	s, c := leaderServiceFixture()
	ctx := context.Background()
	s.leaderPodUID = "pod-a"
	svc, err := c.CoreV1().Services("runtime").Get(ctx, "eruun", metav1.GetOptions{})
	require.NoError(t, err)
	svc.Spec.Selector[runtimeIdentityLabel] = "pod-b"
	_, err = c.CoreV1().Services("runtime").Update(ctx, svc, metav1.UpdateOptions{})
	require.NoError(t, err)
	c.ClearActions()
	require.NoError(t, s.withdrawLeaderService(ctx))
	for _, a := range c.Actions() {
		require.NotEqual(t, "update", a.GetVerb())
	}
}

func TestLeaderServiceRejectsUnmanagedServiceAndCancelledTerm(t *testing.T) {
	s, c := leaderServiceFixture()
	ctx := context.Background()
	svc, err := c.CoreV1().Services("runtime").Get(ctx, "eruun", metav1.GetOptions{})
	require.NoError(t, err)
	delete(svc.Spec.Selector, runtimeIdentityLabel)
	_, err = c.CoreV1().Services("runtime").Update(ctx, svc, metav1.UpdateOptions{})
	require.NoError(t, err)
	require.ErrorContains(t, s.publishLeaderService(ctx), "selector must contain")
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, _, ok := s.requestLeadership(cancelled)
	require.False(t, ok)
	s.cfg.LeaderConfig.ServiceName = ""
	require.NoError(t, s.publishLeaderService(ctx), "local mode does not modify Services")
}

func TestLeaderServiceRejectsReplacementPod(t *testing.T) {
	s, c := leaderServiceFixture()
	s.leaderPodUID = "previous-pod"
	require.ErrorContains(t, s.publishLeaderService(context.Background()), "replaced")
	for _, action := range c.Actions() {
		require.NotEqual(t, "update", action.GetVerb())
		require.NotEqual(t, "patch", action.GetVerb())
	}
}

func TestLeaderServiceRepairsSelectorReset(t *testing.T) {
	s, c := leaderServiceFixture()
	ctx := context.Background()
	require.NoError(t, s.publishLeaderService(ctx))
	svc, err := c.CoreV1().Services("runtime").Get(ctx, "eruun", metav1.GetOptions{})
	require.NoError(t, err)
	svc.Spec.Selector[runtimeIdentityLabel] = unassignedRuntimeIdentity
	_, err = c.CoreV1().Services("runtime").Update(ctx, svc, metav1.UpdateOptions{})
	require.NoError(t, err)
	require.NoError(t, s.publishLeaderService(ctx), "the current term repairs a Helm/recreation reset")
	svc, err = c.CoreV1().Services("runtime").Get(ctx, "eruun", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "pod-a", svc.Spec.Selector[runtimeIdentityLabel])
}
