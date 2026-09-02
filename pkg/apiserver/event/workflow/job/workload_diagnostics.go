package job

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/kube"
)

const (
	workloadDiagnosticsLimitBytes = 60 * 1024
	workloadDiagnosticsMaxPods    = 3
	workloadDiagnosticsMaxEvents  = 10
	diagnosticsTruncatedMarker    = "[diagnostics truncated]"
)

var workloadDiagnosticsTimeout = 10 * time.Second

type workloadPodLogReader func(ctx context.Context, client kubernetes.Interface, namespace, podName, container string, previous bool) (string, error)

var (
	workloadPodLogReaderMu sync.RWMutex
	readWorkloadPodLogs    workloadPodLogReader = readPodContainerLogsWithMode
)

func workloadWaitError(ctx context.Context, client kubernetes.Interface, task *model.JobTask, kind, namespace, name, componentName string, cause error) error {
	if cause == nil {
		return nil
	}
	if errors.Is(cause, context.Canceled) {
		return cause
	}
	diagnostics := collectWorkloadFailureDiagnosticsWithTimeout(ctx, client, task, kind, namespace, name, componentName, cause)
	if strings.TrimSpace(diagnostics) == "" {
		return cause
	}
	return fmt.Errorf("%s", diagnostics)
}

func collectWorkloadFailureDiagnosticsWithTimeout(ctx context.Context, client kubernetes.Interface, task *model.JobTask, kind, namespace, name, componentName string, cause error) string {
	timeout := workloadDiagnosticsTimeout
	if timeout <= 0 {
		return collectWorkloadFailureDiagnostics(ctx, client, task, kind, namespace, name, componentName, cause)
	}

	diagnosticsCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	result := make(chan string, 1)
	go func() {
		result <- collectWorkloadFailureDiagnostics(diagnosticsCtx, client, task, kind, namespace, name, componentName, cause)
	}()

	select {
	case diagnostics := <-result:
		if diagnosticsCtx.Err() != nil {
			return ""
		}
		return diagnostics
	case <-diagnosticsCtx.Done():
		return ""
	}
}

func collectWorkloadFailureDiagnostics(ctx context.Context, client kubernetes.Interface, task *model.JobTask, kind, namespace, name, componentName string, cause error) string {
	if namespace == "" {
		namespace = config.DefaultNamespace
	}
	var appID string
	if task != nil {
		appID = task.AppID
	}

	var builder strings.Builder
	appendSummarySection(&builder, cause)
	appendWorkloadSection(&builder, kind, namespace, name, appID, componentName)

	if client == nil {
		appendSectionLine(&builder, "pods", "error: kubernetes client is nil")
		return truncateDiagnostics(builder.String())
	}
	if appID == "" || componentName == "" {
		appendSectionLine(&builder, "pods", fmt.Sprintf("error: missing pod selector appId=%q component=%q", appID, componentName))
		return truncateDiagnostics(builder.String())
	}

	pods, podErr := listDiagnosticPods(ctx, client, namespace, appID, componentName)
	if podErr != nil {
		appendSectionLine(&builder, "pods", fmt.Sprintf("error: %v", podErr))
		return truncateDiagnostics(builder.String())
	}
	if len(pods) == 0 {
		selector := labels.Set{
			config.LabelAppID:         appID,
			config.LabelComponentName: componentName,
		}.String()
		appendSectionLine(&builder, "pods", fmt.Sprintf("no pods found for selector %q", selector))
		return truncateDiagnostics(builder.String())
	}

	appendPodsSection(&builder, pods)
	appendEventsSection(ctx, &builder, client, namespace, pods)
	appendLogsSection(ctx, &builder, client, namespace, pods, true)
	appendLogsSection(ctx, &builder, client, namespace, pods, false)
	return truncateDiagnostics(builder.String())
}

func appendSummarySection(builder *strings.Builder, cause error) {
	builder.WriteString("summary:\n")
	if cause == nil {
		builder.WriteString("  error: <nil>\n")
		return
	}
	builder.WriteString("  error: ")
	builder.WriteString(strings.TrimSpace(cause.Error()))
	builder.WriteString("\n")
}

func appendWorkloadSection(builder *strings.Builder, kind, namespace, name, appID, componentName string) {
	builder.WriteString("\nworkload:\n")
	builder.WriteString(fmt.Sprintf("  kind: %s\n", blankAsUnknown(kind)))
	builder.WriteString(fmt.Sprintf("  namespace: %s\n", blankAsUnknown(namespace)))
	builder.WriteString(fmt.Sprintf("  name: %s\n", blankAsUnknown(name)))
	builder.WriteString(fmt.Sprintf("  appId: %s\n", blankAsUnknown(appID)))
	builder.WriteString(fmt.Sprintf("  component: %s\n", blankAsUnknown(componentName)))
}

func appendSectionLine(builder *strings.Builder, section, line string) {
	builder.WriteString("\n")
	builder.WriteString(section)
	builder.WriteString(":\n")
	builder.WriteString("  ")
	builder.WriteString(line)
	builder.WriteString("\n")
}

func listDiagnosticPods(ctx context.Context, client kubernetes.Interface, namespace, appID, componentName string) ([]corev1.Pod, error) {
	selector := labels.Set{
		config.LabelAppID:         appID,
		config.LabelComponentName: componentName,
	}.AsSelector().String()
	podList, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, fmt.Errorf("list pods namespace=%s selector=%q: %w", namespace, selector, err)
	}
	pods := append([]corev1.Pod(nil), podList.Items...)
	sort.Slice(pods, func(i, j int) bool {
		leftRank := diagnosticPodRank(&pods[i])
		rightRank := diagnosticPodRank(&pods[j])
		if leftRank != rightRank {
			return leftRank < rightRank
		}
		left := pods[i].CreationTimestamp.Time
		right := pods[j].CreationTimestamp.Time
		if !left.Equal(right) {
			return left.After(right)
		}
		return pods[i].Name < pods[j].Name
	})
	if len(pods) > workloadDiagnosticsMaxPods {
		pods = pods[:workloadDiagnosticsMaxPods]
	}
	return pods, nil
}

func diagnosticPodRank(pod *corev1.Pod) int {
	if podHasCurrentAbnormalState(pod) {
		return 0
	}
	if podIsCurrentlyNotReady(pod) {
		return 1
	}
	if podHasDiagnosticEvidence(pod) {
		return 2
	}
	return 3
}

func podHasCurrentAbnormalState(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	for _, status := range pod.Status.InitContainerStatuses {
		if containerHasCurrentAbnormalState(status) {
			return true
		}
	}
	for _, status := range pod.Status.ContainerStatuses {
		if containerHasCurrentAbnormalState(status) {
			return true
		}
	}
	return false
}

func containerHasCurrentAbnormalState(status corev1.ContainerStatus) bool {
	if status.State.Waiting != nil && kube.IsAbnormalWaitingReason(strings.TrimSpace(status.State.Waiting.Reason)) {
		return true
	}
	return isAbnormalTermination(status.State.Terminated)
}

func podIsCurrentlyNotReady(pod *corev1.Pod) bool {
	if pod == nil || pod.DeletionTimestamp != nil {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status != corev1.ConditionTrue
		}
	}
	return true
}

func podHasDiagnosticEvidence(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	for _, status := range pod.Status.InitContainerStatuses {
		if containerNeedsDiagnostics(status) {
			return true
		}
	}
	for _, status := range pod.Status.ContainerStatuses {
		if containerNeedsDiagnostics(status) {
			return true
		}
	}
	return false
}

func appendPodsSection(builder *strings.Builder, pods []corev1.Pod) {
	builder.WriteString("\npods:\n")
	for _, pod := range pods {
		builder.WriteString(fmt.Sprintf("  - name: %s phase: %s reason: %s message: %s\n",
			pod.Name,
			blankAsUnknown(string(pod.Status.Phase)),
			blankAsUnknown(pod.Status.Reason),
			blankAsNone(pod.Status.Message),
		))
		appendPodConditions(builder, pod.Status.Conditions)
		appendContainerStatuses(builder, "initContainers", pod.Status.InitContainerStatuses)
		appendContainerStatuses(builder, "containers", pod.Status.ContainerStatuses)
	}
}

func appendPodConditions(builder *strings.Builder, conditions []corev1.PodCondition) {
	if len(conditions) == 0 {
		return
	}
	builder.WriteString("    conditions:\n")
	for _, condition := range conditions {
		builder.WriteString(fmt.Sprintf("      - type: %s status: %s reason: %s message: %s\n",
			condition.Type,
			condition.Status,
			blankAsNone(condition.Reason),
			blankAsNone(condition.Message),
		))
	}
}

func appendContainerStatuses(builder *strings.Builder, title string, statuses []corev1.ContainerStatus) {
	if len(statuses) == 0 {
		return
	}
	builder.WriteString("    ")
	builder.WriteString(title)
	builder.WriteString(":\n")
	for _, status := range statuses {
		builder.WriteString(fmt.Sprintf("      - name: %s ready: %t restartCount: %d state: %s\n",
			status.Name,
			status.Ready,
			status.RestartCount,
			containerStateSummary(status.State),
		))
		if last := containerStateSummary(status.LastTerminationState); last != "unknown" {
			builder.WriteString(fmt.Sprintf("        lastState: %s\n", last))
		}
	}
}

func appendEventsSection(ctx context.Context, builder *strings.Builder, client kubernetes.Interface, namespace string, pods []corev1.Pod) {
	builder.WriteString("\nevents:\n")
	events, errLines := collectPodEvents(ctx, client, namespace, pods)
	for _, line := range errLines {
		builder.WriteString("  - ")
		builder.WriteString(line)
		builder.WriteString("\n")
	}
	if len(events) == 0 {
		if len(errLines) == 0 {
			builder.WriteString("  - no events found\n")
		}
		return
	}
	for i, event := range events {
		if i >= workloadDiagnosticsMaxEvents {
			builder.WriteString(fmt.Sprintf("  - omitted %d older events\n", len(events)-i))
			break
		}
		builder.WriteString(fmt.Sprintf("  - %s pod=%s type=%s reason=%s message=%s\n",
			formatEventTime(event),
			event.InvolvedObject.Name,
			blankAsUnknown(event.Type),
			blankAsNone(event.Reason),
			blankAsNone(event.Message),
		))
	}
}

func collectPodEvents(ctx context.Context, client kubernetes.Interface, namespace string, pods []corev1.Pod) ([]corev1.Event, []string) {
	var events []corev1.Event
	var errLines []string
	for _, pod := range pods {
		selector := fields.AndSelectors(
			fields.OneTermEqualSelector("involvedObject.name", pod.Name),
			fields.OneTermEqualSelector("involvedObject.namespace", namespace),
		).String()
		list, err := client.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{FieldSelector: selector})
		if err != nil {
			errLines = append(errLines, fmt.Sprintf("read events for pod %s failed: %v", pod.Name, err))
			continue
		}
		events = append(events, list.Items...)
	}
	sort.Slice(events, func(i, j int) bool {
		return eventTime(events[i]).After(eventTime(events[j]))
	})
	return events, errLines
}

func appendLogsSection(ctx context.Context, builder *strings.Builder, client kubernetes.Interface, namespace string, pods []corev1.Pod, previous bool) {
	if previous {
		builder.WriteString("\nprevious logs:\n")
	} else {
		builder.WriteString("\ncurrent logs:\n")
	}
	wrote := false
	for _, pod := range pods {
		for _, container := range diagnosticContainerNames(&pod) {
			wrote = true
			builder.WriteString(fmt.Sprintf("  === Pod: %s (container: %s) ===\n", pod.Name, container))
			workloadPodLogReaderMu.RLock()
			logReader := readWorkloadPodLogs
			workloadPodLogReaderMu.RUnlock()
			logText, err := logReader(ctx, client, namespace, pod.Name, container, previous)
			if err != nil {
				mode := "current"
				if previous {
					mode = "previous"
				}
				builder.WriteString(fmt.Sprintf("  read %s logs failed: %v\n", mode, err))
				continue
			}
			logText = strings.TrimRight(logText, "\n")
			if strings.TrimSpace(logText) == "" {
				builder.WriteString("  no logs returned\n")
				continue
			}
			appendIndentedBlock(builder, logText)
		}
	}
	if !wrote {
		builder.WriteString("  - no containers found\n")
	}
}

func diagnosticContainerNames(pod *corev1.Pod) []string {
	if pod == nil {
		return nil
	}
	seen := make(map[string]struct{})
	var names []string
	add := func(name string) {
		if name == "" {
			return
		}
		if _, ok := seen[name]; ok {
			return
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	for _, status := range pod.Status.InitContainerStatuses {
		if containerNeedsDiagnostics(status) {
			add(status.Name)
		}
	}
	for _, status := range pod.Status.ContainerStatuses {
		if containerNeedsDiagnostics(status) {
			add(status.Name)
		}
	}
	if len(names) > 0 {
		return names
	}
	return podLogContainerNames(pod)
}

func containerNeedsDiagnostics(status corev1.ContainerStatus) bool {
	if status.RestartCount > 0 {
		return true
	}
	if status.State.Waiting != nil && kube.IsAbnormalWaitingReason(strings.TrimSpace(status.State.Waiting.Reason)) {
		return true
	}
	if isAbnormalTermination(status.State.Terminated) {
		return true
	}
	return isAbnormalTermination(status.LastTerminationState.Terminated)
}

func isAbnormalTermination(terminated *corev1.ContainerStateTerminated) bool {
	if terminated == nil {
		return false
	}
	reason := strings.TrimSpace(terminated.Reason)
	return reason == "OOMKilled" || reason == "Error" || terminated.ExitCode != 0
}

func containerStateSummary(state corev1.ContainerState) string {
	switch {
	case state.Waiting != nil:
		return fmt.Sprintf("waiting reason=%s message=%s", blankAsNone(state.Waiting.Reason), blankAsNone(state.Waiting.Message))
	case state.Running != nil:
		return fmt.Sprintf("running startedAt=%s", formatMetaTime(state.Running.StartedAt))
	case state.Terminated != nil:
		return fmt.Sprintf("terminated reason=%s exitCode=%d signal=%d message=%s finishedAt=%s",
			blankAsNone(state.Terminated.Reason),
			state.Terminated.ExitCode,
			state.Terminated.Signal,
			blankAsNone(state.Terminated.Message),
			formatMetaTime(state.Terminated.FinishedAt),
		)
	default:
		return "unknown"
	}
}

func appendIndentedBlock(builder *strings.Builder, text string) {
	for _, line := range strings.Split(text, "\n") {
		builder.WriteString("  ")
		builder.WriteString(line)
		builder.WriteString("\n")
	}
}

func truncateDiagnostics(text string) string {
	if len(text) <= workloadDiagnosticsLimitBytes {
		return text
	}
	marker := "\n" + diagnosticsTruncatedMarker + "\n"
	if workloadDiagnosticsLimitBytes <= len(marker)+2 {
		return diagnosticsTruncatedMarker
	}
	head := workloadDiagnosticsLimitBytes / 2
	tail := workloadDiagnosticsLimitBytes - head - len(marker)
	if tail < 0 {
		tail = 0
	}
	return text[:head] + marker + text[len(text)-tail:]
}

func eventTime(event corev1.Event) time.Time {
	if !event.EventTime.IsZero() {
		return event.EventTime.Time
	}
	if !event.LastTimestamp.IsZero() {
		return event.LastTimestamp.Time
	}
	if !event.FirstTimestamp.IsZero() {
		return event.FirstTimestamp.Time
	}
	return event.CreationTimestamp.Time
}

func formatEventTime(event corev1.Event) string {
	ts := eventTime(event)
	if ts.IsZero() {
		return "unknown-time"
	}
	return ts.Format(time.RFC3339)
}

func formatMetaTime(ts metav1.Time) string {
	if ts.IsZero() {
		return "unknown"
	}
	return ts.Time.Format(time.RFC3339)
}

func blankAsUnknown(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}
	return value
}

func blankAsNone(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "<none>"
	}
	return value
}
