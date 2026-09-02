package job

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	applyv1 "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	spec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

type DeployServiceJobCtl struct {
	deployNamespacedResourceJobBase
}

func NewDeployServiceJobCtl(job *model.JobTask, client kubernetes.Interface, store datastore.DataStore, ack func(), shareLocker locker.Locker) *DeployServiceJobCtl {
	base, ok := newDeployNamespacedResourceJobBase("NewDeployServiceJobCtl", job, client, store, ack, shareLocker)
	if !ok {
		return nil
	}
	return &DeployServiceJobCtl{
		deployNamespacedResourceJobBase: base,
	}
}

func (c *DeployServiceJobCtl) Clean(ctx context.Context) {
	c.cleanCreated(ctx, config.ResourceService, "service", func(ctx context.Context, namespace, name string) error {
		return c.client.CoreV1().Services(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	}, k8serrors.IsNotFound, "after job failure")
}

func (c *DeployServiceJobCtl) Run(ctx context.Context) error {
	return c.runWithWait(ctx, c.run, c.wait, "DeployServiceJob run error", "")
}

func (c *DeployServiceJobCtl) run(ctx context.Context) error {
	if c.client == nil {
		return fmt.Errorf("client is nil")
	}

	service, err := serviceApplyFromJobInfo(c.job)
	if err != nil {
		return err
	}

	// 必要字段检查
	if service.Name == nil || service.Namespace == nil {
		return fmt.Errorf("service name or namespace is nil")
	}
	name := *service.Name
	namespace := *service.Namespace
	if binding, adopted, sourceErr := adoptedResourceForJob(
		ctx,
		c.store,
		c.job,
		"Service",
		namespace,
		name,
	); sourceErr != nil {
		return sourceErr
	} else if adopted {
		return c.reconcileAdoptedService(ctx, serviceFromApplyConfig(service), binding)
	}

	unlock, skipped, err := resolveSharedResourceAccess(ctx, sharedResourceAccessOptions{
		job:          c.job,
		ack:          c.ack,
		labels:       service.Labels,
		kind:         config.ResourceService,
		lockProvider: c.shareLocker,
		listFn: func(ctx context.Context, opts metav1.ListOptions) (int, error) {
			list, err := c.client.CoreV1().Services(namespace).List(ctx, opts)
			if err != nil {
				return 0, err
			}
			return len(list.Items), nil
		},
		logSkip: func(strategy config.ShareStrategy) {
			if strategy == config.ShareStrategyIgnore {
				klog.Infof("Service %s/%s marked as shared ignore; skipping", namespace, name)
			} else {
				klog.Infof("Service %s/%s already exists and is shared; skipping", namespace, name)
			}
		},
	})
	if err != nil {
		return fmt.Errorf("resolve shared services failed: %w", err)
	}
	if unlock != nil {
		defer unlock()
	}
	if skipped {
		return nil
	}

	if err := trackResourcePresence(ctx, config.ResourceService, namespace, name, func(ctx context.Context) (*corev1.Service, error) {
		return c.client.CoreV1().Services(namespace).Get(ctx, name, metav1.GetOptions{})
	}, k8serrors.IsNotFound); err != nil {
		return fmt.Errorf("check service existence failed: %w", err)
	}

	// 直接使用 ApplyService 处理创建或更新
	updated, err := c.ApplyService(ctx, service)
	if err != nil {
		klog.Errorf("failed to apply service %q: %v", *service.Name, err)
		return fmt.Errorf("apply service failed: %w", err)
	}
	klog.Infof("Service %q applied successfully.", updated.Name)

	return nil
}

func (c *DeployServiceJobCtl) reconcileAdoptedService(
	ctx context.Context,
	desired *corev1.Service,
	binding *adoptedResourceBinding,
) error {
	if desired == nil || desired.Name == "" || desired.Namespace == "" {
		return fmt.Errorf("adopted service desired resource identity is required")
	}
	writable, err := adoptedResourceAllowsWrite(binding)
	if err != nil {
		return err
	}
	if !writable {
		return nil
	}
	cli := c.client.CoreV1().Services(desired.Namespace)
	current, err := cli.Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return c.recreateAdoptedService(ctx, desired, binding)
		}
		return fmt.Errorf("get adopted service %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	if err := validateAdoptedSnapshotUID(current.UID, binding); err != nil {
		recovered, recoverErr := recoverPendingAdoptedDependency(
			ctx, c.store, binding, current, current, c.runtime, c.shareLocker,
		)
		if recoverErr != nil {
			return fmt.Errorf("recover recreated adopted service binding: %w", recoverErr)
		}
		if !recovered {
			return err
		}
	}
	if !adoptedServiceNeedsUpdate(current, desired) {
		markResourceObserved(ctx, config.ResourceService, desired.Namespace, desired.Name)
		return nil
	}
	if err := updateResourceWithRetry(ctx, func(ctx context.Context) (*corev1.Service, error) {
		return cli.Get(ctx, desired.Name, metav1.GetOptions{})
	}, func(ctx context.Context, latest *corev1.Service) error {
		if err := validateAdoptedSnapshotUID(latest.UID, binding); err != nil {
			return err
		}
		candidate := adoptedServiceForExistingUpdate(latest, desired)
		if apiequality.Semantic.DeepEqual(latest.Labels, candidate.Labels) &&
			apiequality.Semantic.DeepEqual(latest.Annotations, candidate.Annotations) &&
			apiequality.Semantic.DeepEqual(latest.Spec, candidate.Spec) {
			return nil
		}
		_, err := cli.Update(ctx, candidate, metav1.UpdateOptions{})
		return err
	}); err != nil {
		return fmt.Errorf("update adopted service %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	markResourceObserved(ctx, config.ResourceService, desired.Namespace, desired.Name)
	return nil
}

func (c *DeployServiceJobCtl) recreateAdoptedService(
	ctx context.Context,
	desired *corev1.Service,
	binding *adoptedResourceBinding,
) error {
	recreation, err := prepareAdoptedDependencyRecreation(c.store, binding)
	if err != nil {
		return fmt.Errorf("prepare adopted service recreation: %w", err)
	}
	var baseline corev1.Service
	if err := json.Unmarshal(recreation.resource.Manifest, &baseline); err != nil {
		return fmt.Errorf("decode adopted service recreation manifest: %w", err)
	}
	if baseline.Name != desired.Name || baseline.Namespace != desired.Namespace {
		return fmt.Errorf(
			"adopted service recreation manifest identity %s/%s does not match %s/%s",
			baseline.Namespace,
			baseline.Name,
			desired.Namespace,
			desired.Name,
		)
	}
	candidate := adoptedServiceForExistingUpdate(&baseline, desired)
	cleanObjectMeta(&candidate.ObjectMeta)
	candidate.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Service"}
	guard, err := recreation.adoptedResourceBinding.prepareRecreationCandidate(ctx, c.store, candidate, c.runtime, c.shareLocker)
	if err != nil {
		return fmt.Errorf("persist adopted service recreation claim: %w", err)
	}
	defer guard.release()
	recreationCtx := guard.Context()
	created, err := c.client.CoreV1().Services(candidate.Namespace).Create(recreationCtx, candidate, metav1.CreateOptions{})
	if err != nil {
		if k8serrors.IsAlreadyExists(err) {
			replacement, getErr := c.client.CoreV1().Services(candidate.Namespace).Get(recreationCtx, candidate.Name, metav1.GetOptions{})
			if getErr != nil {
				return fmt.Errorf("get concurrent recreated adopted service %s/%s: %w", candidate.Namespace, candidate.Name, getErr)
			}
			recovered, recoverErr := recoverPendingAdoptedDependencyLocked(
				recreationCtx,
				c.store,
				&recreation.adoptedResourceBinding,
				replacement,
				replacement,
				c.runtime,
			)
			if recoverErr != nil {
				return fmt.Errorf("recover concurrent adopted service recreation: %w", recoverErr)
			}
			if recovered {
				guard.release()
				return c.reconcileAdoptedService(ctx, desired, &recreation.adoptedResourceBinding)
			}
			return fmt.Errorf(
				"adopted service ownership conflict for %s/%s: expected missing UID %q, found %q",
				candidate.Namespace,
				candidate.Name,
				recreation.resource.Source.UID,
				replacement.UID,
			)
		}
		return fmt.Errorf("recreate adopted service %s/%s: %w", candidate.Namespace, candidate.Name, err)
	}
	if err := recreation.persistCreated(recreationCtx, created, created, c.runtime); err != nil {
		return fmt.Errorf("persist recreated adopted service binding; pending claim retained: %w", err)
	}
	markResourceObserved(ctx, config.ResourceService, created.Namespace, created.Name)
	return nil
}

func adoptedServiceForExistingUpdate(current, desired *corev1.Service) *corev1.Service {
	updated := current.DeepCopy()
	if desired == nil {
		return updated
	}
	updated.Labels = adoptedManagedStringMap(current.Labels, desired.Labels, eruunSystemLabelKeys)
	updated.Annotations = adoptedManagedStringMap(
		current.Annotations,
		desired.Annotations,
		[]string{config.AnnotationComponentName},
	)
	if desired.Spec.Type != "" {
		updated.Spec.Type = desired.Spec.Type
	}
	if desired.Spec.Selector != nil {
		updated.Spec.Selector = utils.CopyStringMap(desired.Spec.Selector)
	}
	if desired.Spec.Ports != nil {
		updated.Spec.Ports = mergeAdoptedServicePorts(current.Spec.Ports, desired.Spec.Ports)
	}
	if desired.Spec.Type == corev1.ServiceTypeExternalName || desired.Spec.ExternalName != "" {
		updated.Spec.ExternalName = desired.Spec.ExternalName
	}
	copyServicePreservedFields(updated, current)
	return updated
}

func mergeAdoptedServicePorts(current, desired []corev1.ServicePort) []corev1.ServicePort {
	merged := append([]corev1.ServicePort(nil), current...)
	for _, desiredPort := range desired {
		index := -1
		for currentIndex := range merged {
			if desiredPort.Name != "" && merged[currentIndex].Name == desiredPort.Name {
				index = currentIndex
				break
			}
			if desiredPort.Name == "" &&
				merged[currentIndex].Port == desiredPort.Port &&
				merged[currentIndex].Protocol == desiredPort.Protocol {
				index = currentIndex
				break
			}
		}
		if index < 0 {
			merged = append(merged, desiredPort)
			continue
		}
		if desiredPort.NodePort == 0 {
			desiredPort.NodePort = merged[index].NodePort
		}
		// A Service trait cannot currently represent a named targetPort.
		// Adopted Services therefore keep the live target identity; supported
		// adopted version updates do not include Service port changes.
		desiredPort.TargetPort = merged[index].TargetPort
		merged[index] = desiredPort
	}
	return merged
}

func adoptedServiceNeedsUpdate(current, desired *corev1.Service) bool {
	if current == nil || desired == nil {
		return false
	}
	updated := adoptedServiceForExistingUpdate(current, desired)
	return !apiequality.Semantic.DeepEqual(current.Labels, updated.Labels) ||
		!apiequality.Semantic.DeepEqual(current.Annotations, updated.Annotations) ||
		!apiequality.Semantic.DeepEqual(current.Spec, updated.Spec)
}

func (c *DeployServiceJobCtl) timeout() int {
	if c.job.Timeout == 0 {
		c.job.Timeout = 60 * 10
	}
	return int(c.job.Timeout)
}

func (c *DeployServiceJobCtl) wait(ctx context.Context) error {
	serviceName := resolveServiceName(c.job)
	if serviceName == "" {
		serviceName = buildServiceName(c.job.Name, c.job.ResourceAppNameOrID())
	}
	return waitForPolledResource(ctx, pollWaitOptions{
		timeout:  time.Duration(c.timeout()) * time.Second,
		interval: 2 * time.Second,
		poll: func(ctx context.Context) (bool, error) {
			return getServiceStatus(ctx, c.client, c.job.Namespace, serviceName)
		},
		onCancel: func(err error) error {
			return NewStatusError(config.StatusCancelled, fmt.Errorf("service %s cancelled: %w", c.job.Name, err))
		},
		onTimeout: func() error {
			klog.Warningf("timed out waiting for service: %s", c.job.Name)
			return NewStatusError(config.StatusTimeout, fmt.Errorf("wait service %s timeout", c.job.Name))
		},
		onError: func(err error) error {
			klog.Errorf("error checking service status: %v", err)
			return fmt.Errorf("wait service %s error: %w", c.job.Name, err)
		},
	})
}

func getServiceStatus(ctx context.Context, kubeClient kubernetes.Interface, namespace string, name string) (bool, error) {
	klog.Infof("Checking service: %s/%s", namespace, name)

	_, err := kubeClient.CoreV1().Services(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			klog.Errorf("service not found: %s/%s", namespace, name)
			return false, nil
		}
		klog.Errorf("check service error:%s", err)
		return false, err
	}

	return true, nil
}

func GenerateService(component *model.ApplicationComponent, properties *model.Properties) *applyv1.ServiceApplyConfiguration {
	var servicePorts []*applyv1.ServicePortApplyConfiguration
	base := utils.ToRFC1123Name(component.Name)

	for _, p := range properties.Ports {
		port := applyv1.ServicePort().
			WithName(defaultServicePortName(base, p.Port)).
			WithPort(p.Port).
			WithTargetPort(intstr.FromInt32(p.Port)).
			WithProtocol(corev1.ProtocolTCP)
		servicePorts = append(servicePorts, port)
	}

	labels := BuildLabels(component, properties)

	selectorLabel := defaultServiceSelector(component)

	serviceName := buildServiceName(component.Name, component.ResourceNameKey())
	svc := applyv1.Service(serviceName, component.Namespace).
		WithLabels(labels).
		WithAnnotations(BuildAnnotations(component)).
		WithSpec(applyv1.ServiceSpec().
			WithSelector(selectorLabel).
			WithPorts(servicePorts...).
			WithType(corev1.ServiceTypeClusterIP)).
		WithKind("Service").
		WithAPIVersion("v1").
		WithName(serviceName).
		WithNamespace(component.Namespace)

	return svc
}

func GenerateServiceFromTrait(component *model.ApplicationComponent, properties *model.Properties, serviceTrait spec.ServiceTraitSpec) *applyv1.ServiceApplyConfiguration {
	var servicePorts []*applyv1.ServicePortApplyConfiguration
	base := utils.ToRFC1123Name(component.Name)

	for _, p := range serviceTrait.Ports {
		if p.Port <= 0 {
			continue
		}
		portName := strings.TrimSpace(p.Name)
		if portName == "" {
			portName = defaultServicePortName(base, p.Port)
		}

		targetPort := p.TargetPort
		if targetPort <= 0 {
			targetPort = p.Port
		}

		port := applyv1.ServicePort().
			WithName(portName).
			WithPort(p.Port).
			WithTargetPort(intstr.FromInt32(targetPort)).
			WithProtocol(serviceProtocolFromTrait(p.Protocol))
		servicePorts = append(servicePorts, port)
	}

	labels := BuildLabels(component, properties)
	for k, v := range naming.NormalizeLabelValues(serviceTrait.Labels) {
		labels[k] = v
	}
	labels = ApplyComponentManagedLabels(labels, component)

	serviceType := serviceTypeFromTrait(serviceTrait.Type)
	selectorLabel := serviceSelectorForTrait(component, properties, serviceType, serviceTrait.Selector)

	serviceName := strings.TrimSpace(serviceTrait.Name)
	if serviceName == "" {
		serviceName = buildServiceName(component.Name, component.ResourceNameKey())
	}

	specConfig := applyv1.ServiceSpec().WithType(serviceType)
	if serviceType == corev1.ServiceTypeExternalName {
		if externalName := strings.TrimSpace(serviceTrait.ExternalName); externalName != "" {
			specConfig.WithExternalName(externalName)
		}
		if len(servicePorts) > 0 {
			specConfig.WithPorts(servicePorts...)
		}
	} else {
		specConfig.WithSelector(selectorLabel).WithPorts(servicePorts...)
	}
	if serviceTrait.Headless && serviceType == corev1.ServiceTypeClusterIP {
		specConfig.WithClusterIP(corev1.ClusterIPNone)
	}

	svc := applyv1.Service(serviceName, component.Namespace).
		WithLabels(labels).
		WithAnnotations(BuildAnnotations(component)).
		WithSpec(specConfig).
		WithKind("Service").
		WithAPIVersion("v1").
		WithName(serviceName).
		WithNamespace(component.Namespace)

	return svc
}

func resolveServiceName(jobTask *model.JobTask) string {
	if jobTask == nil {
		return ""
	}
	service, ok := optionalJobInfo[*applyv1.ServiceApplyConfiguration](jobTask)
	if !ok || service.Name == nil {
		return ""
	}
	return strings.TrimSpace(*service.Name)
}

func serviceTypeFromTrait(raw string) corev1.ServiceType {
	normalized, _ := config.NormalizeServiceAccessType(raw)
	return config.ToKubeServiceType(normalized)
}

func serviceProtocolFromTrait(raw string) corev1.Protocol {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case string(corev1.ProtocolUDP):
		return corev1.ProtocolUDP
	case string(corev1.ProtocolSCTP):
		return corev1.ProtocolSCTP
	default:
		return corev1.ProtocolTCP
	}
}

func defaultServicePortName(base string, port int32) string {
	portName := fmt.Sprintf("%s-%d", base, port)
	if len(portName) > 15 {
		return fmt.Sprintf("p-%d", port)
	}
	return portName
}

func defaultServiceSelector(component *model.ApplicationComponent) map[string]string {
	if component == nil {
		return nil
	}
	selector := map[string]string{
		config.LabelAppID:         component.AppID,
		config.LabelComponentName: naming.BoundedLabelValue(component.Name),
	}
	return selector
}

func serviceSelectorForTrait(component *model.ApplicationComponent, properties *model.Properties, serviceType corev1.ServiceType, selector map[string]string) map[string]string {
	selectorLabel := normalizeServiceTraitSelector(selector, properties)
	bindManagedServiceSelectorIdentity(selectorLabel, component)
	if serviceType != corev1.ServiceTypeExternalName && len(selectorLabel) == 0 {
		return defaultServiceSelector(component)
	}
	return selectorLabel
}

func bindManagedServiceSelectorIdentity(selector map[string]string, component *model.ApplicationComponent) {
	if len(selector) == 0 || component == nil {
		return
	}
	// Adopted workloads retain their live source identity so existing Services
	// continue to match the selector labels preserved on the source Pod template.
	if component.HasSourceWorkload() {
		return
	}
	if _, exists := selector[config.LabelAppID]; exists {
		selector[config.LabelAppID] = component.AppID
	}
	if _, exists := selector[config.LabelComponentID]; exists {
		selector[config.LabelComponentID] = fmt.Sprintf("%d", component.ID)
	}
	if _, exists := selector[config.LabelComponentName]; exists {
		selector[config.LabelComponentName] = naming.BoundedLabelValue(component.Name)
	}
}

func normalizeServiceTraitSelector(selector map[string]string, properties *model.Properties) map[string]string {
	if len(selector) == 0 {
		return nil
	}
	normalized := make(map[string]string, len(selector))
	for key, value := range selector {
		if selectorTargetsGeneratedLabel(key, properties) {
			normalized[key] = naming.NormalizeLabelValue(value)
			continue
		}
		normalized[key] = naming.NormalizeInvalidLabelValue(value)
	}
	return normalized
}

func selectorTargetsGeneratedLabel(key string, properties *model.Properties) bool {
	if key == config.LabelComponentName {
		return true
	}
	if properties == nil {
		return false
	}
	_, ok := properties.Labels[key]
	return ok
}

func stringDeref(input *string) string {
	if input == nil {
		return ""
	}
	return *input
}

func serviceFromApplyConfig(svc *applyv1.ServiceApplyConfiguration) *corev1.Service {
	// 处理可能为 nil 的字段
	var serviceType corev1.ServiceType = corev1.ServiceTypeClusterIP // 默认值
	if svc.Spec.Type != nil {
		serviceType = *svc.Spec.Type
	}

	coreService := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        *svc.Name,
			Namespace:   *svc.Namespace,
			Labels:      svc.Labels,
			Annotations: svc.Annotations,
		},
		Spec: corev1.ServiceSpec{
			Type:         serviceType,
			Selector:     svc.Spec.Selector,
			ExternalName: stringDeref(svc.Spec.ExternalName),
			ClusterIP:    stringDeref(svc.Spec.ClusterIP),
			ClusterIPs:   append([]string(nil), svc.Spec.ClusterIPs...),
			Ports:        make([]corev1.ServicePort, len(svc.Spec.Ports)),
		},
	}

	// 转换端口
	for i, port := range svc.Spec.Ports {
		portName := fmt.Sprintf("port-%d", i)
		if port.Name != nil {
			portName = *port.Name
		}

		// 处理可能为 nil 的字段
		var targetPort intstr.IntOrString
		if port.TargetPort != nil {
			targetPort = *port.TargetPort
		}

		var protocol corev1.Protocol = corev1.ProtocolTCP // 默认值
		if port.Protocol != nil {
			protocol = *port.Protocol
		}

		coreService.Spec.Ports[i] = corev1.ServicePort{
			Name:       portName,
			Port:       *port.Port,
			TargetPort: targetPort,
			Protocol:   protocol,
		}
	}

	return coreService
}

func copyServicePreservedFields(dst, src *corev1.Service) {
	if src == nil || dst == nil {
		return
	}
	dst.ResourceVersion = src.ResourceVersion
	// ClusterIP/ClusterIPs are allocated identities (including the literal
	// "None" for a headless Service), never an apply-time desired state. Keep
	// the live allocation so adopted and native reconciles cannot accidentally
	// replace an immutable address or family assignment.
	if src.Spec.ClusterIP != "" {
		dst.Spec.ClusterIP = src.Spec.ClusterIP
	}
	if len(src.Spec.ClusterIPs) > 0 {
		dst.Spec.ClusterIPs = append([]string(nil), src.Spec.ClusterIPs...)
	}
	if len(src.Spec.IPFamilies) > 0 {
		dst.Spec.IPFamilies = src.Spec.IPFamilies
	}
	if dst.Spec.IPFamilyPolicy == nil && src.Spec.IPFamilyPolicy != nil {
		policy := *src.Spec.IPFamilyPolicy
		dst.Spec.IPFamilyPolicy = &policy
	}
	if dst.Spec.Type == corev1.ServiceTypeLoadBalancer &&
		dst.Spec.AllocateLoadBalancerNodePorts == nil &&
		src.Spec.AllocateLoadBalancerNodePorts != nil {
		allocate := *src.Spec.AllocateLoadBalancerNodePorts
		dst.Spec.AllocateLoadBalancerNodePorts = &allocate
	}
	if dst.Spec.Type == corev1.ServiceTypeNodePort || dst.Spec.Type == corev1.ServiceTypeLoadBalancer {
		copyServiceNodePorts(dst.Spec.Ports, src.Spec.Ports)
	}
	if src.Spec.SessionAffinityConfig != nil {
		dst.Spec.SessionAffinityConfig = src.Spec.SessionAffinityConfig
	}
	if src.Spec.SessionAffinity != "" {
		dst.Spec.SessionAffinity = src.Spec.SessionAffinity
	}
	if src.Spec.LoadBalancerIP != "" {
		dst.Spec.LoadBalancerIP = src.Spec.LoadBalancerIP
	}
	if len(src.Spec.LoadBalancerSourceRanges) > 0 {
		dst.Spec.LoadBalancerSourceRanges = src.Spec.LoadBalancerSourceRanges
	}
	if src.Spec.ExternalTrafficPolicy != "" {
		dst.Spec.ExternalTrafficPolicy = src.Spec.ExternalTrafficPolicy
	}
	if src.Spec.HealthCheckNodePort != 0 {
		dst.Spec.HealthCheckNodePort = src.Spec.HealthCheckNodePort
	}
	if src.Spec.PublishNotReadyAddresses {
		dst.Spec.PublishNotReadyAddresses = src.Spec.PublishNotReadyAddresses
	}
	if src.Spec.InternalTrafficPolicy != nil {
		dst.Spec.InternalTrafficPolicy = src.Spec.InternalTrafficPolicy
	}
	klog.Infof("Copying necessary fields from existing service %s/%s: ResourceVersion=%s, ClusterIP=%s",
		src.Namespace, src.Name, src.ResourceVersion, src.Spec.ClusterIP)
}

func copyServiceNodePorts(dstPorts, srcPorts []corev1.ServicePort) {
	for i := range dstPorts {
		if dstPorts[i].NodePort != 0 {
			continue
		}
		srcPort := matchingServicePort(dstPorts[i], srcPorts, i)
		if srcPort != nil && srcPort.NodePort != 0 {
			dstPorts[i].NodePort = srcPort.NodePort
		}
	}
}

func matchingServicePort(port corev1.ServicePort, ports []corev1.ServicePort, index int) *corev1.ServicePort {
	if port.Name != "" {
		for i := range ports {
			if ports[i].Name == port.Name {
				return &ports[i]
			}
		}
	}
	for i := range ports {
		if ports[i].Port == port.Port &&
			ports[i].Protocol == port.Protocol &&
			apiequality.Semantic.DeepEqual(ports[i].TargetPort, port.TargetPort) {
			return &ports[i]
		}
	}
	if index >= 0 && index < len(ports) {
		return &ports[index]
	}
	return nil
}

func serviceNeedsUpdate(current, desired *corev1.Service) bool {
	if current == nil || desired == nil {
		return false
	}
	updated := desired.DeepCopy()
	copyServicePreservedFields(updated, current)
	if !apiequality.Semantic.DeepEqual(current.Spec, updated.Spec) {
		return true
	}
	if !apiequality.Semantic.DeepEqual(current.Labels, updated.Labels) {
		return true
	}
	if !apiequality.Semantic.DeepEqual(current.Annotations, updated.Annotations) {
		return true
	}
	return false
}

func (c *DeployServiceJobCtl) ApplyService(ctx context.Context, svc *applyv1.ServiceApplyConfiguration) (*corev1.Service, error) {
	coreService := serviceFromApplyConfig(svc)

	updateService := func() (*corev1.Service, error) {
		var appliedSvc *corev1.Service
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			existingService, err := c.client.CoreV1().Services(coreService.Namespace).Get(ctx, coreService.Name, metav1.GetOptions{})
			if err != nil {
				return err
			}
			desired := coreService.DeepCopy()
			if !serviceNeedsUpdate(existingService, desired) {
				appliedSvc = existingService
				klog.Infof("Service %s/%s is up-to-date; skipping update", existingService.Namespace, existingService.Name)
				return nil
			}
			copyServicePreservedFields(desired, existingService)
			var updateErr error
			appliedSvc, updateErr = c.client.CoreV1().Services(coreService.Namespace).Update(ctx, desired, metav1.UpdateOptions{})
			return updateErr
		}); err != nil {
			return nil, err
		}
		return appliedSvc, nil
	}

	if _, err := c.client.CoreV1().Services(coreService.Namespace).Get(ctx, coreService.Name, metav1.GetOptions{}); err != nil {
		if k8serrors.IsNotFound(err) {
			appliedSvc, err := c.client.CoreV1().Services(coreService.Namespace).Create(ctx, coreService, metav1.CreateOptions{})
			if err != nil {
				if k8serrors.IsAlreadyExists(err) {
					appliedSvc, err = updateService()
					if err != nil {
						klog.Errorf("Update failed: %v", err)
						return nil, fmt.Errorf("update service failed: %w", err)
					}
					klog.Infof("Service updated: %s/%s", appliedSvc.Namespace, appliedSvc.Name)
					return appliedSvc, nil
				}
				klog.Errorf("TmpCreate failed: %v", err)
				return nil, fmt.Errorf("create service failed: %w", err)
			}
			klog.InfoS("Service created", "namespace", appliedSvc.Namespace, "name", appliedSvc.Name)
			return appliedSvc, nil
		}
		return nil, fmt.Errorf("failed to check service existence: %w", err)
	}

	appliedSvc, err := updateService()
	if err != nil {
		klog.Errorf("Update failed: %v", err)
		return nil, fmt.Errorf("update service failed: %w", err)
	}
	klog.Infof("Service updated: %s/%s", appliedSvc.Namespace, appliedSvc.Name)
	return appliedSvc, nil
}
