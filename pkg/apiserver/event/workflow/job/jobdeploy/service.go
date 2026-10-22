package jobdeploy

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	spec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobexec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

type DeployServiceJobCtl struct {
	jobexec.DeployNamespacedResourceJobBase
}

func NewDeployServiceJobCtl(job *model.JobTask, runtime *jobexec.Runtime, shareLocker locker.Locker) *DeployServiceJobCtl {
	base, ok := jobexec.NewDeployNamespacedResourceJobBase("NewDeployServiceJobCtl", job, runtime, shareLocker)
	if !ok {
		return nil
	}
	return &DeployServiceJobCtl{
		DeployNamespacedResourceJobBase: base,
	}
}

func (c *DeployServiceJobCtl) Clean(ctx context.Context) {
	c.CleanCreated(ctx, spec.ResourceService, "service", func(ctx context.Context, namespace, name string) error {
		return c.Client.CoreV1().Services(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	}, "after job failure")
}

func (c *DeployServiceJobCtl) Run(ctx context.Context) error {
	return c.RunWithWait(ctx, c.run, c.wait, "DeployServiceJob run error", "")
}

func (c *DeployServiceJobCtl) run(ctx context.Context) error {
	if c.Client == nil {
		return fmt.Errorf("client is nil")
	}

	service, err := jobinfo.ServiceFromJobInfo(c.Job)
	if err != nil {
		return err
	}

	// 必要字段检查
	if service.Name == "" || service.Namespace == "" {
		return fmt.Errorf("service name or namespace is empty")
	}
	name := service.Name
	namespace := service.Namespace

	unlock, skipped, err := jobexec.ResolveSharedResourceAccess(ctx, jobexec.SharedResourceAccessOptions{
		Job:          c.Job,
		Ack:          c.Ack,
		Labels:       service.Labels,
		Kind:         spec.ResourceService,
		LockProvider: c.ShareLocker,
		ListFn: func(ctx context.Context, opts metav1.ListOptions) (int, error) {
			list, err := c.Client.CoreV1().Services(namespace).List(ctx, opts)
			if err != nil {
				return 0, err
			}
			return len(list.Items), nil
		},
		LogSkip: func(strategy spec.ShareStrategy) {
			if strategy == spec.ShareStrategyIgnore {
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

	if err := jobexec.TrackResourcePresence(ctx, spec.ResourceService, namespace, name, func(ctx context.Context) (*corev1.Service, error) {
		return c.Client.CoreV1().Services(namespace).Get(ctx, name, metav1.GetOptions{})
	}); err != nil {
		return fmt.Errorf("check service existence failed: %w", err)
	}

	// 直接使用 ApplyService 处理创建或更新
	updated, err := c.ApplyService(ctx, service)
	if err != nil {
		klog.Errorf("failed to apply service %q: %v", service.Name, err)
		return fmt.Errorf("apply service failed: %w", err)
	}
	klog.Infof("Service %q applied successfully.", updated.Name)

	return nil
}

func (c *DeployServiceJobCtl) timeout() int {
	if c.Job.Timeout == 0 {
		c.Job.Timeout = 60 * 10
	}
	return int(c.Job.Timeout)
}

func (c *DeployServiceJobCtl) wait(ctx context.Context) error {
	serviceName := resolveServiceName(c.Job)
	if serviceName == "" {
		serviceName = naming.ServiceName(c.Job.Name, c.Job.ResourceAppNameOrID())
	}
	return waitForPolledResource(ctx, pollWaitOptions{
		timeout:  time.Duration(c.timeout()) * time.Second,
		interval: 2 * time.Second,
		poll: func(ctx context.Context) (bool, error) {
			return getServiceStatus(ctx, c.Client, c.Job.Namespace, serviceName)
		},
		onCancel: func(err error) error {
			return jobinfo.NewStatusError(config.StatusCancelled, fmt.Errorf("service %s cancelled: %w", c.Job.Name, err))
		},
		onTimeout: func() error {
			klog.Warningf("timed out waiting for service: %s", c.Job.Name)
			return jobinfo.NewStatusError(config.StatusTimeout, fmt.Errorf("wait service %s timeout", c.Job.Name))
		},
		onError: func(err error) error {
			klog.Errorf("error checking service status: %v", err)
			return fmt.Errorf("wait service %s error: %w", c.Job.Name, err)
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

func GenerateService(component *model.ApplicationComponent, properties *model.Properties) *corev1.Service {
	servicePorts := make([]corev1.ServicePort, 0, len(properties.Ports))
	base := utils.ToRFC1123Name(component.Name)
	for _, p := range properties.Ports {
		servicePorts = append(servicePorts, corev1.ServicePort{
			Name:       defaultServicePortName(base, p.Port),
			Port:       p.Port,
			TargetPort: intstr.FromInt32(p.Port),
			Protocol:   corev1.ProtocolTCP,
		})
	}
	return &corev1.Service{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: metav1.ObjectMeta{
			Name:        naming.ServiceName(component.Name, component.ResourceNameKey()),
			Namespace:   component.Namespace,
			Labels:      jobexec.BuildLabels(component, properties),
			Annotations: jobexec.BuildAnnotations(component),
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: defaultServiceSelector(component),
			Ports:    servicePorts,
		},
	}
}

func GenerateServiceFromTrait(component *model.ApplicationComponent, properties *model.Properties, serviceTrait spec.ServiceTraitSpec) *corev1.Service {
	servicePorts := make([]corev1.ServicePort, 0, len(serviceTrait.Ports))
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
		servicePorts = append(servicePorts, corev1.ServicePort{
			Name:       portName,
			Port:       p.Port,
			TargetPort: intstr.FromInt32(targetPort),
			Protocol:   serviceProtocolFromTrait(p.Protocol),
		})
	}

	labels := jobexec.BuildLabels(component, properties)
	for k, v := range naming.NormalizeLabelValues(serviceTrait.Labels) {
		labels[k] = v
	}
	labels = jobexec.ApplyComponentManagedLabels(labels, component)
	serviceType := serviceTypeFromTrait(serviceTrait.Type)
	serviceName := strings.TrimSpace(serviceTrait.Name)
	if serviceName == "" {
		serviceName = naming.ServiceName(component.Name, component.ResourceNameKey())
	}
	svc := &corev1.Service{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: metav1.ObjectMeta{
			Name:        serviceName,
			Namespace:   component.Namespace,
			Labels:      labels,
			Annotations: jobexec.BuildAnnotations(component),
		},
		Spec: corev1.ServiceSpec{Type: serviceType, Ports: servicePorts},
	}
	if serviceType == corev1.ServiceTypeExternalName {
		svc.Spec.ExternalName = strings.TrimSpace(serviceTrait.ExternalName)
	} else {
		svc.Spec.Selector = serviceSelectorForTrait(component, properties, serviceType, serviceTrait.Selector)
	}
	if serviceTrait.Headless && serviceType == corev1.ServiceTypeClusterIP {
		svc.Spec.ClusterIP = corev1.ClusterIPNone
	}
	return svc
}

func resolveServiceName(jobTask *model.JobTask) string {
	if jobTask == nil {
		return ""
	}
	service, ok := jobinfo.OptionalJobInfo[*corev1.Service](jobTask)
	if !ok {
		return ""
	}
	return strings.TrimSpace(service.Name)
}

func serviceTypeFromTrait(raw string) corev1.ServiceType {
	normalized, _ := spec.NormalizeServiceAccessType(raw)
	return spec.ToKubeServiceType(normalized)
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
		spec.LabelAppID:         component.AppID,
		spec.LabelComponentName: naming.BoundedLabelValue(component.Name),
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
	if _, exists := selector[spec.LabelAppID]; exists {
		selector[spec.LabelAppID] = component.AppID
	}
	if _, exists := selector[spec.LabelComponentID]; exists {
		selector[spec.LabelComponentID] = fmt.Sprintf("%d", component.ID)
	}
	if _, exists := selector[spec.LabelComponentName]; exists {
		selector[spec.LabelComponentName] = naming.BoundedLabelValue(component.Name)
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
	if key == spec.LabelComponentName {
		return true
	}
	if properties == nil {
		return false
	}
	_, ok := properties.Labels[key]
	return ok
}

// serviceWithDefaults preserves the execution defaults for imported Service
// manifests as well as generated Services, without mutating the job payload.
func serviceWithDefaults(service *corev1.Service) *corev1.Service {
	service = service.DeepCopy()
	if service.Spec.Type == "" {
		service.Spec.Type = corev1.ServiceTypeClusterIP
	}
	for i := range service.Spec.Ports {
		port := &service.Spec.Ports[i]
		if port.Name == "" {
			port.Name = fmt.Sprintf("port-%d", i)
		}
		if port.Protocol == "" {
			port.Protocol = corev1.ProtocolTCP
		}
	}
	return service
}

func copyServicePreservedFields(dst, src *corev1.Service) {
	if src == nil || dst == nil {
		return
	}
	dst.ResourceVersion = src.ResourceVersion
	// ClusterIP/ClusterIPs are allocated identities (including the literal
	// "None" for a headless Service), never an apply-time desired state. Keep
	// the live allocation so reconciles cannot accidentally replace an
	// immutable address or family assignment.
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

func (c *DeployServiceJobCtl) ApplyService(ctx context.Context, coreService *corev1.Service) (*corev1.Service, error) {
	coreService = serviceWithDefaults(coreService)

	updateService := func() (*corev1.Service, error) {
		var appliedSvc *corev1.Service
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			existingService, err := c.Client.CoreV1().Services(coreService.Namespace).Get(ctx, coreService.Name, metav1.GetOptions{})
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
			appliedSvc, updateErr = c.Client.CoreV1().Services(coreService.Namespace).Update(ctx, desired, metav1.UpdateOptions{})
			return updateErr
		}); err != nil {
			return nil, err
		}
		return appliedSvc, nil
	}

	if _, err := c.Client.CoreV1().Services(coreService.Namespace).Get(ctx, coreService.Name, metav1.GetOptions{}); err != nil {
		if k8serrors.IsNotFound(err) {
			appliedSvc, err := c.Client.CoreV1().Services(coreService.Namespace).Create(ctx, coreService, metav1.CreateOptions{})
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
