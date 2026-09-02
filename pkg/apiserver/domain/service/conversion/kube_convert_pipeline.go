package conversion

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

type kubeObjectBuckets struct {
	workloads           []*unstructured.Unstructured
	jobs                []*unstructured.Unstructured
	cronJobs            []*unstructured.Unstructured
	services            []*unstructured.Unstructured
	configMaps          []*unstructured.Unstructured
	secrets             []*unstructured.Unstructured
	ingresses           []*unstructured.Unstructured
	serviceAccounts     []*unstructured.Unstructured
	roles               []*unstructured.Unstructured
	roleBindings        []*unstructured.Unstructured
	clusterRoles        []*unstructured.Unstructured
	clusterRoleBindings []*unstructured.Unstructured
	pvcs                []*unstructured.Unstructured
}

type kubeConvertState struct {
	components []apis.CreateComponentRequest
	refs       []componentRef
	pvcLookup  map[string]claimTemplateInfo
}

func convertKubeObjectsToComponents(objects []*unstructured.Unstructured) ([]apis.CreateComponentRequest, []string, error) {
	buckets, warnings := classifyKubeObjects(objects)
	state := &kubeConvertState{}

	pvcLookup, pvcWarnings, err := buildPVCInfoLookup(buckets.pvcs)
	warnings = append(warnings, pvcWarnings...)
	if err != nil {
		return nil, warnings, err
	}
	state.pvcLookup = pvcLookup

	stageWarnings, err := state.appendConfigMaps(buckets.configMaps)
	warnings = append(warnings, stageWarnings...)
	if err != nil {
		return nil, warnings, err
	}

	stageWarnings, err = state.appendSecrets(buckets.secrets)
	warnings = append(warnings, stageWarnings...)
	if err != nil {
		return nil, warnings, err
	}

	stageWarnings, err = state.appendWorkloads(buckets.workloads)
	warnings = append(warnings, stageWarnings...)
	if err != nil {
		return nil, warnings, err
	}

	stageWarnings, err = state.appendJobs(buckets.jobs)
	warnings = append(warnings, stageWarnings...)
	if err != nil {
		return nil, warnings, err
	}

	stageWarnings, err = state.appendCronJobs(buckets.cronJobs)
	warnings = append(warnings, stageWarnings...)
	if err != nil {
		return nil, warnings, err
	}

	stageWarnings, err = state.applyServiceTraits(buckets.services)
	warnings = append(warnings, stageWarnings...)
	if err != nil {
		return nil, warnings, err
	}

	stageWarnings, err = state.applyIngressTraits(buckets.ingresses)
	warnings = append(warnings, stageWarnings...)
	if err != nil {
		return nil, warnings, err
	}

	stageWarnings, err = state.applyRBACTraits(
		buckets.serviceAccounts,
		buckets.roles,
		buckets.roleBindings,
		buckets.clusterRoles,
		buckets.clusterRoleBindings,
	)
	warnings = append(warnings, stageWarnings...)
	if err != nil {
		return nil, warnings, err
	}

	return state.components, warnings, nil
}

func ConvertKubeObjectsToComponents(objects []*unstructured.Unstructured) ([]apis.CreateComponentRequest, []string, error) {
	return convertKubeObjectsToComponents(objects)
}

func classifyKubeObjects(objects []*unstructured.Unstructured) (kubeObjectBuckets, []string) {
	var (
		buckets  kubeObjectBuckets
		warnings []string
	)
	for _, obj := range objects {
		if obj == nil {
			continue
		}
		kind := strings.TrimSpace(obj.GetKind())
		if kind == "" {
			continue
		}
		switch kind {
		case string(config.KubeKindStatefulSet),
			string(config.KubeKindDeployment),
			string(config.KubeKindDaemonSet):
			buckets.workloads = append(buckets.workloads, obj)
		case string(config.KubeKindJob):
			buckets.jobs = append(buckets.jobs, obj)
		case string(config.KubeKindCronJob):
			buckets.cronJobs = append(buckets.cronJobs, obj)
		case string(config.KubeKindService):
			buckets.services = append(buckets.services, obj)
		case string(config.KubeKindConfigMap):
			buckets.configMaps = append(buckets.configMaps, obj)
		case string(config.KubeKindSecret):
			buckets.secrets = append(buckets.secrets, obj)
		case string(config.KubeKindIngress):
			buckets.ingresses = append(buckets.ingresses, obj)
		case string(config.KubeKindServiceAccount):
			buckets.serviceAccounts = append(buckets.serviceAccounts, obj)
		case string(config.KubeKindRole):
			buckets.roles = append(buckets.roles, obj)
		case string(config.KubeKindRoleBinding):
			buckets.roleBindings = append(buckets.roleBindings, obj)
		case string(config.KubeKindClusterRole):
			buckets.clusterRoles = append(buckets.clusterRoles, obj)
		case string(config.KubeKindClusterRoleBinding):
			buckets.clusterRoleBindings = append(buckets.clusterRoleBindings, obj)
		case string(config.KubeKindPersistentVolumeClaim):
			buckets.pvcs = append(buckets.pvcs, obj)
		default:
			warnings = append(warnings, fmt.Sprintf("unsupported kind %s ignored", kind))
		}
	}
	return buckets, warnings
}

func (s *kubeConvertState) appendConfigMaps(objects []*unstructured.Unstructured) ([]string, error) {
	var warnings []string
	for _, obj := range objects {
		comp, warns, err := convertConfigMap(obj)
		warnings = append(warnings, warns...)
		if err != nil {
			return warnings, err
		}
		if comp != nil {
			s.components = append(s.components, *comp)
		}
	}
	return warnings, nil
}

func (s *kubeConvertState) appendSecrets(objects []*unstructured.Unstructured) ([]string, error) {
	var warnings []string
	for _, obj := range objects {
		comp, warns, err := convertSecret(obj)
		warnings = append(warnings, warns...)
		if err != nil {
			return warnings, err
		}
		if comp != nil {
			s.components = append(s.components, *comp)
		}
	}
	return warnings, nil
}

func (s *kubeConvertState) appendWorkloads(objects []*unstructured.Unstructured) ([]string, error) {
	var warnings []string
	for _, obj := range objects {
		comp, labels, saName, warns, err := convertWorkloadObject(obj, s.pvcLookup)
		warnings = append(warnings, warns...)
		if err != nil {
			return warnings, err
		}
		s.components, s.refs = appendConvertedComponent(s.components, s.refs, comp, labels, saName)
	}
	return warnings, nil
}

func (s *kubeConvertState) appendJobs(objects []*unstructured.Unstructured) ([]string, error) {
	var warnings []string
	for _, obj := range objects {
		comp, labels, saName, warns, err := convertWorkloadObject(obj, s.pvcLookup)
		warnings = append(warnings, warns...)
		if err != nil {
			return warnings, err
		}
		s.components, s.refs = appendConvertedComponent(s.components, s.refs, comp, labels, saName)
	}
	return warnings, nil
}

func (s *kubeConvertState) appendCronJobs(objects []*unstructured.Unstructured) ([]string, error) {
	var warnings []string
	for _, obj := range objects {
		comp, labels, saName, warns, err := convertWorkloadObject(obj, s.pvcLookup)
		warnings = append(warnings, warns...)
		if err != nil {
			return warnings, err
		}
		s.components, s.refs = appendConvertedComponent(s.components, s.refs, comp, labels, saName)
	}
	return warnings, nil
}

func (s *kubeConvertState) applyServiceTraits(objects []*unstructured.Unstructured) ([]string, error) {
	services, warnings, err := convertServices(objects)
	if err != nil {
		return warnings, err
	}
	if len(services) > 0 {
		warnings = append(warnings, applyServicesToComponents(services, s.components, s.refs)...)
	}
	return warnings, nil
}

func (s *kubeConvertState) applyIngressTraits(objects []*unstructured.Unstructured) ([]string, error) {
	ingresses, warnings, err := convertIngresses(objects)
	if err != nil {
		return warnings, err
	}
	if len(ingresses) > 0 {
		warnings = append(warnings, applyIngressTraits(ingresses, s.components)...)
	}
	return warnings, nil
}

func (s *kubeConvertState) applyRBACTraits(
	serviceAccounts []*unstructured.Unstructured,
	roles []*unstructured.Unstructured,
	roleBindings []*unstructured.Unstructured,
	clusterRoles []*unstructured.Unstructured,
	clusterRoleBindings []*unstructured.Unstructured,
) ([]string, error) {
	policies, warnings, err := convertRBACPolicies(serviceAccounts, roles, roleBindings, clusterRoles, clusterRoleBindings)
	if err != nil {
		return warnings, err
	}
	if len(policies) > 0 {
		warnings = append(warnings, applyRBACPolicies(policies, s.refs, s.components)...)
	}
	return warnings, nil
}
