package traitvalidation

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const maxNameLength = 63

var (
	nameRegexp     = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)
	quantityRegexp = regexp.MustCompile(`^[0-9]+(\.[0-9]+)?(m|Ki|Mi|Gi|Ti|Pi|Ei|K|M|G|T|P|E)?$`)

	validServiceProtocols = map[string]bool{
		"":     true,
		"TCP":  true,
		"UDP":  true,
		"SCTP": true,
	}
)

func ValidateStorageSubPathConflict(storage spec.StorageTraitSpec, field string) []apisv1.ValidationError {
	if strings.TrimSpace(storage.SubPath) == "" || strings.TrimSpace(storage.SubPathExpr) == "" {
		return nil
	}
	return []apisv1.ValidationError{{
		Field:   fmt.Sprintf("%s.subPathExpr", field),
		Code:    apisv1.ErrCodeInvalidTraitConfig,
		Message: "storage subPath and subPathExpr cannot both be set",
	}}
}

func ValidateResourcesTraitSpec(resources spec.ResourceTraitsSpec, field string) []apisv1.ValidationError {
	var errors []apisv1.ValidationError
	cpuValid := true
	cpuLimitValid := true
	memoryValid := true
	memoryLimitValid := true

	if resources.CPU != "" && !isValidQuantity(resources.CPU) {
		cpuValid = false
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.cpu", field),
			Code:    apisv1.ErrCodeInvalidTraitConfig,
			Message: fmt.Sprintf("invalid CPU format: %s, must be a valid Kubernetes quantity (e.g., 500m, 1)", resources.CPU),
		})
	}
	if resources.CPULimit != "" && !isValidQuantity(resources.CPULimit) {
		cpuLimitValid = false
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.cpuLimit", field),
			Code:    apisv1.ErrCodeInvalidTraitConfig,
			Message: fmt.Sprintf("invalid CPU limit format: %s, must be a valid Kubernetes quantity (e.g., 500m, 1)", resources.CPULimit),
		})
	}

	if resources.Memory != "" && !isValidQuantity(resources.Memory) {
		memoryValid = false
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.memory", field),
			Code:    apisv1.ErrCodeInvalidTraitConfig,
			Message: fmt.Sprintf("invalid memory format: %s, must be a valid Kubernetes quantity (e.g., 512Mi, 1Gi)", resources.Memory),
		})
	}
	if resources.MemoryLimit != "" && !isValidQuantity(resources.MemoryLimit) {
		memoryLimitValid = false
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.memoryLimit", field),
			Code:    apisv1.ErrCodeInvalidTraitConfig,
			Message: fmt.Sprintf("invalid memory limit format: %s, must be a valid Kubernetes quantity (e.g., 512Mi, 1Gi)", resources.MemoryLimit),
		})
	}

	if cpuValid && cpuLimitValid {
		errors = append(errors, validateResourceLimitNotBelowRequest(resources.CPU, resources.CPULimit, fmt.Sprintf("%s.cpuLimit", field), "cpuLimit must be greater than or equal to cpu request")...)
	}
	if memoryValid && memoryLimitValid {
		errors = append(errors, validateResourceLimitNotBelowRequest(resources.Memory, resources.MemoryLimit, fmt.Sprintf("%s.memoryLimit", field), "memoryLimit must be greater than or equal to memory request")...)
	}

	return errors
}

func validateResourceLimitNotBelowRequest(requestValue, limitValue, field, message string) []apisv1.ValidationError {
	if requestValue == "" || limitValue == "" {
		return nil
	}

	requestQty, err := resource.ParseQuantity(requestValue)
	if err != nil {
		return []apisv1.ValidationError{{
			Field:   field,
			Code:    apisv1.ErrCodeInvalidTraitConfig,
			Message: fmt.Sprintf("invalid resource request format: %s", requestValue),
		}}
	}
	limitQty, err := resource.ParseQuantity(limitValue)
	if err != nil {
		return []apisv1.ValidationError{{
			Field:   field,
			Code:    apisv1.ErrCodeInvalidTraitConfig,
			Message: fmt.Sprintf("invalid resource limit format: %s", limitValue),
		}}
	}
	if limitQty.Cmp(requestQty) < 0 {
		return []apisv1.ValidationError{{
			Field:   field,
			Code:    apisv1.ErrCodeInvalidTraitConfig,
			Message: message,
		}}
	}

	return nil
}

func isValidQuantity(quantity string) bool {
	return quantityRegexp.MatchString(quantity)
}

func ValidateServiceTraitSpec(service spec.ServiceTraitSpec, field string) []apisv1.ValidationError {
	var errors []apisv1.ValidationError

	if serviceName := strings.TrimSpace(service.Name); serviceName != "" {
		errors = append(errors, ValidateKubeResourceName(serviceName, fmt.Sprintf("%s.name", field))...)
	}
	errors = append(errors, validateReservedLabelMap(service.Labels, fmt.Sprintf("%s.labels", field), "traits.service.labels")...)

	serviceType, known := spec.NormalizeServiceAccessType(service.Type)
	if !known {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.type", field),
			Code:    apisv1.ErrCodeInvalidTraitConfig,
			Message: fmt.Sprintf("invalid service type: %s, must be one of: internal, node, public, external", service.Type),
		})
	}

	if service.Headless && serviceType != spec.ServiceAccessInternal {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.headless", field),
			Code:    apisv1.ErrCodeInvalidTraitConfig,
			Message: "headless service only supports internal type",
		})
	}

	if serviceType != spec.ServiceAccessExternal && len(service.Selector) == 0 {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.selector", field),
			Code:    apisv1.ErrCodeMissingRequiredField,
			Message: "selector is required for non-external service",
		})
	}
	if serviceType == spec.ServiceAccessExternal && strings.TrimSpace(service.ExternalName) == "" {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.externalName", field),
			Code:    apisv1.ErrCodeMissingRequiredField,
			Message: "externalName is required for external service",
		})
	}

	if len(service.Ports) == 0 {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.ports", field),
			Code:    apisv1.ErrCodeMissingRequiredField,
			Message: "service ports are required",
		})
		return errors
	}
	for i, port := range service.Ports {
		errors = append(errors, validateServicePortTrait(port, fmt.Sprintf("%s.ports[%d]", field, i))...)
	}
	return errors
}

func ValidateKubeResourceName(name, field string) []apisv1.ValidationError {
	var errors []apisv1.ValidationError
	if len(name) > maxNameLength {
		errors = append(errors, apisv1.ValidationError{
			Field:   field,
			Code:    apisv1.ErrCodeNameTooLong,
			Message: fmt.Sprintf("%s must be at most %d characters", field, maxNameLength),
		})
		return errors
	}
	if !nameRegexp.MatchString(name) {
		errors = append(errors, apisv1.ValidationError{
			Field:   field,
			Code:    apisv1.ErrCodeInvalidNameFormat,
			Message: fmt.Sprintf("%s must match DNS-1123 subdomain (lowercase alphanumeric, may contain hyphens, must start and end with alphanumeric)", field),
		})
	}
	return errors
}

func validateServicePortTrait(port spec.ServicePortTraitSpec, field string) []apisv1.ValidationError {
	var errors []apisv1.ValidationError

	if port.Port <= 0 {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.port", field),
			Code:    apisv1.ErrCodeMissingRequiredField,
			Message: "service port is required and must be positive",
		})
	}

	if port.TargetPort < 0 {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.targetPort", field),
			Code:    apisv1.ErrCodeInvalidTraitConfig,
			Message: "targetPort must be greater than or equal to zero",
		})
	}

	protocol := strings.ToUpper(strings.TrimSpace(port.Protocol))
	if !validServiceProtocols[protocol] {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.protocol", field),
			Code:    apisv1.ErrCodeInvalidTraitConfig,
			Message: fmt.Sprintf("invalid service protocol: %s, must be one of: TCP, UDP, SCTP", port.Protocol),
		})
	}

	return errors
}

func ValidateRolloutTrait(componentType config.JobType, rollout *spec.RolloutTraitSpec, field string, isNested bool) []apisv1.ValidationError {
	if rollout == nil {
		return nil
	}
	if isNested {
		return []apisv1.ValidationError{{
			Field:   field,
			Code:    apisv1.ErrCodeInvalidTraitConfig,
			Message: "rollout is a workload-level trait and only supports component-level traits",
		}}
	}

	switch componentType {
	case config.ServerJob:
		return validateDeploymentRolloutTrait(rollout, field)
	case config.StoreJob:
		return validateStatefulSetRolloutTrait(rollout, field)
	default:
		return []apisv1.ValidationError{{
			Field:   field,
			Code:    apisv1.ErrCodeInvalidTraitConfig,
			Message: fmt.Sprintf("rollout is only supported for webservice and store components, got %s", componentType),
		}}
	}
}

func validateDeploymentRolloutTrait(rollout *spec.RolloutTraitSpec, field string) []apisv1.ValidationError {
	var errors []apisv1.ValidationError
	strategyType := appsv1.DeploymentStrategyType(strings.TrimSpace(rollout.Type))
	switch strategyType {
	case appsv1.RollingUpdateDeploymentStrategyType:
		if rollout.RollingUpdate == nil {
			errors = append(errors, apisv1.ValidationError{Field: fmt.Sprintf("%s.rollingUpdate", field), Code: apisv1.ErrCodeMissingRequiredField, Message: "deployment rollout type RollingUpdate requires rollingUpdate"})
			return errors
		}
		if rollout.RollingUpdate.MaxSurge == nil {
			errors = append(errors, apisv1.ValidationError{Field: fmt.Sprintf("%s.rollingUpdate.maxSurge", field), Code: apisv1.ErrCodeMissingRequiredField, Message: "deployment rollout type RollingUpdate requires rollingUpdate.maxSurge"})
		}
		if rollout.RollingUpdate.MaxUnavailable == nil {
			errors = append(errors, apisv1.ValidationError{Field: fmt.Sprintf("%s.rollingUpdate.maxUnavailable", field), Code: apisv1.ErrCodeMissingRequiredField, Message: "deployment rollout type RollingUpdate requires rollingUpdate.maxUnavailable"})
		}
		errors = append(errors, validateIntOrPercent(rollout.RollingUpdate.MaxSurge, fmt.Sprintf("%s.rollingUpdate.maxSurge", field))...)
		errors = append(errors, validateIntOrPercent(rollout.RollingUpdate.MaxUnavailable, fmt.Sprintf("%s.rollingUpdate.maxUnavailable", field))...)
		if rollout.RollingUpdate.Partition != nil {
			errors = append(errors, apisv1.ValidationError{Field: fmt.Sprintf("%s.rollingUpdate.partition", field), Code: apisv1.ErrCodeInvalidTraitConfig, Message: "deployment rollout does not support rollingUpdate.partition"})
		}
		if rollout.RollingUpdate.MaxSurge != nil && rollout.RollingUpdate.MaxUnavailable != nil && intOrPercentIsZero(rollout.RollingUpdate.MaxSurge) && intOrPercentIsZero(rollout.RollingUpdate.MaxUnavailable) {
			errors = append(errors, apisv1.ValidationError{Field: fmt.Sprintf("%s.rollingUpdate", field), Code: apisv1.ErrCodeInvalidTraitConfig, Message: "deployment rollout maxSurge and maxUnavailable cannot both be 0"})
		}
	case appsv1.RecreateDeploymentStrategyType:
		if rollout.RollingUpdate != nil {
			errors = append(errors, apisv1.ValidationError{Field: fmt.Sprintf("%s.rollingUpdate", field), Code: apisv1.ErrCodeInvalidTraitConfig, Message: "deployment rollout type Recreate does not support rollingUpdate"})
		}
	case "":
		errors = append(errors, apisv1.ValidationError{Field: fmt.Sprintf("%s.type", field), Code: apisv1.ErrCodeMissingRequiredField, Message: "rollout type is required"})
	default:
		errors = append(errors, apisv1.ValidationError{Field: fmt.Sprintf("%s.type", field), Code: apisv1.ErrCodeInvalidTraitConfig, Message: fmt.Sprintf("invalid deployment rollout type: %s, must be one of: RollingUpdate, Recreate", rollout.Type)})
	}
	return errors
}

func validateStatefulSetRolloutTrait(rollout *spec.RolloutTraitSpec, field string) []apisv1.ValidationError {
	var errors []apisv1.ValidationError
	strategyType := appsv1.StatefulSetUpdateStrategyType(strings.TrimSpace(rollout.Type))
	switch strategyType {
	case appsv1.RollingUpdateStatefulSetStrategyType:
		if rollout.RollingUpdate != nil {
			if rollout.RollingUpdate.MaxSurge != nil {
				errors = append(errors, apisv1.ValidationError{Field: fmt.Sprintf("%s.rollingUpdate.maxSurge", field), Code: apisv1.ErrCodeInvalidTraitConfig, Message: "statefulset rollout does not support rollingUpdate.maxSurge"})
			}
			errors = append(errors, validateIntOrPercent(rollout.RollingUpdate.MaxUnavailable, fmt.Sprintf("%s.rollingUpdate.maxUnavailable", field))...)
			if rollout.RollingUpdate.MaxUnavailable != nil && intOrPercentIsZero(rollout.RollingUpdate.MaxUnavailable) {
				errors = append(errors, apisv1.ValidationError{Field: fmt.Sprintf("%s.rollingUpdate.maxUnavailable", field), Code: apisv1.ErrCodeInvalidTraitConfig, Message: "statefulset rollout maxUnavailable must be greater than 0"})
			}
			if rollout.RollingUpdate.Partition != nil && *rollout.RollingUpdate.Partition < 0 {
				errors = append(errors, apisv1.ValidationError{Field: fmt.Sprintf("%s.rollingUpdate.partition", field), Code: apisv1.ErrCodeInvalidTraitConfig, Message: "statefulset rollout partition must be greater than or equal to 0"})
			}
		}
	case appsv1.OnDeleteStatefulSetStrategyType:
		if rollout.RollingUpdate != nil {
			errors = append(errors, apisv1.ValidationError{Field: fmt.Sprintf("%s.rollingUpdate", field), Code: apisv1.ErrCodeInvalidTraitConfig, Message: "statefulset rollout type OnDelete does not support rollingUpdate"})
		}
	case "":
		errors = append(errors, apisv1.ValidationError{Field: fmt.Sprintf("%s.type", field), Code: apisv1.ErrCodeMissingRequiredField, Message: "rollout type is required"})
	default:
		errors = append(errors, apisv1.ValidationError{Field: fmt.Sprintf("%s.type", field), Code: apisv1.ErrCodeInvalidTraitConfig, Message: fmt.Sprintf("invalid statefulset rollout type: %s, must be one of: RollingUpdate, OnDelete", rollout.Type)})
	}
	return errors
}

func validateIntOrPercent(value *intstr.IntOrString, field string) []apisv1.ValidationError {
	if value == nil {
		return nil
	}
	if !intOrPercentValid(value) {
		return []apisv1.ValidationError{{Field: field, Code: apisv1.ErrCodeInvalidTraitConfig, Message: "value must be a non-negative JSON integer or percentage string, for example 1 or \"25%\""}}
	}
	return nil
}

func intOrPercentValid(value *intstr.IntOrString) bool {
	switch value.Type {
	case intstr.Int:
		return value.IntVal >= 0
	case intstr.String:
		parsed, ok := parsePercentString(value.StrVal)
		return ok && parsed >= 0
	default:
		return false
	}
}

func parsePercentString(raw string) (int, bool) {
	value := strings.TrimSpace(raw)
	if value == "" || !strings.HasSuffix(value, "%") {
		return 0, false
	}
	number := strings.TrimSuffix(value, "%")
	if number == "" {
		return 0, false
	}
	parsed, err := strconv.Atoi(number)
	return parsed, err == nil
}

func intOrPercentIsZero(value *intstr.IntOrString) bool {
	if value == nil {
		return false
	}
	switch value.Type {
	case intstr.Int:
		return value.IntVal == 0
	case intstr.String:
		parsed, ok := parsePercentString(value.StrVal)
		return ok && parsed == 0
	default:
		return false
	}
}

func validateReservedLabelMap(labels map[string]string, field, source string) []apisv1.ValidationError {
	keys := spec.ReservedComponentLabelsIn(labels)
	errors := make([]apisv1.ValidationError, 0, len(keys))
	for _, key := range keys {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.%s", field, key),
			Code:    apisv1.ErrCodeInvalidTraitConfig,
			Message: fmt.Sprintf("%s key %q is reserved and cannot be overridden", source, key),
		})
	}
	return errors
}
