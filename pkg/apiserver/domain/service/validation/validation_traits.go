package validation

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/internal/traitvalidation"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	k8svalidation "k8s.io/apimachinery/pkg/util/validation"
)

// validateTraits validates the traits configuration
func (v *validationServiceImpl) validateTraits(traits apisv1.Traits, fieldPrefix string, isNested bool, componentType config.JobType) []apisv1.ValidationError {
	var errors []apisv1.ValidationError

	// Validate storage traits
	for i, storage := range traits.Storage {
		errors = append(errors, v.validateStorageTrait(storage, fmt.Sprintf("%s.storage[%d]", fieldPrefix, i))...)
	}

	// Validate probe traits
	for i, probe := range traits.Probes {
		errors = append(errors, v.validateProbeTrait(probe, fmt.Sprintf("%s.probes[%d]", fieldPrefix, i))...)
	}

	// Validate init traits
	for i, init := range traits.Init {
		errors = append(errors, v.validateInitTrait(init, fmt.Sprintf("%s.init[%d]", fieldPrefix, i), isNested)...)
	}

	// Validate sidecar traits
	for i, sidecar := range traits.Sidecar {
		errors = append(errors, v.validateSidecarTrait(sidecar, fmt.Sprintf("%s.sidecar[%d]", fieldPrefix, i), isNested)...)
	}

	// Validate RBAC traits
	for i, rbac := range traits.RBAC {
		errors = append(errors, v.validateRBACTrait(rbac, fmt.Sprintf("%s.rbac[%d]", fieldPrefix, i))...)
	}

	// Validate Ingress traits
	for i, ingress := range traits.Ingress {
		errors = append(errors, v.validateIngressTrait(ingress, fmt.Sprintf("%s.ingress[%d]", fieldPrefix, i))...)
	}
	errors = append(errors, validateIngressBackendServiceReferences(traits, fieldPrefix)...)

	// Validate Service traits
	for i, svc := range traits.Service {
		errors = append(errors, v.validateServiceTrait(svc, fmt.Sprintf("%s.service[%d]", fieldPrefix, i))...)
	}

	// Validate EnvFrom traits
	for i, envFrom := range traits.EnvFrom {
		errors = append(errors, v.validateEnvFromTrait(envFrom, fmt.Sprintf("%s.envFrom[%d]", fieldPrefix, i))...)
	}

	// Validate Envs traits
	for i, env := range traits.Envs {
		errors = append(errors, v.validateEnvsTrait(env, fmt.Sprintf("%s.envs[%d]", fieldPrefix, i))...)
	}

	// Validate Resources trait
	if traits.Resources != nil {
		errors = append(errors, v.validateResourcesTrait(*traits.Resources, fmt.Sprintf("%s.resources", fieldPrefix))...)
	}

	if len(traits.TargetWorkEnv) > 0 || isNested {
		errors = append(errors, v.validateTargetWorkEnvTrait(traits.TargetWorkEnv, fmt.Sprintf("%s.targetWorkEnv", fieldPrefix), isNested)...)
	}
	if traits.Rollout != nil || isNested {
		errors = append(errors, validateRolloutTrait(componentType, traits.Rollout, fmt.Sprintf("%s.rollout", fieldPrefix), isNested)...)
	}

	return errors
}

func (v *validationServiceImpl) validateTargetWorkEnvTrait(targetWorkEnv map[string]string, field string, isNested bool) []apisv1.ValidationError {
	var errors []apisv1.ValidationError

	if isNested && len(targetWorkEnv) > 0 {
		errors = append(errors, apisv1.ValidationError{
			Field:   field,
			Code:    apisv1.ErrCodeInvalidTraitConfig,
			Message: "targetWorkEnv is a pod-level trait and only supports component-level traits",
		})
		return errors
	}

	if len(targetWorkEnv) == 0 {
		return errors
	}

	for key, value := range targetWorkEnv {
		itemField := fmt.Sprintf("%s[%q]", field, key)
		if causes := k8svalidation.IsQualifiedName(key); len(causes) > 0 {
			errors = append(errors, apisv1.ValidationError{
				Field:   itemField,
				Code:    apisv1.ErrCodeInvalidTraitConfig,
				Message: fmt.Sprintf("targetWorkEnv key must be a valid label key: %s", strings.Join(causes, "; ")),
			})
		}
		if value == "" {
			errors = append(errors, apisv1.ValidationError{
				Field:   itemField,
				Code:    apisv1.ErrCodeInvalidTraitConfig,
				Message: "targetWorkEnv value must not be empty",
			})
			continue
		}
		if causes := k8svalidation.IsValidLabelValue(value); len(causes) > 0 {
			errors = append(errors, apisv1.ValidationError{
				Field:   itemField,
				Code:    apisv1.ErrCodeInvalidTraitConfig,
				Message: fmt.Sprintf("targetWorkEnv value must be a valid label value: %s", strings.Join(causes, "; ")),
			})
		}
	}

	return errors
}

func validateRolloutTrait(componentType config.JobType, rollout *spec.RolloutTraitSpec, field string, isNested bool) []apisv1.ValidationError {
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
			errors = append(errors, apisv1.ValidationError{
				Field:   fmt.Sprintf("%s.rollingUpdate", field),
				Code:    apisv1.ErrCodeMissingRequiredField,
				Message: "deployment rollout type RollingUpdate requires rollingUpdate",
			})
			return errors
		}
		if rollout.RollingUpdate.MaxSurge == nil {
			errors = append(errors, apisv1.ValidationError{
				Field:   fmt.Sprintf("%s.rollingUpdate.maxSurge", field),
				Code:    apisv1.ErrCodeMissingRequiredField,
				Message: "deployment rollout type RollingUpdate requires rollingUpdate.maxSurge",
			})
		}
		if rollout.RollingUpdate.MaxUnavailable == nil {
			errors = append(errors, apisv1.ValidationError{
				Field:   fmt.Sprintf("%s.rollingUpdate.maxUnavailable", field),
				Code:    apisv1.ErrCodeMissingRequiredField,
				Message: "deployment rollout type RollingUpdate requires rollingUpdate.maxUnavailable",
			})
		}
		errors = append(errors, validateIntOrPercent(rollout.RollingUpdate.MaxSurge, fmt.Sprintf("%s.rollingUpdate.maxSurge", field))...)
		errors = append(errors, validateIntOrPercent(rollout.RollingUpdate.MaxUnavailable, fmt.Sprintf("%s.rollingUpdate.maxUnavailable", field))...)
		if rollout.RollingUpdate.Partition != nil {
			errors = append(errors, apisv1.ValidationError{
				Field:   fmt.Sprintf("%s.rollingUpdate.partition", field),
				Code:    apisv1.ErrCodeInvalidTraitConfig,
				Message: "deployment rollout does not support rollingUpdate.partition",
			})
		}
		if rollout.RollingUpdate.MaxSurge != nil &&
			rollout.RollingUpdate.MaxUnavailable != nil &&
			intOrPercentIsZero(rollout.RollingUpdate.MaxSurge) &&
			intOrPercentIsZero(rollout.RollingUpdate.MaxUnavailable) {
			errors = append(errors, apisv1.ValidationError{
				Field:   fmt.Sprintf("%s.rollingUpdate", field),
				Code:    apisv1.ErrCodeInvalidTraitConfig,
				Message: "deployment rollout maxSurge and maxUnavailable cannot both be 0",
			})
		}
	case appsv1.RecreateDeploymentStrategyType:
		if rollout.RollingUpdate != nil {
			errors = append(errors, apisv1.ValidationError{
				Field:   fmt.Sprintf("%s.rollingUpdate", field),
				Code:    apisv1.ErrCodeInvalidTraitConfig,
				Message: "deployment rollout type Recreate does not support rollingUpdate",
			})
		}
	case "":
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.type", field),
			Code:    apisv1.ErrCodeMissingRequiredField,
			Message: "rollout type is required",
		})
	default:
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.type", field),
			Code:    apisv1.ErrCodeInvalidTraitConfig,
			Message: fmt.Sprintf("invalid deployment rollout type: %s, must be one of: RollingUpdate, Recreate", rollout.Type),
		})
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
				errors = append(errors, apisv1.ValidationError{
					Field:   fmt.Sprintf("%s.rollingUpdate.maxSurge", field),
					Code:    apisv1.ErrCodeInvalidTraitConfig,
					Message: "statefulset rollout does not support rollingUpdate.maxSurge",
				})
			}
			errors = append(errors, validateIntOrPercent(rollout.RollingUpdate.MaxUnavailable, fmt.Sprintf("%s.rollingUpdate.maxUnavailable", field))...)
			if rollout.RollingUpdate.MaxUnavailable != nil && intOrPercentIsZero(rollout.RollingUpdate.MaxUnavailable) {
				errors = append(errors, apisv1.ValidationError{
					Field:   fmt.Sprintf("%s.rollingUpdate.maxUnavailable", field),
					Code:    apisv1.ErrCodeInvalidTraitConfig,
					Message: "statefulset rollout maxUnavailable must be greater than 0",
				})
			}
			if rollout.RollingUpdate.Partition != nil && *rollout.RollingUpdate.Partition < 0 {
				errors = append(errors, apisv1.ValidationError{
					Field:   fmt.Sprintf("%s.rollingUpdate.partition", field),
					Code:    apisv1.ErrCodeInvalidTraitConfig,
					Message: "statefulset rollout partition must be greater than or equal to 0",
				})
			}
		}
	case appsv1.OnDeleteStatefulSetStrategyType:
		if rollout.RollingUpdate != nil {
			errors = append(errors, apisv1.ValidationError{
				Field:   fmt.Sprintf("%s.rollingUpdate", field),
				Code:    apisv1.ErrCodeInvalidTraitConfig,
				Message: "statefulset rollout type OnDelete does not support rollingUpdate",
			})
		}
	case "":
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.type", field),
			Code:    apisv1.ErrCodeMissingRequiredField,
			Message: "rollout type is required",
		})
	default:
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.type", field),
			Code:    apisv1.ErrCodeInvalidTraitConfig,
			Message: fmt.Sprintf("invalid statefulset rollout type: %s, must be one of: RollingUpdate, OnDelete", rollout.Type),
		})
	}
	return errors
}

func validateIntOrPercent(value *intstr.IntOrString, field string) []apisv1.ValidationError {
	if value == nil {
		return nil
	}
	if !intOrPercentValid(value) {
		return []apisv1.ValidationError{{
			Field:   field,
			Code:    apisv1.ErrCodeInvalidTraitConfig,
			Message: "value must be a non-negative JSON integer or percentage string, for example 1 or \"25%\"",
		}}
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

// validateStorageTrait validates a storage trait
func (v *validationServiceImpl) validateStorageTrait(storage spec.StorageTraitSpec, field string) []apisv1.ValidationError {
	var errors []apisv1.ValidationError

	// Validate type
	if storage.Type == "" {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.type", field),
			Code:    apisv1.ErrCodeMissingRequiredField,
			Message: "storage type is required",
		})
	} else if !validStorageTypes[storage.Type] {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.type", field),
			Code:    apisv1.ErrCodeInvalidStorageType,
			Message: fmt.Sprintf("invalid storage type: %s, must be one of: persistent, ephemeral, config, secret", storage.Type),
		})
	}

	// Validate mountPath
	if storage.MountPath == "" {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.mountPath", field),
			Code:    apisv1.ErrCodeMissingRequiredField,
			Message: "storage mountPath is required",
		})
	}

	errors = append(errors, traitvalidation.ValidateStorageSubPathConflict(storage, field)...)

	// Validate size for persistent storage with tmpCreate=true
	if storage.Type == config.StorageTypePersistent && storage.TmpCreate && storage.Size != "" {
		if !storageQuantityRegexp.MatchString(storage.Size) {
			errors = append(errors, apisv1.ValidationError{
				Field:   fmt.Sprintf("%s.size", field),
				Code:    apisv1.ErrCodeInvalidStorageSize,
				Message: fmt.Sprintf("invalid storage size format: %s, must be a valid Kubernetes quantity (e.g., 1Gi, 500Mi)", storage.Size),
			})
		}
	}

	// Validate sourceName for config/secret types
	if (storage.Type == config.StorageTypeConfig || storage.Type == config.StorageTypeSecret) && storage.SourceName == "" && storage.Name == "" {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.sourceName", field),
			Code:    apisv1.ErrCodeMissingRequiredField,
			Message: "sourceName or name is required for config/secret storage types",
		})
	}

	return errors
}

// validateProbeTrait validates a probe trait
func (v *validationServiceImpl) validateProbeTrait(probe spec.ProbeTraitsSpec, field string) []apisv1.ValidationError {
	var errors []apisv1.ValidationError

	// Validate type
	if probe.Type == "" {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.type", field),
			Code:    apisv1.ErrCodeMissingRequiredField,
			Message: "probe type is required",
		})
	} else if !validProbeTypes[probe.Type] {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.type", field),
			Code:    apisv1.ErrCodeInvalidProbeType,
			Message: fmt.Sprintf("invalid probe type: %s, must be one of: liveness, readiness, startup", probe.Type),
		})
	}

	// Validate that exactly one probe method is specified
	probeMethodCount := 0
	if probe.Exec != nil && len(probe.Exec.Command) > 0 {
		probeMethodCount++
	}
	if probe.HTTPGet != nil {
		probeMethodCount++
	}
	if probe.TCPSocket != nil {
		probeMethodCount++
	}

	if probeMethodCount == 0 {
		errors = append(errors, apisv1.ValidationError{
			Field:   field,
			Code:    apisv1.ErrCodeInvalidProbeConfig,
			Message: "probe must specify exactly one of exec, httpGet, or tcpSocket",
		})
	} else if probeMethodCount > 1 {
		errors = append(errors, apisv1.ValidationError{
			Field:   field,
			Code:    apisv1.ErrCodeInvalidProbeConfig,
			Message: "probe must specify exactly one of exec, httpGet, or tcpSocket, not multiple",
		})
	}

	// Validate HTTPGet probe
	if probe.HTTPGet != nil {
		if probe.HTTPGet.Port <= 0 {
			errors = append(errors, apisv1.ValidationError{
				Field:   fmt.Sprintf("%s.httpGet.port", field),
				Code:    apisv1.ErrCodeMissingRequiredField,
				Message: "httpGet probe port is required and must be positive",
			})
		}
	}

	// Validate TCPSocket probe
	if probe.TCPSocket != nil {
		if probe.TCPSocket.Port <= 0 {
			errors = append(errors, apisv1.ValidationError{
				Field:   fmt.Sprintf("%s.tcpSocket.port", field),
				Code:    apisv1.ErrCodeMissingRequiredField,
				Message: "tcpSocket probe port is required and must be positive",
			})
		}
	}

	return errors
}

// validateInitTrait validates an init container trait
func (v *validationServiceImpl) validateInitTrait(init spec.InitTraitSpec, field string, isNested bool) []apisv1.ValidationError {
	var errors []apisv1.ValidationError
	if init.Properties.FailurePolicy != nil {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.properties.failurePolicy", field),
			Code:    apisv1.ErrCodeInvalidJobFailurePolicy,
			Message: "failurePolicy is only supported for top-level job component properties",
		})
	}

	// Check for forbidden nested init
	if isNested {
		errors = append(errors, apisv1.ValidationError{
			Field:   field,
			Code:    apisv1.ErrCodeNestedTraitForbidden,
			Message: "init trait cannot be nested inside another init or sidecar trait",
		})
		return errors
	}

	// Validate image
	if init.Image == "" {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.image", field),
			Code:    apisv1.ErrCodeMissingImage,
			Message: "init container image is required",
		})
	}

	// Validate nested traits (without init/sidecar)
	errors = append(errors, v.validateTraits(init.Traits, fmt.Sprintf("%s.traits", field), true, "")...)

	return errors
}

func validateTemplateRequestNestedJobFailurePolicies(components []apisv1.CreateComponentRequest) []apisv1.ValidationError {
	var errors []apisv1.ValidationError
	for i, component := range components {
		if component.Template == nil || strings.TrimSpace(component.Template.ID) == "" {
			continue
		}
		errors = append(errors, validateNestedJobFailurePolicies(component.Traits, fmt.Sprintf("component[%d].traits", i))...)
	}
	return errors
}

func validateNestedJobFailurePolicies(traits apisv1.Traits, fieldPrefix string) []apisv1.ValidationError {
	var errors []apisv1.ValidationError
	for i, init := range traits.Init {
		field := fmt.Sprintf("%s.init[%d]", fieldPrefix, i)
		if init.Properties.FailurePolicy != nil {
			errors = append(errors, apisv1.ValidationError{
				Field:   fmt.Sprintf("%s.properties.failurePolicy", field),
				Code:    apisv1.ErrCodeInvalidJobFailurePolicy,
				Message: "failurePolicy is only supported for top-level job component properties",
			})
		}
		errors = append(errors, validateNestedJobFailurePolicies(init.Traits, fmt.Sprintf("%s.traits", field))...)
	}
	for i, sidecar := range traits.Sidecar {
		errors = append(errors, validateNestedJobFailurePolicies(sidecar.Traits, fmt.Sprintf("%s.sidecar[%d].traits", fieldPrefix, i))...)
	}
	return errors
}

// validateSidecarTrait validates a sidecar container trait
func (v *validationServiceImpl) validateSidecarTrait(sidecar spec.SidecarTraitsSpec, field string, isNested bool) []apisv1.ValidationError {
	var errors []apisv1.ValidationError

	// Check for forbidden nested sidecar
	if isNested {
		errors = append(errors, apisv1.ValidationError{
			Field:   field,
			Code:    apisv1.ErrCodeNestedTraitForbidden,
			Message: "sidecar trait cannot be nested inside another init or sidecar trait",
		})
		return errors
	}

	// Validate image
	if sidecar.Image == "" {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.image", field),
			Code:    apisv1.ErrCodeMissingImage,
			Message: "sidecar container image is required",
		})
	}

	// Validate nested traits (without init/sidecar)
	errors = append(errors, v.validateTraits(sidecar.Traits, fmt.Sprintf("%s.traits", field), true, "")...)

	return errors
}

// validateRBACTrait validates an RBAC trait
func (v *validationServiceImpl) validateRBACTrait(rbac spec.RBACPolicySpec, field string) []apisv1.ValidationError {
	var errors []apisv1.ValidationError

	// Validate rules
	if len(rbac.Rules) == 0 {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.rules", field),
			Code:    apisv1.ErrCodeMissingRBACRules,
			Message: "rbac rules are required",
		})
		return errors
	}

	// Validate each rule has verbs
	for i, rule := range rbac.Rules {
		if len(rule.Verbs) == 0 {
			errors = append(errors, apisv1.ValidationError{
				Field:   fmt.Sprintf("%s.rules[%d].verbs", field, i),
				Code:    apisv1.ErrCodeMissingRBACVerbs,
				Message: "rbac rule verbs are required",
			})
		}
	}

	return errors
}

// validateEnvFromTrait validates an envFrom trait
func (v *validationServiceImpl) validateEnvFromTrait(envFrom spec.EnvFromSourceSpec, field string) []apisv1.ValidationError {
	var errors []apisv1.ValidationError

	// Validate type
	if envFrom.Type == "" {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.type", field),
			Code:    apisv1.ErrCodeMissingRequiredField,
			Message: "envFrom type is required",
		})
	} else if !validEnvFromTypes[envFrom.Type] {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.type", field),
			Code:    apisv1.ErrCodeInvalidEnvFromType,
			Message: fmt.Sprintf("invalid envFrom type: %s, must be one of: secret, configMap", envFrom.Type),
		})
	}

	// Validate sourceName
	if envFrom.SourceName == "" {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.sourceName", field),
			Code:    apisv1.ErrCodeMissingRequiredField,
			Message: "envFrom sourceName is required",
		})
	}

	return errors
}

// validateEnvsTrait validates an envs trait
func (v *validationServiceImpl) validateEnvsTrait(env spec.SimplifiedEnvSpec, field string) []apisv1.ValidationError {
	var errors []apisv1.ValidationError

	// Validate name
	if env.Name == "" {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.name", field),
			Code:    apisv1.ErrCodeMissingRequiredField,
			Message: "env name is required",
		})
	}

	// Validate that exactly one value source is specified
	sourceCount := 0
	if env.ValueFrom.Static != nil {
		sourceCount++
	}
	if env.ValueFrom.Secret != nil {
		sourceCount++
	}
	if env.ValueFrom.Config != nil {
		sourceCount++
	}
	if env.ValueFrom.Field != nil {
		sourceCount++
	}

	if sourceCount == 0 {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.valueFrom", field),
			Code:    apisv1.ErrCodeInvalidEnvValueSource,
			Message: "env valueFrom must specify exactly one of static, secret, config, or field",
		})
	} else if sourceCount > 1 {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.valueFrom", field),
			Code:    apisv1.ErrCodeInvalidEnvValueSource,
			Message: "env valueFrom must specify exactly one of static, secret, config, or field, not multiple",
		})
	}

	// Validate secret reference
	if env.ValueFrom.Secret != nil {
		if env.ValueFrom.Secret.Name == "" {
			errors = append(errors, apisv1.ValidationError{
				Field:   fmt.Sprintf("%s.valueFrom.secret.name", field),
				Code:    apisv1.ErrCodeMissingRequiredField,
				Message: "secret name is required",
			})
		}
		if env.ValueFrom.Secret.Key == "" {
			errors = append(errors, apisv1.ValidationError{
				Field:   fmt.Sprintf("%s.valueFrom.secret.key", field),
				Code:    apisv1.ErrCodeMissingRequiredField,
				Message: "secret key is required",
			})
		}
	}

	// Validate config reference
	if env.ValueFrom.Config != nil {
		if env.ValueFrom.Config.Name == "" {
			errors = append(errors, apisv1.ValidationError{
				Field:   fmt.Sprintf("%s.valueFrom.config.name", field),
				Code:    apisv1.ErrCodeMissingRequiredField,
				Message: "configMap name is required",
			})
		}
		if env.ValueFrom.Config.Key == "" {
			errors = append(errors, apisv1.ValidationError{
				Field:   fmt.Sprintf("%s.valueFrom.config.key", field),
				Code:    apisv1.ErrCodeMissingRequiredField,
				Message: "configMap key is required",
			})
		}
	}

	return errors
}

// validateResourcesTrait validates a resources trait
func (v *validationServiceImpl) validateResourcesTrait(resources spec.ResourceTraitsSpec, field string) []apisv1.ValidationError {
	return traitvalidation.ValidateResourcesTraitSpec(resources, field)
}
