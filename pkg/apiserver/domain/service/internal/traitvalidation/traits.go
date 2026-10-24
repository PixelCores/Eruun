package traitvalidation

import (
	"fmt"
	"regexp"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

var (
	storageQuantityRegexp = regexp.MustCompile(`^[0-9]+(\.[0-9]+)?(Ki|Mi|Gi|Ti|Pi|Ei|K|M|G|T|P|E)?$`)

	validStorageTypes = map[string]bool{
		spec.StorageTypePersistent: true,
		spec.StorageTypeEphemeral:  true,
		spec.StorageTypeConfig:     true,
		spec.StorageTypeSecret:     true,
	}
	validProbeTypes = map[string]bool{
		"liveness":  true,
		"readiness": true,
		"startup":   true,
	}
	validEnvFromTypes = map[string]bool{
		"secret":    true,
		"configMap": true,
	}
)

// ValidateTraits is the single rule set for component traits. Try reports
// every error; writes reject the request on the first one.
func ValidateTraits(traits spec.Traits, field string, componentType config.JobType) []apisv1.ValidationError {
	return validateTraits(traits, field, componentType, false)
}

func validateTraits(traits spec.Traits, field string, componentType config.JobType, nested bool) []apisv1.ValidationError {
	var errors []apisv1.ValidationError
	if nested && traits.Evaluation != nil {
		if err := spec.NormalizeComponentEvaluation(string(componentType), "", spec.Properties{}, &traits); err != nil {
			errors = append(errors, apisv1.ValidationError{Field: field + ".eval", Code: apisv1.ErrCodeInvalidTraitConfig, Message: err.Error()})
		}
	}
	for i, storage := range traits.Storage {
		errors = append(errors, validateStorageTrait(storage, fmt.Sprintf("%s.storage[%d]", field, i))...)
	}
	for i, probe := range traits.Probes {
		errors = append(errors, validateProbeTrait(probe, fmt.Sprintf("%s.probes[%d]", field, i))...)
	}
	for i, init := range traits.Init {
		errors = append(errors, validateInitTrait(init, fmt.Sprintf("%s.init[%d]", field, i), nested)...)
	}
	for i, sidecar := range traits.Sidecar {
		errors = append(errors, validateSidecarTrait(sidecar, fmt.Sprintf("%s.sidecar[%d]", field, i), nested)...)
	}
	for i, service := range traits.Service {
		errors = append(errors, ValidateServiceTraitSpec(service, fmt.Sprintf("%s.service[%d]", field, i))...)
	}
	for i, envFrom := range traits.EnvFrom {
		errors = append(errors, validateEnvFromTrait(envFrom, fmt.Sprintf("%s.envFrom[%d]", field, i))...)
	}
	for i, env := range traits.Envs {
		errors = append(errors, validateEnvTrait(env, fmt.Sprintf("%s.envs[%d]", field, i))...)
	}
	if traits.Resources != nil {
		errors = append(errors, ValidateResourcesTraitSpec(*traits.Resources, field+".resources")...)
	}
	if !nested {
		errors = append(errors, ValidateNestedNodePool(traits, field)...)
		errors = append(errors, ValidateNestedGateway(traits, field)...)
	}
	return append(errors, ValidateRolloutTrait(componentType, traits.Rollout, field+".rollout", nested)...)
}

// ValidateNestedJobFailurePolicies rejects Job retry and failure policies on
// init containers, including ones carried by template overrides.
func ValidateNestedJobFailurePolicies(traits spec.Traits, field string) []apisv1.ValidationError {
	var errors []apisv1.ValidationError
	for i, init := range traits.Init {
		initField := fmt.Sprintf("%s.init[%d]", field, i)
		errors = append(errors, validateInitJobPolicies(init, initField)...)
		errors = append(errors, ValidateNestedJobFailurePolicies(init.Traits, initField+".traits")...)
	}
	for i, sidecar := range traits.Sidecar {
		errors = append(errors, ValidateNestedJobFailurePolicies(sidecar.Traits, fmt.Sprintf("%s.sidecar[%d].traits", field, i))...)
	}
	return errors
}

func validateInitJobPolicies(init spec.InitTraitSpec, field string) []apisv1.ValidationError {
	var errors []apisv1.ValidationError
	if init.Properties.JobRetryPolicy != nil {
		errors = append(errors, apisv1.ValidationError{
			Field:   field + ".properties.jobRetryPolicy",
			Code:    apisv1.ErrCodeInvalidJobFailurePolicy,
			Message: "jobRetryPolicy is only supported for top-level job component properties",
		})
	}
	if init.Properties.FailurePolicy != nil {
		errors = append(errors, apisv1.ValidationError{
			Field:   field + ".properties.failurePolicy",
			Code:    apisv1.ErrCodeInvalidJobFailurePolicy,
			Message: "failurePolicy is only supported for top-level job component properties",
		})
	}
	return errors
}

func validateStorageTrait(storage spec.StorageTraitSpec, field string) []apisv1.ValidationError {
	var errors []apisv1.ValidationError
	if storage.Type == "" {
		errors = append(errors, apisv1.ValidationError{
			Field:   field + ".type",
			Code:    apisv1.ErrCodeMissingRequiredField,
			Message: "storage type is required",
		})
	} else if !validStorageTypes[storage.Type] {
		errors = append(errors, apisv1.ValidationError{
			Field:   field + ".type",
			Code:    apisv1.ErrCodeInvalidStorageType,
			Message: fmt.Sprintf("invalid storage type: %s, must be one of: persistent, ephemeral, config, secret", storage.Type),
		})
	}
	if storage.MountPath == "" {
		errors = append(errors, apisv1.ValidationError{
			Field:   field + ".mountPath",
			Code:    apisv1.ErrCodeMissingRequiredField,
			Message: "storage mountPath is required",
		})
	}
	errors = append(errors, ValidateStorageSubPathConflict(storage, field)...)
	if storage.Type == spec.StorageTypePersistent && storage.TmpCreate && storage.Size != "" && !storageQuantityRegexp.MatchString(storage.Size) {
		errors = append(errors, apisv1.ValidationError{
			Field:   field + ".size",
			Code:    apisv1.ErrCodeInvalidStorageSize,
			Message: fmt.Sprintf("invalid storage size format: %s, must be a valid Kubernetes quantity (e.g., 1Gi, 500Mi)", storage.Size),
		})
	}
	if (storage.Type == spec.StorageTypeConfig || storage.Type == spec.StorageTypeSecret) && storage.SourceName == "" && storage.Name == "" {
		errors = append(errors, apisv1.ValidationError{
			Field:   field + ".sourceName",
			Code:    apisv1.ErrCodeMissingRequiredField,
			Message: "sourceName or name is required for config/secret storage types",
		})
	}
	return errors
}

// validateProbeTrait mirrors the handler rules of the probe trait processor:
// a handler counts as specified when it is present, so an exec handler must
// also carry a command.
func validateProbeTrait(probe spec.ProbeTraitsSpec, field string) []apisv1.ValidationError {
	var errors []apisv1.ValidationError
	if probe.Type == "" {
		errors = append(errors, apisv1.ValidationError{
			Field:   field + ".type",
			Code:    apisv1.ErrCodeMissingRequiredField,
			Message: "probe type is required",
		})
	} else if !validProbeTypes[probe.Type] {
		errors = append(errors, apisv1.ValidationError{
			Field:   field + ".type",
			Code:    apisv1.ErrCodeInvalidProbeType,
			Message: fmt.Sprintf("invalid probe type: %s, must be one of: liveness, readiness, startup", probe.Type),
		})
	}

	handlers := 0
	if probe.Exec != nil {
		handlers++
		if len(probe.Exec.Command) == 0 {
			errors = append(errors, apisv1.ValidationError{
				Field:   field + ".exec.command",
				Code:    apisv1.ErrCodeMissingRequiredField,
				Message: "exec probe command is required",
			})
		}
	}
	if probe.HTTPGet != nil {
		handlers++
		if probe.HTTPGet.Port <= 0 {
			errors = append(errors, apisv1.ValidationError{
				Field:   field + ".httpGet.port",
				Code:    apisv1.ErrCodeMissingRequiredField,
				Message: "httpGet probe port is required and must be positive",
			})
		}
	}
	if probe.TCPSocket != nil {
		handlers++
		if probe.TCPSocket.Port <= 0 {
			errors = append(errors, apisv1.ValidationError{
				Field:   field + ".tcpSocket.port",
				Code:    apisv1.ErrCodeMissingRequiredField,
				Message: "tcpSocket probe port is required and must be positive",
			})
		}
	}
	if handlers != 1 {
		errors = append(errors, apisv1.ValidationError{
			Field:   field,
			Code:    apisv1.ErrCodeInvalidProbeConfig,
			Message: "probe must specify exactly one of exec, httpGet, or tcpSocket",
		})
	}
	return errors
}

func validateInitTrait(init spec.InitTraitSpec, field string, nested bool) []apisv1.ValidationError {
	errors := validateInitJobPolicies(init, field)
	if nested {
		return append(errors, apisv1.ValidationError{
			Field:   field,
			Code:    apisv1.ErrCodeNestedTraitForbidden,
			Message: "init trait cannot be nested inside another init or sidecar trait",
		})
	}
	if init.Image == "" {
		errors = append(errors, apisv1.ValidationError{
			Field:   field + ".image",
			Code:    apisv1.ErrCodeMissingImage,
			Message: "init container image is required",
		})
	}
	return append(errors, validateTraits(init.Traits, field+".traits", "", true)...)
}

func validateSidecarTrait(sidecar spec.SidecarTraitsSpec, field string, nested bool) []apisv1.ValidationError {
	if nested {
		return []apisv1.ValidationError{{
			Field:   field,
			Code:    apisv1.ErrCodeNestedTraitForbidden,
			Message: "sidecar trait cannot be nested inside another init or sidecar trait",
		}}
	}
	var errors []apisv1.ValidationError
	if sidecar.Image == "" {
		errors = append(errors, apisv1.ValidationError{
			Field:   field + ".image",
			Code:    apisv1.ErrCodeMissingImage,
			Message: "sidecar container image is required",
		})
	}
	return append(errors, validateTraits(sidecar.Traits, field+".traits", "", true)...)
}

func validateEnvFromTrait(envFrom spec.EnvFromSourceSpec, field string) []apisv1.ValidationError {
	var errors []apisv1.ValidationError
	if envFrom.Type == "" {
		errors = append(errors, apisv1.ValidationError{
			Field:   field + ".type",
			Code:    apisv1.ErrCodeMissingRequiredField,
			Message: "envFrom type is required",
		})
	} else if !validEnvFromTypes[envFrom.Type] {
		errors = append(errors, apisv1.ValidationError{
			Field:   field + ".type",
			Code:    apisv1.ErrCodeInvalidEnvFromType,
			Message: fmt.Sprintf("invalid envFrom type: %s, must be one of: secret, configMap", envFrom.Type),
		})
	}
	if envFrom.SourceName == "" {
		errors = append(errors, apisv1.ValidationError{
			Field:   field + ".sourceName",
			Code:    apisv1.ErrCodeMissingRequiredField,
			Message: "envFrom sourceName is required",
		})
	}
	return errors
}

func validateEnvTrait(env spec.SimplifiedEnvSpec, field string) []apisv1.ValidationError {
	var errors []apisv1.ValidationError
	if env.Name == "" {
		errors = append(errors, apisv1.ValidationError{
			Field:   field + ".name",
			Code:    apisv1.ErrCodeMissingRequiredField,
			Message: "env name is required",
		})
	}
	sources := 0
	for _, set := range []bool{env.ValueFrom.Static != nil, env.ValueFrom.Secret != nil, env.ValueFrom.Config != nil, env.ValueFrom.Field != nil} {
		if set {
			sources++
		}
	}
	if sources != 1 {
		errors = append(errors, apisv1.ValidationError{
			Field:   field + ".valueFrom",
			Code:    apisv1.ErrCodeInvalidEnvValueSource,
			Message: "env valueFrom must specify exactly one of static, secret, config, or field",
		})
	}
	if secret := env.ValueFrom.Secret; secret != nil {
		if secret.Name == "" {
			errors = append(errors, apisv1.ValidationError{Field: field + ".valueFrom.secret.name", Code: apisv1.ErrCodeMissingRequiredField, Message: "secret name is required"})
		}
		if secret.Key == "" {
			errors = append(errors, apisv1.ValidationError{Field: field + ".valueFrom.secret.key", Code: apisv1.ErrCodeMissingRequiredField, Message: "secret key is required"})
		}
	}
	if configMap := env.ValueFrom.Config; configMap != nil {
		if configMap.Name == "" {
			errors = append(errors, apisv1.ValidationError{Field: field + ".valueFrom.config.name", Code: apisv1.ErrCodeMissingRequiredField, Message: "configMap name is required"})
		}
		if configMap.Key == "" {
			errors = append(errors, apisv1.ValidationError{Field: field + ".valueFrom.config.key", Code: apisv1.ErrCodeMissingRequiredField, Message: "configMap key is required"})
		}
	}
	return errors
}
