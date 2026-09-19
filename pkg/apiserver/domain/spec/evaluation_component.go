package spec

import "fmt"

// NormalizeComponentEvaluation applies the same evaluation contract as a
// standalone Job and rejects combinations that would override the trusted Runner.
func NormalizeComponentEvaluation(componentType, image string, properties Properties, traits *Traits) error {
	if traits == nil {
		return nil
	}
	if err := ValidateNestedEvaluationTraits(traits); err != nil {
		return err
	}
	if traits.Evaluation == nil {
		return nil
	}
	if componentType != "job" {
		return fmt.Errorf("evaluation is only supported on a top-level job component")
	}
	if image != "" || len(properties.Command) > 0 || len(properties.Env) > 0 || len(properties.Ports) > 0 ||
		len(properties.Conf) > 0 || len(properties.Secret) > 0 || properties.Cloud != nil {
		return fmt.Errorf("evaluation owns its image, command and runtime properties; use credential env traits")
	}
	if properties.StartTime != 0 || properties.Schedule != "" || properties.RunPolicy != "" ||
		properties.JobRetryPolicy != nil || properties.SuccessfulJobsHistoryLimit != nil || properties.FailedJobsHistoryLimit != nil {
		return fmt.Errorf("evaluation only supports immediate execution without scheduling, runPolicy or retry overrides")
	}
	if len(traits.Init)+len(traits.Sidecar)+len(traits.Ingress)+len(traits.Service)+len(traits.RBAC)+len(traits.Probes)+len(traits.TargetWorkEnv) > 0 ||
		traits.Share != nil || traits.Rollout != nil {
		return fmt.Errorf("evaluation supports resources and credential envs only")
	}
	jobTraits := JobTraits{Evaluation: traits.Evaluation, Resources: traits.Resources, Envs: traits.Envs,
		Storage: traits.Storage, EnvFrom: traits.EnvFrom, SecurityPolicy: traits.SecurityPolicy}
	if err := NormalizeEvaluationTraits(&jobTraits); err != nil {
		return err
	}
	traits.Resources = jobTraits.Resources
	return nil
}

// ValidateNestedEvaluationTraits rejects execution traits below the component level.
func ValidateNestedEvaluationTraits(traits *Traits) error {
	if traits == nil {
		return nil
	}
	for _, init := range traits.Init {
		if init.Traits.Evaluation != nil {
			return fmt.Errorf("evaluation is not supported in init traits")
		}
		if err := ValidateNestedEvaluationTraits(&init.Traits); err != nil {
			return err
		}
	}
	for _, sidecar := range traits.Sidecar {
		if sidecar.Traits.Evaluation != nil {
			return fmt.Errorf("evaluation is not supported in sidecar traits")
		}
		if err := ValidateNestedEvaluationTraits(&sidecar.Traits); err != nil {
			return err
		}
	}
	return nil
}
