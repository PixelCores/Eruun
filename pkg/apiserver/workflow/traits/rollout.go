package traits

import (
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	spec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

// RolloutProcessor applies workload-level rollout/update strategy settings.
type RolloutProcessor struct{}

// Name returns the name of the trait.
func (p *RolloutProcessor) Name() string {
	return "rollout"
}

// Process converts a rollout trait into the native workload update strategy.
func (p *RolloutProcessor) Process(ctx *TraitContext) (*TraitResult, error) {
	rollout, ok := ctx.TraitData.(*spec.RolloutTraitSpec)
	if !ok {
		return nil, fmt.Errorf("unexpected type for rollout trait: %T", ctx.TraitData)
	}
	if rollout == nil {
		return nil, nil
	}

	switch ctx.Workload.(type) {
	case *appsv1.Deployment:
		strategy, err := deploymentStrategyFromRollout(rollout)
		if err != nil {
			return nil, err
		}
		return &TraitResult{DeploymentStrategy: strategy}, nil
	case *appsv1.StatefulSet:
		strategy, err := statefulSetUpdateStrategyFromRollout(rollout)
		if err != nil {
			return nil, err
		}
		return &TraitResult{StatefulSetUpdateStrategy: strategy}, nil
	default:
		return nil, fmt.Errorf("rollout trait supports deployment and statefulset workloads, got %T", ctx.Workload)
	}
}

func deploymentStrategyFromRollout(rollout *spec.RolloutTraitSpec) (*appsv1.DeploymentStrategy, error) {
	switch strategyType := appsv1.DeploymentStrategyType(strings.TrimSpace(rollout.Type)); strategyType {
	case appsv1.RollingUpdateDeploymentStrategyType:
		if rollout.RollingUpdate == nil {
			return nil, fmt.Errorf("deployment rollout type %s requires rollingUpdate", strategyType)
		}
		if rollout.RollingUpdate.MaxSurge == nil {
			return nil, fmt.Errorf("deployment rollout type %s requires rollingUpdate.maxSurge", strategyType)
		}
		if rollout.RollingUpdate.MaxUnavailable == nil {
			return nil, fmt.Errorf("deployment rollout type %s requires rollingUpdate.maxUnavailable", strategyType)
		}
		strategy := &appsv1.DeploymentStrategy{Type: strategyType}
		if rollout.RollingUpdate.Partition != nil {
			return nil, fmt.Errorf("deployment rollout does not support rollingUpdate.partition")
		}
		strategy.RollingUpdate = &appsv1.RollingUpdateDeployment{
			MaxSurge:       copyIntOrString(rollout.RollingUpdate.MaxSurge),
			MaxUnavailable: copyIntOrString(rollout.RollingUpdate.MaxUnavailable),
		}
		return strategy, nil
	case appsv1.RecreateDeploymentStrategyType:
		if rollout.RollingUpdate != nil {
			return nil, fmt.Errorf("deployment rollout type %s does not support rollingUpdate", strategyType)
		}
		return &appsv1.DeploymentStrategy{Type: strategyType}, nil
	case "":
		return nil, fmt.Errorf("rollout type is required")
	default:
		return nil, fmt.Errorf("unsupported deployment rollout type %q", rollout.Type)
	}
}

func statefulSetUpdateStrategyFromRollout(rollout *spec.RolloutTraitSpec) (*appsv1.StatefulSetUpdateStrategy, error) {
	switch strategyType := appsv1.StatefulSetUpdateStrategyType(strings.TrimSpace(rollout.Type)); strategyType {
	case appsv1.RollingUpdateStatefulSetStrategyType:
		strategy := &appsv1.StatefulSetUpdateStrategy{Type: strategyType}
		if rollout.RollingUpdate != nil {
			if rollout.RollingUpdate.MaxSurge != nil {
				return nil, fmt.Errorf("statefulset rollout does not support rollingUpdate.maxSurge")
			}
			strategy.RollingUpdate = &appsv1.RollingUpdateStatefulSetStrategy{
				Partition:      copyInt32(rollout.RollingUpdate.Partition),
				MaxUnavailable: copyIntOrString(rollout.RollingUpdate.MaxUnavailable),
			}
		}
		return strategy, nil
	case appsv1.OnDeleteStatefulSetStrategyType:
		if rollout.RollingUpdate != nil {
			return nil, fmt.Errorf("statefulset rollout type %s does not support rollingUpdate", strategyType)
		}
		return &appsv1.StatefulSetUpdateStrategy{Type: strategyType}, nil
	case "":
		return nil, fmt.Errorf("rollout type is required")
	default:
		return nil, fmt.Errorf("unsupported statefulset rollout type %q", rollout.Type)
	}
}

func copyIntOrString(value *intstr.IntOrString) *intstr.IntOrString {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}

func copyInt32(value *int32) *int32 {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}
