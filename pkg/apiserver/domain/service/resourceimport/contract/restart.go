package contract

import (
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
)

// ValidateStatefulSetRestartStrategy rejects update strategies for which a
// PodTemplate annotation change cannot roll every StatefulSet Pod.
func ValidateStatefulSetRestartStrategy(statefulSet *appsv1.StatefulSet) error {
	if statefulSet == nil {
		return fmt.Errorf("statefulset is nil")
	}
	strategy := statefulSet.Spec.UpdateStrategy
	if strategy.Type == appsv1.OnDeleteStatefulSetStrategyType {
		return fmt.Errorf("updateStrategy.type=OnDelete does not roll Pods automatically")
	}
	if strategy.RollingUpdate != nil &&
		strategy.RollingUpdate.Partition != nil &&
		*strategy.RollingUpdate.Partition > 0 {
		return fmt.Errorf(
			"updateStrategy.rollingUpdate.partition=%d would leave lower-ordinal Pods unchanged",
			*strategy.RollingUpdate.Partition,
		)
	}
	return nil
}
