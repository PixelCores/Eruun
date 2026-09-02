package config

import corev1 "k8s.io/api/core/v1"

const DefaultWorkflowImagePullPolicy corev1.PullPolicy = corev1.PullAlways
