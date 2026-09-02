package job

import (
	"strings"

	corev1 "k8s.io/api/core/v1"
)

func podSpecImages(spec corev1.PodSpec) []string {
	seen := make(map[string]struct{})
	images := make([]string, 0, len(spec.InitContainers)+len(spec.Containers))
	addImage := func(image string) {
		image = strings.TrimSpace(image)
		if image == "" {
			return
		}
		if _, ok := seen[image]; ok {
			return
		}
		seen[image] = struct{}{}
		images = append(images, image)
	}
	for _, container := range spec.InitContainers {
		addImage(container.Image)
	}
	for _, container := range spec.Containers {
		addImage(container.Image)
	}
	return images
}
