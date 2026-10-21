package traits

import "github.com/PixelCores/Eruun/pkg/apiserver/domain/model"

// IdentityNodePool supplies only the referenced node pool name, for resource
// identity and immutable-field inspection. It must never be used for
// deployment: the selector and tolerations are empty.
func IdentityNodePool(component *model.ApplicationComponent) *model.NodePool {
	if component == nil || component.Traits == nil {
		return nil
	}
	if name, ok := (*component.Traits)["nodePool"].(string); ok && name != "" {
		return &model.NodePool{Name: name}
	}
	return nil
}
