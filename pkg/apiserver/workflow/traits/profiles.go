package traits

import "github.com/PixelCores/Eruun/pkg/apiserver/domain/model"

// Profiles carries the named scheduling profiles a component references,
// resolved from MySQL for deployment.
type Profiles struct {
	NodePool *model.NodePool
}

// IdentityProfiles supplies names only for resource identity and immutable-field
// inspection. It must never be used for deployment: profile contents are empty.
func IdentityProfiles(component *model.ApplicationComponent) Profiles {
	var profiles Profiles
	if component == nil || component.Traits == nil {
		return profiles
	}
	if name, ok := (*component.Traits)["nodePool"].(string); ok && name != "" {
		profiles.NodePool = &model.NodePool{Name: name}
	}
	return profiles
}
