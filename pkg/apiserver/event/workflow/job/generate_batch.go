package job

import (
	"fmt"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	traitsPlu "github.com/PixelCores/Eruun/pkg/apiserver/workflow/traits"
)

// GenerateInstantJob Job builders.
func GenerateInstantJob(component *model.ApplicationComponent, properties *model.Properties, runPolicy string) (*GenerateServiceResult, error) {
	job := buildJob(component, properties, jobBuildOptions{runPolicy: runPolicy})
	if job == nil {
		return nil, nil
	}
	additionalObjects, err := traitsPlu.ApplyTraits(component, job)
	if err != nil {
		return nil, fmt.Errorf("generate component %s: %w", component.Name, err)
	}
	return &GenerateServiceResult{
		Service:           job,
		AdditionalObjects: additionalObjects,
	}, nil
}

func GenerateScheduledCronJob(component *model.ApplicationComponent, properties *model.Properties, schedule string) (*GenerateServiceResult, error) {
	cron := buildCronJob(component, properties, schedule)
	if cron == nil {
		return nil, nil
	}
	additionalObjects, err := traitsPlu.ApplyTraits(component, cron)
	if err != nil {
		return nil, fmt.Errorf("generate component %s: %w", component.Name, err)
	}
	return &GenerateServiceResult{
		Service:           cron,
		AdditionalObjects: additionalObjects,
	}, nil
}
