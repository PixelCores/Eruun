package jobdeploy

import (
	"fmt"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobexec"
	traitsPlu "github.com/PixelCores/Eruun/pkg/apiserver/workflow/traits"
)

// GenerateInstantJob Job builders.
func GenerateInstantJob(component *model.ApplicationComponent, properties *model.Properties, runPolicy string, nodePool *model.NodePool) (*jobexec.GenerateServiceResult, error) {
	job := jobexec.BuildJob(component, properties, jobexec.JobBuildOptions{RunPolicy: runPolicy})
	if job == nil {
		return nil, nil
	}
	additionalObjects, err := traitsPlu.ApplyTraits(component, job, nodePool)
	if err != nil {
		return nil, fmt.Errorf("generate component %s: %w", component.Name, err)
	}
	return &jobexec.GenerateServiceResult{
		Service:           job,
		AdditionalObjects: additionalObjects,
	}, nil
}

func GenerateScheduledCronJob(component *model.ApplicationComponent, properties *model.Properties, schedule string, nodePool *model.NodePool) (*jobexec.GenerateServiceResult, error) {
	cron := jobexec.BuildCronJob(component, properties, schedule)
	if cron == nil {
		return nil, nil
	}
	additionalObjects, err := traitsPlu.ApplyTraits(component, cron, nodePool)
	if err != nil {
		return nil, fmt.Errorf("generate component %s: %w", component.Name, err)
	}
	return &jobexec.GenerateServiceResult{
		Service:           cron,
		AdditionalObjects: additionalObjects,
	}, nil
}
