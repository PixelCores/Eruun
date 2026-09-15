package grpcapi

import (
	"context"
	"fmt"

	"github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	eruunv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/grpc/pb/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

func importMappings(items []*eruunv1.ImportNamespaceApplicationMapping) []v1.ImportNamespaceApplicationMapping {
	result := make([]v1.ImportNamespaceApplicationMapping, 0, len(items))
	for _, item := range items {
		if item == nil {
			continue
		}
		mapping := v1.ImportNamespaceApplicationMapping{
			Name: item.Name, Alias: item.Alias, TargetAppID: item.TargetAppId,
		}
		for _, component := range item.Components {
			if component == nil {
				continue
			}
			c := v1.ImportNamespaceComponentMapping{Name: component.Name}
			if component.Workload != nil {
				c.Workload = v1.ImportNamespaceWorkloadReference{
					APIVersion: component.Workload.ApiVersion,
					Kind:       component.Workload.Kind, Name: component.Workload.Name,
				}
			}
			mapping.Components = append(mapping.Components, c)
		}
		result = append(result, mapping)
	}
	return result
}

func resourceImportAccepted(item *v1.ResourceImportJobAcceptedResponse) *eruunv1.ResourceImportJobAcceptedResponse {
	if item == nil {
		return nil
	}
	return &eruunv1.ResourceImportJobAcceptedResponse{
		TaskId: item.TaskID, Type: string(item.Type), Status: item.Status,
	}
}

func (s *AdministrationServer) SubmitResourceImportScan(ctx context.Context, req *eruunv1.ResourceImportScanJobRequest) (*eruunv1.ResourceImportJobAcceptedResponse, error) {
	if req == nil {
		return nil, rpcError(bcode.ErrApplicationConfig)
	}
	input := v1.ResourceImportScanJobRequest{Namespace: req.Namespace}
	for _, rule := range req.Rules {
		if rule == nil {
			return nil, rpcError(bcode.ErrApplicationConfig)
		}
		input.Rules = append(input.Rules, v1.ResourceImportScanRule{
			Kinds: rule.Kinds, NameRegex: rule.NameRegex, LabelSelector: rule.LabelSelector,
		})
	}
	accepted, err := s.ResourceImport.SubmitScanJob(ctx, input)
	return resourceImportAccepted(accepted), rpcError(err)
}

func (s *AdministrationServer) SubmitResourceImportManage(ctx context.Context, req *eruunv1.ResourceImportManageJobRequest) (*eruunv1.ResourceImportJobAcceptedResponse, error) {
	if req == nil {
		return nil, rpcError(bcode.ErrApplicationConfig)
	}
	accepted, err := s.ResourceImport.SubmitManageJob(ctx, v1.ResourceImportManageJobRequest{
		ScanTaskID: req.ScanTaskId, Applications: importMappings(req.Applications),
	})
	return resourceImportAccepted(accepted), rpcError(err)
}

func (s *AdministrationServer) GetResourceImportJob(ctx context.Context, req *eruunv1.ResourceImportJobIDRequest) (*eruunv1.ResourceImportJobResponse, error) {
	if req == nil || req.TaskId == "" {
		return nil, rpcError(bcode.ErrWorkflowConfig)
	}
	item, err := s.ResourceImport.GetJob(ctx, req.TaskId)
	if err != nil {
		return nil, rpcError(err)
	}
	if item == nil {
		return nil, rpcError(fmt.Errorf("resource import returned empty job"))
	}
	resp := &eruunv1.ResourceImportJobResponse{
		TaskId: item.TaskID, Type: string(item.Type), Status: item.Status, Error: item.Error,
	}
	if len(item.Result) != 0 {
		value := &structpb.Value{}
		if err := protojson.Unmarshal(item.Result, value); err != nil {
			return nil, rpcError(fmt.Errorf("decode resource import result: %w", err))
		}
		resp.Result = value
	}
	return resp, nil
}
