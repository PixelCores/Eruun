package grpcapi

import (
	"context"
	"errors"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/applicationstatus"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	eruunv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/grpc/pb/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func (s *ApplicationsServer) statusRules() applicationstatus.Service {
	return applicationstatus.Service{Applications: s.Applications, Runtime: s.Runtime}
}

func (s *ApplicationsServer) GetApplicationStatus(ctx context.Context, req *eruunv1.ApplicationIDRequest) (*eruunv1.AppDTOApplicationStatusResponse, error) {
	if req == nil || appID(req.AppId) != nil {
		return nil, rpcError(bcode.ErrApplicationConfig)
	}
	resp, err := s.statusRules().GetStatus(ctx, req.AppId)
	return typedResult(resp, err, &eruunv1.AppDTOApplicationStatusResponse{})
}

func (s *ApplicationsServer) GetApplicationComponentStatus(ctx context.Context, req *eruunv1.ApplicationIDRequest) (*eruunv1.AppDTOApplicationComponentStatusResponse, error) {
	if req == nil || appID(req.AppId) != nil {
		return nil, rpcError(bcode.ErrApplicationConfig)
	}
	resp, err := s.statusRules().GetComponentStatus(ctx, req.AppId)
	return typedResult(resp, err, &eruunv1.AppDTOApplicationComponentStatusResponse{})
}

func batchSafeMessage(err error) string {
	var bc *bcode.Bcode
	if errors.As(err, &bc) && bc != nil {
		return bc.Message
	}
	return "lookup failed"
}

func (s *ApplicationsServer) BatchApplicationComponentStatus(ctx context.Context, req *eruunv1.AppDTOBatchApplicationComponentStatusRequest) (*eruunv1.AppDTOBatchApplicationComponentStatusResponse, error) {
	input, err := decodeTypedRequest[apis.BatchApplicationComponentStatusRequest](req)
	if err != nil || len(input.AppIDs) == 0 {
		return nil, rpcError(bcode.ErrApplicationConfig)
	}
	resp := s.statusRules().Batch(ctx, input.AppIDs, batchSafeMessage)
	return typedResult(resp, nil, &eruunv1.AppDTOBatchApplicationComponentStatusResponse{})
}
