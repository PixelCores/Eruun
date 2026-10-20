package grpcapi

import (
	"context"

	assembler "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/assembler/v1"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	eruunv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/grpc/pb/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"google.golang.org/protobuf/types/known/emptypb"
)

func (s *AdministrationServer) ListNodePools(ctx context.Context, _ *eruunv1.ListNodePoolsRequest) (*eruunv1.ListNodePoolsResponse, error) {
	items, err := s.NodePools.List(ctx)
	return typedResult(apis.ListNodePoolsResponse{NodePools: assembler.NodePoolModelsToDTO(items)}, err, &eruunv1.ListNodePoolsResponse{})
}

func (s *AdministrationServer) ListNodePoolOptions(ctx context.Context, _ *eruunv1.ListNodePoolsRequest) (*eruunv1.ListNodePoolOptionsResponse, error) {
	items, err := s.NodePools.List(ctx)
	return typedResult(apis.ListNodePoolOptionsResponse{NodePools: assembler.NodePoolModelsToOptions(items)}, err, &eruunv1.ListNodePoolOptionsResponse{})
}

func (s *AdministrationServer) GetNodePool(ctx context.Context, req *eruunv1.NodePoolNameRequest) (*eruunv1.NodePool, error) {
	if req == nil {
		return nil, rpcError(bcode.ErrNodePoolInvalid)
	}
	item, err := s.NodePools.Get(ctx, req.Name)
	return typedResult(assembler.NodePoolModelToDTO(item), err, &eruunv1.NodePool{})
}

func (s *AdministrationServer) CreateNodePool(ctx context.Context, req *eruunv1.CreateNodePoolRequest) (*eruunv1.NodePool, error) {
	if req == nil {
		return nil, rpcError(bcode.ErrNodePoolInvalid)
	}
	request, err := decodeTypedRequest[apis.CreateNodePoolRequest](req)
	if err != nil {
		return nil, rpcError(bcode.ErrNodePoolInvalid)
	}
	item, err := s.NodePools.Create(ctx, assembler.CreateNodePoolCommand(request))
	return typedResult(assembler.NodePoolModelToDTO(item), err, &eruunv1.NodePool{})
}

func (s *AdministrationServer) UpdateNodePool(ctx context.Context, req *eruunv1.UpdateNodePoolRequest) (*eruunv1.NodePool, error) {
	if req == nil {
		return nil, rpcError(bcode.ErrNodePoolInvalid)
	}
	request, err := decodeTypedRequest[apis.CreateNodePoolRequest](req)
	if err != nil {
		return nil, rpcError(bcode.ErrNodePoolInvalid)
	}
	item, err := s.NodePools.Update(ctx, request.Name, assembler.UpdateNodePoolCommand(request.UpdateNodePoolRequest))
	return typedResult(assembler.NodePoolModelToDTO(item), err, &eruunv1.NodePool{})
}

func (s *AdministrationServer) DeleteNodePool(ctx context.Context, req *eruunv1.NodePoolNameRequest) (*emptypb.Empty, error) {
	if req == nil {
		return nil, rpcError(bcode.ErrNodePoolInvalid)
	}
	if err := s.NodePools.Delete(ctx, req.Name); err != nil {
		return nil, rpcError(err)
	}
	return &emptypb.Empty{}, nil
}
