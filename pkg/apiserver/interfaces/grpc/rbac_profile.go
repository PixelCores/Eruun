package grpcapi

import (
	"context"

	assembler "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/assembler/v1"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	eruunv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/grpc/pb/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"google.golang.org/protobuf/types/known/emptypb"
)

func (s *AdministrationServer) ListRBACProfiles(ctx context.Context, _ *eruunv1.ListRBACProfilesRequest) (*eruunv1.ListRBACProfilesResponse, error) {
	items, err := s.RBACProfiles.List(ctx)
	return typedResult(apis.ListRBACProfilesResponse{RBACProfiles: assembler.RBACProfileModelsToDTO(items)}, err, &eruunv1.ListRBACProfilesResponse{})
}

func (s *AdministrationServer) ListRBACProfileOptions(ctx context.Context, _ *eruunv1.ListRBACProfilesRequest) (*eruunv1.ListRBACProfileOptionsResponse, error) {
	items, err := s.RBACProfiles.List(ctx)
	return typedResult(apis.ListRBACProfileOptionsResponse{RBACProfiles: assembler.RBACProfileModelsToOptions(items)}, err, &eruunv1.ListRBACProfileOptionsResponse{})
}

func (s *AdministrationServer) GetRBACProfile(ctx context.Context, req *eruunv1.RBACProfileNameRequest) (*eruunv1.RBACProfile, error) {
	if req == nil {
		return nil, rpcError(bcode.ErrRBACProfileInvalid)
	}
	item, err := s.RBACProfiles.Get(ctx, req.Name)
	return typedResult(assembler.RBACProfileModelToDTO(item), err, &eruunv1.RBACProfile{})
}

func (s *AdministrationServer) CreateRBACProfile(ctx context.Context, req *eruunv1.CreateRBACProfileRequest) (*eruunv1.RBACProfile, error) {
	if req == nil {
		return nil, rpcError(bcode.ErrRBACProfileInvalid)
	}
	request, err := decodeTypedRequest[apis.CreateRBACProfileRequest](req)
	if err != nil {
		return nil, rpcError(bcode.ErrRBACProfileInvalid)
	}
	item, err := s.RBACProfiles.Create(ctx, assembler.CreateRBACProfileCommand(request))
	return typedResult(assembler.RBACProfileModelToDTO(item), err, &eruunv1.RBACProfile{})
}

func (s *AdministrationServer) UpdateRBACProfile(ctx context.Context, req *eruunv1.UpdateRBACProfileRequest) (*eruunv1.RBACProfile, error) {
	if req == nil {
		return nil, rpcError(bcode.ErrRBACProfileInvalid)
	}
	request, err := decodeTypedRequest[apis.CreateRBACProfileRequest](req)
	if err != nil {
		return nil, rpcError(bcode.ErrRBACProfileInvalid)
	}
	item, err := s.RBACProfiles.Update(ctx, request.Name, assembler.UpdateRBACProfileCommand(request.UpdateRBACProfileRequest))
	return typedResult(assembler.RBACProfileModelToDTO(item), err, &eruunv1.RBACProfile{})
}

func (s *AdministrationServer) DeleteRBACProfile(ctx context.Context, req *eruunv1.RBACProfileNameRequest) (*emptypb.Empty, error) {
	if req == nil {
		return nil, rpcError(bcode.ErrRBACProfileInvalid)
	}
	if err := s.RBACProfiles.Delete(ctx, req.Name); err != nil {
		return nil, rpcError(err)
	}
	return &emptypb.Empty{}, nil
}
