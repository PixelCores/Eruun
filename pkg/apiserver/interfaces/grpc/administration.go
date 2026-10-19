package grpcapi

import (
	"context"
	"fmt"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	eruunv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/grpc/pb/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

// AdministrationServer calls the same setting and programming-language
// services used by HTTP. Its dependencies are populated by the API IoC graph.
type AdministrationServer struct {
	eruunv1.UnimplementedSettingsServiceServer
	Settings service.SystemSettingService `inject:""`
}

func valueBytes(value *structpb.Value) ([]byte, error) {
	if value == nil {
		return nil, bcode.ErrSystemSettingValueInvalid
	}
	data, err := protojson.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal setting value: %w", err)
	}
	return data, nil
}

func settingMessage(item *apisv1.SystemSetting) (*eruunv1.SystemSetting, error) {
	if item == nil {
		return nil, fmt.Errorf("setting service returned nil item")
	}
	value := &structpb.Value{}
	if err := protojson.Unmarshal(item.Value, value); err != nil {
		return nil, fmt.Errorf("decode setting value: %w", err)
	}
	return &eruunv1.SystemSetting{
		Type: item.Type, Value: value,
		CreateTime: timeMessage(item.CreateTime), UpdateTime: timeMessage(item.UpdateTime),
	}, nil
}

func (s *AdministrationServer) ListSettings(ctx context.Context, _ *eruunv1.ListSettingsRequest) (*eruunv1.ListSettingsResponse, error) {
	items, err := s.Settings.List(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	resp := &eruunv1.ListSettingsResponse{}
	for _, item := range items {
		converted, convertErr := settingMessage(item)
		if convertErr != nil {
			return nil, rpcError(convertErr)
		}
		resp.Settings = append(resp.Settings, converted)
	}
	return resp, nil
}

func (s *AdministrationServer) GetSetting(ctx context.Context, req *eruunv1.GetSettingRequest) (*eruunv1.SystemSetting, error) {
	if req == nil {
		return nil, rpcError(bcode.ErrSystemSettingTypeInvalid)
	}
	item, err := s.Settings.Get(ctx, req.Type)
	if err != nil {
		return nil, rpcError(err)
	}
	converted, err := settingMessage(item)
	return converted, rpcError(err)
}

func (s *AdministrationServer) CreateSetting(ctx context.Context, req *eruunv1.CreateSettingRequest) (*eruunv1.SystemSetting, error) {
	if req == nil {
		return nil, rpcError(bcode.ErrSystemSettingValueInvalid)
	}
	value, err := valueBytes(req.Value)
	if err != nil {
		return nil, rpcError(err)
	}
	item, err := s.Settings.Create(ctx, apisv1.CreateSystemSettingRequest{Type: req.Type, Value: value})
	if err != nil {
		return nil, rpcError(err)
	}
	converted, err := settingMessage(item)
	return converted, rpcError(err)
}

func (s *AdministrationServer) UpdateSetting(ctx context.Context, req *eruunv1.UpdateSettingRequest) (*eruunv1.SystemSetting, error) {
	if req == nil {
		return nil, rpcError(bcode.ErrSystemSettingValueInvalid)
	}
	value, err := valueBytes(req.Value)
	if err != nil {
		return nil, rpcError(err)
	}
	item, err := s.Settings.Update(ctx, req.Type, apisv1.UpdateSystemSettingRequest{Value: value})
	if err != nil {
		return nil, rpcError(err)
	}
	converted, err := settingMessage(item)
	return converted, rpcError(err)
}

func (s *AdministrationServer) DeleteSetting(ctx context.Context, req *eruunv1.DeleteSettingRequest) (*eruunv1.DeleteSettingResponse, error) {
	if req == nil {
		return nil, rpcError(bcode.ErrSystemSettingTypeInvalid)
	}
	if err := s.Settings.Delete(ctx, req.Type); err != nil {
		return nil, rpcError(err)
	}
	return &eruunv1.DeleteSettingResponse{Type: req.Type}, nil
}
