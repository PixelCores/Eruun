package grpcapi

import (
	"context"
	"fmt"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service"
	assembler "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/assembler/v1"
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
	eruunv1.UnimplementedProgrammingLanguagesServiceServer
	eruunv1.UnimplementedResourceImportServiceServer
	Settings             service.SystemSettingService       `inject:""`
	ProgrammingLanguages service.ProgrammingLanguageService `inject:""`
	ResourceImport       service.ResourceImportService      `inject:""`
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

func programmingLanguageMessage(item *model.ProgrammingLanguage) (*eruunv1.ProgrammingLanguage, error) {
	if item == nil || item.Enabled == nil {
		return nil, fmt.Errorf("programming-language service returned incomplete item")
	}
	return &eruunv1.ProgrammingLanguage{
		Id: item.ID, Code: item.Code, Name: item.Name, Version: item.Version,
		Enabled: *item.Enabled, CpuReq: item.CPUReq, MemReq: item.MemReq,
		CreateTime: timeMessage(item.CreateTime), UpdateTime: timeMessage(item.UpdateTime),
	}, nil
}

func (s *AdministrationServer) ListProgrammingLanguages(ctx context.Context, _ *eruunv1.ListProgrammingLanguagesRequest) (*eruunv1.ListProgrammingLanguagesResponse, error) {
	items, err := s.ProgrammingLanguages.List(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	resp := &eruunv1.ListProgrammingLanguagesResponse{}
	for _, item := range items {
		converted, convertErr := programmingLanguageMessage(item)
		if convertErr != nil {
			return nil, rpcError(convertErr)
		}
		resp.Languages = append(resp.Languages, converted)
	}
	return resp, nil
}

func (s *AdministrationServer) GetProgrammingLanguage(ctx context.Context, req *eruunv1.ProgrammingLanguageIDRequest) (*eruunv1.ProgrammingLanguage, error) {
	if req == nil {
		return nil, rpcError(bcode.ErrProgrammingLanguageInvalid)
	}
	item, err := s.ProgrammingLanguages.Get(ctx, req.Id)
	if err != nil {
		return nil, rpcError(err)
	}
	converted, err := programmingLanguageMessage(item)
	return converted, rpcError(err)
}

func (s *AdministrationServer) CreateProgrammingLanguage(ctx context.Context, req *eruunv1.CreateProgrammingLanguageRequest) (*eruunv1.ProgrammingLanguage, error) {
	if req == nil || req.Enabled == nil {
		return nil, rpcError(bcode.ErrProgrammingLanguageInvalid)
	}
	command := assembler.CreateProgrammingLanguageCommand(apisv1.CreateProgrammingLanguageRequest{
		Name: req.Name, Version: req.Version, Enabled: req.Enabled,
		CPUReq: req.CpuReq, MemReq: req.MemReq,
	})
	item, err := s.ProgrammingLanguages.Create(ctx, command)
	if err != nil {
		return nil, rpcError(err)
	}
	converted, err := programmingLanguageMessage(item)
	return converted, rpcError(err)
}

func (s *AdministrationServer) UpdateProgrammingLanguage(ctx context.Context, req *eruunv1.UpdateProgrammingLanguageRequest) (*eruunv1.ProgrammingLanguage, error) {
	if req == nil {
		return nil, rpcError(bcode.ErrProgrammingLanguageInvalid)
	}
	command := assembler.UpdateProgrammingLanguageCommand(apisv1.UpdateProgrammingLanguageRequest{
		Name: req.Name, Version: req.Version, Enabled: req.Enabled,
		CPUReq: req.CpuReq, MemReq: req.MemReq,
	})
	item, err := s.ProgrammingLanguages.Update(ctx, req.Id, command)
	if err != nil {
		return nil, rpcError(err)
	}
	converted, err := programmingLanguageMessage(item)
	return converted, rpcError(err)
}

func (s *AdministrationServer) DeleteProgrammingLanguage(ctx context.Context, req *eruunv1.ProgrammingLanguageIDRequest) (*eruunv1.DeleteProgrammingLanguageResponse, error) {
	if req == nil {
		return nil, rpcError(bcode.ErrProgrammingLanguageInvalid)
	}
	if err := s.ProgrammingLanguages.Delete(ctx, req.Id); err != nil {
		return nil, rpcError(err)
	}
	return &eruunv1.DeleteProgrammingLanguageResponse{Id: req.Id}, nil
}
