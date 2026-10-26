package conversion

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	applicationservice "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/application"
	urlpolicy "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/systemsetting"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/clients"
	v1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

const defaultConvertAppName = "converted-app"

const convertYAMLMaxSize = 10 * 1024 * 1024

const (
	convertSchemeHTTP  = "http://"
	convertSchemeHTTPS = "https://"
)

type ConversionService interface {
	ConvertKubeResources(ctx context.Context, req v1.ConvertApplicationsRequest) (*v1.ConvertApplicationsResponse, error)
}

type ValidationService interface {
	TryApplication(ctx context.Context, req v1.CreateApplicationsRequest) *v1.TryApplicationResponse
}

type conversionServiceImpl struct {
	Cfg                       *config.Config      `inject:""`
	ValidationService         ValidationService   `inject:""`
	URLSecurityPolicyProvider *urlpolicy.Provider `inject:""`
}

func NewConversionService() ConversionService {
	return &conversionServiceImpl{}
}

func (c *conversionServiceImpl) ConvertKubeResources(ctx context.Context, req v1.ConvertApplicationsRequest) (*v1.ConvertApplicationsResponse, error) {
	var urlPolicy *spec.URLSecurityPolicySpec
	var err error
	if strings.TrimSpace(req.FileURL) != "" {
		urlPolicy, err = applicationservice.LoadURLSecurityPolicy(ctx, c.URLSecurityPolicyProvider)
		if err != nil {
			return nil, err
		}
	}
	yamlText, err := resolveConvertYAML(ctx, req, urlPolicy)
	if err != nil {
		return nil, err
	}

	components, warnings, err := convertKubeYAMLToComponents(yamlText, c.Cfg)
	if err != nil {
		klog.Errorf("convert kube yaml failed: %v", err)
		// Unsupported Ingress shapes carry an explicit reason for the client.
		if bcode.SafeClientMessage(err) != "" {
			return nil, err
		}
		return nil, bcode.ErrApplicationConfig
	}
	if len(components) == 0 {
		return nil, bcode.ErrApplicationConfig
	}

	validate := true
	if req.Validate != nil {
		validate = *req.Validate
	}

	resp := &v1.ConvertApplicationsResponse{
		Components: components,
		Valid:      true,
		Warnings:   warnings,
	}
	if validate && c.ValidationService != nil {
		validation := c.ValidationService.TryApplication(ctx, v1.CreateApplicationsRequest{
			Name:       defaultConvertAppName,
			Components: components,
		})
		if validation != nil {
			resp.Valid = validation.Valid
			resp.Errors = make([]v1.ValidationError, len(validation.Errors))
			for i, validationErr := range validation.Errors {
				resp.Errors[i] = v1.ValidationError{
					Field:   validationErr.Field,
					Code:    validationErr.Code,
					Message: validationErr.Message,
				}
			}
		}
	}

	return resp, nil
}

func resolveConvertYAML(ctx context.Context, req v1.ConvertApplicationsRequest, urlPolicy *spec.URLSecurityPolicySpec) (string, error) {
	fileURL := strings.TrimSpace(req.FileURL)
	if fileURL != "" {
		if !strings.HasPrefix(fileURL, convertSchemeHTTP) && !strings.HasPrefix(fileURL, convertSchemeHTTPS) {
			return "", bcode.ErrApplicationConfig
		}
		content, err := readYAMLFromURL(ctx, fileURL, urlPolicy)
		if err != nil {
			return "", bcode.ErrApplicationConfig
		}
		return strings.TrimSpace(string(content)), nil
	}

	yamlText := strings.TrimSpace(req.YAML)
	if yamlText == "" {
		return "", bcode.ErrApplicationConfig
	}
	if len(yamlText) > convertYAMLMaxSize {
		return "", bcode.ErrApplicationConfig
	}
	return yamlText, nil
}

func readYAMLFromURL(ctx context.Context, rawURL string, policy *spec.URLSecurityPolicySpec) ([]byte, error) {
	data, err := clients.ReadURL(ctx, rawURL, policy, convertYAMLMaxSize+1)
	if err != nil {
		return nil, err
	}
	if len(data) > convertYAMLMaxSize {
		return nil, fmt.Errorf("file size %d bytes exceeds convert yaml maximum size %d bytes", len(data), convertYAMLMaxSize)
	}
	return data, nil
}
