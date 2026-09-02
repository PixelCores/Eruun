package aliyun

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/cloudjob/contracts"
)

func requireCloudParamString(req *contracts.CloudJobRequest, key string) (string, error) {
	if req == nil {
		return "", fmt.Errorf("cloud job request is nil")
	}
	value, err := requireCloudMapString(req.Params, key)
	if err != nil {
		return "", err
	}
	return value, nil
}

func requireCloudParamStrings(req *contracts.CloudJobRequest, keys ...string) error {
	for _, key := range keys {
		if _, err := requireCloudParamString(req, key); err != nil {
			return err
		}
	}
	return nil
}

func rejectCloudParamStrings(req *contracts.CloudJobRequest, keys ...string) error {
	if req == nil {
		return fmt.Errorf("cloud job request is nil")
	}
	for _, key := range keys {
		if !cloudValuePresent(req.Params[key]) {
			continue
		}
		return fmt.Errorf("cloud job forbids params.%s; configure system setting %s.%s instead", key, model.SystemSettingTypeAliyunCloud, key)
	}
	return nil
}

func rejectCloudStateParams(req *contracts.CloudJobRequest, keys ...string) error {
	if req == nil {
		return fmt.Errorf("cloud job request is nil")
	}
	for _, key := range keys {
		if !cloudValuePresent(req.Params[key]) {
			continue
		}
		return fmt.Errorf("cloud job forbids params.%s; this field is managed by workflow state", key)
	}
	return nil
}

func requireCloudMapString(values map[string]interface{}, key string) (string, error) {
	value := cloudMapString(values, key)
	if value == "" {
		return "", fmt.Errorf("cloud job requires params.%s", key)
	}
	return value, nil
}

func cloudMapInt64(values map[string]interface{}, key string) (int64, bool, error) {
	raw, ok := cloudMapValue(values, key)
	if !ok || raw == nil {
		return 0, false, nil
	}
	switch v := raw.(type) {
	case int:
		return int64(v), true, nil
	case int32:
		return int64(v), true, nil
	case int64:
		return v, true, nil
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return 0, true, fmt.Errorf("unsupported numeric value %v", v)
		}
		if math.Trunc(v) != v {
			return 0, true, fmt.Errorf("fractional numeric value %v is not allowed", v)
		}
		return int64(v), true, nil
	case string:
		parsed, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if err != nil {
			return 0, true, err
		}
		return parsed, true, nil
	default:
		return 0, true, fmt.Errorf("unsupported numeric type %T", raw)
	}
}

func cloudPollInterval(params map[string]interface{}) time.Duration {
	if len(params) == 0 {
		return DefaultPollInterval
	}
	raw, ok := params[ParamPollIntervalSec]
	if !ok || raw == nil {
		return DefaultPollInterval
	}
	seconds := int64(0)
	switch v := raw.(type) {
	case int:
		seconds = int64(v)
	case int32:
		seconds = int64(v)
	case int64:
		seconds = v
	case float64:
		seconds = int64(v)
	case string:
		parsed, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if err == nil {
			seconds = parsed
		}
	}
	if seconds <= 0 {
		return DefaultPollInterval
	}
	return time.Duration(seconds) * time.Second
}
