package api

import (
	"encoding/json"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

type apiResponse struct {
	Code    int32           `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

func decodeResponse(t *testing.T, body []byte, out interface{}) apiResponse {
	t.Helper()

	var resp apiResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode response envelope: %v", err)
	}
	if out != nil {
		if err := json.Unmarshal(resp.Data, out); err != nil {
			t.Fatalf("decode response data: %v", err)
		}
	}
	return resp
}

func requireSuccessResponse(t *testing.T, body []byte, out interface{}) apiResponse {
	t.Helper()

	resp := decodeResponse(t, body, out)
	if resp.Code != bcode.SuccessCode {
		t.Fatalf("unexpected response code: %d message: %s", resp.Code, resp.Message)
	}
	return resp
}
