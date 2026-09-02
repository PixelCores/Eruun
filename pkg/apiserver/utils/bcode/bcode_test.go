package bcode

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/require"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

func snapshotBcodeRegistry() (map[int32]*Bcode, error) {
	snapshot := make(map[int32]*Bcode, len(bcodeMap))
	for code, value := range bcodeMap {
		snapshot[code] = value
	}
	return snapshot, bcodeInitErr
}

func restoreBcodeRegistry(snapshot map[int32]*Bcode, initErr error) {
	bcodeMap = snapshot
	bcodeInitErr = initErr
}

func TestNewBcode_DuplicateDoesNotPanic(t *testing.T) {
	oldMap, oldErr := snapshotBcodeRegistry()
	bcodeMap = map[int32]*Bcode{}
	bcodeInitErr = nil
	defer restoreBcodeRegistry(oldMap, oldErr)

	first := NewBcode(400, 99001, "first")
	second := NewBcode(400, 99001, "second")

	require.Same(t, first, second)
	require.Error(t, Init())
}

func TestInit_NoErrorWhenRegistryClean(t *testing.T) {
	oldMap, oldErr := snapshotBcodeRegistry()
	bcodeMap = map[int32]*Bcode{}
	bcodeInitErr = nil
	defer restoreBcodeRegistry(oldMap, oldErr)

	NewBcode(400, 99002, "ok")
	require.NoError(t, Init())

	bcodeInitErr = errors.New("forced")
	require.Error(t, Init())
}

func TestReturnErrorSanitizesGenericError(t *testing.T) {
	c, recorder := testGinContext()
	readLogs := captureKlogOutput(t)

	ReturnError(c, errors.New("mysql://user:dsn-secret@db:3306/eruun password=cache-secret token=agent-token sk-test-api-key connection refused"))

	require.Equal(t, http.StatusInternalServerError, recorder.Code)
	resp := decodeBcodeResponse(t, recorder)
	require.Equal(t, ErrServer.BusinessCode, resp.Code)
	require.Equal(t, ErrServer.Message, resp.Message)

	body := recorder.Body.String()
	require.NotContains(t, body, "mysql://")
	require.NotContains(t, body, "dsn-secret")
	require.NotContains(t, body, "cache-secret")
	require.NotContains(t, body, "agent-token")
	require.NotContains(t, body, "sk-test-api-key")

	logs := readLogs()
	require.Contains(t, logs, "returning generic server error response")
	require.Contains(t, logs, "errorType")
	require.Contains(t, logs, "errors.errorString")
	require.NotContains(t, logs, "mysql://")
	require.NotContains(t, logs, "dsn-secret")
	require.NotContains(t, logs, "cache-secret")
	require.NotContains(t, logs, "agent-token")
	require.NotContains(t, logs, "sk-test-api-key")
}

func TestReturnErrorSanitizesRecordNotExistError(t *testing.T) {
	c, recorder := testGinContext()

	ReturnError(c, datastore.ErrRecordNotExist)

	require.Equal(t, http.StatusNotFound, recorder.Code)
	resp := decodeBcodeResponse(t, recorder)
	require.Equal(t, ErrNotFound.BusinessCode, resp.Code)
	require.Equal(t, ErrNotFound.Message, resp.Message)
	require.NotContains(t, recorder.Body.String(), "data record is not exist")
}

func TestReturnErrorKeepsBusinessErrorMessage(t *testing.T) {
	c, recorder := testGinContext()

	ReturnError(c, ErrApplicationNotExist)

	require.Equal(t, int(ErrApplicationNotExist.HTTPCode), recorder.Code)
	resp := decodeBcodeResponse(t, recorder)
	require.Equal(t, ErrApplicationNotExist.BusinessCode, resp.Code)
	require.Equal(t, ErrApplicationNotExist.Message, resp.Message)
}

func TestReturnErrorUsesExplicitSafeClientMessage(t *testing.T) {
	internalErr := fmt.Errorf("%w: before=secret-old after=secret-new", ErrApplicationConfig)
	err := WithSafeClientMessage(internalErr, "component mysql changes StatefulSet immutable field serviceName; explicit migration or recreation is required")
	require.ErrorIs(t, err, ErrApplicationConfig)

	c, recorder := testGinContext()
	ReturnError(c, err)

	require.Equal(t, int(ErrApplicationConfig.HTTPCode), recorder.Code)
	resp := decodeBcodeResponse(t, recorder)
	require.Equal(t, ErrApplicationConfig.BusinessCode, resp.Code)
	require.Equal(t, "component mysql changes StatefulSet immutable field serviceName; explicit migration or recreation is required", resp.Message)
	require.NotContains(t, recorder.Body.String(), "secret-old")
	require.NotContains(t, recorder.Body.String(), "secret-new")
}

func TestReturnErrorMapsValidatorErrorToApplicationConfig(t *testing.T) {
	type request struct {
		Name string `validate:"required"`
	}
	err := validator.New().Struct(request{})
	require.Error(t, err)

	c, recorder := testGinContext()
	ReturnError(c, err)

	require.Equal(t, int(ErrApplicationConfig.HTTPCode), recorder.Code)
	resp := decodeBcodeResponse(t, recorder)
	require.Equal(t, ErrApplicationConfig.BusinessCode, resp.Code)
	require.Equal(t, ErrApplicationConfig.Message, resp.Message)
}

func testGinContext() (*gin.Context, *httptest.ResponseRecorder) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	return c, recorder
}

func captureKlogOutput(t *testing.T) func() string {
	t.Helper()
	oldStderr := os.Stderr
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stderr = w
	klog.SetOutput(w)

	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = w.Close()
			_ = r.Close()
		}
		os.Stderr = oldStderr
		klog.SetOutput(os.Stderr)
	})

	return func() string {
		if closed {
			return ""
		}
		klog.Flush()
		_ = w.Close()
		os.Stderr = oldStderr
		klog.SetOutput(os.Stderr)
		var logOutput bytes.Buffer
		_, _ = io.Copy(&logOutput, r)
		_ = r.Close()
		closed = true
		return logOutput.String()
	}
}

func decodeBcodeResponse(t *testing.T, recorder *httptest.ResponseRecorder) Response {
	t.Helper()
	var resp Response
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &resp))
	return resp
}
