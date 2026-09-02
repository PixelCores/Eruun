package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type testAPIHandler struct{}

func (t *testAPIHandler) RegisterRoutes(group *gin.RouterGroup) {}

func TestRegisterAPI_IdempotentByType(t *testing.T) {
	ResetAPIRegistryForTest()
	defer ResetAPIRegistryForTest()

	RegisterAPI(&testAPIHandler{})
	RegisterAPI(&testAPIHandler{})

	apis := GetRegisteredAPI()
	require.Len(t, apis, 1)
}

func TestInitAPIBean_Idempotent(t *testing.T) {
	ResetAPIRegistryForTest()
	defer ResetAPIRegistryForTest()

	first := InitAPIBean()
	second := InitAPIBean()

	require.NotEmpty(t, first)
	require.Len(t, second, len(first))

	r := gin.New()
	for _, bean := range second {
		handler, ok := bean.(Interface)
		require.True(t, ok)
		handler.RegisterRoutes(r.Group("/api/v1"))
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)
	require.Equal(t, http.StatusOK, resp.Code)
}

func TestResetAPIRegistryForTest_AllowsReinit(t *testing.T) {
	ResetAPIRegistryForTest()
	defer ResetAPIRegistryForTest()

	first := InitAPIBean()
	require.NotEmpty(t, first)

	ResetAPIRegistryForTest()
	second := InitAPIBean()
	require.NotEmpty(t, second)
	require.Len(t, second, len(first))
}
