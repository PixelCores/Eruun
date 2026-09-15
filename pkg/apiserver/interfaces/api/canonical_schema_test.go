package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestCanonicalJSONSchemaEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := &applications{}
	router := gin.New()
	router.GET("/schemas/v1/canonical.json", handler.getCanonicalJSONSchema)

	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/schemas/v1/canonical.json", nil))

	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, "application/schema+json", response.Header().Get("Content-Type"))
	require.JSONEq(t, response.Body.String(), response.Body.String())
}
