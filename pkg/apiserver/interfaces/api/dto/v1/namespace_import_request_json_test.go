package v1

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestImportNamespaceApplicationsRequestAcceptsLegacyAndExplicitShapes(t *testing.T) {
	for _, payload := range []string{
		`{"namespace":"production"}`,
		`{"namespace":"production","mode":"dry-run","includeKinds":["deployments"]}`,
		`{"namespace":"production","mode":"dry-run","managementMode":"adopted","applications":[{"name":"api","components":[{"name":"web","workload":{"apiVersion":"apps/v1","kind":"Deployment","name":"web"}}]}]}`,
	} {
		var request ImportNamespaceApplicationsRequest
		require.NoError(t, json.Unmarshal([]byte(payload), &request))
		assert.Equal(t, "production", request.Namespace)
	}
}

func TestImportNamespaceApplicationsRequestRejectsUnknownAndDuplicateFields(t *testing.T) {
	for _, payload := range []string{
		`{"namespace":"production","namesapce":"typo"}`,
		`{"namespace":"production","namespace":"other"}`,
		`{"namespace":"production","applications":[{"name":"api","components":[],"unexpected":true}]}`,
		`{"namespace":"production","applications":[{"targetAppId":"first","TARGETAPPID":"second","components":[]}]}`,
		`{"namespace":"production","applications":[{"name":"api","components":[{"name":"first","Name":"second"}]}]}`,
		`{"namespace":"production","applications":[{"name":"api","components":[],"componentſ":[]}]}`,
	} {
		t.Run(payload, func(t *testing.T) {
			var request ImportNamespaceApplicationsRequest
			require.Error(t, json.Unmarshal([]byte(payload), &request))
		})
	}
}

func TestFoldNamespaceImportJSONFieldMatchesUnicodeSimpleFolding(t *testing.T) {
	for _, pair := range [][2]string{{"targetAppId", "TARGETAPPID"}, {"components", "componentſ"}, {"kelvinK", "kelvinK"}} {
		assert.True(t, strings.EqualFold(pair[0], pair[1]))
		assert.Equal(t, foldNamespaceImportJSONField(pair[0]), foldNamespaceImportJSONField(pair[1]))
	}
}
