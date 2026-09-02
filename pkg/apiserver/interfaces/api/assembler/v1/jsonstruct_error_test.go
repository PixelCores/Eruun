package v1

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
)

func TestConvertWorkflowStepsPropagatesMarshalErrors(t *testing.T) {
	invalid := model.JSONStruct{"invalid": func() {}}

	_, _, err := convertWorkflowSteps(&invalid)
	var unsupportedType *json.UnsupportedTypeError
	require.ErrorAs(t, err, &unsupportedType)
}
