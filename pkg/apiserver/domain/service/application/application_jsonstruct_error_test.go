package application

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
)

func TestJSONStructDecodersPropagateMarshalErrors(t *testing.T) {
	invalid := model.JSONStruct{"invalid": func() {}}
	tests := []struct {
		name string
		run  func() error
	}{
		{
			name: "cleanup properties",
			run: func() error {
				_, err := versionUpdateCleanupPropertiesDescriptor(&invalid)
				return err
			},
		},
		{
			name: "cleanup traits",
			run: func() error {
				_, err := versionUpdateCleanupTraitsDescriptor(&invalid)
				return err
			},
		},
		{
			name: "stored workflow failure policy",
			run: func() error {
				_, err := storedWorkflowFailurePolicy(&invalid)
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.run()
			var unsupportedType *json.UnsupportedTypeError
			require.ErrorAs(t, err, &unsupportedType)
		})
	}
}
