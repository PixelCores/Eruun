package job

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
)

func TestAdoptedJSONPathsRejectUnsupportedValuesBeforePersistence(t *testing.T) {
	invalid := model.JSONStruct{"invalid": func() {}}

	t.Run("decode ciphertext", func(t *testing.T) {
		_, err := decodeAdoptedSecretEnvelopes(&invalid)
		var unsupportedType *json.UnsupportedTypeError
		require.ErrorAs(t, err, &unsupportedType)
	})

	t.Run("persist ciphertext", func(t *testing.T) {
		component := &model.ApplicationComponent{AppID: "app", Name: "secret"}
		store := &adoptedSourceStore{component: component}

		err := compareAndSwapAdoptedSecretData(context.Background(), store, component, &invalid)
		var unsupportedType *json.UnsupportedTypeError
		require.ErrorAs(t, err, &unsupportedType)
		require.Zero(t, store.componentCASCount)
	})

	t.Run("persist snapshot", func(t *testing.T) {
		app := &model.Applications{ID: "app"}
		store := &adoptedSourceStore{app: app}

		err := compareAndSwapRecreatedAdoptionSnapshot(context.Background(), store, app, &invalid)
		var unsupportedType *json.UnsupportedTypeError
		require.ErrorAs(t, err, &unsupportedType)
		require.Zero(t, store.applicationCASCount)
	})
}
