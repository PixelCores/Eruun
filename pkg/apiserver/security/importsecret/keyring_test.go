package importsecret

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncryptDecryptAndAADTamperDetection(t *testing.T) {
	keyring := mustKeyring(t, "active", map[string][]byte{
		"active": []byte("0123456789abcdef0123456789abcdef"),
	})
	aad := ResourceAAD("app-1", "prod", "v1", "Secret", "mysql", "password")
	envelope, err := keyring.Encrypt([]byte("not-for-logs"), aad)
	require.NoError(t, err)
	require.NotContains(t, envelope.Ciphertext, "not-for-logs")

	plaintext, err := keyring.Decrypt(envelope, aad)
	require.NoError(t, err)
	require.Equal(t, "not-for-logs", string(plaintext))

	_, err = keyring.Decrypt(envelope, ResourceAAD("app-2", "prod", "v1", "Secret", "mysql", "password"))
	require.Error(t, err)

	envelope.Ciphertext = strings.Repeat("A", len(envelope.Ciphertext))
	_, err = keyring.Decrypt(envelope, aad)
	require.Error(t, err)
}

func TestPreviousKeyDecryptsAndVerifiesOldPlan(t *testing.T) {
	oldKeyring := mustKeyring(t, "old", map[string][]byte{
		"old": []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
	})
	payload := []byte(`{"stable":"plan"}`)
	fingerprint, err := oldKeyring.SignPlan(payload)
	require.NoError(t, err)
	envelope, err := oldKeyring.Encrypt([]byte("secret"), []byte("aad"))
	require.NoError(t, err)

	rotated := mustKeyring(t, "new", map[string][]byte{
		"new": []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
		"old": []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
	})
	require.NoError(t, rotated.VerifyPlan(payload, fingerprint))
	plaintext, err := rotated.Decrypt(envelope, []byte("aad"))
	require.NoError(t, err)
	require.Equal(t, "secret", string(plaintext))
	require.True(t, rotated.NeedsRotation(envelope))

	newEnvelope, err := rotated.Encrypt(plaintext, []byte("aad"))
	require.NoError(t, err)
	require.Equal(t, "new", newEnvelope.KeyID)
	require.False(t, rotated.NeedsRotation(newEnvelope))
}

func TestPlanFingerprintRejectsDriftAndTampering(t *testing.T) {
	keyring := mustKeyring(t, "active", map[string][]byte{
		"active": []byte("0123456789abcdef0123456789abcdef"),
	})
	fingerprint, err := keyring.SignPlan([]byte("before"))
	require.NoError(t, err)
	require.NoError(t, keyring.VerifyPlan([]byte("before"), fingerprint))
	require.ErrorIs(t, keyring.VerifyPlan([]byte("after"), fingerprint), ErrInvalidFingerprint)

	parts := strings.Split(fingerprint, ":")
	parts[2] = strings.Repeat("0", len(parts[2]))
	require.ErrorIs(t, keyring.VerifyPlan([]byte("before"), strings.Join(parts, ":")), ErrInvalidFingerprint)
}

func TestParseRejectsInvalidKeyring(t *testing.T) {
	_, err := Parse([]byte(`{"activeKeyId":"missing","keys":{}}`))
	require.Error(t, err)
	_, err = Parse([]byte(`{"activeKeyId":"bad:key","keys":{"bad:key":"AAAA"}}`))
	require.Error(t, err)
	_, err = Load(`{"activeKeyId":"key","keys":{}}`, "/mounted/keyring")
	require.ErrorContains(t, err, "mutually exclusive")
	_, err = (&Keyring{}).Decrypt(Envelope{Version: envelopeVersion, KeyID: "missing"}, nil)
	require.True(t, errors.Is(err, ErrUnknownKey))
}

func TestParseRejectsDuplicateNormalizedKeyIDs(t *testing.T) {
	first := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))
	second := base64.StdEncoding.EncodeToString([]byte("abcdef0123456789abcdef0123456789"))
	_, err := Parse([]byte(fmt.Sprintf(
		`{"activeKeyId":"prod","keys":{"prod":%q," prod ":%q}}`,
		first,
		second,
	)))
	require.ErrorContains(t, err, `duplicate import secret key id "prod"`)
}

func TestParseRejectsExactDuplicateFieldsAndKeyIDs(t *testing.T) {
	encoded := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))
	for name, payload := range map[string]string{
		"active key id": fmt.Sprintf(
			`{"activeKeyId":"prod","activeKeyId":"other","keys":{"prod":%q}}`,
			encoded,
		),
		"keys field": fmt.Sprintf(
			`{"activeKeyId":"prod","keys":{"prod":%q},"keys":{"prod":%q}}`,
			encoded,
			encoded,
		),
		"key id": fmt.Sprintf(
			`{"activeKeyId":"prod","keys":{"prod":%q,"prod":%q}}`,
			encoded,
			encoded,
		),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(payload))
			require.ErrorContains(t, err, "duplicate")
		})
	}
}

func TestParseKeepsUnknownFieldsBackwardCompatible(t *testing.T) {
	encoded := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))
	keyring, err := Parse([]byte(fmt.Sprintf(
		`{"activeKeyId":"prod","keys":{"prod":%q},"metadata":{"rotation":"planned"}}`,
		encoded,
	)))
	require.NoError(t, err)
	require.Equal(t, "prod", keyring.ActiveKeyID())
}

func mustKeyring(t *testing.T, active string, keys map[string][]byte) *Keyring {
	t.Helper()
	encoded := make(map[string]string, len(keys))
	for keyID, key := range keys {
		encoded[keyID] = base64.StdEncoding.EncodeToString(key)
	}
	payload, err := json.Marshal(keyringDocument{ActiveKeyID: active, Keys: encoded})
	require.NoError(t, err)
	keyring, err := Parse(payload)
	require.NoError(t, err)
	return keyring
}
