// Package importsecret provides encryption and integrity primitives for
// adopted Kubernetes Secret payloads and import plans.
package importsecret

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
)

const (
	envelopeVersion    = "v1"
	fingerprintVersion = "v1"
	aes256KeyBytes     = 32
)

var keyIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

var (
	ErrKeyringNotConfigured = errors.New("import secret keyring is not configured")
	ErrUnknownKey           = errors.New("import secret key is unavailable")
	ErrInvalidFingerprint   = errors.New("invalid import plan fingerprint")
)

// Envelope is the persisted AES-256-GCM representation of one Secret value.
// Ciphertext includes the GCM authentication tag. Plaintext must never be
// copied into the application properties or adoption snapshot.
type Envelope struct {
	Version    string `json:"version"`
	KeyID      string `json:"keyId"`
	Nonce      string `json:"nonce"`
	Ciphertext string `json:"ciphertext"`
}

type keyringDocument struct {
	ActiveKeyID string            `json:"activeKeyId"`
	Keys        map[string]string `json:"keys"`
}

// Keyring contains one active key and any previous keys retained for
// decryption and plan verification during rotation.
type Keyring struct {
	activeKeyID string
	keys        map[string][]byte
}

// Load reads a keyring from either inline JSON or a mounted file. Configuring
// both is rejected so operators cannot accidentally use a different key than
// the one they intended.
func Load(inlineJSON, filePath string) (*Keyring, error) {
	inlineJSON = strings.TrimSpace(inlineJSON)
	filePath = strings.TrimSpace(filePath)
	if inlineJSON != "" && filePath != "" {
		return nil, fmt.Errorf("import secret keyring and keyring file are mutually exclusive")
	}
	if inlineJSON == "" && filePath == "" {
		return nil, nil
	}

	payload := []byte(inlineJSON)
	if filePath != "" {
		var err error
		payload, err = os.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("read import secret keyring file: %w", err)
		}
	}
	return Parse(payload)
}

// Parse validates and materializes a keyring document. Every key must decode
// to exactly 32 bytes.
func Parse(payload []byte) (*Keyring, error) {
	document, err := decodeKeyringDocument(payload)
	if err != nil {
		return nil, fmt.Errorf("parse import secret keyring: %w", err)
	}
	document.ActiveKeyID = strings.TrimSpace(document.ActiveKeyID)
	if !keyIDPattern.MatchString(document.ActiveKeyID) {
		return nil, fmt.Errorf("invalid active import secret key id %q", document.ActiveKeyID)
	}
	if len(document.Keys) == 0 {
		return nil, fmt.Errorf("import secret keyring has no keys")
	}

	keys := make(map[string][]byte, len(document.Keys))
	for rawKeyID, encoded := range document.Keys {
		keyID := strings.TrimSpace(rawKeyID)
		if !keyIDPattern.MatchString(keyID) {
			return nil, fmt.Errorf("invalid import secret key id %q", rawKeyID)
		}
		key, err := base64.StdEncoding.DecodeString(strings.TrimSpace(encoded))
		if err != nil {
			return nil, fmt.Errorf("decode import secret key %s: %w", keyID, err)
		}
		if len(key) != aes256KeyBytes {
			return nil, fmt.Errorf("import secret key %s must decode to %d bytes", keyID, aes256KeyBytes)
		}
		if _, duplicate := keys[keyID]; duplicate {
			return nil, fmt.Errorf("duplicate import secret key id %q after whitespace normalization", keyID)
		}
		keys[keyID] = append([]byte(nil), key...)
	}
	if _, ok := keys[document.ActiveKeyID]; !ok {
		return nil, fmt.Errorf("active import secret key %s is missing", document.ActiveKeyID)
	}
	return &Keyring{activeKeyID: document.ActiveKeyID, keys: keys}, nil
}

func decodeKeyringDocument(payload []byte) (keyringDocument, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	token, err := decoder.Token()
	if err != nil {
		return keyringDocument{}, err
	}
	delimiter, ok := token.(json.Delim)
	if !ok || delimiter != '{' {
		return keyringDocument{}, fmt.Errorf("keyring document must be a JSON object")
	}

	var document keyringDocument
	seen := make(map[string]struct{}, 2)
	for decoder.More() {
		token, err = decoder.Token()
		if err != nil {
			return keyringDocument{}, err
		}
		field, ok := token.(string)
		if !ok {
			return keyringDocument{}, fmt.Errorf("keyring document contains a non-string field name")
		}
		if _, duplicate := seen[field]; duplicate {
			return keyringDocument{}, fmt.Errorf("keyring document contains duplicate field %q", field)
		}
		seen[field] = struct{}{}
		switch field {
		case "activeKeyId":
			err = decoder.Decode(&document.ActiveKeyID)
		case "keys":
			document.Keys, err = decodeKeyringKeys(decoder)
		default:
			var ignored json.RawMessage
			err = decoder.Decode(&ignored)
		}
		if err != nil {
			return keyringDocument{}, fmt.Errorf("decode keyring field %q: %w", field, err)
		}
	}
	if _, err = decoder.Token(); err != nil {
		return keyringDocument{}, err
	}
	var extra interface{}
	if err = decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return keyringDocument{}, fmt.Errorf("keyring document contains multiple JSON values")
		}
		return keyringDocument{}, err
	}
	return document, nil
}

func decodeKeyringKeys(decoder *json.Decoder) (map[string]string, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delimiter, ok := token.(json.Delim)
	if !ok || delimiter != '{' {
		return nil, fmt.Errorf("keys must be a JSON object")
	}
	keys := make(map[string]string)
	for decoder.More() {
		token, err = decoder.Token()
		if err != nil {
			return nil, err
		}
		keyID, ok := token.(string)
		if !ok {
			return nil, fmt.Errorf("keys contains a non-string key id")
		}
		if _, duplicate := keys[keyID]; duplicate {
			return nil, fmt.Errorf("keys contains duplicate key id %q", keyID)
		}
		var encoded string
		if err := decoder.Decode(&encoded); err != nil {
			return nil, fmt.Errorf("decode key %q: %w", keyID, err)
		}
		keys[keyID] = encoded
	}
	if _, err = decoder.Token(); err != nil {
		return nil, err
	}
	return keys, nil
}

func (k *Keyring) ActiveKeyID() string {
	if k == nil {
		return ""
	}
	return k.activeKeyID
}

// Encrypt seals plaintext using the active key and the supplied stable
// app/resource identity as additional authenticated data.
func (k *Keyring) Encrypt(plaintext, aad []byte) (Envelope, error) {
	if k == nil {
		return Envelope{}, ErrKeyringNotConfigured
	}
	key, ok := k.keys[k.activeKeyID]
	if !ok {
		return Envelope{}, fmt.Errorf("%w: %s", ErrUnknownKey, k.activeKeyID)
	}
	aead, err := newGCM(key)
	if err != nil {
		return Envelope{}, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return Envelope{}, fmt.Errorf("generate import secret nonce: %w", err)
	}
	sealed := aead.Seal(nil, nonce, plaintext, aad)
	return Envelope{
		Version:    envelopeVersion,
		KeyID:      k.activeKeyID,
		Nonce:      base64.RawStdEncoding.EncodeToString(nonce),
		Ciphertext: base64.RawStdEncoding.EncodeToString(sealed),
	}, nil
}

// Decrypt authenticates and opens an envelope using its referenced active or
// previous key.
func (k *Keyring) Decrypt(envelope Envelope, aad []byte) ([]byte, error) {
	if k == nil {
		return nil, ErrKeyringNotConfigured
	}
	if envelope.Version != envelopeVersion {
		return nil, fmt.Errorf("unsupported import secret envelope version %q", envelope.Version)
	}
	key, ok := k.keys[envelope.KeyID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownKey, envelope.KeyID)
	}
	aead, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	nonce, err := base64.RawStdEncoding.DecodeString(envelope.Nonce)
	if err != nil || len(nonce) != aead.NonceSize() {
		return nil, fmt.Errorf("invalid import secret nonce")
	}
	ciphertext, err := base64.RawStdEncoding.DecodeString(envelope.Ciphertext)
	if err != nil {
		return nil, fmt.Errorf("decode import secret ciphertext: %w", err)
	}
	plaintext, err := aead.Open(nil, nonce, ciphertext, aad)
	if err != nil {
		return nil, fmt.Errorf("authenticate import secret ciphertext: %w", err)
	}
	return plaintext, nil
}

func (k *Keyring) NeedsRotation(envelope Envelope) bool {
	return k != nil && envelope.KeyID != k.activeKeyID
}

// SignPlan returns a key-versioned HMAC for a canonical plan payload.
func (k *Keyring) SignPlan(payload []byte) (string, error) {
	if k == nil {
		return "", ErrKeyringNotConfigured
	}
	key, ok := k.keys[k.activeKeyID]
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrUnknownKey, k.activeKeyID)
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(payload)
	return strings.Join([]string{fingerprintVersion, k.activeKeyID, hex.EncodeToString(mac.Sum(nil))}, ":"), nil
}

// VerifyPlan accepts fingerprints issued by either the active key or a
// retained previous key.
func (k *Keyring) VerifyPlan(payload []byte, fingerprint string) error {
	if k == nil {
		return ErrKeyringNotConfigured
	}
	parts := strings.Split(fingerprint, ":")
	if len(parts) != 3 || parts[0] != fingerprintVersion || !keyIDPattern.MatchString(parts[1]) {
		return ErrInvalidFingerprint
	}
	key, ok := k.keys[parts[1]]
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnknownKey, parts[1])
	}
	provided, err := hex.DecodeString(parts[2])
	if err != nil || len(provided) != sha256.Size {
		return ErrInvalidFingerprint
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(payload)
	expected := mac.Sum(nil)
	if subtle.ConstantTimeCompare(expected, provided) != 1 {
		return ErrInvalidFingerprint
	}
	return nil
}

// ResourceAAD builds an unambiguous AAD value from stable Eruun and
// Kubernetes identities. The Secret key name is included so encrypted values
// cannot be swapped within the same object.
func ResourceAAD(appID, namespace, apiVersion, kind, name, secretKey string) []byte {
	parts := []string{appID, namespace, apiVersion, kind, name, secretKey}
	var builder strings.Builder
	builder.WriteString("eruun-import-secret")
	for _, part := range parts {
		builder.WriteByte(0)
		builder.WriteString(strings.TrimSpace(part))
	}
	return []byte(builder.String())
}

func newGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("create AES-256 cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create AES-GCM cipher: %w", err)
	}
	return aead, nil
}
