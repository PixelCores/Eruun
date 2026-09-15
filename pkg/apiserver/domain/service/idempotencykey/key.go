package idempotencykey

import (
	"fmt"
	"strings"
)

const MaxLength = 128

// Validate applies the common HTTP and gRPC application execution contract.
func Validate(value string) error {
	if value == "" || value != strings.TrimSpace(value) || len(value) > MaxLength {
		return fmt.Errorf("Idempotency-Key must be one non-empty value of at most 128 characters without surrounding whitespace")
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || strings.ContainsRune("._:-", char) {
			continue
		}
		return fmt.Errorf("Idempotency-Key may contain only letters, numbers, dot, underscore, colon, and hyphen")
	}
	return nil
}
