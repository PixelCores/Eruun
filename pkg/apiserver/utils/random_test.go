package utils

import (
	"regexp"
	"testing"
)

// TestRandStringByNumLowercase tests the RandStringByNumLowercase function.
func TestRandStringByNumLowercase(t *testing.T) {
	for i := 0; i < 10; i++ {
		s := RandStringByNumLowercase(i)
		if len(s) != i {
			t.Errorf("RandStringByNumLowercase(%d) length = %d; want %d", i, len(s), i)
		}
		// Check if the string contains only allowed characters (lowercase alphanumeric)
		if !regexp.MustCompile(`^[a-z0-9]*$`).MatchString(s) {
			t.Errorf("RandStringByNumLowercase(%d) = %q, contains invalid characters", i, s)
		}
	}
}

// TestToRFC1123Name tests the ToRFC1123Name function.
func TestToRFC1123Name(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{"empty string", "", "port"},
		{"simple string", "hello", "hello"},
		{"uppercase", "World", "world"},
		{"with spaces", "hello world", "hello-world"},
		{"with special chars", "a!b@c#d$e%f^g&h*i(j)k_l+m=", "a-b-c-d-e-f-g-h-i-j-k-l-m"},
		{"leading/trailing hyphens", "-a-b-", "a-b"},
		{"consecutive hyphens", "a--b", "a-b"},
		{"long string", "this-is-a-very-long-string-that-should-be-handled-correctly", "this-is-a-very-long-string-that-should-be-handled-correctly"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := ToRFC1123Name(tc.input)
			if result != tc.expected {
				t.Errorf("ToRFC1123Name(%q) = %q; want %q", tc.input, result, tc.expected)
			}
		})
	}
}
