package utils

import "testing"

func TestNormalizeCronSchedule(t *testing.T) {
	testCases := []struct {
		name      string
		input     string
		want      string
		expectErr bool
	}{
		{"empty", "", "", true},
		{"five fields allowed", "0 0 * * *", "0 0 * * *", false},
		{"six fields seconds zero", "0 0 * * * *", "0 * * * *", false},
		{"six fields seconds not zero", "1 0 * * * *", "", true},
		{"four fields not allowed", "0 * * *", "", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NormalizeCronSchedule(tc.input)
			if tc.expectErr {
				if err == nil {
					t.Fatalf("expected error for %q, got none", tc.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for %q: %v", tc.input, err)
			}
			if got != tc.want {
				t.Fatalf("expected %q, got %q", tc.want, got)
			}
		})
	}
}
