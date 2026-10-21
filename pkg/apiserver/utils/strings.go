package utils

import "strings"

func Int64Ptr(i int64) *int64 {
	return &i
}

func StringPtr(val string) *string {
	if val == "" {
		return nil
	}
	v := val
	return &v
}

func CopyStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

var trimLowerReplacer = strings.NewReplacer(
	" ", "",
	"\n", "",
	"\r", "",
	"\t", "",
)

// NormalizeLowerStrip removes whitespace runes we do not want and returns a lowercase copy.
func NormalizeLowerStrip(val string) string {
	if val == "" {
		return ""
	}
	return trimLowerReplacer.Replace(strings.ToLower(val))
}

// UniqueTrimmed trims values and drops empty and repeated ones, keeping order.
func UniqueTrimmed(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}
