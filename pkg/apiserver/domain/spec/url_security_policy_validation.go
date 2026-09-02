package spec

import (
	"fmt"
	"net"
	"regexp"
	"strings"
)

var dnsLabelRegexp = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// NormalizeURLSecurityPolicy trims and canonicalizes outbound URL security policy fields.
func NormalizeURLSecurityPolicy(setting URLSecurityPolicySpec) URLSecurityPolicySpec {
	setting.AllowedHostPatterns = normalizeHostPatterns(setting.AllowedHostPatterns)
	setting.AllowedCIDRs = normalizeCIDRs(setting.AllowedCIDRs)
	return setting
}

// ValidateURLSecurityPolicy validates outbound URL security policy fields.
func ValidateURLSecurityPolicy(setting URLSecurityPolicySpec) error {
	setting = NormalizeURLSecurityPolicy(setting)

	for i, pattern := range setting.AllowedHostPatterns {
		if err := validateHostPattern(pattern); err != nil {
			return fmt.Errorf("allowedHostPatterns[%d]: %w", i, err)
		}
	}
	for i, cidr := range setting.AllowedCIDRs {
		if _, _, err := net.ParseCIDR(cidr); err != nil {
			return fmt.Errorf("allowedCIDRs[%d]: invalid cidr %q", i, cidr)
		}
	}
	return nil
}

func normalizeHostPatterns(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, item := range in {
		value := strings.ToLower(strings.TrimSpace(item))
		value = strings.TrimSuffix(value, ".")
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

func normalizeCIDRs(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, item := range in {
		value := strings.TrimSpace(item)
		if value == "" {
			continue
		}
		_, ipNet, err := net.ParseCIDR(value)
		if err == nil && ipNet != nil {
			value = ipNet.String()
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func validateHostPattern(pattern string) error {
	if pattern == "" {
		return fmt.Errorf("empty host pattern")
	}
	if strings.Contains(pattern, "://") {
		return fmt.Errorf("must not include url scheme")
	}
	if strings.Contains(pattern, "/") {
		return fmt.Errorf("must not include path")
	}
	if strings.Contains(pattern, ":") && net.ParseIP(pattern) == nil {
		return fmt.Errorf("must not include port")
	}

	if strings.HasPrefix(pattern, "*.") {
		base := strings.TrimPrefix(pattern, "*.")
		if strings.Contains(base, "*") {
			return fmt.Errorf("wildcard is only allowed as prefix '*.'")
		}
		if err := validateHostname(base); err != nil {
			return fmt.Errorf("invalid wildcard host: %w", err)
		}
		return nil
	}

	if strings.Contains(pattern, "*") {
		return fmt.Errorf("wildcard is only allowed as prefix '*.'")
	}
	if ip := net.ParseIP(pattern); ip != nil {
		return nil
	}
	if err := validateHostname(pattern); err != nil {
		return fmt.Errorf("invalid host: %w", err)
	}
	return nil
}

func validateHostname(host string) error {
	if host == "" {
		return fmt.Errorf("empty host")
	}
	if len(host) > 253 {
		return fmt.Errorf("host too long")
	}
	labels := strings.Split(host, ".")
	for _, label := range labels {
		if label == "" {
			return fmt.Errorf("host contains empty label")
		}
		if len(label) > 63 {
			return fmt.Errorf("label %q too long", label)
		}
		if !dnsLabelRegexp.MatchString(label) {
			return fmt.Errorf("label %q has invalid chars", label)
		}
	}
	return nil
}
