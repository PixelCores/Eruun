package utils

import (
	"fmt"
	"strings"
)

// NormalizeCronSchedule normalizes 5 or 6-field cron expressions.
// For 6-field expressions, it only accepts a leading zero second field and strips it for CronJob usage.
func NormalizeCronSchedule(schedule string) (string, error) {
	trimmed := strings.TrimSpace(schedule)
	if trimmed == "" {
		return "", fmt.Errorf("schedule is empty")
	}
	fields := strings.Fields(trimmed)
	switch len(fields) {
	case 5:
		return strings.Join(fields, " "), nil
	case 6:
		if fields[0] != "0" && fields[0] != "00" {
			return "", fmt.Errorf("cron seconds field must be 0")
		}
		return strings.Join(fields[1:], " "), nil
	default:
		return "", fmt.Errorf("cron expression must have 5 or 6 fields")
	}
}
