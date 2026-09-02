package config

import (
	"fmt"
	"strings"
)

const defaultMessagingChannelPrefix = "eruun"

// MessagingChannelPrefix normalizes the configured queue/topic prefix.
func MessagingChannelPrefix(prefix string) string {
	normalized := strings.TrimSpace(prefix)
	if normalized == "" {
		return defaultMessagingChannelPrefix
	}
	return normalized
}

func DispatchTopic(prefix string) string {
	return fmt.Sprintf("%s.workflow.dispatch", MessagingChannelPrefix(prefix))
}

func DelayTopic(prefix string) string {
	return fmt.Sprintf("%s.job.delay", MessagingChannelPrefix(prefix))
}

func ResultTopic(prefix string) string {
	return fmt.Sprintf("%s.job.result", MessagingChannelPrefix(prefix))
}

func KafkaTopics(prefix string) []string {
	return []string{
		DispatchTopic(prefix),
		DelayTopic(prefix),
		ResultTopic(prefix),
	}
}
