package workflow

import (
	"strings"
	"unicode/utf8"
)

const (
	terminalCallbackPendingPrefix    = "terminal callback pending: "
	TerminalCallbackReconciledReason = "terminal callback reconciled"
)

// TerminalCallbackPendingReason stores a bounded, durable callback intent on
// the workflow row before cancellation can outlive its current process.
func TerminalCallbackPendingReason(reason string) string {
	normalized := strings.TrimSpace(reason)
	limit := 255 - len(terminalCallbackPendingPrefix)
	for len(normalized) > limit {
		_, size := utf8.DecodeLastRuneInString(normalized)
		normalized = normalized[:len(normalized)-size]
	}
	return terminalCallbackPendingPrefix + normalized
}

func IsTerminalCallbackPending(reason string) bool {
	return strings.HasPrefix(reason, terminalCallbackPendingPrefix)
}

func TerminalCallbackReason(reason string) string {
	return strings.TrimSpace(strings.TrimPrefix(reason, terminalCallbackPendingPrefix))
}
