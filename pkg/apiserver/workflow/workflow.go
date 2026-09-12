package workflow

import (
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
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

// LintWorkflow 验证工作流是否符合标准
func LintWorkflow(workflow *model.Workflow) error {
	workflow.Name = strings.ToLower(workflow.Name)
	if workflow.ProjectID == "" {
		err := fmt.Errorf("project should not be empty")
		klog.Errorf("%v", err)
		return err
	}
	// 判断工作流的名称是否符合正则表达式的规范
	match, err := regexp.MatchString(config.WorkflowRegx, workflow.Name)
	if err != nil {
		klog.Errorf("reg compile failed: %v", err)
		return err
	}
	if !match {
		errMsg := "workflow identifier supports uppercase and lowercase letters, digits, and hyphens"
		klog.Error(errMsg)
		return fmt.Errorf("%s", errMsg)
	}
	return nil
}
