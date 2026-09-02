package bcode

var ErrWorkflowConfig = NewBcode(400, 20000, "workflow config does not comply with OAM specification")

var ErrWorkflowExist = NewBcode(400, 20001, "workflow name is exist")

var ErrCreateWorkflow = NewBcode(400, 20002, "workflow create failure")

var ErrCreateComponents = NewBcode(400, 20003, "workflow components create failure")

var ErrExecWorkflow = NewBcode(400, 20004, "workflow exec failure")

var ErrWorkflowNotExist = NewBcode(404, 20005, "workflow not found")

var ErrWorkflowTaskNotExist = NewBcode(404, 20006, "workflow task not found")

var ErrWorkflowTaskRunning = NewBcode(409, 20007, "workflow task is running")

var ErrWorkflowTaskCancelling = NewBcode(409, 20008, "workflow task is cancelling")

var ErrWorkflowTaskNotAwaitingApproval = NewBcode(409, 20009, "workflow task is not awaiting approval")

var ErrWorkflowApprovalActionInvalid = NewBcode(400, 20010, "workflow approval action is invalid")

var ErrWorkflowEmpty = NewBcode(400, 20011, "workflow is empty, please edit workflow before executing")

var ErrWorkflowCancelSignalUnavailable = NewBcode(503, 20012, "workflow cancel signal backend is unavailable")

var ErrWorkflowTaskNotCancellable = NewBcode(409, 20013, "workflow task cannot be cancelled in current status")

var ErrWorkflowTaskCancelConflict = NewBcode(409, 20014, "workflow task state changed while cancelling; retry")
