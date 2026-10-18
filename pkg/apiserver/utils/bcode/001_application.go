package bcode

// ErrApplicationConfig application config does not comply with OAM specification
var ErrApplicationConfig = NewBcode(400, 10000, "application config does not comply with OAM specification")

var ErrComponentNotImageSet = NewBcode(400, 10001, "the image of the component has not been set..")

var ErrComponentBuild = NewBcode(400, 10002, "application component build error")

// ErrApplicationExist application is existed
var ErrApplicationExist = NewBcode(400, 10003, "application name is exist")

// ErrInvalidProperties properties(trait or component or others) is invalid
var ErrInvalidProperties = NewBcode(400, 10004, "properties is invalid")

// ErrApplicationNotExist application is not found
var ErrApplicationNotExist = NewBcode(200, 10005, "application not found")

// ErrTemplateNotEnabled template app is not marked as usable
var ErrTemplateNotEnabled = NewBcode(400, 10007, "template application is not enabled")

// ErrTemplateComponentMissing template app has no components to clone
var ErrTemplateComponentMissing = NewBcode(400, 10008, "template application has no components")

// ErrTemplateIDMissing template id is required when using tmp
var ErrTemplateIDMissing = NewBcode(400, 10009, "template id is required")

// ErrTemplateTargetNotFound template target not found or mismatch
var ErrTemplateTargetNotFound = NewBcode(400, 10010, "template target component not found or type mismatch")

// ErrVersionUpdateFailed version update operation failed
var ErrVersionUpdateFailed = NewBcode(500, 10011, "version update failed")

// ErrInvalidUpdateStrategy invalid update strategy
var ErrInvalidUpdateStrategy = NewBcode(400, 10012, "invalid update strategy")

// ErrComponentNotFound component not found in application
var ErrComponentNotFound = NewBcode(404, 10013, "component not found in application")

// ErrComponentAlreadyExists component already exists when trying to add
var ErrComponentAlreadyExists = NewBcode(400, 10015, "component already exists")

// ErrInvalidComponentAction invalid component action type
var ErrInvalidComponentAction = NewBcode(400, 10016, "invalid component action, must be update, add, remove, or restart")

// Validation-related error codes (10020-10039)

// ErrDuplicateComponentName duplicate component name in application
var ErrDuplicateComponentName = NewBcode(400, 10028, "duplicate component name in application")

// ErrComponentLogUnavailable component logs are unavailable
var ErrComponentLogUnavailable = NewBcode(404, 10029, "component logs unavailable")

// ErrComponentPendingScheduling component pod is pending scheduling
var ErrComponentPendingScheduling = NewBcode(409, 10030, "The component is pending scheduling.")

// ErrApplicationOperationLocked application operation is locked by another request
var ErrApplicationOperationLocked = NewBcode(409, 10031, "application operation is locked, please retry")

// ErrDistributedLockUnavailable distributed lock backend is unavailable
var ErrDistributedLockUnavailable = NewBcode(503, 10032, "distributed lock backend is unavailable")

// ErrVersionUpdateTaskNotCancellable delayed version update task cannot be cancelled
var ErrVersionUpdateTaskNotCancellable = NewBcode(409, 10033, "version update task is not cancellable")

// ErrComponentLogContainerInvalid requested component log container is invalid
var ErrComponentLogContainerInvalid = NewBcode(400, 10034, "invalid container name for component logs")

// ErrComponentContainerInvalid requested component operation container is invalid
var ErrComponentContainerInvalid = NewBcode(400, 10035, "invalid container name for component operation")

// ErrComponentPodUnavailable component pod is unavailable for operation
var ErrComponentPodUnavailable = NewBcode(404, 10036, "component pod unavailable")

// ErrComponentFilePathInvalid requested component export path is invalid
var ErrComponentFilePathInvalid = NewBcode(400, 10037, "invalid file path for component export")

// ErrComponentShellScriptInvalid requested component shell script is invalid
var ErrComponentShellScriptInvalid = NewBcode(400, 10038, "invalid shell script for component exec")

// ErrApplicationNoComponents application has no components and cannot be deployed
var ErrApplicationNoComponents = NewBcode(400, 10039, "application has no components, deployment is not allowed")

// ErrVersionUpdateConflict indicates that application configuration changed
// after version-update preflight and the request must be validated again.
var ErrVersionUpdateConflict = NewBcode(409, 10043, "version update preflight is stale; read the application and retry")
