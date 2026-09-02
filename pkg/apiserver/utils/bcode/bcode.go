package bcode

import (
	"errors"
	"fmt"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

// Bcode business error code
type Bcode struct {
	HTTPCode     int32  `json:"-"`
	BusinessCode int32  `json:"BusinessCode"`
	Message      string `json:"Message"`
}

type safeClientMessageError struct {
	err     error
	message string
}

func (e *safeClientMessageError) Error() string {
	return e.err.Error()
}

func (e *safeClientMessageError) Unwrap() error {
	return e.err
}

func (e *safeClientMessageError) safeClientMessage() string {
	return e.message
}

// WithSafeClientMessage marks message as safe to return to API clients while
// preserving err for errors.Is/errors.As and internal diagnostics.
func WithSafeClientMessage(err error, message string) error {
	if err == nil {
		return nil
	}
	message = strings.TrimSpace(message)
	if message == "" {
		return err
	}
	return &safeClientMessageError{err: err, message: message}
}

// SafeClientMessage returns an explicitly approved client-facing error message.
func SafeClientMessage(err error) string {
	var safeMessage interface {
		safeClientMessage() string
	}
	if !errors.As(err, &safeMessage) {
		return ""
	}
	return strings.TrimSpace(safeMessage.safeClientMessage())
}

func (b Bcode) Error() string {
	return fmt.Sprintf("HTTPCode:%d BusinessCode:%d Message:%s", b.HTTPCode, b.BusinessCode, b.Message)
}

var (
	bcodeMap     map[int32]*Bcode
	bcodeInitErr error
)

// NewBcode new business code
func NewBcode(httpCode, businessCode int32, message string) *Bcode {
	if bcodeMap == nil {
		bcodeMap = make(map[int32]*Bcode)
	}
	if existing, exit := bcodeMap[businessCode]; exit {
		err := fmt.Errorf("bcode business code already exists: %d", businessCode)
		bcodeInitErr = errors.Join(bcodeInitErr, err)
		klog.ErrorS(err, "duplicate bcode registration", "businessCode", businessCode)
		return existing
	}
	bcode := &Bcode{HTTPCode: httpCode, BusinessCode: businessCode, Message: message}
	bcodeMap[businessCode] = bcode
	return bcode
}

// Init returns initialization errors captured during bcode registration.
func Init() error {
	return bcodeInitErr
}

// ReturnError Unified handling of all types of errors, generating a standard return structure.
func ReturnError(c *gin.Context, err error) {
	if err == nil {
		return
	}

	var bc *Bcode
	if errors.As(err, &bc) {
		ReturnErrorWithMessage(c, bc, SafeClientMessage(err))
		return
	}

	if errors.Is(err, datastore.ErrRecordNotExist) {
		ReturnErrorWithMessage(c, ErrNotFound, "")
		return
	}

	var validErr validator.ValidationErrors
	if errors.As(err, &validErr) {
		ReturnErrorWithMessage(c, ErrApplicationConfig, "")
		return
	}

	klog.ErrorS(errors.New("generic server error response"), "returning generic server error response", "errorType", fmt.Sprintf("%T", err))
	ReturnErrorWithMessage(c, ErrServer, "")
}

// ErrServer an unexpected mistake.
var ErrServer = NewBcode(500, 500, "The service has lapsed.")

// ErrForbidden check user perms failure
var ErrForbidden = NewBcode(403, 403, "403 Forbidden")

// ErrUnauthorized check user auth failure
var ErrUnauthorized = NewBcode(401, 401, "401 Unauthorized")

// ErrNotFound the request resource is not found
var ErrNotFound = NewBcode(404, 404, "404 Not Found")

// ErrUpstreamNotFound the proxy upstream is not found
var ErrUpstreamNotFound = NewBcode(502, 502, "Upstream not found")

// ErrNotImplemented indicates the API is not implemented.
var ErrNotImplemented = NewBcode(501, 501, "Not implemented")

// ErrServiceUnavailable indicates the service is unavailable.
var ErrServiceUnavailable = NewBcode(503, 503, "Service unavailable")
