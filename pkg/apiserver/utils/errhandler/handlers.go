package errhandler

// ErrorHandler handler for error
type ErrorHandler func(error)

// NilChannelPolicy controls behavior when errChan is nil.
type NilChannelPolicy int

const (
	// NilChannelPanic keeps legacy behavior and panics on errors.
	NilChannelPanic NilChannelPolicy = iota
	// NilChannelIgnore drops errors when errChan is nil.
	NilChannelIgnore
)

// NotifyOptions controls error notification behavior.
type NotifyOptions struct {
	NilChannelPolicy NilChannelPolicy
	Fallback         func(error)
}

// Notify sends errors to errChan if possible, otherwise applies fallback policy.
func Notify(errChan chan error, opts NotifyOptions) ErrorHandler {
	return func(err error) {
		if err == nil {
			return
		}
		if errChan != nil {
			errChan <- err
			return
		}
		if opts.Fallback != nil {
			opts.Fallback(err)
		}
		if opts.NilChannelPolicy == NilChannelIgnore {
			return
		}
		panic(err)
	}
}

// NotifyOrPanic if given errChan is nil, panic on error, otherwise send error
// to errChan
func NotifyOrPanic(errChan chan error) ErrorHandler {
	return Notify(errChan, NotifyOptions{NilChannelPolicy: NilChannelPanic})
}

// NotifyWithFallback sends error to errChan, and uses fallback when errChan is nil.
func NotifyWithFallback(errChan chan error, fallback func(error)) ErrorHandler {
	return Notify(errChan, NotifyOptions{
		NilChannelPolicy: NilChannelIgnore,
		Fallback:         fallback,
	})
}
