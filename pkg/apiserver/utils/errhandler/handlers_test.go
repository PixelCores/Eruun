package errhandler

import (
	"errors"
	"testing"
)

func TestNotifyOrPanicSendsToChannel(t *testing.T) {
	errChan := make(chan error, 1)
	expected := errors.New("boom")

	NotifyOrPanic(errChan)(expected)

	select {
	case got := <-errChan:
		if !errors.Is(got, expected) {
			t.Fatalf("expected %v, got %v", expected, got)
		}
	default:
		t.Fatal("expected error to be sent to channel")
	}
}

func TestNotifyOrPanicPanicsWhenChannelNil(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when channel is nil")
		}
	}()
	NotifyOrPanic(nil)(errors.New("boom"))
}

func TestNotifyWithFallbackUsesFallbackWhenChannelNil(t *testing.T) {
	called := false
	var gotErr error
	expected := errors.New("boom")

	NotifyWithFallback(nil, func(err error) {
		called = true
		gotErr = err
	})(expected)

	if !called {
		t.Fatal("expected fallback to be called")
	}
	if !errors.Is(gotErr, expected) {
		t.Fatalf("expected %v, got %v", expected, gotErr)
	}
}

func TestNotifyIgnoresWhenNilChannelPolicyIgnore(t *testing.T) {
	Notify(nil, NotifyOptions{NilChannelPolicy: NilChannelIgnore})(errors.New("boom"))
}
