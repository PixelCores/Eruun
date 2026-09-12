package profiling

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/pprof"
	"runtime"

	"k8s.io/klog/v2"
)

// NewProfilingHandler create a profiling handler
func NewProfilingHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.HandleFunc("/mem/stat", func(writer http.ResponseWriter, request *http.Request) {
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		bs, _ := json.Marshal(ms)
		_, _ = writer.Write(bs)
	})
	mux.HandleFunc("/gc", func(writer http.ResponseWriter, request *http.Request) {
		runtime.GC()
	})
	return mux
}

// StartProfilingServer serves profiling requests until ctx is canceled.
// Startup/runtime errors are reported to errChan, or logged when it is nil.
func StartProfilingServer(ctx context.Context, errChan chan error) {
	if Addr == "" || ctx.Err() != nil {
		return
	}
	klog.InfoS("starting profiling server", "address", Addr)
	server := &http.Server{Addr: Addr, Handler: NewProfilingHandler()}
	shutdownDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(shutdownDone)
		if err := server.Close(); err != nil {
			klog.ErrorS(err, "close profiling server failed")
		}
	})
	defer func() {
		if !stop() {
			<-shutdownDone
		}
	}()
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		err = fmt.Errorf("serve profiling: %w", err)
		if errChan == nil {
			klog.ErrorS(err, "profiling server exited")
			return
		}
		select {
		case errChan <- err:
		case <-ctx.Done():
		}
	}
}
