package app

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/cmd/server/app/options"
	server "github.com/PixelCores/Eruun/pkg/apiserver"
	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/observability"
	api "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/profiling"
	"github.com/PixelCores/Eruun/version"
)

var errServerShutdownTimedOut = errors.New("server shutdown timed out")

var migrateSchema = server.MigrateSchema

// NewAPIServerCommand creates a *cobra.Command object with default parameters
func NewAPIServerCommand() *cobra.Command {
	s := options.NewServerRunOptions()

	// Initialize log flags
	klog.InitFlags(nil)

	cmd := &cobra.Command{
		Use:  "eruun-server",
		Long: `The Eruun API service, which provides application deployment and Istio operations`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := config.ApplyEnvOverrides(cmd.Flags(), config.EnvPrefix); err != nil {
				return fmt.Errorf("apply env overrides: %w", err)
			}
			if err := s.Validate(); err != nil {
				return err
			}
			return Run(s)
		},
		SilenceUsage: true,
	}

	fs := cmd.Flags()
	namedFlagSets := s.Flags()
	// Add log flags to the command's flag set
	namedFlagSets.FlagSet("klog").AddGoFlagSet(flag.CommandLine)

	for _, set := range namedFlagSets.FlagSets {
		fs.AddFlagSet(set)
	}
	return cmd
}

// Run runs the specified APIServer. This should never exit.
func Run(s *options.ServerRunOptions) error {
	if s != nil && s.GenericServerRunOptions != nil && s.GenericServerRunOptions.MigrateSchemaOnly() {
		migrationCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		return migrateSchema(migrationCtx, *s.GenericServerRunOptions)
	}
	// The server is not terminal, there is no color default.
	// Force set to false, this is useful for the dry-run API.
	color.NoColor = false
	if err := api.InitRuntime(); err != nil {
		return fmt.Errorf("init api runtime: %w", err)
	}

	errChan := make(chan error)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the analysis service
	go profiling.StartProfilingServer(errChan)

	// Start log cleanup service
	logDir := flag.Lookup("log_dir").Value.String()

	// Ensure the log directory exists before starting services that log to files.
	if err := ensureLogDir(logDir); err != nil {
		return err
	}
	go utils.StartLogCleanup(logDir, 7*24*time.Hour)

	runErrChan := make(chan error, 1)
	go func() {
		err := run(ctx, s, errChan)
		if err != nil {
			err = fmt.Errorf("failed to run apiserver: %w", err)
		}
		runErrChan <- err
	}()
	term := make(chan os.Signal, 1)
	signal.Notify(term, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(term)

	err := waitForServerRun(term, errChan, runErrChan, cancel, serverShutdownWaitTimeout(s))
	if err != nil {
		if errors.Is(err, errServerShutdownTimedOut) {
			klog.ErrorS(err, "server shutdown exceeded its drain budget; exiting")
			return nil
		}
		klog.Errorf("Received an error: %s, exiting gracefully...", err.Error())
		return err
	}
	klog.Infof("See you next time!")
	klog.Flush()
	return nil
}

func waitForServerRun(
	term <-chan os.Signal,
	runtimeErrors <-chan error,
	runDone <-chan error,
	cancel context.CancelFunc,
	shutdownTimeout time.Duration,
) error {
	var firstErr error
	select {
	case sig := <-term:
		klog.InfoS("received termination signal; waiting for graceful shutdown", "signal", sig)
		cancel()
	case err := <-runtimeErrors:
		firstErr = err
		cancel()
	case err := <-runDone:
		return err
	}

	timer := time.NewTimer(shutdownTimeout)
	defer timer.Stop()
	for {
		select {
		case err, ok := <-runtimeErrors:
			if !ok {
				runtimeErrors = nil
				continue
			}
			if firstErr == nil {
				firstErr = err
			}
		case err, ok := <-runDone:
			if !ok {
				err = nil
			}
			if firstErr != nil {
				return firstErr
			}
			return err
		case <-timer.C:
			if firstErr != nil {
				return firstErr
			}
			return errServerShutdownTimedOut
		}
	}
}

func serverShutdownWaitTimeout(s *options.ServerRunOptions) time.Duration {
	drainTimeout := config.DefaultWorkerDrainTimeout
	if s != nil &&
		s.GenericServerRunOptions != nil &&
		s.GenericServerRunOptions.Workflow.WorkerDrainTimeout > 0 {
		drainTimeout = s.GenericServerRunOptions.Workflow.WorkerDrainTimeout
	}
	if drainTimeout < config.DefaultHTTPShutdownTimeout {
		drainTimeout = config.DefaultHTTPShutdownTimeout
	}
	return drainTimeout + 5*time.Second
}

func ensureLogDir(logDir string) error {
	if logDir == "" {
		return nil
	}
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return fmt.Errorf("create log directory %s: %w", logDir, err)
	}
	return nil
}

func run(ctx context.Context, s *options.ServerRunOptions, errChan chan error) error {
	klog.Infof("Eruun information: version: %v", version.EruunVersion)

	// Simplified auto-tracing: do not rely on replica count
	explicit := s.GenericServerRunOptions.EnableTracing
	// Treat supported messaging backends as external/distributed queues.
	hasExternalQueue := func(typ string) bool {
		t := strings.ToLower(strings.TrimSpace(typ))
		return t == config.REDIS || t == config.KAFKA
	}
	auto := s.GenericServerRunOptions.AutoTracing &&
		(s.GenericServerRunOptions.JaegerEndpoint != "" || hasExternalQueue(s.GenericServerRunOptions.Messaging.Type))
	effective := explicit || auto
	// Propagate effective value so server middleware aligns with provider init
	s.GenericServerRunOptions.EnableTracing = effective
	if auto && !explicit {
		klog.InfoS("Auto tracing enabled", "jaegerEndpoint", s.GenericServerRunOptions.JaegerEndpoint, "msgType", s.GenericServerRunOptions.Messaging.Type)
	}

	if effective {
		klog.InfoS("Distributed tracing enabled", "jaegerEndpoint", s.GenericServerRunOptions.JaegerEndpoint)
		shutdown, err := observability.InitTracerProvider("eruun-server", s.GenericServerRunOptions.JaegerEndpoint)
		if err != nil {
			return fmt.Errorf("failed to init tracer provider: %w", err)
		}
		defer func() {
			if err := shutdown(context.Background()); err != nil {
				klog.ErrorS(err, "Failed to shutdown tracer provider")
			}
		}()
	}

	apiServer := server.New(*s.GenericServerRunOptions)
	return apiServer.Run(ctx, errChan)
}
