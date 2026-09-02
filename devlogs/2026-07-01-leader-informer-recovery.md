# Leader Informer Recovery

Date: 2026-07-01

## Context

Runtime logs showed workflows entering `cancelled` after Kubernetes API lease renewal timed out. The leader context was cancelled, which stopped the Informer Manager and workflow worker path while the HTTP server could continue serving requests when `exit-on-lost-leader=false`.

## Decision

- Keep the effective default leader lease duration near the previous failover window at `15s`; longer leases such as `5m` remain available only as explicit `--duration` / `ERUUN_DURATION` opt-in.
- Make leader election use the configured duration instead of a hard-coded short lease.
- Reject leader lease durations below `4s` during configuration validation so invalid short values fail before client-go leader election starts.
- Disable client-go's built-in `ReleaseOnCancel` and run Eruun's own short-timeout best-effort release after leader-scoped work has stopped.
- In `exit-on-lost-leader=true` mode, release the lease before reporting the fatal lost-leader error so the main process cannot exit before the best-effort release runs.
- When `exit-on-lost-leader=false`, retry leader election in the same process only after the previous leader-scoped workflow runners and watchers have quiesced.
- Keep existing dispatcher `Start(ctx)` methods asynchronous for compatibility, and add blocking `Run(ctx)` methods so `Workflow.Start` can wait for schedule, main dispatch, delay, result, and result-outbox dispatchers before releasing leadership.
- Make the Informer Manager restartable by rebuilding its informer factory and stop channel on each start while preserving the shared `ResourceReadyWaiter`.
- Clear informer-derived Pod ready and Pod restart snapshots whenever a new Informer runtime is built, while preserving waiter callbacks, pending waiters, and async executors.

## Consequences

- Single-replica deployments can recover workflow execution without requiring a pod restart after transient leader lease loss.
- Existing cancelled workflow tasks remain terminal; recovery only restores the execution path for subsequent or requeued work.
- The waiter remains process-scoped and is not closed during normal informer stop, because job controllers keep using that same injected waiter.
- Rebuilt informers no longer let vanished Pods satisfy later component readiness checks from stale in-memory snapshots; current Pods are reloaded through the new factory's initial list.
- Multi-replica partitions no longer depend on a long release attempt to stop leader-scoped work. Release failure or timeout only delays voluntary lease cleanup; it does not keep informer or workflow workers alive.
- Fatal leader-loss exits may wait up to the short release timeout, but they do not restore client-go's long `ReleaseOnCancel` path.
- Pod kill or node disappearance failover stays near the previous seconds-level default because the runtime `LeaseDuration` defaults to `15s`; operators who choose a longer lease accept the corresponding failover wait if best-effort release cannot run.
- Re-election in `exit-on-lost-leader=false` no longer reuses the same `Workflow` object while old task controllers or dispatcher loops are still unwinding, so `InitQueue` does not requeue under a process-restart assumption until the prior in-process controllers and queue dispatchers have stopped.
