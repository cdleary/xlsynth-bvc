# Persistent Docker Runner Design

## Status

This document describes the implemented persistent-runner architecture in `xlsynth-bvc`.

The original goal was to replace repeated one-shot Docker execution with long-lived containers that
serve requests from a host-managed queue. That goal is now implemented for Docker-backed action
execution.

What changed during implementation:

- the runtime cutover happened at the existing `run_docker_script()` seam, which has now been
  renamed to `execute_persistent_runner_script()`
- the host/worker protocol stayed intentionally thin: `script + env + mounts`
- the fully typed `request_kind` and per-member batch protocol from the original design was not
  required to get the operational win

## Implemented Runtime Model

### Host-Side Ownership

The host remains the source of truth for:

- queue claiming and retirement
- dependency readiness checks
- lease management
- artifact finalization
- failure and cancellation semantics

Nothing inside the container mutates `queue/pending`, `queue/running`, `queue/done`,
`queue/failed`, or `queue/canceled`.

### Container Role

Each persistent container acts as a single-runner request loop:

- watch its runner-specific inbox
- claim one request file at a time
- materialize requested mounts inside the container
- execute the provided shell script
- write `stdout.log`, `stderr.log`, `result.json`, and heartbeat updates

The worker does not know anything about the global queue or provenance model.

## Cutover Boundary

All Docker-backed action execution now flows through
`execute_persistent_runner_script()` in `src/service/runtime_docker.rs`.

That includes:

- xlsynth-driver-backed actions in `src/executor.rs`
- Yosys/ABC-backed actions in `src/executor.rs`
- structural-hash execution in `src/service/core.rs`
- driver capability checks, which now read cached runner metadata instead of issuing live probe
  requests

The only remaining direct Docker CLI usage is runner lifecycle management inside
`src/service/runtime_docker.rs`:

- image build / inspect
- container start / inspect / cleanup

There are no remaining production `Command::new("docker")` call sites outside that runtime layer.

## Actual On-Disk Layout

The implemented runner state lives under:

```text
bvc-artifacts/
  persistent-runners/
    runners/
      <runner_key>/
        live.json
        heartbeat.json
        capabilities.json
        inbox/
        processing/
        results/
        archive/
        jobs/
          <request_id>/
            request.json
            stdout.log
            stderr.log
```

This is simpler than the original draft:

- no `dispatchers/`
- no `desired.json`
- no top-level `schema.json`

Those pieces were not needed for the current single-host dispatcher model.

## Actual Request Protocol

### Request Shape

The implemented request schema is intentionally generic:

- `request_id`
- `runner_key`
- `runner_instance_id`
- `container_name`
- `image`
- `job_root`
- `heartbeat_path`
- `timeout_secs`
- `env`
- `script`
- `mounts`

This means the host still constructs the concrete shell script for each action, and the worker
simply executes it.

### Result Shape

The worker writes a single result record containing:

- request identity
- runner identity
- timestamps
- command argv
- exit code
- timeout state
- stdout/stderr tails
- final status

The host still validates artifacts and writes provenance using the normal finalize path.

## Runner Keys

The implemented runner key is:

`sha256(docker_image + store_root)[0:24]`

This is simpler than the richer semantic key from the original plan. In practice it works because:

- the Docker image already encodes the executable environment
- the store root scopes the mounted artifact/cache world
- runners are intentionally local to one store

If runner sharing across stores becomes important later, the key can be widened then.

## Capabilities

Boot-time capabilities are now cached at runner startup in `capabilities.json`.

Current driver capability metadata includes:

- runner family
- known driver subcommand support
- known driver help-token support

Today this is used for:

- `aig2ir`
- `ir-equiv`
- `ir2gates`
- `ir2gates --prepared-ir-out`

The host reads this cached metadata instead of issuing live probe requests as fake actions.

## Why The Design Ended Up Simpler

The original plan called for a more explicit dispatcher abstraction and a typed request protocol.

That turned out to be unnecessary for the first real win:

- the existing executor already had a single Docker execution seam
- replacing that seam delivered persistent container reuse immediately
- keeping the request shape generic avoided duplicating action logic on the worker side

This keeps the code smaller and preserves the current host-side action semantics.

## What We Explicitly Did Not Implement

These parts of the original design are still intentionally absent:

- typed `request_kind`
- per-request `members[]`
- a distinct `PersistentRunnerManager` type
- worker-owned action semantics
- dispatcher-local state files outside the queue itself

Those remain optional future work, not required infrastructure.

## Operational Validation

The implementation is validated in three layers:

- compile and unit/integration coverage in Rust tests
- an ignored Docker integration test that drains representative Docker-backed actions across
  multiple runtimes
- real bounded corpus runs against the live `xlsynth-driver` and Yosys images

The steady-state property we care about is confirmed by warm reruns:

- the same runner containers stay alive across queue drains
- subsequent work avoids container/image cold start
- action goodput improves materially after the first drain

## Remaining Follow-Ups

There are still a few worthwhile follow-ups, but they are incremental:

- broaden cached capability metadata if new driver feature probes appear
- add optional stale-runner retirement if idle containers become a resource problem
- improve request/result typing only if debugging or multi-member reporting becomes painful

## Summary

The current implementation should be understood as:

- fully migrated Docker-backed execution at runtime
- intentionally thin host/worker protocol
- host-owned queue and provenance semantics
- persistent containers used as reusable execution engines

That is the architecture the codebase now assumes.
