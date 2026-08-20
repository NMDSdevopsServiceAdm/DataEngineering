---
name: oom-diagnostics
description: Use before/when investigating an OOM or memory blowup in a Polars pipeline job, or comparing candidate fixes/implementations by memory usage, or when asked to "add diagnostics", "instrument this job for memory", "profile this pipeline run". Documents how to run the `RunDiagnostics` utility (`polars_utils/run_diagnostics.py`) via a throwaway Fargate task, choose a sampling interval, and read back its S3 output.
---

## What it captures

`RunDiagnostics` runs a background `psutil` sampler (RSS + thread count every
`sample_interval_seconds`), records `.explain()`-plan checkpoints at points
you choose, and tees Polars' `POLARS_VERBOSE=1` stderr output (an fd-level
redirect — Polars' Rust engine writes streaming-fallback notices straight to
OS stderr, bypassing `sys.stderr`, so a plain Python redirect won't catch
them). Every sample and checkpoint is written to S3 immediately, not
buffered, so evidence survives the process getting SIGKILLed by the OOM
killer before anything else could flush. Stderr lines are the one exception —
they're batched and flushed every `stderr_flush_interval_seconds` (default
1s) rather than one S3 write per line, since Polars' verbose output can
arrive fast enough that a synchronous per-line S3 call would back up the
underlying pipe and stall the very process being diagnosed.

**`POLARS_VERBOSE=1` must be set in the process environment *before the
process starts*** — e.g. on the throwaway task's Fargate `environment` block
(see step 3 below), not left for `RunDiagnostics` to set. Polars' Rust core
reads the flag once, and by the time any Python code (including `start()`)
runs, `polars` has already been imported — setting the env var at that point
is too late. `start()` only warns if it looks unset; it can't fix it for you.
Without this, the stderr capture mechanism still works, it just has nothing
Polars-related to catch.

## This is a flexible starting point, not a frozen API

It's fine to adapt `run_diagnostics.py` in-branch for one investigation —
tune the intervals, add snapshot fields, drop what you don't need. `main`'s
version is the shared baseline: **never commit a modification back to it
without asking the user first**, even if it worked well for you.

## Setting up a throwaway diagnostics task

Run instrumented code as its own throwaway Fargate task — never by editing
the real job in place. This is the pattern ticket 1881 used to measure 8 fix
candidates against real production data:

1. **Copy the job** (or just the stage under investigation) into a new
   `<job>_prototype.py` in the same `fargate/` folder, docstring clearly
   marking it throwaway. Wrap with `start()`/`try`/`finally: stop()` and add
   `checkpoint(stage_name, lf)` calls around the suspect operation:

   ```python
   from polars_utils.run_diagnostics import RunDiagnostics
   from polars_utils.utils import split_s3_uri

   def main(..., destination: str) -> None:
       data_bucket, _ = split_s3_uri(destination)
       diagnostics = RunDiagnostics("my_job_prototype", data_bucket).start()
       print(f"Run diagnostics: s3://{diagnostics.bucket}/{diagnostics.prefix}")
       try:
           lf = ...
           diagnostics.checkpoint("before_suspect_join", lf)
           lf = suspect_join(lf, ...)
           diagnostics.checkpoint("after_suspect_join", lf)
           utils.sink_to_parquet(lf, destination)  # write to a *distinct* dataset name
       finally:
           diagnostics.stop()
   ```

   If the suspect logic is nested in helper functions, thread
   `diagnostics: RunDiagnostics | None = None` through as an optional param
   and guard every call with `if diagnostics:`. Point the script's output at
   a **distinctly-named destination** so a throwaway run can never clobber
   the real pipeline's output.

2. **Dockerfile**: check whether this pipeline's Dockerfile copies files with
   a `*.py` wildcard or lists them individually — only the wildcard case
   needs no change. E.g. `_03_independent_cqc`'s shared Dockerfile lists
   every job file individually, so a new prototype script there needs an
   explicit `COPY` line added.

3. **Terraform**: add a new Fargate task module to `terraform/pipeline/fargate.tf`,
   following an existing `module` block's structure — same `ecr_repo_name`
   (reuses the real job's image), sized deliberately for what you're
   measuring (1881 pinned its prototype task to the *real* job's pre-stopgap
   sizing so results reflected the true OOM boundary — raise the sizing
   choice with the user rather than assuming a default is right), and set
   `POLARS_VERBOSE=1` in the task's own `environment` block — the only place
   it can actually take effect (see the callout above). Then wire the new
   task into `terraform/pipeline/step-function.tf` in all 3 places: the
   `sf_pipelines` template vars (`task_arn`, `security_group_id`), and both
   IAM policy `Resource` lists (the `ecs:RunTask` list, and the task/exec
   role ARN list).

4. **Step Function**: add a new manual-start-only definition under
   `terraform/pipeline/step-functions/dynamic/`, following an existing
   definition's structure, referencing the new task's ARN vars and pointing
   `Command` args at the distinctly-named output.

5. **CircleCI**: check `.circleci/config.yml`'s `copy-main-data` job for every
   input your prototype script reads — add any missing `aws s3 sync` line so
   the branch bucket actually has the data before the task runs.

6. **Run it manually** — start the Step Function execution by hand; never
   auto-triggered by an orchestrator.

### Comparing candidate implementations

This same infra generalizes beyond root-causing one OOM: parameterize the
prototype script with an env var (1881 used `FIX_VARIANT`), give each
candidate its own Step Function definition and distinctly-named output
destination, all pointed at the one throwaway task.

### Cleanup

Delete the prototype script, the terraform module and all 3 wiring points,
and the Step Function definition(s) once the investigation concludes — none
of this throwaway infra is meant to reach `main`.

## Choosing `sample_interval_seconds`

Defaults to 10s. A fast, single-operation allocation spike can happen
entirely between two samples and be invisible in the resulting curve — a run
that "survived" is weaker evidence than it looks. Don't just take the
default: ask whoever's running the investigation whether they suspect a fast
spike (a single eager collect/join) or a slow climb (fan-out/duplication) —
the former may need a much shorter interval than 10s to actually catch it.

## Where the output lands

`s3://<workspace>-pipeline-resources/diagnostics/<job_name>/<run_id>/{samples,checkpoints,stderr}/`,
with the pipeline-resources bucket derived from the datasets bucket as
`data_bucket[:-8] + "pipeline-resources"`. The exact `<run_id>` is printed to
the task's own log at `start()` (`Run diagnostics: s3://...`) — read it from
there rather than reconstructing it.

## Reading back the output

No AWS access from this environment — give a ready-to-run command with the
bucket/prefix already filled in (from the branch name and the printed run
ID), `--profile non-prod` per `DEPLOY.md`'s AWS CLI convention:

```bash
aws s3 sync "s3://sfc-<branch>-pipeline-resources/diagnostics/<job_name>/<run_id>/" ./diagnostics/<run_id>/ --profile non-prod
```

Then read the pulled-down files locally:

- `samples/*.json` (`timestamp`, `rss_bytes`, `num_threads`) — read the RSS
  curve for a steady climb (fan-out/duplication) vs. one sharp spike (a
  single eager materialization).
- `checkpoints/<stage>_<timestamp>.json` (adds `stage`, `explain`) — diff the
  before/after plan text around a suspect transform; a missing `CACHE` node
  or a repeated scan is evidence of duplicated computation.
- `stderr/<timestamp>.log` — grep for streaming-fallback notices. Cross-check
  against the `polars-streaming-check` skill's known-non-streaming-op list
  before concluding a fallback is unexpected.

## Dependencies

`psutil` is already a direct dependency (`pyproject.toml` and
`docker_requirements/requirements.txt`) as of the ticket that landed this
utility. If it ever grows new dependencies, remember Fargate images install
from `docker_requirements/*.txt`, not `pyproject.toml` alone — a dependency
only added to the latter fails silently in the real container despite every
local test passing.
