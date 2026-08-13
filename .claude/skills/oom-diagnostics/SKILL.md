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
killer before anything else could flush.

## This is a flexible starting point, not a frozen API

It's fine to adapt `run_diagnostics.py` in-branch for a specific
investigation — tune `sample_interval_seconds`, add fields to the snapshot
payload, drop a part you don't need. The version on `main` is the stable
baseline everyone else starts from — **never commit a modification back to
it without asking the user first**, even if it worked well for your
investigation.

## Setting up a throwaway diagnostics task

Run instrumented code as its own throwaway Fargate task — never by editing
the real job in place. This is the pattern ticket 1881 used to measure 8 fix
candidates against real production data (see its unmerged branch
`1881-cqc-loc-fix`, commits `e19194253`/`b889604e1`/`32ac4d38d`):

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
   and guard every call with `if diagnostics:` — see `postcode_matcher.py` on
   the same branch for the multi-call-site version of this. Point the
   script's output at a **distinctly-named destination** so a throwaway run
   can never clobber the real pipeline's output.

2. **Dockerfile**: usually no change needed — the existing
   `COPY .../fargate/*.py .` wildcard already picks up the new script. Check
   this before assuming otherwise.

3. **Terraform**: add a new Fargate task module, reusing the real job's ECR
   image, sized deliberately for what you're measuring (1881 pinned its
   prototype task to the *real* job's pre-stopgap sizing so results reflected
   the true OOM boundary — raise the sizing choice with the user rather than
   assuming a default is right):

   ```hcl
   module "my-job-proto" {
     source        = "../modules/fargate-task"
     task_name     = "my-job-proto"
     ecr_repo_name = "fargate/my-job"        # same image as the real task
     cluster_arn   = aws_ecs_cluster.polars_cluster.arn
     tag_name      = terraform.workspace
     cpu_size      = 8192                     # match whatever you're measuring against
     ram_size      = 61440
     environment   = [{ "name" : "AWS_REGION", "value" : "eu-west-2" }]
   }
   ```

   Then wire it into `terraform/pipeline/step-function.tf` in all 3 places:
   the `sf_pipelines` template vars (`task_arn`, `security_group_id`), and
   both IAM policy `Resource` lists (the `ecs:RunTask` list, and the
   task/exec role ARN list).

4. **Step Function**: add a new manual-start-only definition under
   `terraform/pipeline/step-functions/dynamic/`, referencing the new task:

   ```json
   {
     "Comment": "Throwaway: measure X. Not auto-triggered - start manually. Delete once the investigation concludes.",
     "StartAt": "Run prototype",
     "States": {
       "Run prototype": {
         "Type": "Task",
         "Resource": "arn:aws:states:::ecs:runTask.sync",
         "Parameters": {
           "Cluster": "${polars_cluster_arn}",
           "TaskDefinition": "${my_job_proto_task_arn}",
           "LaunchType": "FARGATE",
           "NetworkConfiguration": {
             "AwsvpcConfiguration": {
               "Subnets": ${public_subnet_ids},
               "SecurityGroups": ["${my_job_proto_security_group_id}"],
               "AssignPublicIp": "ENABLED"
             }
           },
           "Overrides": {
             "ContainerOverrides": [{
               "Name": "my-job-proto-container",
               "Command": ["my_job_prototype.py", "--destination", "${dataset_bucket_uri}/domain=.../dataset=..._prototype/"]
             }]
           }
         },
         "End": true
       }
     }
   }
   ```

5. **CircleCI**: check `.circleci/config.yml`'s `copy-main-data` job for every
   input your prototype script reads — add any missing `aws s3 sync` line so
   the branch bucket actually has the data before the task runs:

   ```
   aws s3 sync "s3://sfc-main-datasets/domain=.../dataset=.../" "s3://$BRANCH_DATASET_BUCKET/domain=.../dataset=.../" --delete
   ```

6. **Run it manually** — start the Step Function execution by hand; never
   auto-triggered by an orchestrator.

### Comparing candidate implementations

This same infra generalizes beyond root-causing one OOM: parameterize the
prototype script with an env var (1881 used `FIX_VARIANT`), give each
candidate its own Step Function definition and distinctly-named output
destination, all pointed at the one throwaway task. That's how 1881 measured
8 fix candidates against real production data in a single spike.

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

There's no AWS access from this Claude Code environment, so give the user a
ready-to-run command with the bucket/prefix already filled in (from the
branch name and the printed run ID), using `--profile non-prod` (this repo's
AWS CLI convention — see `DEPLOY.md`):

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
