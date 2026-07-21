# SPEC: Add missing error notifications to Step Functions

## Context

Of 16 step function JSON definitions under `terraform/pipeline/step-functions/`, 13 consistently follow a `Catch → Publish Error Notification (Lambda) → Fail` pattern that alerts via SNS (`aws_sns_topic.pipeline_failures`) on any job failure. Three gaps were found:

1. `CQC-And-ASCWDS-Orchestrator.json` — no `Catch` anywhere.
2. `Ingest-ASCWDS.json`, `Ingest-Capacity-Tracker-Care-Home.json`, `Ingest-Capacity-Tracker-Non-Res.json` — the nested `Run-Crawler` state machine execution (`run_crawler_state_machine_arn`) is invoked with no `Catch`, so a crawler failure fails the execution silently.

`Direct-Payment-Recipients.json` (also missing all error handling) is **explicitly out of scope** for this work.

Branch: `1809-add-lambda`, cut from `main`.

## Branch-base note (resolved during implementation)

Commit `d000ceeec` ("1808 - Increase wait time on orchestrator") changed the `Wait For Worker`/`Wait For Workplace` interval in `CQC-And-ASCWDS-Orchestrator.json` from 10s to 60s, but that commit lives only on `1808-inc-wait` and is **not yet merged to `main`**. `1809-add-lambda` was cut from `main` per the original plan, so the Wait interval here is still 10s.

Decision: keep `1809-add-lambda` based on `main` as-is, and keep `ascwds_polling_max_attempts = 120` unchanged. This means the real timeout is currently ~20 minutes (120 × 10s), not the ~2 hours originally envisioned (120 × 60s). This will self-correct once `1808-inc-wait` merges to `main` and this branch is rebased — no code change needed at that point, just a rebase. Flagged here so it isn't mistaken for an oversight.

## Decisions made (interview outcomes)

| Topic | Decision |
|---|---|
| Orchestrator double-notification risk | Accept it. Wrap the whole `Ingest CQC And ASCWDS` Parallel *and* the `Trigger Workforce Intelligence Pipeline` step in Catches. A CQC-API failure will fire two alerts (child pipeline's own + orchestrator's) — accepted as the simpler, gap-free option over risking silent misses. |
| Crawler failure severity | Same as any other error: notify then `Fail`. No special-casing — consistent with the existing repo-wide convention. |
| Error classification in Lambda | No changes to `lambdas/error_notifications/error_notifications.py`. Nested Step Functions failures (from `startExecution`/`startExecution.sync:2`) will fall through to `generic_failure_message` — acceptable, out of scope. |
| CT crawler catch structure | Additive only: wrap the existing `Run Crawler and Handle Error` Parallel state in a new outer `Catch`, routing to a **new** sibling state that duplicates the Lambda-invoke notification. Do not touch the existing, working upstream-error branch inside that Parallel. |
| Retry before notify | None. Fail straight to notification on first error, matching all 13 existing pipelines — no new Retry blocks. |
| Verification | Visual/structural review against the existing working pattern (e.g. `Transform-ASCWDS-Data.json`). No live deploy/test as part of this change — there's no ASL test harness in this repo. |
| ASCWDS polling loop | In scope (expanded from the original ask): add a bounded max-wait timeout to the `Check Worker` / `Check Workplace` loops. |
| Timeout mechanism | Iteration counter, ~120 attempts. At the current (pre-`1808-inc-wait`-merge) 10s Wait interval that's ~20 minutes; intended to be ~2 hours once the 60s interval lands on `main`. |
| Timeout outcome | Treated as an error: routes into the same `$.error → Publish Error Notification → Fail` path as everything else. |
| Configurability of the max-attempts number | Terraform `local` (not a raw literal in the JSON, not an execution-input parameter, not SSM) — see "Configurable timeout" section below. |
| Duration vs raw count | Raw attempt count (120), independent of the Wait interval. **Caveat, called out explicitly in code**: this decouples the configured number from actual wall-clock time — if the Wait interval is ever changed again, the real timeout duration shifts silently unless someone also revisits this number. |
| Worker vs Workplace | One shared max-attempts value for both loops. |
| Per-workspace override | Yes — short value in non-main workspaces (so the timeout path can actually be exercised in minutes), full value in `main`, following the existing `terraform.workspace` conditional `locals` pattern already in `step-function.tf` (e.g. `local.ind_cqc_job_role_estimates_dataset_name`). Non-main value: **5 attempts**. |

## Configurable timeout (max-attempts) mechanism

**Why this needs its own design:** commit `d000ceeec` changed the `Wait` interval in this same file from 10s to 60s by hand-editing a literal buried in nested JSON — a full PR + `terraform apply` for a one-number tweak, with no clearly named place to find it. The new max-attempts value should be easy to find and change the same way other tunable pipeline config already is in this repo (ruled out: raw JSON literal, execution-input parameter, SSM Parameter Store).

**Implementation**, following the existing pattern in `terraform/pipeline/step-function.tf`'s `locals` block (`ind_cqc_job_role_estimates_dataset_name` etc.):

```hcl
locals {
  # existing entries (ind_cqc_job_role_estimates_dataset_name, ...) stay as-is

  # Max polling attempts (at the Wait interval configured in "Wait For Worker"/
  # "Wait For Workplace" in CQC-And-ASCWDS-Orchestrator.json) before the ASC-WDS
  # worker/workplace file-arrival check gives up and notifies.
  # NOTE: coupled to that Wait interval — if it changes, this number no longer
  # represents the same wall-clock timeout and should be revisited.
  ascwds_polling_max_attempts = terraform.workspace == "main" ? 120 : 5
}
```

Injected into the existing `cqc_and_ascwds_orchestrator_state_machine` `templatefile()` call in `step-function.tf`, alongside a `pipeline_failure_lambda_function_arn` reference this resource did not previously pass (needed for the new notification state).

Used in the ASL as a plain templated literal (both `Check Worker` and `Check Workplace` branches share it):
```json
{
  "Variable": "$.worker_attempts",
  "NumericGreaterThanEquals": ${ascwds_polling_max_attempts},
  "Next": "Worker Timeout"
}
```

To change the timeout later: edit one number in `step-function.tf`'s `locals` block and `terraform apply` — no JSON spelunking required.

## File-by-file changes

### 1. `terraform/pipeline/step-functions/CQC-And-ASCWDS-Orchestrator.json`

- Add `Catch: [{ErrorEquals: ["States.ALL"], ResultPath: "$.error", Next: "Publish Error Notification"}]` to the top-level `Ingest CQC And ASCWDS` Parallel state.
- Add the same `Catch` (same `Next` target) to `Trigger Workforce Intelligence Pipeline`.
- Add one new shared `Publish Error Notification` state (top-level, sibling to existing states) using the standard payload shape, `Next: "Fail"`.
- Add a `Fail` state (top-level).
- Inside `Check Worker` / `Check Workplace`: add an attempts counter (`$.worker_attempts` / `$.workplace_attempts`, initialized to 0 via the shared `Check ASCWDS Ingest` Parallel's `Parameters`, alongside the existing year/month/day extraction), incremented each pass through the Wait loop via a new `Increment Worker/Workplace Attempts` Pass state using `States.MathAdd`. A new `Worker/Workplace Attempts Exceeded?` Choice routes to a local `Worker Timeout` / `Workplace Timeout` Fail state (`Error: "ASCWDSFileTimeout"`) once the templated `ascwds_polling_max_attempts` is reached; otherwise falls through to the existing `Wait For Worker`/`Wait For Workplace`. The local Fail propagates up through the nested Parallels to the new top-level Catch on `Ingest CQC And ASCWDS`, reusing the same notification path.

### 2. `terraform/pipeline/step-functions/dynamic/Ingest-ASCWDS.json`

- Add a `Catch: [{ErrorEquals: ["States.ALL"], ResultPath: "$.error", Next: "Publish error notification"}]` to the existing `Run Data Engineering ASC WDS Crawler` task.
- No new states needed — reuses the existing `Publish error notification` → `Fail` states as-is.

### 3 & 4. `Ingest-Capacity-Tracker-Care-Home.json` and `Ingest-Capacity-Tracker-Non-Res.json` (identical structure/fix)

- Add a `Catch: [{ErrorEquals: ["States.ALL"], ResultPath: "$.error", Next: "Handle Crawler Error"}]` to the outer `Run Crawler and Handle Error` Parallel state.
- Add a new top-level `Handle Crawler Error` state: a `Publish Error Notification (Crawler)` Lambda-invoke (`lambda:invoke.waitForTaskToken`, same payload shape as the existing one in this file) → `Next: "Fail (Crawler)"`.
- Add a new top-level `Fail (Crawler)` Fail state.
- Do not modify the existing `Run Crawler and Handle Error` Parallel's internals — purely additive.

## Testing / Verification

No automated ASL test harness exists in this repo. Verification is visual/structural: diff each new `Catch`/state block against the working pattern already used in `Transform-ASCWDS-Data.json` and confirm:
- Every new `Catch` has `ErrorEquals: ["States.ALL"]` and `ResultPath: "$.error"`.
- Every new notification state's `Payload` matches the standard 6-field shape used elsewhere.
- No duplicate state names within the same ASL scope.
- `terraform validate` / `terraform plan` against the affected `aws_sfn_state_machine` resources to confirm the templated JSON is syntactically valid before merging.

## Changelog

Per repo convention, add one `### Fixed` bullet to `CHANGELOG.md`'s `## [Unreleased]` section once implemented.

## Out of scope (explicit)

- `Direct-Payment-Recipients.json` — has no error handling at all, but excluded from this plan per user instruction.
- Any change to `lambdas/error_notifications/error_notifications.py` (error classification stays as-is).
- SNS topic subscriptions (managed in AWS directly, not Terraform).
