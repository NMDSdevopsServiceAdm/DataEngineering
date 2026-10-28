# Runbook: update run log flags

## Purpose
`scripts/update_run_log.sh` sets a flag to `True` for one run in the job role estimates archive run log (the tracking table) on S3. It never sets a flag back to `False`, except that selecting a run moves the month's selection. The run log has one row per run.

Run log location: `s3://<bucket>/domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_run_log/archive_date=<date>/run_number=<n>/`. The production bucket is `sfc-main-datasets`.

| Flag | What it records | Process |
|---|---|---|
| `approved` | We assessed the run and are satisfied it is accurate | [Approving a run](#approving-a-run-approved) |
| `selected_for_publication` | The one run chosen to publish this month | [Selecting a run for publication](#selecting-a-run-for-publication-selected_for_publication) |
| `promoted` | The run's estimates, metadata and run-log partitions are copied into the long-term archive bucket (set automatically, not chosen at the prompt) | [Promoting a run](#promoting-a-run-promoted) |
| `reconciled` | The public output's totals match the archived estimates (set automatically by Publication-Output; choosing it at the prompt only forces the flag) | [Reconciling a run](#reconciling-a-run-reconciled) |
| `locally_checked` | The refreshed Tableau file passed its local check | [Stub](#marking-a-run-locally-checked-locally_checked) |

The `tag` column is not set by this script. `promoted_at` and `reconciled_at` are set automatically alongside `promoted` and `reconciled` respectively (the current time) and are not themselves flags you choose. Assessing a run is the review of its assessment data and has no flag of its own. Approving, selecting, promoting and reconciling are separate decisions and separate updates (see `projects/ARCHIVING_AND_PUBLICATION.md`).

## Prerequisites
Run these once per terminal session, from the DataEngineering repo root, in PowerShell:

1. Set the HOME environment variable (`aws-mfa` reads it to find your `.aws` folder, and Windows may not set it). This only applies to the current terminal, so repeat it in each new one:

```
$Env:HOME = $Env:USERPROFILE
```

2. Provide your MFA token (the temporary credentials last 12 hours):

```
aws-mfa --mfa-profile prod --token xxxxxx
```

3. Install the Python environment (first time, or after dependencies change):

```
uv sync
```

The script needs Git Bash (bash 4 or later), so run it from there. Steps 1 and 2 can be done in a separate PowerShell window: the credentials are saved in your `.aws` folder and shared. `aws` and `uv` must be on the Git Bash PATH. The AWS profiles the script uses must be set up as described in `WINDOWS_SETUP.md`.

## How the script works
Every flag uses the same script and the same prompts:

1. It asks for the **AWS profile** (Enter for the default, `run-log-updater`; use `non-prod` for a branch bucket) and the **bucket**. Press Enter at the bucket prompt only for production (`sfc-main-datasets`); otherwise type a branch bucket name, without `s3://` or a trailing `/`.
2. It copies the run log down and lists the runs archived this month (from the 1st): run number, archive date, the five flags and the tag. Nothing is changed yet.
3. It asks for the **flag** and the **run number**, then shows the four answers and asks `Are these correct?`.
4. It builds the update in `run_log_updated/` and prints each changed run **Before** and **After**.
5. Only after you answer `y` at the upload prompt are the changed runs' folders replaced on S3. Anything other than y/yes/n/no is asked again, and `n` deletes the updated copy and uploads nothing.

`run_log_original/` and `run_log_updated/` are created in the repo root and ignored by git; each run clears both first. To only look at the list, press Ctrl-C at the Flag prompt.

## Approving a run (`approved`)

### Use case
After a run has been assessed, record that we are satisfied it is accurate. Several runs can be approved in a month, for example the first run of the month and a rerun after more source data arrives. Approving does not publish anything and does not start any pipeline. A run must be approved before it can be selected for publication.

Do this when you have reviewed the run's assessment data and are satisfied with it. The assessment data's `archive_date` and `run_number` partitions show which run you assessed.

### Steps
1. From the repo root, in Git Bash, run `bash scripts/update_run_log.sh`.
2. Enter the AWS profile and the bucket.
3. Check the list: the run you assessed is there with `approved False`. A run from an earlier month is not listed but can still be approved by its run number.
4. Flag: `approved`. Run number: the run you assessed.
5. Check the four answers and answer `y`.
6. Check **Before** and **After**: the only difference is `approved: False` becoming `approved: True`, and the run number and archive date are the ones you meant.
7. Answer `y` to upload.

### Expected result
- `Done: approved is now True for run <n> in s3://<bucket>/.`
- On a branch bucket, one restore command is printed. On production none is printed.
- No question about starting anything.
- Running the script again and looking at the list shows the run with `approved True`.

### If something is wrong
- **Run number not found**: nothing is uploaded; check the number against the assessment data.
- **`Run number <n> is already approved`**: nothing to do.
- **The upload fails part way, or an answer was wrong**: see "Undoing an update". Approval cannot be removed by the script; restore the run.

## Selecting a run for publication (`selected_for_publication`)

### Use case
Choose the one run to publish this month. This is a separate decision from approving, made after at least one run is approved. It can pick any approved run, not necessarily the latest: if the first run of the month is approved and a later rerun is also approved and preferred, select the later one. If you selected the wrong run, select the right one: the selection moves.

Selecting a run also automatically copies it into the long-term archive bucket (see "Promoting a run" below — this step runs on its own, with no extra confirmation), and lets you start the `Publication-Output` step function for it, which builds the public data (the publication summary behind the Tableau output and the T0, T1 and T2 data-download tables).

Rules:
- The run must be approved. Approve it first (see "Approving a run") if it is not.
- The run must have been archived on or after the 1st of the current month. Earlier runs are not listed and cannot be selected.
- At most one run is selected since the 1st. Selecting a different run clears the previous selection in the same update. Selections from earlier months are never changed.

### Steps
1. From the repo root, in Git Bash, run `bash scripts/update_run_log.sh`.
2. Enter the AWS profile and the bucket.
3. Check the list: the run you want shows `approved True`. Note which run, if any, already shows `selected_for_publication True`.
4. Flag: `selected_for_publication`. Run number: the run you want to publish.
5. Check the four answers and answer `y`.
6. Check **Before** and **After**:
   - First selection this month: one block, for your run, where only `selected_for_publication` changes from `False` to `True`.
   - Moving the selection: two blocks, your run (`False` to `True`) and the previously selected run (`True` to `False`).
7. Answer `y` at the upload prompt. A move shows `Upload 2 changed run(s)`, and both folders are replaced after this one confirmation.
8. The script automatically promotes the run (see "Promoting a run" below) — no prompt, it just runs. If this fails, it stops here; see that section's "If something is wrong".
9. The script offers to start `<workspace>-Publication-Output` (`main-Publication-Output` for `sfc-main-datasets`; `<branch>-Publication-Output` for a branch bucket `sfc-<branch>-datasets`). It shows the state machine name, its ARN and the input `{"run_number": <n>}`. Answer `y` to start it, or `n` to start it yourself later with that input.

### Expected result
- `Done: selected_for_publication is now True for run <n> in s3://<bucket>/.`
- For a move, a line `The selection moved, so these runs were changed: ...`.
- On a branch bucket, one restore command per changed run.
- The promotion step's own output (see "Promoting a run"), ending `Promoted run <n> (archived <date>) to s3://<workspace>-archive-promoted/.`.
- If you answered `y` at step 9, the execution's ARN and `Started.`. In the Step Functions console the new execution of `<workspace>-Publication-Output` has the input `{"run_number": <n>}`.

`Publication-Output` builds the public data from the run's prepared data, which `Publication-Assessment` stores per run. `Publication-Assessment` starts itself when the estimates pipeline succeeds in `main`; on a branch it must be started by hand with `{"run_number": <n>}`. The script does not check that the run was prepared.

### If something is wrong
- **`Run number <n> is not approved`**: nothing is uploaded. Approve the run first.
- **`Run number <n> was archived before <1st of the month>`**: nothing is uploaded. Only this month's runs can be selected; this includes a run that was selected in an earlier month.
- **`Run number <n> is already selected_for_publication`**: that run is already the selection; nothing to do.
- **Run number not found**: nothing is uploaded; pick a run from the list.
- **The upload fails part way**: the script names the runs it uploaded and the one that failed. A moved selection can be left with no run selected, or two runs selected. Restore every run it names (on a branch bucket, run the restore commands it prints; see "Undoing an update"), then run the script again.
- **`The selection is saved, but promotion did not complete`**: see "Promoting a run"'s "If something is wrong" — the selection itself does not need redoing.
- **`State machine ... was not found`, `could not list state machines`, or `could not start`**: the selection is already saved. The script prints `Start Publication-Output by hand with {"run_number": <n>}`; start `<workspace>-Publication-Output` with that input. A branch has the step function only once the branch has deployed it, and your profile needs permission to list and start state machines in that workspace.
- **`Publication-Output` starts but fails with `run_number <n> not found ... Available runs`**: the run was never prepared. The selection stays set. Start `<workspace>-Publication-Assessment` with `{"run_number": <n>}` (on main the `run-log-updater` role can't start it; use another profile with access), wait for it to finish, then start `Publication-Output` again with the same input.

## Promoting a run (`promoted`)

### Use case
Copy a run's estimates, metadata and run-log partitions into the long-term archive bucket
(`sfc-<workspace>-archive-promoted`), where they are kept indefinitely — the rolling archive in
`sfc-<workspace>-datasets` expires runs that were never promoted after about 2 months. This runs
**automatically** as part of selecting a run for publication (see "Selecting a run for
publication" above, step 8). `promoted` is not one of the flags `update_run_log.sh` lets you choose
directly — recover a failed attempt with the commands below instead, not by re-running the script.

### Steps (automatic, via selection)
No separate steps — this runs as part of "Selecting a run for publication" above. It:
1. Marks the run `promoted` in the run log and uploads that change, the same way every other flag
   is uploaded.
2. Copies the run's estimates partition into the long-term bucket, then its metadata partition,
   then its (now `promoted: True`) run-log partition, printing a count of files copied for each.

### Expected result
- One `Copied <n> object(s) for <prefix>/<partition>.` line per dataset (estimates, metadata,
  run log).
- `Promoted run <n> (archived <date>) to s3://<workspace>-archive-promoted/.`
- The run log in both buckets shows `promoted: True` for the run, with the same `promoted_at`
  timestamp.

### If something is wrong
- **`could not mark run <n> as promoted`**: the flag update itself failed, so nothing was copied —
  the run log does **not** yet show `promoted: True`. If the message says the run is "already
  promoted", there's nothing to do (a previous attempt already finished — check the long-term bucket
  to confirm). This also happens when you reselect a run that was promoted earlier: the script stops
  at this point and never offers `Publication-Output`, so start `<workspace>-Publication-Output` by
  hand with `{"run_number": <n>}`. If it says the run is "not selected_for_publication", select it
  first. Otherwise, recover with "Recovering a promotion that failed before the flag was set" below.
- **`could not upload the promoted run log partition`**: the run log on S3 does **not** yet show
  `promoted: True` and nothing was copied. Restore the run with the restore command the script
  prints, then recover with "Recovering a promotion that failed before the flag was set" below.
- **`could not copy <prefix>/<partition>`**, **`copy of <prefix>/<partition> looks incomplete`** or
  **`could not list s3://<workspace>-archive-promoted/...`**: these all happen *after* the flag update
  already succeeded (the run log genuinely shows `promoted: True`). Finish the copy by hand instead:
  the script prints the exact `aws s3 cp ... --recursive --profile <profile>` command for the
  dataset that failed — run it, then re-check the file count matches between source and destination
  (see "Not sure whether a promotion fully finished" below). Then do the same for the datasets after
  it: the script stops at the first failure, so the estimates, metadata and run log are copied in that
  order and every later one still needs copying. An `AccessDenied` here means the profile's role is
  missing a permission; see "Role permissions for promotion" below.
- **`nothing found at s3://<bucket>/<prefix>/<partition>/`** or **`could not list s3://<bucket>/...`**:
  also after the flag update. The source partition has no files or could not be read, so check the
  run's partition in `<bucket>` exists before copying anything.
- **Not sure whether a promotion fully finished**: for each of the estimates, metadata and run log
  datasets, count the files in the run's partition in `<bucket>` and then in the long-term bucket. Replace
  `<profile>`, `<bucket>`, `<workspace>` and `<partition>` (`archive_date=<date>/run_number=<n>`), then run:

  ```bash
  aws s3 ls --recursive "s3://<bucket>/domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates/<partition>/" --profile <profile> | wc -l
  aws s3 ls --recursive "s3://<workspace>-archive-promoted/domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates/<partition>/" --profile <profile> | wc -l
  aws s3 ls --recursive "s3://<bucket>/domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_metadata/<partition>/" --profile <profile> | wc -l
  aws s3 ls --recursive "s3://<workspace>-archive-promoted/domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_metadata/<partition>/" --profile <profile> | wc -l
  aws s3 ls --recursive "s3://<bucket>/domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_run_log/<partition>/" --profile <profile> | wc -l
  aws s3 ls --recursive "s3://<workspace>-archive-promoted/domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_run_log/<partition>/" --profile <profile> | wc -l
  ```

  The numbers in each pair (bucket, then long-term bucket) should be the same, and the run log's should be 1.

### Recovering a promotion that failed before the flag was set
If the run log does **not** yet show `promoted: True` for this run (the flag
update itself failed), there is no automatic retry — run these by hand, in order, from the repo
root in Git Bash. Replace `<profile>`, `<bucket>` (the `sfc-<workspace>-datasets` bucket the run was
selected on), `<n>` and `<partition>` (`archive_date=<date>/run_number=<n>`, from the run's row in
the listing):

```bash
aws s3 sync "s3://<bucket>/domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_run_log/" run_log_original --profile <profile>
uv run python -m scripts.update_run_log --run_log_dir run_log_original --output_dir run_log_promoted --run_number <n> --flag promoted
aws s3 sync "run_log_promoted/<partition>/" "s3://<bucket>/domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_run_log/<partition>/" --delete --profile <profile>
aws s3 cp "s3://<bucket>/domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates/<partition>/" "s3://<workspace>-archive-promoted/domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates/<partition>/" --recursive --profile <profile>
aws s3 cp "s3://<bucket>/domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_metadata/<partition>/" "s3://<workspace>-archive-promoted/domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_metadata/<partition>/" --recursive --profile <profile>
aws s3 cp "s3://<bucket>/domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_run_log/<partition>/" "s3://<workspace>-archive-promoted/domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_run_log/<partition>/" --recursive --profile <profile>
rm -rf run_log_original run_log_promoted
```

The first `update_run_log.py` call still enforces the guard (fails if the run isn't
`selected_for_publication`, or if it's already `promoted`), so this is safe to run without first
checking the run log by hand.

### Role permissions for promotion
The `run-log-updater` role lives in the production account and is not defined in this repo. Its
identity policy must allow everything below (all prefixes are under
`domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates`, plus `_metadata` and
`_run_log`). Nothing is granted on the archive bucket beyond `PutObject` and `ListBucket`: its
bucket policy denies deletes to everyone except the Admin role.

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "ListDatasetPrefixes",
            "Effect": "Allow",
            "Action": ["s3:ListBucket", "s3:ListBucketVersions"],
            "Resource": "arn:aws:s3:::sfc-main-datasets",
            "Condition": {
                "StringLike": {
                    "s3:prefix": [
                        "domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates",
                        "domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates/*",
                        "domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_metadata",
                        "domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_metadata/*",
                        "domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_run_log",
                        "domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_run_log/*"
                    ]
                }
            }
        },
        {
            "Sid": "ReadEstimatesAndMetadata",
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:GetObjectVersion"],
            "Resource": [
                "arn:aws:s3:::sfc-main-datasets/domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates/*",
                "arn:aws:s3:::sfc-main-datasets/domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_metadata/*"
            ]
        },
        {
            "Sid": "ReadWriteDeleteRunLogObjects",
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:GetObjectVersion", "s3:PutObject", "s3:DeleteObject"],
            "Resource": "arn:aws:s3:::sfc-main-datasets/domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_run_log/*"
        },
        {
            "Sid": "ListPromotedArchivePrefixes",
            "Effect": "Allow",
            "Action": "s3:ListBucket",
            "Resource": "arn:aws:s3:::sfc-main-archive-promoted",
            "Condition": {
                "StringLike": {
                    "s3:prefix": [
                        "domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates/*",
                        "domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_metadata/*",
                        "domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_run_log/*"
                    ]
                }
            }
        },
        {
            "Sid": "WritePromotedArchive",
            "Effect": "Allow",
            "Action": "s3:PutObject",
            "Resource": [
                "arn:aws:s3:::sfc-main-archive-promoted/domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates/*",
                "arn:aws:s3:::sfc-main-archive-promoted/domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_metadata/*",
                "arn:aws:s3:::sfc-main-archive-promoted/domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_run_log/*"
            ]
        },
        {
            "Sid": "WhoAmI",
            "Effect": "Allow",
            "Action": "sts:GetCallerIdentity",
            "Resource": "*"
        },
        {
            "Sid": "FindPublicationStateMachine",
            "Effect": "Allow",
            "Action": "states:ListStateMachines",
            "Resource": "*"
        },
        {
            "Sid": "StartPublicationOutput",
            "Effect": "Allow",
            "Action": "states:StartExecution",
            "Resource": "arn:aws:states:eu-west-2:344210435447:stateMachine:main-Publication-Output"
        }
    ]
}
```

Before the first promotion on main, with an Admin profile:
1. Check the objects being copied: if they carry tags, also allow `s3:GetObjectTagging` on the source
   prefixes and `s3:PutObjectTagging` on the archive prefixes; if either bucket is encrypted with a
   customer-managed KMS key, also allow that key (`kms:Decrypt` for the source, `kms:GenerateDataKey`
   for the destination).
2. Run `aws iam simulate-principal-policy --policy-source-arn arn:aws:iam::344210435447:role/run-log-updater --action-names s3:GetObject --resource-arns "<object arn>" --profile <admin-profile>` (or use the console Policy Simulator) for each action and resource above, and for `s3:DeleteObject` on the archive bucket, which must come back denied.
3. Promote a run and confirm each `Copied <n> object(s)` line appears with no `AccessDenied`.

## Reconciling a run (`reconciled`)

### Use case
Confirm the public output's total filled posts for a run matches its promoted archive's total, within a small tolerance, before the run is considered fit to tag and publish. This runs **automatically** as the last stage of the `Publication-Output` state machine — you don't normally choose this flag yourself. The run must already be `promoted` for either path below to work, since the comparison reads the long-term archive.

### Steps (automatic, via Publication-Output)
No separate steps — this runs as the last stage of `Publication-Output` (started per "Selecting a run for publication" above, or by hand). It:
1. Waits for the run's run log partition to appear in the long-term archive bucket ("Check Promoted Archive"; promotion copies the run log last, so it marks a finished copy), rechecking on a fixed schedule (`reconciliation_polling_*` in `terraform/pipeline/step-function.tf`) before failing with `PromotionTimeout` if the run was never promoted.
2. Recomputes the public output's and the promoted archive's filled posts independently and compares each period within a small relative tolerance ("Compare Totals").
3. On a match, marks the run `reconciled` in the run log (recording `reconciled_at`). On a mismatch, the execution fails without changing the run log, and the pipeline's usual failure notification fires.

Don't approve or select runs while `Publication-Output` is finishing: marking the run reconciled rewrites that run's run log row from a copy read moments earlier, which could undo a selection made in between.

### Steps (manual, only to force the flag without the comparison)
Choosing this flag yourself skips the comparison entirely — only do this if the automated check is known to be wrong for a specific run (for example, a data correction already reconciled by hand). The run must already be `promoted`.
1. From the repo root, in Git Bash, run `bash scripts/update_run_log.sh`.
2. Enter the AWS profile and the bucket.
3. Flag: `reconciled`. Run number: the run to mark reconciled.
4. Check the four answers and answer `y`, then **Before**/**After** and answer `y` to upload.

### Expected result
- In the Step Functions console, `Publication-Output`'s "Check Promoted Archive" and "Compare Totals" steps both succeed.
- The run log shows `reconciled: True` for the run, with a `reconciled_at` timestamp.

### If something is wrong
- **`Publication-Output` fails with `PromotionTimeout`**: the run was never promoted, or promotion hasn't finished. Select/promote the run first (see "Promoting a run"), then start `Publication-Output` again with the same `run_number`.
- **`Publication-Output` fails with `PromotionCheckFailed`**: the check task itself failed, not just "not promoted yet" (for example a permissions error). Nothing is retried; see the task's CloudWatch log.
- **`Publication-Output` fails at "Compare Totals"**: one or more periods differ by more than the tolerance (the error lists them), or a period is missing from the archive. This needs investigating, not rerunning — the task's CloudWatch log prints both totals and the largest period difference.
- **`Run number <n> is not promoted`** (manual path): promote the run first.
- **`Run number <n> is already reconciled`**: nothing to do.

## Marking a run locally checked (`locally_checked`)
**Stub: process not yet written.**

- Use case: record that the analysts' check of the refreshed Tableau file passed.
- Who and when: to be decided. Nothing reads this flag yet.
- Steps: to be written. The script can already set the flag by following the prompts in "How the script works" with Flag `locally_checked`.

## If something is wrong (any flag)
- **Flag unsupported**: only `approved`, `selected_for_publication`, `reconciled` and `locally_checked` are accepted at the prompt — `promoted` is set automatically, not chosen directly (see "Promoting a run").
- **Run number not found, or the flag is already True**: the script stops with an error and uploads nothing. Fix the input and run it again.
- **You answer `n` at the upload prompt (or the update fails)**: the change is reverted. `run_log_updated/` is deleted, `run_log_original/` is left as it was, and nothing is uploaded.
- **Could not authenticate**: repeat the prerequisites (the MFA credentials have expired after 12 hours).
- **No run log found in the bucket**: check the bucket name. Nothing is uploaded.
- **The upload fails part way**: the script names the runs it had already uploaded and the one that failed. The failed run's S3 folder may hold two parquet files, the old one and the new one, which reads as two rows for the run. A rerun then fails with "matches multiple rows". Restore every run it names (on a branch bucket, run the restore commands it prints), then run the script again from the start (see "Undoing an update").

## Undoing an update
- **Branch bucket**: after a successful upload the script prints one restore command per changed run (an `aws s3 cp --recursive` then `aws s3 sync --delete` from `run_log_original/`). Run all of them to put the runs back as they were. **Run them before running the script again**: every run replaces `run_log_original/`, and restoring from a later copy puts the changed value back. A failed upload prints the same commands for every run it uploaded and for the one that failed, which may be half written. Undoing does not stop a step function execution that was already started.
- **Production bucket (`sfc-main-datasets`)**: the bucket is versioned. No restore command is printed; restore the previous version of each changed run's parquet file from S3 object versions.

## Testing safely
Trial runs MUST use a branch dataset bucket, never production. CircleCI copies the production run log into each branch's dataset bucket (the `copy-main-data` job), so enter the branch bucket name at the Bucket prompt. Check that the bucket prompt shows the branch bucket before confirming. Each push wipes the branch run log again, so a branch run log used for selection tests (which needs several runs this month) has to be seeded again after CI.
