# Runbook: update run log flags

## Purpose
`scripts/update_run_log.sh` sets a flag to `True` for one run in the job role estimates archive run log (the tracking table) on S3. It never sets a flag back to `False`, except that selecting a run moves the month's selection. The run log has one row per run.

Run log location: `s3://<bucket>/domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_run_log/archive_date=<date>/run_number=<n>/`. The production bucket is `sfc-main-datasets`.

| Flag | What it records | Process |
|---|---|---|
| `approved` | We assessed the run and are satisfied it is accurate | [Approving a run](#approving-a-run-approved) |
| `selected_for_publication` | The one run chosen to publish this month | [Selecting a run for publication](#selecting-a-run-for-publication-selected_for_publication) |
| `reconciled` | The public output's totals match the archived estimates | [Stub](#reconciling-a-run-reconciled) |
| `locally_checked` | The refreshed Tableau file passed its local check | [Stub](#marking-a-run-locally-checked-locally_checked) |

The `tag` column is not set by this script: the CircleCI workflow in [Cutting the release tag](#cutting-the-release-tag-tag) sets it. Assessing a run is the review of its assessment data and has no flag of its own. Approving and selecting are two separate decisions and two separate updates (see `projects/ARCHIVING_AND_PUBLICATION.md`).

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

The script needs Git Bash (bash 4 or later), so run it from there. Steps 1 and 2 can be done in a separate PowerShell window: the credentials are saved in your `.aws` folder and shared. `aws` and `uv` must be on the Git Bash PATH. The `aws-mfa` login with the `prod` profile also covers `non-prod`, which assumes its role through it.

## How the script works
Every flag uses the same script and the same prompts:

1. It asks for the **AWS profile** and the **bucket**. Press Enter at the bucket prompt only for production (`sfc-main-datasets`); otherwise type a branch bucket name, without `s3://` or a trailing `/`.
2. It copies the run log down and lists the runs archived this month (from the 1st): run number, archive date, the four flags and the tag. Nothing is changed yet.
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

Selecting a run also lets you start the `Publication-Output` step function for it, which builds the public data (the publication summary behind the Tableau output and the T0, T1 and T2 data-download tables).

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
8. The script offers to start `<workspace>-Publication-Output` (`main-Publication-Output` for `sfc-main-datasets`; `<branch>-Publication-Output` for a branch bucket `sfc-<branch>-datasets`). It shows the state machine name, its ARN and the input `{"run_number": <n>}`. Answer `y` to start it, or `n` to start it yourself later with that input.

### Expected result
- `Done: selected_for_publication is now True for run <n> in s3://<bucket>/.`
- For a move, a line `The selection moved, so these runs were changed: ...`.
- On a branch bucket, one restore command per changed run.
- If you answered `y` at step 8, the execution's ARN and `Started.`. In the Step Functions console the new execution of `<workspace>-Publication-Output` has the input `{"run_number": <n>}`.

`Publication-Output` builds the public data from the run's prepared data, which `Publication-Assessment` stores per run. `Publication-Assessment` starts itself when the estimates pipeline succeeds in `main`; on a branch it must be started by hand with `{"run_number": <n>}`. The script does not check that the run was prepared.

### If something is wrong
- **`Run number <n> is not approved`**: nothing is uploaded. Approve the run first.
- **`Run number <n> was archived before <1st of the month>`**: nothing is uploaded. Only this month's runs can be selected; this includes a run that was selected in an earlier month.
- **`Run number <n> is already selected_for_publication`**: that run is already the selection; nothing to do.
- **Run number not found**: nothing is uploaded; pick a run from the list.
- **The upload fails part way**: the script names the runs it uploaded and the one that failed. A moved selection can be left with no run selected, or two runs selected. Restore every run it names (on a branch bucket, run the restore commands it prints; see "Undoing an update"), then run the script again.
- **`State machine ... was not found`, `could not list state machines`, or `could not start`**: the selection is already saved. The script prints `Start Publication-Output by hand with {"run_number": <n>}`; start `<workspace>-Publication-Output` with that input. A branch has the step function only once the branch has deployed it, and your profile needs permission to list and start state machines in that workspace.
- **`Publication-Output` starts but fails with `run_number <n> not found ... Available runs`**: the run was never prepared. The selection stays set. Start `<workspace>-Publication-Assessment` with `{"run_number": <n>}`, wait for it to finish, then start `Publication-Output` again with the same input.

## Reconciling a run (`reconciled`)
**Stub: process not yet written.**

- Use case: record that the public output's totals for the run match the archived estimates.
- Who and when: to be decided. Nothing reads this flag yet.
- Steps: to be written. The script can already set the flag by following the prompts in "How the script works" with Flag `reconciled`.

## Marking a run locally checked (`locally_checked`)
**Stub: process not yet written.**

- Use case: record that the analysts' check of the refreshed Tableau file passed.
- Who and when: to be decided. Nothing reads this flag yet.
- Steps: to be written. The script can already set the flag by following the prompts in "How the script works" with Flag `locally_checked`.

## Cutting the release tag (`tag`)

### Use case
Once the selected run is reconciled, tag the exact code that produced its data as `vYYYY.MM.N` (MM is the month before the run's archive date, N the next free number for that month) and move that commit's CHANGELOG entries under the new version. CircleCI does this, not AWS and not this script, because only CircleCI holds GitHub credentials.

The run must be `selected_for_publication` and `reconciled` and have no `tag`. The tag goes on the run's `commit_sha`, with the message "Codebase used for data as at <Month YYYY>". The workflow then writes the tag to the run log, and opens a PR from `rel-<tag>` that moves into the new version only the `[Unreleased]` entries present at that commit. Entries merged to `main` since stay under `[Unreleased]`.

### Steps
1. In CircleCI choose the DataEngineering project, **Trigger Pipeline**, branch `main`, and add the parameters `cut_release_tag` (boolean) `true` and `release_run_number` (integer) the selected run's number.
2. Open the `plan-release-tag` job. It prints the run, its commit, the tag it would cut and the tag message. Check the commit and tag are what you expect.
3. Approve the `approve-release-tag` job, then watch `cut-release-tag`.
4. Merge the `rel-<tag>` PR once its checks pass.

### Expected result
- The tag exists on GitHub, pointing at the run's commit.
- The run's `tag` in the run log shows it (`update_run_log.sh` lists it).
- A PR `Release <tag>: CHANGELOG` is open.

### One-off setup
A CircleCI context named `github-release` with `GITHUB_TOKEN`: a GitHub token for this repository only that can write contents and pull requests. It is used by `cut-release-tag` only.

### If something is wrong
- **`Run number <n> is not reconciled` / `not selected_for_publication` / `already tagged`**: the plan job stops and nothing changes. Fix the run's state first.
- **`Commit <sha> is not on main`**: the run was archived from a branch. Nothing is tagged.
- **`The plan changed since it was approved`**: another tag or run log change landed between plan and approval. Start the workflow again.
- **The job fails after the tag was pushed**: start the workflow again. It reuses the existing tag, skips any step already done (including an existing `rel-<tag>` branch) and carries on.
- **A tag was cut for the wrong run**: delete the tag on GitHub, restore the run's run log partition (see "Undoing an update"), and start again.

### Testing safely
Trigger the pipeline from your branch with `cut_release_tag` `true`, `release_dry_run` `true` and `release_bucket` your branch dataset bucket (`sfc-<branch>-datasets`). That runs the same jobs with the non-prod context and prints every tag push, S3 write and PR instead of making it. The bucket's run log needs a run that is selected and reconciled, set with `update_run_log.sh`. A real run only starts from `main`.

## If something is wrong (any flag)
- **Flag unsupported**: only the four flags in the table at the top are accepted.
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
