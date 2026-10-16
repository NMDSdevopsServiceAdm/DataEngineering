# Runbook: approve and select runs in the run log

## Purpose
Sets a flag to `True` for one run in the job role estimates archive run log (the tracking table) on S3. The supported flags are `approved`, `selected_for_publication`, `reconciled` and `locally_checked`. `tag` is not supported. Setting a flag back to `False` is not supported, except that selecting a run moves the selection (see "Selecting a run").

Run log location: `s3://<bucket>/domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_run_log/archive_date=<date>/run_number=<n>/`. The production bucket is `sfc-main-datasets`.

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

## Steps
1. From the repo root, run:

```
bash scripts/update_run_log.sh
```

2. Answer the first two prompts:
   - **AWS profile**: the profile with access to the bucket (for example `prod`).
   - **Bucket**: press Enter for `sfc-main-datasets`, or type a branch bucket name. Give the name only, without `s3://` or a trailing `/`.
3. The script copies the run log down and lists the runs archived this month (from the 1st): run number, archive date, the four flags and the tag. Nothing is changed yet.
4. Answer the next two prompts:
   - **Flag**: one of the supported flags.
   - **Run number**: a positive whole number from the list, for example `12`.
5. Check the four inputs shown and answer `y` (or `n` to cancel; nothing has been uploaded). Anything other than y/yes/n/no is asked again.
6. The script makes the update in `run_log_updated/` and prints each changed run **Before** and **After**. Check that only the chosen flag changed, and that the run numbers and archive dates are the ones you meant.
7. Answer `y` to upload. Only the changed runs' folders are replaced on S3.

## Selecting a run
Choose `selected_for_publication` as the flag. A run can only be selected when:
- it is approved, and
- it was archived on or after the 1st of the current month (runs from earlier months are not changed, and cannot be selected).

Only one run is selected from the 1st of the month. If another run this month is already selected, selecting a different run moves the selection: the script shows both runs, the old one going `True` to `False` and the new one `False` to `True`, and one confirmation uploads both. Selections from earlier months are never changed.

After the upload the script offers to start the `<workspace>-Publication-Output` step function (`main-Publication-Output` for `sfc-main-datasets`; `<branch>-Publication-Output` for a branch bucket `sfc-<branch>-datasets`) with `{"run_number": <n>}`. It shows the state machine name, its ARN and the input before asking. Answer `n` to start it yourself later with the same input. The prompt only appears when you select a run.

`Publication-Output` builds the public data from the run's prepared data, which `Publication-Assessment` stores per run. `Publication-Assessment` starts itself when the estimates pipeline succeeds in `main`, but on a branch it must be started by hand with `{"run_number": <n>}`. The script does not check that the run was prepared.

## If something is wrong
- **Run number not found, flag already True, or flag unsupported**: the script stops with an error and uploads nothing. Fix the input and run it again.
- **Run is not approved, or was archived before the 1st of the month** (selecting only): the script stops with an error and uploads nothing. Approve the run first, or pick a run from the list.
- **You answer `n` at the upload prompt (or the update fails)**: the change is reverted. `run_log_updated/` is deleted, `run_log_original/` is left as it was, and nothing is uploaded.
- **Could not authenticate**: repeat the prerequisites (the MFA credentials have expired after 12 hours).
- **No run log found in the bucket**: check the bucket name. Nothing is uploaded.
- **The upload fails part way**: the script names the runs it had already uploaded and the one that failed. The failed run's S3 folder may hold two parquet files, the old one and the new one, which reads as two rows for the run. A rerun then fails with "matches multiple rows". When a selection moved, one run may be updated and the other not, which leaves two runs selected or none. Restore each affected run's original folder, then run the script again from the start. On a branch bucket, copy `run_log_original/archive_date=<date>/run_number=<n>/` back over the same folder in S3 with `aws s3 cp --recursive`, then `aws s3 sync --delete`, as in the restore commands in "Undoing an update". In the production bucket, restore the previous object versions of each affected run's folder from S3.
- **Publication-Output is not found, you are not allowed to list or start it, or it fails to start**: the selection is already saved. The script says so and prints the input; start `<workspace>-Publication-Output` by hand with `{"run_number": <n>}`. The step function only exists on a branch once the branch has deployed it.
- **Publication-Output starts but fails with "run_number N not found ... Available runs"**: the run was never prepared. The selection stays set. Start `<workspace>-Publication-Assessment` with `{"run_number": <n>}`, wait for it to finish, then start `Publication-Output` again with the same input.

## Undoing an update
- **Branch bucket**: after a successful upload the script prints one restore command per changed run (an `aws s3 cp --recursive` then `aws s3 sync --delete` from `run_log_original/`). Run all of them to put the runs back as they were. **Run them before running the script again**: every run replaces `run_log_original/`, and restoring from a later copy puts the changed value back. Undoing does not stop a step function execution that was already started.
- **Production bucket (`sfc-main-datasets`)**: the bucket is versioned. No restore command is printed; restore the previous version of each changed run's parquet file from S3 object versions.

## Testing safely
Trial runs MUST use a branch dataset bucket, never production. CircleCI copies the production run log into each branch's dataset bucket (the `copy-main-data` job), so enter the branch bucket name at the Bucket prompt. Check that the bucket prompt shows the branch bucket before confirming. Each push wipes the branch run log again, so a branch run log used for selection tests (which needs several runs this month) has to be seeded again after CI.

## Notes
- `run_log_original/` and `run_log_updated/` are created in the repo root and ignored by git. Each run clears both first.
- The script never runs with changes unconfirmed: nothing is written to S3 until the upload `y`.
- To list the runs without changing anything, press Ctrl-C at the Flag prompt.
