# Runbook: mark a run as approved in the run log

## Purpose
Sets `approved = True` for one run in the job role estimates archive run log (the tracking table) on S3. Only `approved` is supported. Un-approving is not supported.

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

The script is a bash script, so run it from Git Bash.

## Steps
1. From the repo root, run:

```
bash scripts/update_run_log.sh
```

2. Answer the prompts:
   - **AWS profile**: the profile with access to the bucket (for example `prod`).
   - **Bucket**: press Enter for `sfc-main-datasets`, or type a branch bucket name.
   - **Run number**: a positive whole number, for example `12`.
   - **Flag**: `approved` (the only supported flag).
3. Check the four inputs shown and answer `y` (or `n` to cancel; nothing has been copied yet). Anything other than y/yes/n/no is asked again.
4. The script copies the run log to `run_log_original/` and makes the update in `run_log_updated/`. It prints the run's row **Before** and **After**.
5. Check that only `approved` changed (`False` to `True`) and that the run number and archive date are the ones you meant.
6. Answer `y` to upload. Only that run's folder is replaced on S3.

## If something is wrong
- **Run number not found, already approved, or flag unsupported**: the script stops with an error and uploads nothing. Fix the input and run it again.
- **You answer `n` at the upload prompt (or the update fails)**: the change is reverted. `run_log_updated/` is deleted, `run_log_original/` is left as it was, and nothing is uploaded.
- **Could not authenticate**: repeat the prerequisites (the MFA credentials have expired after 12 hours).
- **No run log found in the bucket**: check the bucket name. Nothing is uploaded.

## Undoing an update
- **Branch bucket**: after a successful upload the script prints a restore command (an `aws s3 sync ... --delete` from `run_log_original/`). Run it to put that run back as it was.
- **Production bucket (`sfc-main-datasets`)**: the bucket is versioned. No restore command is printed; restore the previous version of the run's parquet file from S3 object versions.

## Testing safely
Trial runs should use a branch dataset bucket, not production. CircleCI copies the production run log into each branch's dataset bucket (the `copy-main-data` job), so enter the branch bucket name at the Bucket prompt. Check that the bucket prompt shows the branch bucket before confirming.

## Notes
- `run_log_original/` and `run_log_updated/` are created in the repo root and ignored by git. Each run clears both first.
- The script never runs with changes unconfirmed: nothing is written to S3 until the final `y`.
