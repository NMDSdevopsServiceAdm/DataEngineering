#!/bin/bash
# Sets a flag to True for one run in the archive run log on S3.
#
# Copies the run log down, lists this month's runs, updates a local copy with
# update_run_log.py, shows the changed rows, and replaces only the changed runs'
# partitions on S3. Selecting a run for publication moves the selection, so two
# partitions can change. The original and updated copies are kept until you
# confirm the changed rows; if you decline, the updated copy is deleted and
# nothing is uploaded. After a selection is uploaded you can start the bucket's
# Publication-Output state machine for the run.
#
# Prerequisites: see update_run_log_runbook.md (aws-mfa login, uv sync).
set -euo pipefail

MAIN_BUCKET="sfc-main-datasets"
RUN_LOG_PREFIX="domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_run_log"
SUPPORTED_FLAGS="approved selected_for_publication reconciled locally_checked"
SELECTION_FLAG="selected_for_publication"
ORIGINAL_DIR="run_log_original"
UPDATED_DIR="run_log_updated"

# Relative paths below are resolved from the repo root, wherever this is run from.
cd "$(dirname "${BASH_SOURCE[0]}")/.."

# The updated copy never outlives a run: it is removed on success and on any failure.
trap 'rm -rf "$UPDATED_DIR"' EXIT

# Asks until the answer is y/yes or n/no (any case); returns 0 for yes, 1 for no.
confirm() {
  local answer
  while true; do
    # Exits on end of input (Ctrl-D, piped stdin) instead of re-asking forever.
    read -r -p "$1 [y/n]: " answer || exit 1
    case "${answer,,}" in
      y | yes) return 0 ;;
      n | no) return 1 ;;
      *) echo "Please answer y or n." ;;
    esac
  done
}

# Starts the bucket's Publication-Output state machine for the selected run.
# Returns 0 if it started or you declined, 1 if it could not be started.
start_publication_output() {
  local workspace name arn input
  input="{\"run_number\": $run_number}"

  # Dataset buckets are sfc-<workspace>-datasets; the state machine is <workspace>-Publication-Output.
  if [[ ! "$bucket" =~ ^sfc-(.+)-datasets$ ]]; then
    echo "Cannot work out the Publication-Output state machine from bucket '$bucket'." >&2
    return 1
  fi
  workspace="${BASH_REMATCH[1]}"
  name="$workspace-Publication-Output"

  arn="$(aws stepfunctions list-state-machines --profile "$profile" \
    --query "stateMachines[?name=='$name'].stateMachineArn | [0]" --output text)" || {
    echo "Error: could not list state machines with profile '$profile'." >&2
    return 1
  }
  if [[ -z "$arn" || "$arn" == "None" ]]; then
    echo "State machine '$name' was not found. It may not be deployed yet." >&2
    return 1
  fi

  echo
  echo "State machine : $name"
  echo "                $arn"
  echo "Input         : $input"
  if ! confirm "Start it now?"; then
    echo "Not started. Start $name by hand with the input above."
    return 0
  fi

  if ! aws stepfunctions start-execution --state-machine-arn "$arn" \
    --input "$input" --profile "$profile" --query executionArn --output text; then
    echo "Error: could not start $name with profile '$profile'." >&2
    return 1
  fi
  echo "Started. The line above is the execution ARN."
}

for command_name in aws uv; do
  if ! command -v "$command_name" > /dev/null; then
    echo "Error: '$command_name' not found on PATH." >&2
    exit 1
  fi
done

# --- Where ---
profile=""
while [[ -z "$profile" ]]; do
  read -r -p "AWS profile (e.g. prod or non-prod): " profile
done

bucket=""
until [[ "$bucket" =~ ^[a-z0-9.-]+$ ]]; do
  read -r -p "Bucket name only, no s3:// or / [$MAIN_BUCKET]: " bucket
  bucket="${bucket:-$MAIN_BUCKET}"
done

# --- Copy down ---
if ! aws sts get-caller-identity --profile "$profile" > /dev/null; then
  echo "Error: could not authenticate with profile '$profile'. Run aws-mfa again." >&2
  exit 1
fi

rm -rf "$ORIGINAL_DIR" "$UPDATED_DIR"
if ! aws s3 sync "s3://$bucket/$RUN_LOG_PREFIX/" "$ORIGINAL_DIR" --profile "$profile"; then
  echo "Error: could not copy the run log from s3://$bucket/$RUN_LOG_PREFIX/." >&2
  echo "Check the bucket name exists and that profile '$profile' can read it. Nothing changed." >&2
  exit 1
fi

if [[ -z "$(find "$ORIGINAL_DIR" -name '*.parquet' -print -quit 2> /dev/null)" ]]; then
  echo "Error: no run log found at s3://$bucket/$RUN_LOG_PREFIX/. Nothing changed." >&2
  exit 1
fi

# --- This month's runs ---
echo
echo "Runs archived this month in s3://$bucket/:"
if ! uv run python -m scripts.update_run_log --run_log_dir "$ORIGINAL_DIR" --list; then
  echo "Error: could not list the runs. Nothing changed." >&2
  exit 1
fi

# --- What ---
echo
flag=""
until [[ " $SUPPORTED_FLAGS " == *" $flag "* && -n "$flag" ]]; do
  read -r -p "Flag to set to True ($SUPPORTED_FLAGS): " flag
done

run_number=""
until [[ "$run_number" =~ ^[1-9][0-9]*$ ]]; do
  read -r -p "Run number (positive whole number): " run_number
done

echo
echo "AWS profile : $profile"
echo "Bucket      : $bucket"
echo "Run number  : $run_number"
echo "Flag        : $flag (will be set to True)"
if ! confirm "Are these correct?"; then
  echo "Cancelled. Nothing was uploaded or changed."
  exit 1
fi

# --- Update the local copy ---
if ! uv run python -m scripts.update_run_log \
  --run_log_dir "$ORIGINAL_DIR" \
  --output_dir "$UPDATED_DIR" \
  --run_number "$run_number" \
  --flag "$flag"; then
  echo "Update rejected. Nothing was uploaded." >&2
  exit 1
fi

# The changed partitions, e.g. archive_date=2026-10-08/run_number=3. An empty or
# unexpected partition would aim the delete-capable sync at the wrong prefix.
partitions=()
for partition_dir in "$UPDATED_DIR"/archive_date=*/run_number=*/; do
  partition="${partition_dir#"$UPDATED_DIR"/}"
  partition="${partition%/}"
  if [[ ! "$partition" =~ ^archive_date=[^/]+/run_number=[1-9][0-9]*$ ]]; then
    echo "Error: unexpected updated partition '$partition'. Nothing was uploaded." >&2
    exit 1
  fi
  partitions+=("$partition")
done
if [[ ${#partitions[@]} -eq 0 ]]; then
  echo "Error: no updated partitions found. Nothing was uploaded." >&2
  exit 1
fi

# --- Confirm the change ---
echo
if ! confirm "Upload ${#partitions[@]} changed run(s) to s3://$bucket/$RUN_LOG_PREFIX/?"; then
  echo "Reverted: the updated copy was deleted and nothing was uploaded." >&2
  exit 1
fi

# --- Replace the changed partitions on S3 ---
uploaded=()
for partition in "${partitions[@]}"; do
  if ! aws s3 sync "$UPDATED_DIR/$partition/" "s3://$bucket/$RUN_LOG_PREFIX/$partition/" \
    --delete --profile "$profile"; then
    echo "Error: the upload to s3://$bucket/ failed, so the run log may be unchanged or partly updated." >&2
    echo "Uploaded before the failure: ${uploaded[*]:-none}. Failed: $partition." >&2
    echo "Check profile '$profile' can write to the bucket, then check these runs in S3 before retrying." >&2
    exit 1
  fi
  uploaded+=("$partition")
done

echo
echo "Done: $flag is now True for run $run_number in s3://$bucket/."
if [[ ${#partitions[@]} -gt 1 ]]; then
  echo "The selection moved, so these runs were changed: ${partitions[*]}"
fi

# The main bucket is versioned, so a bad update there is fixed manually from S3 versions.
if [[ "$bucket" != "$MAIN_BUCKET" ]]; then
  # cp overwrites unconditionally: sync alone skips a same-sized file that isn't newer than S3's.
  echo "To undo this on the branch bucket, run these before running the script again (each run replaces $ORIGINAL_DIR):"
  for partition in "${partitions[@]}"; do
    restore_source="$ORIGINAL_DIR/$partition/"
    restore_target="s3://$bucket/$RUN_LOG_PREFIX/$partition/"
    echo "  aws s3 cp \"$restore_source\" \"$restore_target\" --recursive --profile $profile && aws s3 sync \"$restore_source\" \"$restore_target\" --delete --profile $profile"
  done
fi

# --- Start the public output for a selected run ---
if [[ "$flag" == "$SELECTION_FLAG" ]]; then
  if ! start_publication_output; then
    echo "The selection is saved. Start Publication-Output by hand with {\"run_number\": $run_number}." >&2
    exit 1
  fi
fi
