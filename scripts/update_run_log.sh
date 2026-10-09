#!/bin/bash
# Marks one run as approved in the archive run log on S3.
#
# Copies the run log down, updates a local copy with update_run_log.py, shows the
# changed row, and replaces only that run's partition on S3. The original and
# updated copies are kept until you confirm the changed row; if you decline, the
# updated copy is deleted and nothing is uploaded.
#
# Prerequisites: see update_run_log_runbook.md (aws-mfa login, uv sync).
set -euo pipefail

MAIN_BUCKET="sfc-main-datasets"
RUN_LOG_PREFIX="domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_run_log"
SUPPORTED_FLAGS="approved"
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
    read -r -p "$1 [y/n]: " answer
    case "${answer,,}" in
      y | yes) return 0 ;;
      n | no) return 1 ;;
      *) echo "Please answer y or n." ;;
    esac
  done
}

for command_name in aws uv; do
  if ! command -v "$command_name" > /dev/null; then
    echo "Error: '$command_name' not found on PATH." >&2
    exit 1
  fi
done

# --- Inputs ---
profile=""
while [[ -z "$profile" ]]; do
  read -r -p "AWS profile (e.g. prod or non-prod): " profile
done

read -r -p "Bucket [$MAIN_BUCKET]: " bucket
bucket="${bucket:-$MAIN_BUCKET}"

run_number=""
until [[ "$run_number" =~ ^[1-9][0-9]*$ ]]; do
  read -r -p "Run number (positive whole number): " run_number
done

flag=""
until [[ " $SUPPORTED_FLAGS " == *" $flag "* && -n "$flag" ]]; do
  read -r -p "Flag to set to True ($SUPPORTED_FLAGS): " flag
done

echo
echo "AWS profile : $profile"
echo "Bucket      : $bucket"
echo "Run number  : $run_number"
echo "Flag        : $flag (will be set to True)"
if ! confirm "Are these correct?"; then
  echo "Cancelled. Nothing was copied or changed."
  exit 1
fi

# --- Copy down ---
if ! aws sts get-caller-identity --profile "$profile" > /dev/null; then
  echo "Error: could not authenticate with profile '$profile'. Run aws-mfa again." >&2
  exit 1
fi

rm -rf "$ORIGINAL_DIR" "$UPDATED_DIR"
aws s3 sync "s3://$bucket/$RUN_LOG_PREFIX/" "$ORIGINAL_DIR" --profile "$profile"

if [[ -z "$(find "$ORIGINAL_DIR" -name '*.parquet' -print -quit 2> /dev/null)" ]]; then
  echo "Error: no run log found at s3://$bucket/$RUN_LOG_PREFIX/. Nothing changed." >&2
  exit 1
fi

# --- Update the local copy ---
if ! uv run python scripts/update_run_log.py \
  --run_log_dir "$ORIGINAL_DIR" \
  --output_dir "$UPDATED_DIR" \
  --run_number "$run_number" \
  --flag "$flag"; then
  echo "Update rejected. Nothing was uploaded." >&2
  exit 1
fi

# --- Confirm the change ---
echo
if ! confirm "Upload this change to s3://$bucket/$RUN_LOG_PREFIX/?"; then
  echo "Reverted: the updated copy was deleted and nothing was uploaded." >&2
  exit 1
fi

# --- Replace the run's partition on S3 ---
partition_dir="$(find "$UPDATED_DIR" -type d -name "run_number=$run_number" -print -quit)"
partition="${partition_dir#"$UPDATED_DIR"/}"
aws s3 sync "$UPDATED_DIR/$partition/" "s3://$bucket/$RUN_LOG_PREFIX/$partition/" \
  --delete --profile "$profile"

echo
echo "Done: run $run_number now has $flag = True in s3://$bucket/."

# The main bucket is versioned, so a bad update there is fixed manually from S3 versions.
if [[ "$bucket" != "$MAIN_BUCKET" ]]; then
  echo "To undo this on the branch bucket, run:"
  echo "  aws s3 sync \"$ORIGINAL_DIR/$partition/\" \"s3://$bucket/$RUN_LOG_PREFIX/$partition/\" --delete --profile $profile"
fi
