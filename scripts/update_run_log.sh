#!/bin/bash
# Sets a flag to True for one run in the archive run log on S3.
#
# Copies the run log down, lists this month's runs, updates a local copy with
# update_run_log.py, shows the changed rows, and after you confirm replaces only
# the changed runs' partitions on S3; declining deletes the updated copy and
# uploads nothing. Selecting a run for publication can change two runs, and
# automatically copies it into the long-term archive bucket, offers to cut the
# release tag for it (cut_release_tag.sh; a dry run for any bucket but production),
# then offers to start the bucket's Publication-Output state machine for it. The tag
# comes first so its run log write is done before Publication-Output can rewrite the
# same row.
#
# Prerequisites: see update_run_log_runbook.md (aws-mfa login, uv sync).
set -euo pipefail

MAIN_BUCKET="sfc-main-datasets"
DEFAULT_PROFILE="run-log-updater"
ESTIMATES_PREFIX="domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates"
METADATA_PREFIX="domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_metadata"
RUN_LOG_PREFIX="domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_run_log"
SUPPORTED_FLAGS="approved selected_for_publication reconciled locally_checked"
SELECTION_FLAG="selected_for_publication"
ORIGINAL_DIR="run_log_original"
UPDATED_DIR="run_log_updated"
PROMOTED_DIR="run_log_promoted"

# Relative paths below are resolved from the repo root, wherever this is run from.
cd "$(dirname "${BASH_SOURCE[0]}")/.."

# Neither copy outlives a run: both are removed on success and on any failure.
trap 'rm -rf "$UPDATED_DIR" "$PROMOTED_DIR"' EXIT

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

# Prints the commands that put the given runs' partitions back as they were. The main
# bucket is versioned, so a bad update there is fixed manually from S3 versions.
print_restore_commands() {
  [[ "$bucket" != "$MAIN_BUCKET" ]] || return 0
  local partition restore_source restore_target
  echo "To undo this on the branch bucket, run these before running the script again (each run replaces $ORIGINAL_DIR):"
  for partition in "$@"; do
    restore_source="$ORIGINAL_DIR/$partition/"
    restore_target="s3://$bucket/$RUN_LOG_PREFIX/$partition/"
    # cp overwrites unconditionally: sync alone skips a same-sized file that isn't newer than S3's.
    echo "  aws s3 cp \"$restore_source\" \"$restore_target\" --recursive --profile $profile && aws s3 sync \"$restore_source\" \"$restore_target\" --delete --profile $profile"
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

  # The listing is paged and --query runs per page, so filter here, not in the query.
  arn="$(aws stepfunctions list-state-machines --profile "$profile" \
    --query "stateMachines[].[name,stateMachineArn]" --output text |
    awk -v name="$name" '$1 == name { print $2 }')" || {
    echo "Error: could not list state machines with profile '$profile'." >&2
    return 1
  }
  if [[ -z "$arn" ]]; then
    echo "State machine '$name' was not found. It may not be deployed yet." >&2
    return 1
  fi
  if [[ "$arn" == *$'\n'* ]]; then
    echo "Error: more than one state machine is named '$name'." >&2
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

# Shows the release tag cut_release_tag.sh would cut for the selected run, and after
# you confirm cuts it. Any bucket but the production one only dry-runs.
# Returns 0 if it was cut or you declined, 1 if it could not be.
cut_release_tag() {
  local dry_run=true question="Dry run the release tag now?"
  if [[ "$bucket" == "$MAIN_BUCKET" ]]; then
    dry_run=false
    question="Cut the release tag now?"
  fi
  local release_env=(RELEASE_RUN_NUMBER="$run_number" RELEASE_BUCKET="$bucket"
    RELEASE_DRY_RUN="$dry_run" AWS_PROFILE="$profile")

  echo
  if ! env "${release_env[@]}" bash scripts/cut_release_tag.sh plan; then
    echo "Error: could not work out the release tag with profile '$profile'." >&2
    return 1
  fi
  echo
  if ! confirm "$question"; then
    echo "Not cut. Cut it by hand, only while no Publication-Output execution is finishing:"
    echo "  ${release_env[*]} bash scripts/cut_release_tag.sh plan"
    echo "  ${release_env[*]} bash scripts/cut_release_tag.sh cut"
    return 0
  fi
  if ! env "${release_env[@]}" bash scripts/cut_release_tag.sh cut; then
    echo "Error: the release tag was not fully cut. It is safe to run it again." >&2
    return 1
  fi
}

# Prints how many objects are under an S3 prefix. aws s3 ls exits 1 when nothing
# matches, which is a count of 0, not an error; any other failure is returned.
count_objects() {
  local listing status=0
  listing="$(aws s3 ls --recursive "$1" --profile "$profile")" || status=$?
  if [[ $status -ne 0 && $status -ne 1 ]]; then
    return "$status"
  fi
  if [[ -z "$listing" ]]; then
    echo 0
  else
    printf '%s\n' "$listing" | wc -l
  fi
}

# Marks the selected run promoted, then copies its estimates and metadata
# partitions from $bucket into the long-term archive bucket. Runs automatically
# after a successful selection; never prompts. Returns 0 on success, 1 on any
# failure; a failure before the copy loop copies nothing.
copy_run_to_long_term_archive() {
  local destination_bucket partition archive_date prefix
  local source_count destination_count partitions

  # Dataset buckets are sfc-<workspace>-datasets; the long-term bucket is sfc-<workspace>-archive-promoted.
  if [[ ! "$bucket" =~ ^sfc-(.+)-datasets$ ]]; then
    echo "Error: cannot work out the long-term archive bucket from bucket '$bucket'." >&2
    return 1
  fi
  destination_bucket="sfc-${BASH_REMATCH[1]}-archive-promoted"

  # --- Mark the run promoted (same mechanic as every other flag) ---
  if ! uv run python -m scripts.update_run_log \
    --run_log_dir "$UPDATED_DIR" \
    --output_dir "$PROMOTED_DIR" \
    --run_number "$run_number" \
    --flag promoted; then
    echo "Error: could not mark run $run_number as promoted. Nothing was copied." >&2
    return 1
  fi

  # Exactly one partition: promoting has no "clear other runs" side effect.
  partitions=()
  for partition_dir in "$PROMOTED_DIR"/archive_date=*/run_number=*/; do
    partition="${partition_dir#"$PROMOTED_DIR"/}"
    partition="${partition%/}"
    if [[ ! "$partition" =~ ^archive_date=[^/]+/run_number=[1-9][0-9]*$ ]]; then
      echo "Error: unexpected promoted partition '$partition'. Nothing was uploaded or copied." >&2
      return 1
    fi
    partitions+=("$partition")
  done
  if [[ ${#partitions[@]} -ne 1 ]]; then
    echo "Error: expected exactly one promoted partition, found ${#partitions[@]} (${partitions[*]:-none}). Nothing was uploaded or copied." >&2
    return 1
  fi
  partition="${partitions[0]}"
  [[ "$partition" =~ ^archive_date=([^/]+)/run_number=[1-9][0-9]*$ ]]
  archive_date="${BASH_REMATCH[1]}"

  if ! aws s3 sync "$PROMOTED_DIR/$partition/" "s3://$bucket/$RUN_LOG_PREFIX/$partition/" \
    --delete --profile "$profile"; then
    echo "Error: could not upload the promoted run log partition to s3://$bucket/." >&2
    print_restore_commands "$partition" >&2
    return 1
  fi

  # --- Copy estimates, then metadata, into the long-term bucket ---
  # Metadata goes last: it is one file, so its partition appearing is what
  # Publication-Output waits for as the sign the copy has finished.
  for prefix in "$ESTIMATES_PREFIX" "$METADATA_PREFIX"; do
    if ! source_count="$(count_objects "s3://$bucket/$prefix/$partition/")"; then
      echo "Error: could not list s3://$bucket/$prefix/$partition/. Nothing copied for this dataset." >&2
      return 1
    fi
    if [[ "$source_count" -eq 0 ]]; then
      echo "Error: nothing found at s3://$bucket/$prefix/$partition/. Nothing copied for this dataset." >&2
      return 1
    fi

    if ! aws s3 cp "s3://$bucket/$prefix/$partition/" "s3://$destination_bucket/$prefix/$partition/" \
      --recursive --only-show-errors --profile "$profile"; then
      echo "Error: could not copy $prefix/$partition to s3://$destination_bucket/. Check profile '$profile' can write there. Retry by hand:" >&2
      echo "  aws s3 cp \"s3://$bucket/$prefix/$partition/\" \"s3://$destination_bucket/$prefix/$partition/\" --recursive --profile $profile" >&2
      return 1
    fi

    if ! destination_count="$(count_objects "s3://$destination_bucket/$prefix/$partition/")"; then
      echo "Error: could not list s3://$destination_bucket/$prefix/$partition/ to verify the copy. Retry by hand:" >&2
      echo "  aws s3 cp \"s3://$bucket/$prefix/$partition/\" \"s3://$destination_bucket/$prefix/$partition/\" --recursive --profile $profile" >&2
      return 1
    fi
    if [[ "$destination_count" -eq 0 || "$destination_count" != "$source_count" ]]; then
      echo "Error: copy of $prefix/$partition looks incomplete ($destination_count of $source_count file(s) at the destination). Retry by hand:" >&2
      echo "  aws s3 cp \"s3://$bucket/$prefix/$partition/\" \"s3://$destination_bucket/$prefix/$partition/\" --recursive --profile $profile" >&2
      return 1
    fi
    echo "Copied $destination_count object(s) for $prefix/$partition."
  done

  echo "Promoted run $run_number (archived $archive_date) to s3://$destination_bucket/."
}

for command_name in aws uv; do
  if ! command -v "$command_name" > /dev/null; then
    echo "Error: '$command_name' not found on PATH." >&2
    exit 1
  fi
done

# --- Where ---
read -r -p "AWS profile (non-prod for a branch bucket) [$DEFAULT_PROFILE]: " profile
profile="${profile:-$DEFAULT_PROFILE}"

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
    # The failed run's folder may be half written, so it is restored too.
    print_restore_commands "${uploaded[@]+"${uploaded[@]}"}" "$partition" >&2
    exit 1
  fi
  uploaded+=("$partition")
done

echo
echo "Done: $flag is now True for run $run_number in s3://$bucket/."
if [[ ${#partitions[@]} -gt 1 ]]; then
  echo "The selection moved, so these runs were changed: ${partitions[*]}"
fi

print_restore_commands "${partitions[@]}"

# --- Promote the selected run, cut its release tag, then start the public output for it ---
if [[ "$flag" == "$SELECTION_FLAG" ]]; then
  if ! copy_run_to_long_term_archive; then
    echo "The selection is saved, but promotion did not complete. See update_run_log_runbook.md to finish it by hand." >&2
    exit 1
  fi

  # The tag goes first: it rewrites the run's run log row, which Publication-Output
  # rewrites when it marks the run reconciled. Neither depends on the other, so both
  # are always attempted.
  steps_failed=false
  if ! cut_release_tag; then
    echo "The selection is saved. Cut the release tag by hand once Publication-Output has finished: see \"Cutting the release tag\" in update_run_log_runbook.md." >&2
    steps_failed=true
  fi
  if ! start_publication_output; then
    echo "The selection is saved. Start Publication-Output by hand with {\"run_number\": $run_number}." >&2
    steps_failed=true
  fi
  [[ "$steps_failed" == "false" ]] || exit 1
fi
