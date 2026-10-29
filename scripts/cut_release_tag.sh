#!/bin/bash
# Cuts the release tag for a selected, reconciled run. Needs git, uv, the AWS CLI
# and, outside a dry run, GITHUB_TOKEN. See update_run_log_runbook.md.
#
#   cut_release_tag.sh plan   Works out the run's commit SHA and the tag to cut, and
#                             checks the CHANGELOG release can be built. Writes the
#                             plan to $RELEASE_PLAN_FILE. Changes nothing.
#   cut_release_tag.sh cut    Re-works the plan, stops if it differs from the one
#                             that was approved, then in this order: pushes the
#                             annotated tag on the run's commit, writes the tag to
#                             the run log on S3, and pushes a branch and opens a PR
#                             that moves the commit's CHANGELOG entries under the
#                             tag. Each step skips work an earlier attempt did, so a
#                             failed run can be started again.
#
# Settings come from the environment, never from arguments spliced into commands:
#   RELEASE_RUN_NUMBER   the run to tag (required)
#   RELEASE_DRY_RUN      "true" prints the changing commands instead of running them
#   RELEASE_BUCKET       dataset bucket holding the run log (default sfc-main-datasets)
#   RELEASE_PLAN_FILE    where plan writes, and cut reads, the approved plan
set -euo pipefail

MODE="${1:-}"
RUN_NUMBER="${RELEASE_RUN_NUMBER:-}"
DRY_RUN="${RELEASE_DRY_RUN:-false}"
BUCKET="${RELEASE_BUCKET:-sfc-main-datasets}"
PLAN_FILE="${RELEASE_PLAN_FILE:-/tmp/release/plan.json}"
RUN_LOG_PREFIX="domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_run_log"
WORK_DIR="/tmp/release_work"
REPO="${CIRCLE_PROJECT_USERNAME:-}/${CIRCLE_PROJECT_REPONAME:-}"
CLI="uv run python -m scripts.cut_release_tag"

cd "$(dirname "${BASH_SOURCE[0]}")/.."

fail() {
  echo "Error: $*" >&2
  exit 1
}

[[ "$MODE" == "plan" || "$MODE" == "cut" ]] || fail "usage: cut_release_tag.sh plan|cut"
[[ "$RUN_NUMBER" =~ ^[0-9]+$ ]] || fail "RELEASE_RUN_NUMBER must be a whole number."
[[ "$DRY_RUN" == "true" || "$DRY_RUN" == "false" ]] || fail "RELEASE_DRY_RUN must be true or false."
[[ "$BUCKET" =~ ^sfc-[a-z0-9-]+-datasets$ ]] || fail "RELEASE_BUCKET '$BUCKET' is not a dataset bucket."
[[ "$REPO" =~ ^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+$ ]] || fail "Cannot work out the GitHub repository from CIRCLE_PROJECT_USERNAME and CIRCLE_PROJECT_REPONAME."

if [[ "$DRY_RUN" == "false" && "${CIRCLE_BRANCH:-}" != "main" ]]; then
  fail "A real release only runs from main (this is '${CIRCLE_BRANCH:-}'). Use a dry run to test."
fi

# Runs a command that changes something outside the job, or only prints it in a dry run.
change() {
  if [[ "$DRY_RUN" == "true" ]]; then
    echo "DRY RUN: $*"
  else
    "$@"
  fi
}

# Prints the value of one key of the plan JSON on stdin.
plan_value() {
  uv run python -c 'import json, sys; print(json.load(sys.stdin)[sys.argv[1]])' "$1"
}

# Git over HTTPS with the token in a header, so it is never in a URL, the remote
# config or the command line (git reads the header from its environment). Only
# called outside a dry run, when the token is required.
github_git() {
  local auth
  auth="$(printf 'x-access-token:%s' "$GITHUB_TOKEN" | base64 -w0)"
  GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0="http.https://github.com/.extraheader" \
    GIT_CONFIG_VALUE_0="Authorization: Basic $auth" git "$@"
}

# Calls the GitHub API for this repo and prints the response. The token goes to curl
# on stdin, so it is not in the process list. On failure the response body, which
# says why, is printed to stderr.
# Args: HTTP method, path under /repos/<repo>/, optional JSON body.
github_api() {
  local method="$1" path="$2" response
  local body_args=()
  [[ -z "${3:-}" ]] || body_args=(-d "$3")
  response="$(printf 'header = "Authorization: Bearer %s"\n' "$GITHUB_TOKEN" |
    curl --fail-with-body -sS -K - -X "$method" \
      -H "Accept: application/vnd.github+json" \
      ${body_args[@]+"${body_args[@]}"} \
      "https://api.github.com/repos/$REPO/$path")" || {
    echo "GitHub API $method $path failed: $response" >&2
    return 1
  }
  printf '%s' "$response"
}

# Works out the plan from S3 and the repo into $WORK_DIR/plan.json, and the
# CHANGELOG release into $WORK_DIR/CHANGELOG.new unless its branch already exists.
make_plan() {
  rm -rf "$WORK_DIR"
  mkdir -p "$WORK_DIR"

  echo "Copying the run log from s3://$BUCKET/ ..."
  aws s3 sync "s3://$BUCKET/$RUN_LOG_PREFIX/" "$WORK_DIR/run_log_original/" --only-show-errors

  # The commit's CHANGELOG, the ancestry check and the tag list need full history and tags.
  if [[ -f .git/shallow ]]; then
    git fetch --unshallow origin
  fi
  git fetch --force --tags origin
  git fetch --no-tags origin +refs/heads/main:refs/remotes/origin/main

  # `<tag> <commit>` for every tag; an annotated tag's commit is its peeled object.
  git for-each-ref refs/tags \
    --format='%(refname:lstrip=2) %(if)%(*objectname)%(then)%(*objectname)%(else)%(objectname)%(end)' \
    >"$WORK_DIR/tags"

  $CLI plan --run_log_dir "$WORK_DIR/run_log_original" --run_number "$RUN_NUMBER" \
    --tags_file "$WORK_DIR/tags" >"$WORK_DIR/plan.json"

  commit_sha="$(plan_value commit_sha <"$WORK_DIR/plan.json")"
  tag="$(plan_value tag <"$WORK_DIR/plan.json")"
  # A full SHA: shorter or ref-like values would never match the tag list.
  [[ "$commit_sha" =~ ^[0-9a-f]{40}$ ]] || fail "The run's commit_sha '$commit_sha' is not a full commit SHA."
  git merge-base --is-ancestor "$commit_sha" origin/main ||
    fail "Commit $commit_sha is not on main."

  # The branch name is at most 16 characters (it becomes a dev environment's name).
  # ls-remote exits 2 for "no such branch"; any other failure is not an answer.
  branch="rel-$tag"
  local status=0
  git ls-remote --exit-code --heads origin "$branch" >/dev/null 2>&1 || status=$?
  case "$status" in
    0) branch_exists=true ;;
    2) branch_exists=false ;;
    *) fail "Could not list the branches of origin." ;;
  esac

  # Built here so a CHANGELOG that cannot be released fails the plan, before the
  # tag is pushed.
  if [[ "$branch_exists" == "false" ]]; then
    git show "$commit_sha:CHANGELOG.md" >"$WORK_DIR/changelog_at_commit.md"
    git show origin/main:CHANGELOG.md >"$WORK_DIR/changelog_at_main.md"
    $CLI changelog --changelog_at_commit "$WORK_DIR/changelog_at_commit.md" \
      --changelog_at_main "$WORK_DIR/changelog_at_main.md" --tag "$tag" \
      --release_date "$(date +%Y-%m-%d)" --output "$WORK_DIR/CHANGELOG.new"
  fi
}

make_plan
archive_date="$(plan_value archive_date <"$WORK_DIR/plan.json")"
tag_message="$(plan_value tag_message <"$WORK_DIR/plan.json")"

echo
echo "Run          : $RUN_NUMBER (archived $archive_date)"
echo "Commit       : $commit_sha"
echo "Tag          : $tag"
echo "Tag message  : $tag_message"
echo "Run log      : s3://$BUCKET/$RUN_LOG_PREFIX/"
echo "CHANGELOG PR : branch $branch (already exists: $branch_exists)"
if [[ "$DRY_RUN" == "true" ]]; then
  echo "Dry run      : nothing will be changed"
fi

if [[ "$MODE" == "plan" ]]; then
  mkdir -p "$(dirname "$PLAN_FILE")"
  cp "$WORK_DIR/plan.json" "$PLAN_FILE"
  exit 0
fi

# cut: only do what was approved.
[[ -f "$PLAN_FILE" ]] || fail "No approved plan at $PLAN_FILE."
if ! cmp -s "$PLAN_FILE" "$WORK_DIR/plan.json"; then
  fail "The plan changed since it was approved. Start the workflow again. Approved: $(cat "$PLAN_FILE")"
fi
if [[ "$DRY_RUN" == "false" && -z "${GITHUB_TOKEN:-}" ]]; then
  fail "GITHUB_TOKEN is not set."
fi

# 1. The tag. An earlier attempt may already have pushed it.
if grep -qxF "$tag $commit_sha" "$WORK_DIR/tags"; then
  echo "Tag $tag is already on $commit_sha."
else
  git -c user.name="CircleCI release" -c user.email="noreply@circleci.com" \
    tag -a "$tag" "$commit_sha" -m "$tag_message"
  change github_git push "https://github.com/$REPO.git" "refs/tags/$tag"
fi

# 2. The run log. The partition is replaced the way update_run_log.sh replaces it:
# cp overwrites unconditionally (sync skips a same-sized file that isn't newer),
# then sync --delete removes the old parquet file.
$CLI record-tag --run_log_dir "$WORK_DIR/run_log_original" \
  --output_dir "$WORK_DIR/run_log_updated" --run_number "$RUN_NUMBER" --tag "$tag"
partition="archive_date=$archive_date/run_number=$RUN_NUMBER"
change aws s3 cp "$WORK_DIR/run_log_updated/$partition/" \
  "s3://$BUCKET/$RUN_LOG_PREFIX/$partition/" --recursive --only-show-errors
change aws s3 sync "$WORK_DIR/run_log_updated/$partition/" \
  "s3://$BUCKET/$RUN_LOG_PREFIX/$partition/" --delete --only-show-errors

# 3. The CHANGELOG branch, built in a separate working tree: checking out main here
# would replace this script and scripts/cut_release_tag.py with main's versions,
# which may not match (or exist).
if [[ "$branch_exists" == "true" ]]; then
  echo "Branch $branch already exists, so it is not pushed again."
else
  release_tree="$WORK_DIR/release_tree"
  git worktree prune
  git worktree add -B "$branch" "$release_tree" origin/main
  cp "$WORK_DIR/CHANGELOG.new" "$release_tree/CHANGELOG.md"
  git -C "$release_tree" add CHANGELOG.md
  git -C "$release_tree" -c user.name="CircleCI release" -c user.email="noreply@circleci.com" \
    commit -m "Release $tag - move released CHANGELOG entries under the version"
  change github_git -C "$release_tree" push "https://github.com/$REPO.git" "$branch"
fi

# 4. The PR. Decided by whether an open PR exists, not the branch: an earlier attempt
# may have pushed the branch and failed to open the PR. Closed PRs are ignored, so a
# PR closed to rebuild it does not stop a new one.
pull_request="$(uv run python -c '
import json, sys
tag, branch, sha = sys.argv[1:]
print(json.dumps({
    "title": "Release {}: CHANGELOG".format(tag),
    "head": branch,
    "base": "main",
    "body": "Moves the Unreleased entries present at {} under {}. Opened by the cut-release-tag workflow.".format(sha, tag),
}))
' "$tag" "$branch" "$commit_sha")"
if [[ "$DRY_RUN" == "true" ]]; then
  echo "DRY RUN: open a PR from $branch to main unless one is open: $pull_request"
else
  existing="$(github_api GET "pulls?head=${REPO%%/*}:$branch&state=open")"
  existing_count="$(printf '%s' "$existing" | uv run python -c 'import json, sys; print(len(json.load(sys.stdin)))')"
  if [[ "$existing_count" != "0" ]]; then
    echo "An open PR from $branch already exists."
  else
    github_api POST pulls "$pull_request" >/dev/null
    echo "Opened a PR from $branch to main."
  fi
fi
