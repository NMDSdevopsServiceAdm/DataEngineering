#!/bin/bash
# Cuts the release tag for a run selected for publication, from your own machine.
# Needs git, uv, the AWS CLI and, outside a dry run, gh (signed in) and a git
# login that can push to origin. update_run_log.sh runs this after a selection;
# see update_run_log_runbook.md for running it by hand.
#
#   cut_release_tag.sh plan   Works out the run's commit SHA and the tag to cut, and
#                             checks the CHANGELOG release can be built. Writes the
#                             plan to $RELEASE_PLAN_FILE. Changes nothing.
#   cut_release_tag.sh cut    Re-works the plan, stops if it differs from the one
#                             that was shown, then in this order: pushes the
#                             annotated tag on the run's commit, writes the tag to
#                             the run log on S3, and pushes a branch and opens a PR
#                             that moves the commit's CHANGELOG entries under the
#                             tag. Each step skips work an earlier attempt did, so a
#                             failed run can be started again.
#
# Settings come from the environment, never from arguments spliced into commands:
#   RELEASE_RUN_NUMBER   the run to tag (required)
#   RELEASE_DRY_RUN      "false" makes the changes; anything else prints the changing
#                        commands instead (default true)
#   RELEASE_BUCKET       dataset bucket holding the run log (default sfc-main-datasets);
#                        a real release needs sfc-main-datasets
#   RELEASE_PLAN_FILE    where plan writes, and cut reads, the plan
#   AWS_PROFILE          the AWS profile for the run log
set -euo pipefail

MODE="${1:-}"
RUN_NUMBER="${RELEASE_RUN_NUMBER:-}"
DRY_RUN="${RELEASE_DRY_RUN:-true}"
BUCKET="${RELEASE_BUCKET:-sfc-main-datasets}"
MAIN_BUCKET="sfc-main-datasets"
REPO="NMDSdevopsServiceAdm/DataEngineering"
SCRATCH="${TMPDIR:-/tmp}"
PLAN_FILE="${RELEASE_PLAN_FILE:-$SCRATCH/release_plan.json}"
RUN_LOG_PREFIX="domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_run_log"
WORK_DIR="$SCRATCH/release_work"
CLI="uv run python -m scripts.cut_release_tag"

cd "$(dirname "${BASH_SOURCE[0]}")/.."

fail() {
  echo "Error: $*" >&2
  exit 1
}

[[ "$MODE" == "plan" || "$MODE" == "cut" ]] || fail "usage: cut_release_tag.sh plan|cut"
[[ "$RUN_NUMBER" =~ ^[0-9]+$ ]] || fail "RELEASE_RUN_NUMBER must be a whole number."
[[ "$BUCKET" =~ ^sfc-[a-z0-9-]+-datasets$ ]] || fail "RELEASE_BUCKET '$BUCKET' is not a dataset bucket."
[[ "$DRY_RUN" == "false" ]] || DRY_RUN="true"

for command_name in git uv aws; do
  command -v "$command_name" >/dev/null || fail "'$command_name' not found on PATH."
done

# A real release pushes to GitHub, so it needs the production bucket, origin to be
# this repository (not a fork) and gh.
if [[ "$DRY_RUN" == "false" ]]; then
  [[ "$BUCKET" == "$MAIN_BUCKET" ]] || fail "A real release needs RELEASE_BUCKET=$MAIN_BUCKET (this is '$BUCKET'). Use a dry run to test."
  command -v gh >/dev/null || fail "'gh' not found on PATH."
  origin_url="$(git remote get-url origin)"
  [[ "$origin_url" =~ github\.com[:/]$REPO(\.git)?$ ]] || fail "origin is '$origin_url', not $REPO."
fi

# Runs a command that changes something outside this machine's scratch folder, or
# only prints it in a dry run.
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

# Removes the working tree and local branch the CHANGELOG branch was built in, so
# none is left behind. Only a branch this run created is deleted.
created_local_branch=false
cleanup_release_tree() {
  git worktree remove --force "$WORK_DIR/release_tree" 2>/dev/null || true
  git worktree prune
  if [[ "$created_local_branch" == "true" ]]; then
    git branch -D "$branch" >/dev/null 2>&1 || true
  fi
}
trap cleanup_release_tree EXIT

# Works out the plan from S3 and the repo into $WORK_DIR/plan.json, and the
# CHANGELOG release into $WORK_DIR/CHANGELOG.new unless its branch already exists.
make_plan() {
  rm -rf "$WORK_DIR"
  mkdir -p "$WORK_DIR"

  echo "Copying the run log from s3://$BUCKET/ ..."
  aws s3 sync "s3://$BUCKET/$RUN_LOG_PREFIX/" "$WORK_DIR/run_log_original/" --only-show-errors

  # `<tag> <commit>` for every tag on origin; an annotated tag's commit is its
  # peeled (`^{}`) line. Read from origin so local tags are never changed.
  local listing
  listing="$(git ls-remote --tags origin)" || fail "Could not list the tags of origin."
  printf '%s\n' "$listing" | awk '
    NF == 2 {
      name = $2
      sub("^refs/tags/", "", name)
      peeled = (name ~ /\^\{\}$/)
      sub(/\^\{\}$/, "", name)
      if (peeled || !(name in commit)) commit[name] = $1
    }
    END { for (name in commit) print name, commit[name] }
  ' >"$WORK_DIR/tags"

  git fetch --no-tags origin +refs/heads/main:refs/remotes/origin/main

  $CLI plan --run_log_dir "$WORK_DIR/run_log_original" --run_number "$RUN_NUMBER" \
    --tags_file "$WORK_DIR/tags" >"$WORK_DIR/plan.json"

  commit_sha="$(plan_value commit_sha <"$WORK_DIR/plan.json")"
  tag="$(plan_value tag <"$WORK_DIR/plan.json")"
  # A full SHA: shorter or ref-like values would never match the tag list.
  [[ "$commit_sha" =~ ^[0-9a-f]{40}$ ]] || fail "The run's commit_sha '$commit_sha' is not a full commit SHA."
  git cat-file -e "$commit_sha^{commit}" 2>/dev/null || fail "Commit $commit_sha is not in this clone."
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

if [[ "$MODE" == "plan" ]]; then
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
  mkdir -p "$(dirname "$PLAN_FILE")"
  cp "$WORK_DIR/plan.json" "$PLAN_FILE"
  exit 0
fi

# cut: only do what plan showed.
[[ -f "$PLAN_FILE" ]] || fail "No plan at $PLAN_FILE. Run plan first."
if ! cmp -s "$PLAN_FILE" "$WORK_DIR/plan.json"; then
  fail "The plan changed since it was shown. Run plan again. Shown: $(cat "$PLAN_FILE")"
fi

echo "Cutting $tag on $commit_sha"

# 1. The tag. An earlier attempt may already have pushed it, or created it here
# without pushing it.
if grep -qxF "$tag $commit_sha" "$WORK_DIR/tags"; then
  echo "Tag $tag is already on $commit_sha."
else
  local_commit="$(git rev-parse -q --verify "refs/tags/$tag^{commit}" || true)"
  if [[ -z "$local_commit" ]]; then
    change git tag -a "$tag" "$commit_sha" -m "$tag_message"
  elif [[ "$local_commit" != "$commit_sha" ]]; then
    fail "A local tag $tag already points at $local_commit, not $commit_sha. Delete it with git tag -d $tag."
  fi
  change git push origin "refs/tags/$tag"
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

# 3. The CHANGELOG branch, built in a separate working tree so this checkout is
# never switched or changed.
if [[ "$branch_exists" == "true" ]]; then
  echo "Branch $branch already exists, so it is not pushed again."
elif [[ "$DRY_RUN" == "true" ]]; then
  echo "DRY RUN: commit $WORK_DIR/CHANGELOG.new as CHANGELOG.md on $branch from origin/main and git push origin $branch"
else
  release_tree="$WORK_DIR/release_tree"
  git worktree prune
  if git show-ref --verify --quiet "refs/heads/$branch"; then
    fail "A local branch $branch already exists. Delete it with git branch -D $branch."
  fi
  git worktree add --no-track -b "$branch" "$release_tree" origin/main
  created_local_branch=true
  cp "$WORK_DIR/CHANGELOG.new" "$release_tree/CHANGELOG.md"
  git -C "$release_tree" add CHANGELOG.md
  git -C "$release_tree" commit -m "Release $tag - move released CHANGELOG entries under the version"
  git -C "$release_tree" push origin "$branch"
fi

# 4. The PR. Decided by whether an open PR exists, not the branch: an earlier attempt
# may have pushed the branch and failed to open the PR. Closed PRs are ignored, so a
# PR closed to rebuild it does not stop a new one.
pr_title="Release $tag: CHANGELOG"
pr_body="Moves the Unreleased entries present at $commit_sha under $tag. Opened by cut_release_tag.sh."
if [[ "$DRY_RUN" == "true" ]]; then
  echo "DRY RUN: open a PR from $branch to main unless one is open: $pr_title - $pr_body"
else
  open_prs="$(gh pr list --repo "$REPO" --head "$branch" --state open --json number --jq length)"
  if [[ "$open_prs" != "0" ]]; then
    echo "An open PR from $branch already exists."
  else
    gh pr create --repo "$REPO" --base main --head "$branch" --title "$pr_title" --body "$pr_body"
  fi
fi
echo "Done."
