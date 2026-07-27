#!/usr/bin/env bash
#
# Give a freshly created git worktree a working Python environment.
#
# Pipenv keys its virtualenv to the project *directory*, so a new worktree does
# not inherit the main checkout's environment. Left unprovisioned, the first
# `pipenv run ...` in that worktree doesn't fail loudly — it silently creates a
# new, empty venv built from whatever interpreter pyenv defaults to, and ordinary
# third-party imports then fail with ModuleNotFoundError. That reads like a broken
# code change rather than a missing environment, and has cost real debugging time.
#
# By default this script points the worktree at the main checkout's existing venv
# (one line in a gitignored .env, which pipenv reads before resolving the venv).
# That is safe because the venv contains no repo code — nothing is installed
# editable, so imports still resolve against whichever worktree you run from.
# Pipfile and Pipfile.lock change in well under 1% of commits here, so sharing is
# nearly always right, and it avoids a ~1.2GB, several-minute install per worktree.
#
# If this worktree's Pipfile or Pipfile.lock differs from the main checkout's,
# sharing would give you the wrong dependencies, so an isolated venv is built
# instead. Pass --isolated to force that. Re-run this script if either file
# changes later (after a rebase, say) — nothing re-checks on your behalf.
#
# Sharing has one consequence worth knowing: from inside a worktree, `pipenv --rm`,
# `install`, `uninstall` and `clean` all act on the *shared* venv, so they affect
# every other worktree and the main checkout. See SETUP.md.
#
# Note also that pipenv loads .env with override=True, so the name set here beats
# the surrounding shell environment and is inherited by pytest/python subprocesses.
#
# Run from the root of the new worktree:
#
#   bash scripts/setup_worktree.sh
#
set -euo pipefail

info() { printf '  %s\n' "$*"; }
warn() { printf 'warning: %s\n' "$*" >&2; }
die() {
    printf 'error: %s\n' "$*" >&2
    exit 1
}

# Read `key = value` from an ini-style file, dropping CRLF line endings and the
# quotes around a TOML string (pyvenv.cfg is unquoted, the Pipfile's version isn't).
read_cfg_value() {
    awk -v key="$2" \
        'index($0, key " = ") == 1 { print substr($0, length(key) + 4); exit }' \
        "$1" | tr -d '\r"'
}

# Set PIPENV_CUSTOM_VENV_NAME in $1, preserving everything else in the file.
#
# Deliberately avoids interpolating the name into a sed program: an `&` in the
# replacement expands to the whole match (silently corrupting the value) and a `|`
# ends the s/// delimiter. The mktemp template keeps the file on the same
# filesystem, so the mv is an atomic rename and the contents never transit /tmp.
write_env_file() {
    local env_file="$1" env_line="PIPENV_CUSTOM_VENV_NAME=$2" tmp verb
    tmp="$(mktemp "$env_file.XXXXXX")"
    trap 'rm -f "$tmp" "$tmp.filtered"' EXIT

    if [ -f "$env_file" ]; then
        # Normalise CRLF rather than preserving it: the line we append is LF, and
        # mixing the two leaves a stray \r inside the preceding variable's value.
        # (Git Bash's grep strips \r and Linux's doesn't, so without this the two
        # platforms disagree about what this function does.)
        tr -d '\r' <"$env_file" >"$tmp"
        # A final line with no newline would otherwise be joined to ours, both
        # destroying its value and leaving PIPENV_CUSTOM_VENV_NAME unset.
        if [ -s "$tmp" ] && [ -n "$(tail -c1 "$tmp")" ]; then
            printf '\n' >>"$tmp"
        fi
        # Drop any previous setting, tolerating `export ` and leading whitespace.
        grep -vE '^[[:space:]]*(export[[:space:]]+)?PIPENV_CUSTOM_VENV_NAME=' \
            "$tmp" >"$tmp.filtered" || true
        mv "$tmp.filtered" "$tmp"
        chmod --reference="$env_file" "$tmp" 2>/dev/null || true
        verb="updated"
    else
        verb="wrote"
    fi

    printf '%s\n' "$env_line" >>"$tmp"
    mv "$tmp" "$env_file"
    trap - EXIT
    info "$(printf '%-15s' "$verb").env"
}

# ------------------------------------------------------------------- arguments

force_isolated=false
while [ $# -gt 0 ]; do
    case "$1" in
        --isolated) force_isolated=true ;;
        -h | --help)
            sed -n '/^# Give a freshly/,/^# *bash scripts/p' "$0" |
                sed 's/^# \{0,1\}//'
            exit 0
            ;;
        --write-env-only)
            # Set the given name in ./.env and do nothing else, so tests can
            # exercise the merge without building a virtualenv.
            [ $# -ge 2 ] || die "--write-env-only needs a virtualenv name"
            write_env_file "$PWD/.env" "$2"
            exit 0
            ;;
        *)
            echo "error: unknown argument '$1' (try --help)" >&2
            exit 2
            ;;
    esac
    shift
done

# An active virtualenv takes precedence over PIPENV_CUSTOM_VENV_NAME unless
# PIPENV_ACTIVE is set, and neither `source .../activate` nor VS Code's automatic
# activation sets it. Left alone, that would pin .env to the wrong venv, make the
# verify step pass without exercising the .env we just wrote, and — in isolated
# mode — reach a default-yes pipenv prompt that deletes the active venv.
if [ -n "${VIRTUAL_ENV:-}" ]; then
    warn "ignoring the virtualenv active in this shell ($VIRTUAL_ENV) — VS Code may still be pointing at it, so reopen the terminal if imports look wrong afterwards"
fi
export PIPENV_IGNORE_VIRTUALENVS=1
unset VIRTUAL_ENV

# ---------------------------------------------------------------- locate things

git rev-parse --is-inside-work-tree >/dev/null 2>&1 ||
    die "not inside a git working tree"

worktree_root="$(git rev-parse --show-toplevel)"

# The first entry of `git worktree list` is always the main working tree.
main_checkout="$(git worktree list --porcelain |
    awk '/^worktree /{print substr($0, 10); exit}')"

[ -n "$main_checkout" ] && [ -f "$main_checkout/Pipfile" ] ||
    die "could not locate the main checkout (no Pipfile at '$main_checkout')"

if [ "$worktree_root" = "$main_checkout" ]; then
    die "this is the main checkout, not a worktree — set it up with 'pipenv install --dev'"
fi

echo "Setting up worktree environment"
info "worktree:      $worktree_root"
info "main checkout: $main_checkout"

# ------------------------------------------------------- pick shared vs isolated

# Compare the actual files rather than a git ref: the shared venv reflects the
# main checkout's working tree, which is what we'd be borrowing. Check existence
# first — `cmp -s` reports a missing file the same way it reports a differing one,
# which would silently mean a multi-minute isolated build for the wrong reason.
for f in Pipfile Pipfile.lock; do
    [ -f "$worktree_root/$f" ] || die "this worktree has no $f"
    [ -f "$main_checkout/$f" ] ||
        die "the main checkout has no $f — run 'pipenv install --dev' there first"
done

deps_match=true
for f in Pipfile Pipfile.lock; do
    cmp -s "$worktree_root/$f" "$main_checkout/$f" || deps_match=false
done

if [ "$force_isolated" = true ]; then
    isolated=true
    info "mode:          isolated (--isolated)"
elif [ "$deps_match" = false ]; then
    isolated=true
    info "mode:          isolated (Pipfile/Pipfile.lock differ from main checkout)"
else
    isolated=false
    info "mode:          shared with main checkout"
fi

# ------------------------------------------------------------- resolve venv name

# Discovered rather than hardcoded, so this keeps working if the main checkout's
# venv is ever removed and rebuilt under a different hash suffix. Resolve this
# before exporting PIPENV_CUSTOM_VENV_NAME below, or it resolves to itself.
if main_venv_path="$(cd "$main_checkout" && pipenv --venv 2>&1)"; then
    # tail: don't let anything pipenv prints ahead of the path into the name.
    main_venv_path="$(printf '%s\n' "$main_venv_path" | tail -n1 | tr -d '\r')"
    main_venv_diag=""
else
    main_venv_diag="$main_venv_path"
    main_venv_path=""
fi

if [ -z "$main_venv_path" ]; then
    [ "$isolated" = true ] ||
        die "no virtualenv found for the main checkout — run 'pipenv install --dev' there first. pipenv said: $main_venv_diag"
    warn "the main checkout has no virtualenv, so its interpreter can't be reused"
fi

if [ "$isolated" = true ]; then
    # The worktree directory is conventionally already named DataEngineering-<slug>,
    # so strip that prefix before adding it back rather than doubling it. Note two
    # worktrees sharing a basename under different parents map to one venv.
    worktree_dir="${worktree_root##*[/\\]}"
    venv_name="DataEngineering-${worktree_dir#DataEngineering-}"
else
    # basename, tolerating either separator (pipenv prints native Windows paths).
    venv_name="${main_venv_path##*[/\\]}"
fi

info "virtualenv:    $venv_name"

# Exported rather than written to .env up front: if the build below fails, .env
# must not be left pinning the worktree to a half-populated venv — that is exactly
# the misleading ModuleNotFoundError this script exists to prevent. pipenv reads
# the variable from the environment as readily as from .env, so the file is only
# needed for later shells, and is written once everything has been verified.
#
# DONT_LOAD_ENV matters for the same reason: pipenv loads .env with override=True,
# so a .env left by an earlier run would win over the name resolved just above.
# Re-running --isolated in a worktree already pointing at the shared venv would
# then "reuse" that venv and sync this branch's dependencies straight into it.
export PIPENV_CUSTOM_VENV_NAME="$venv_name"
export PIPENV_DONT_LOAD_ENV=1

# ------------------------------------------------------------ build if isolated

if [ "$isolated" = true ]; then
    if existing_venv="$(cd "$worktree_root" && pipenv --venv 2>/dev/null)" &&
        [ -f "$existing_venv/pyvenv.cfg" ]; then
        # `pipenv --python` deletes and rebuilds an existing venv, so re-running
        # after a failed sync would otherwise cost the whole install again.
        info "reusing        existing isolated virtualenv"
    else
        if [ -n "$main_venv_path" ] && [ -f "$main_venv_path/pyvenv.cfg" ]; then
            # Reuse the interpreter a known-good venv was built from rather than
            # letting pipenv fall back to pyenv's default, which is the wrong
            # minor version here.
            base_python="$(read_cfg_value "$main_venv_path/pyvenv.cfg" base-executable)"

            # That interpreter is only right if it matches what this branch asks
            # for. A Pipfile bump to a new Python is a plausible reason for the
            # drift that put us on this path, and precisely the case where
            # inheriting the old interpreter is wrong.
            pinned_version="$(read_cfg_value "$worktree_root/Pipfile" python_full_version)"
            shared_version="$(read_cfg_value "$main_venv_path/pyvenv.cfg" version_info |
                cut -d. -f1-3)"
            if [ -n "$pinned_version" ] && [ -n "$shared_version" ] &&
                [ "$pinned_version" != "$shared_version" ]; then
                warn "this Pipfile pins Python $pinned_version but the reused interpreter is $shared_version — the isolated venv won't match CI. Install $pinned_version with pyenv and rebuild the main checkout's venv to fix this properly."
            fi
        else
            base_python=""
        fi

        echo "Building isolated virtualenv (this takes a few minutes)"
        if [ -n "$base_python" ]; then
            info "interpreter:   $base_python"
            (cd "$worktree_root" && pipenv --python "$base_python")
        else
            warn "falling back to pipenv's default interpreter — check the version it picks"
            (cd "$worktree_root" && pipenv --python 3.11)
        fi
    fi
    # sync, not install: installs exactly what Pipfile.lock specifies and cannot
    # relock, so it can't leave surprise Pipfile.lock churn in the first diff.
    (cd "$worktree_root" && pipenv sync --dev)
fi

# --------------------------------------------------------------- copy .vscode

# Gitignored, so a new worktree never gets it — without it VS Code won't discover
# tests or format on save in this window.
if [ "$isolated" = true ]; then
    # settings.json hardcodes the shared venv in python.defaultInterpreterPath and
    # black-formatter.interpreter, and python.envFile sets variables rather than
    # choosing an interpreter. Copying it here would leave VS Code running tests
    # and formatting against the very venv this mode exists to avoid.
    info "skipped        .vscode/ (isolated mode)"
    info "               run 'Python: Select Interpreter' and pick $venv_name"
elif [ ! -d "$main_checkout/.vscode" ]; then
    : # nothing to copy
elif [ ! -d "$worktree_root/.vscode" ]; then
    cp -r "$main_checkout/.vscode" "$worktree_root/.vscode"
    info "copied         .vscode/ from the main checkout"
else
    # VS Code writes .vscode/ itself on some actions, and a partial directory
    # would otherwise silently suppress the copy with no indication why.
    info "kept           existing .vscode/ (not refreshed from the main checkout)"
fi

# ------------------------------------------------------------------- verify

# Pre-commit needs no per-worktree action: git resolves the hooks directory to the
# common dir even from a linked worktree, and the installed hook names the main
# checkout's interpreter by absolute path. Just confirm it's actually installed.
if [ ! -f "$(git rev-parse --git-path hooks)/pre-commit" ]; then
    warn "no pre-commit hook installed — run 'pipenv run pre-commit install' in the main checkout"
fi

echo "Verifying"
(cd "$worktree_root" && pipenv run python -c 'import boto3, polars' 2>/dev/null) ||
    die "'pipenv run python -c \"import boto3, polars\"' failed in the worktree"
info "imports ok"

# ------------------------------------------------------------------- write .env

write_env_file "$worktree_root/.env" "$venv_name"

echo "Done. '$worktree_root' is ready."

if [ "$isolated" = false ]; then
    cat <<'EOF'

This worktree shares the main checkout's virtualenv, so from inside it:
  pipenv --rm        deletes the main checkout's venv, breaking every worktree
                     and the pre-commit hook that names its interpreter
  pipenv install     installs into the shared venv, so other worktrees get a
                     package their Pipfile.lock doesn't list
  pipenv uninstall   removes it for everyone, including worktrees still using it
  pipenv clean       strips whatever isn't in *this* worktree's lock, for everyone
Remove a finished worktree with 'git worktree remove' from the main checkout.
EOF
fi
