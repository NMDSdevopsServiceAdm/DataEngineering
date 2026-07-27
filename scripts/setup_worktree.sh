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
# Pipfile.lock changes on ~1 in 1000 commits here, so sharing is nearly always
# right, and it avoids a ~1.2GB, several-minute install per worktree.
#
# If this worktree's Pipfile or Pipfile.lock differs from the main checkout's,
# sharing would give you the wrong dependencies, so an isolated venv is built
# instead. Pass --isolated to force that.
#
# Run from anywhere inside the new worktree:
#
#   bash scripts/setup_worktree.sh
#
set -euo pipefail

force_isolated=false
case "${1:-}" in
    --isolated) force_isolated=true ;;
    -h | --help)
        sed -n '3,25p' "$0" | sed 's/^# \{0,1\}//'
        exit 0
        ;;
    "") ;;
    *)
        echo "error: unknown argument '$1' (try --help)" >&2
        exit 2
        ;;
esac

info() { printf '  %s\n' "$*"; }
warn() { printf 'warning: %s\n' "$*" >&2; }
die() {
    printf 'error: %s\n' "$*" >&2
    exit 1
}

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
# main checkout's working tree, which is what we'd be borrowing.
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
# venv is ever removed and rebuilt under a different hash suffix.
main_venv_path="$(cd "$main_checkout" && pipenv --venv 2>/dev/null)" || main_venv_path=""

if [ -z "$main_venv_path" ]; then
    [ "$isolated" = true ] ||
        die "the main checkout has no virtualenv yet — run 'pipenv install --dev' there first"
    warn "the main checkout has no virtualenv, so its interpreter can't be reused"
fi

if [ "$isolated" = true ]; then
    # Strip a trailing hash suffix if the worktree dir already carries one.
    venv_name="DataEngineering-${worktree_root##*[/\\]}"
else
    # basename, tolerating either separator (pipenv prints native Windows paths).
    venv_name="${main_venv_path##*[/\\]}"
fi

info "virtualenv:    $venv_name"

# ------------------------------------------------------------------- write .env

# .env is gitignored, and .vscode/settings.json already points python.envFile at
# it, so this doubles as the file VS Code loads.
env_file="$worktree_root/.env"
env_line="PIPENV_CUSTOM_VENV_NAME=$venv_name"

if [ -f "$env_file" ] && grep -q '^PIPENV_CUSTOM_VENV_NAME=' "$env_file"; then
    tmp="$(mktemp)"
    sed "s|^PIPENV_CUSTOM_VENV_NAME=.*|$env_line|" "$env_file" >"$tmp"
    mv "$tmp" "$env_file"
    info "updated        .env"
else
    printf '%s\n' "$env_line" >>"$env_file"
    info "wrote          .env"
fi

# ------------------------------------------------------------ build if isolated

if [ "$isolated" = true ]; then
    if [ -n "$main_venv_path" ] && [ -f "$main_venv_path/pyvenv.cfg" ]; then
        # Reuse the interpreter a known-good venv was built from rather than
        # letting pipenv fall back to pyenv's default, which is the wrong minor
        # version here. A patch-version mismatch against the Pipfile's
        # python_full_version is tolerated — pipenv's warning about it is noise.
        base_python="$(awk -F' = ' '/^base-executable/{print $2; exit}' \
            "$main_venv_path/pyvenv.cfg")"
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
    # sync, not install: installs exactly what Pipfile.lock specifies and cannot
    # relock, so it can't leave surprise Pipfile.lock churn in the first diff.
    (cd "$worktree_root" && pipenv sync --dev)
fi

# --------------------------------------------------------------- copy .vscode

# Gitignored, so a new worktree never gets it — without it VS Code won't discover
# tests or format on save in this window.
if [ -d "$main_checkout/.vscode" ] && [ ! -d "$worktree_root/.vscode" ]; then
    cp -r "$main_checkout/.vscode" "$worktree_root/.vscode"
    info "copied         .vscode/ from the main checkout"
fi

# ------------------------------------------------------------------- verify

# Pre-commit needs no per-worktree action: git resolves the hooks directory to the
# common dir even from a linked worktree, and the installed hook names the main
# checkout's interpreter by absolute path. Just confirm it's actually installed.
if [ ! -f "$(git rev-parse --git-path hooks)/pre-commit" ]; then
    warn "no pre-commit hook installed — run 'pipenv run pre-commit install' in the main checkout"
fi

echo "Verifying"
if (cd "$worktree_root" && pipenv run python -c 'import boto3, polars' 2>/dev/null); then
    info "imports ok"
    echo "Done. '$worktree_root' is ready."
else
    die "'pipenv run python -c \"import boto3, polars\"' failed in the worktree"
fi
