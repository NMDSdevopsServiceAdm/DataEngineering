# Local Environment Setup

## Prerequisites
| Tool | Windows | Mac/Linux |
|------|---------|-----------|
| Python 3.11.9 | https://www.python.org/downloads/ | https://www.python.org/downloads/ |
| Git | https://github.com/git-guides/install-git | https://github.com/git-guides/install-git |
| Pyenv | https://github.com/pyenv-win/pyenv-win | https://github.com/pyenv/pyenv |
| Pipenv | https://www.pythontutorial.net/python-basics/install-pipenv-windows/ | https://pipenv-fork.readthedocs.io/en/latest/install.html |
| Java JDK8 | https://www.java.com/en/download/ | https://www.java.com/en/download/ |

## MacOS Java Installation
This project currently uses SPARK_VERSION 3.5.4 as this is compatible with Python 3.11 and Glue 5.0.

For Spark installation, note that several JDKs can be used with Spark 3.5, see the guide on [Spark 3.5.4](https://spark.apache.org/docs/3.5.4/)

Curently, users are using jdk8. We recommend using Brew (https://brew.sh) to install the java development kit. This project is using **jdk8**.
```
brew update
brew install adoptopenjdk8
```

## Project setup
```
git clone https://github.com/NMDSdevopsServiceAdm/DataEngineering.git
cd DataEngineering
pipenv install --dev
pipenv shell
```

To exit the environment
```
exit
```
_Do not use `deactivate` or `source deactivate`_

For detailed Windows setup, see [WindowsSetup.md](https://github.com/NMDSdevopsServiceAdm/DataEngineering/blob/main/WindowsSetup.md)

## Working in a git worktree

If you develop a ticket in its own git worktree, that worktree starts with **no
Python environment**. Pipenv keys its virtualenv to the project *directory*, so a
worktree doesn't inherit the main checkout's. This doesn't fail loudly — the first
`pipenv run ...` silently creates a new, empty venv from whatever interpreter
pyenv defaults to, and ordinary imports then fail with `ModuleNotFoundError`,
which reads like a broken code change rather than a missing environment.

Run this once, from inside the new worktree:

```
bash scripts/setup_worktree.sh
```

It points the worktree at the main checkout's existing virtualenv via a
gitignored `.env`, copies `.vscode/` across (also gitignored, and needed for test
discovery and format-on-save), and verifies the result. There's nothing to
install and nothing to wait for.

Sharing one virtualenv is safe: it contains no repo code, so imports resolve
against whichever worktree you run from. If the worktree's `Pipfile` or
`Pipfile.lock` differs from the main checkout's, the script detects it and builds
an isolated virtualenv instead — pass `--isolated` to force that. Pre-commit
needs no per-worktree setup; `core.hooksPath` already points every worktree at the
shared hooks directory.
