### Rationale

We want to switch from pyenv/ pipenv to uv so that we can manage our environment accross multiple worktrees. This will allow us to work on multiple tickets simultaneously - helpful for working with ClaudeCode.

### Successful Process

1 - Install uv using:

```
pip install uv
```

2 - Install uv migration tool using:

```
uv tool install pipenv-uv-migrate
```

3 - Delete existing pyproject.toml file

4 - Create a new  pyproject.toml file

```
uv init
```

5- Migrate Pipfile to pyproject.toml
Note - I Updated the existing pipfile and change the format which is recognised by uv

```
uvx pipenv-uv-migrate -f Pipfile -t pyproject.toml
```

6 -Pin the python version

```
uv python pin 3.11.12
```

6- Generate lock file

```
uv lock
```

7- Installing dependencies

```
uv sync
```

8-

### Dead-ends/ what didn't work

- `uv tool install pipenv-uv-migrate`

### Gotchas

Multiple python versions on MH laptop blocked the uv lock file:
```
Using CPython 3.9.25
  × No solution found when resolving dependencies for split (markers: python_full_version == '3.10.*'):
  ╰─▶ Because the requested Python version (>=3.9) does not satisfy Python>=3.11 and numpy==2.3.2 depends on Python>=3.11, we can conclude that numpy==2.3.2 cannot be used.
      And because your project depends on numpy==2.3.2, we can conclude that your project's requirements are unsatisfiable.

hint: While the active Python version is 3.9, the resolution failed for other Python versions supported by your project. Consider limiting your project's supported Python versions using `requires-python`.
hint: The `requires-python` value (>=3.9) includes Python versions that are not supported by your dependencies (e.g., numpy==2.3.2 only supports >=3.11). Consider using a more restrictive `requires-python` value (like >=3.11).
```
