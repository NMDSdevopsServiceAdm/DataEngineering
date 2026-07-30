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
Note - I Update the existing pipfile and change the format which is recognised by uv

```
pipenv-uv-migrate -f Pipfile -t pyproject.toml
```

6- Generate lock file

```
uv lock
```

### Dead-ends/ what didn't work

- `uv tool install pipenv-uv-migrate`

### Gotchas
