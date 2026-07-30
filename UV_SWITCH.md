### Rationale

We want to switch from pyenv/ pipenv to uv so that we can manage our environment accross multiple worktrees. This will allow us to work on multiple tickets simultaneously - helpful for working with ClaudeCode.

### Successful Process

1 - Install uv migration tool using:

```
pipx install pipenv-uv-migrate
```

### Dead-ends/ what didn't work

- `uv tool install pipenv-uv-migrate`

### Gotchas
