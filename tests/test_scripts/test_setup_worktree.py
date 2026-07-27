"""Tests for the .env merge in scripts/setup_worktree.sh.

Only the .env merge is covered. It is pure text manipulation, so it can be
exercised without building a virtualenv, and it is where two silent-corruption
bugs lived: appending to a file whose last line had no newline destroyed that
line's value, and interpolating the virtualenv name into a `sed` program let an
`&` in the name expand to the whole match.

The script's `--write-env-only` flag writes ./.env from PIPENV_CUSTOM_VENV_NAME
and does nothing else, so no git worktree or virtualenv is needed.

Two Windows details shape how the script is invoked. It is copied beside the
working directory and run by a relative path, because an absolute path handed
from Python to Git Bash crosses a native-to-MSYS boundary that leaves it
untranslated and unopenable. And the command is passed to `bash -c` as one
POSIX-quoted string rather than as separate argv entries, because Windows
rebuilds a command line from argv and eats an `&` in the virtualenv name before
bash ever sees it.
"""

import shlex
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

import pytest

SCRIPT = Path(__file__).parents[2] / "scripts" / "setup_worktree.sh"


@pytest.fixture
def workdir(tmp_path: Path) -> Path:
    """Return a directory to write .env into, with the script copied alongside.

    Args:
        tmp_path (Path): pytest temporary directory.

    Returns:
        Path: the working directory the script should be run from.
    """
    shutil.copy(SCRIPT, tmp_path / "setup_worktree.sh")
    work = tmp_path / "work"
    work.mkdir()
    return work


def write_env(workdir: Path, venv_name: str | None) -> subprocess.CompletedProcess:
    """Run the script's .env merge in a directory.

    Args:
        workdir (Path): directory whose .env should be written.
        venv_name (str|None): virtualenv name, or None to omit the argument.

    Returns:
        subprocess.CompletedProcess: the completed process.
    """
    command = "../setup_worktree.sh --write-env-only"
    if venv_name is not None:
        command = f"{command} {shlex.quote(venv_name)}"

    return subprocess.run(
        ["bash", "-c", command],
        cwd=workdir,
        capture_output=True,
    )


@dataclass
class EnvFileTestCase:
    id: str
    initial: str | None
    venv_name: str
    expected: str

    def as_pytest_param(self):
        """Return test case as pytest ParameterSet."""
        return pytest.param(self.initial, self.venv_name, self.expected, id=self.id)


test_cases = [
    EnvFileTestCase(
        id="creates_file_when_absent",
        initial=None,
        venv_name="DataEngineering-abc123",
        expected="PIPENV_CUSTOM_VENV_NAME=DataEngineering-abc123\n",
    ),
    EnvFileTestCase(
        id="keeps_final_line_lacking_a_newline",
        initial="AWS_PROFILE=dev",
        venv_name="DataEngineering-abc123",
        expected="AWS_PROFILE=dev\nPIPENV_CUSTOM_VENV_NAME=DataEngineering-abc123\n",
    ),
    EnvFileTestCase(
        id="keeps_unrelated_variables",
        initial="AWS_PROFILE=dev\nSOME_FLAG=1\n",
        venv_name="shared",
        expected="AWS_PROFILE=dev\nSOME_FLAG=1\nPIPENV_CUSTOM_VENV_NAME=shared\n",
    ),
    EnvFileTestCase(
        id="replaces_existing_value",
        initial="PIPENV_CUSTOM_VENV_NAME=old\n",
        venv_name="new",
        expected="PIPENV_CUSTOM_VENV_NAME=new\n",
    ),
    EnvFileTestCase(
        id="replaces_exported_and_indented_values",
        initial="KEEP=1\nexport PIPENV_CUSTOM_VENV_NAME=old\n   PIPENV_CUSTOM_VENV_NAME=older\n",
        venv_name="new",
        expected="KEEP=1\nPIPENV_CUSTOM_VENV_NAME=new\n",
    ),
    EnvFileTestCase(
        id="keeps_ampersand_in_name_intact",
        initial="PIPENV_CUSTOM_VENV_NAME=old\n",
        venv_name="DataEngineering-a&b",
        expected="PIPENV_CUSTOM_VENV_NAME=DataEngineering-a&b\n",
    ),
    EnvFileTestCase(
        id="normalises_crlf_rather_than_mixing_endings",
        initial="KEEP=1\r\nPIPENV_CUSTOM_VENV_NAME=old\r\n",
        venv_name="new",
        expected="KEEP=1\nPIPENV_CUSTOM_VENV_NAME=new\n",
    ),
]


class TestWriteEnvFile:
    @pytest.mark.parametrize(
        "initial, venv_name, expected",
        [case.as_pytest_param() for case in test_cases],
    )
    def test_env_file_contents(
        self, workdir: Path, initial: str | None, venv_name: str, expected: str
    ):
        env_file = workdir / ".env"
        if initial is not None:
            env_file.write_bytes(initial.encode())

        result = write_env(workdir, venv_name)

        assert result.returncode == 0, result.stderr
        assert env_file.read_bytes().decode() == expected

    def test_repeated_runs_do_not_accumulate_lines(self, workdir: Path):
        write_env(workdir, "shared")
        write_env(workdir, "shared")

        contents = (workdir / ".env").read_bytes().decode()
        assert contents == "PIPENV_CUSTOM_VENV_NAME=shared\n"

    def test_leaves_no_temporary_files_behind(self, workdir: Path):
        (workdir / ".env").write_bytes(b"KEEP=1\n")

        write_env(workdir, "shared")

        assert [path.name for path in workdir.iterdir()] == [".env"]

    def test_errors_when_no_name_is_set(self, workdir: Path):
        result = write_env(workdir, None)

        # Assert on the message, not just the exit code: a script that failed to
        # launch at all would otherwise satisfy this test.
        assert b"needs a virtualenv name" in result.stderr
        assert not (workdir / ".env").exists()
