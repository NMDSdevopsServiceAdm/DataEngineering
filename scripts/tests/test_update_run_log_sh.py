import os
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

SCRIPT_PATH = Path(__file__).parents[1] / "update_run_log.sh"
ARCHIVE_BUCKET = "sfc-main-archive-promoted"
DATASETS_BUCKET = "sfc-main-datasets"
DATASET_PREFIXES = [
    "domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates",
    "domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_metadata",
    "domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_run_log",
]
PARTITION = "archive_date=2026-10-01/run_number=3"
# Profile and bucket take their defaults; then flag, run number, confirm, confirm
# upload, and decline starting Publication-Output.
SCRIPT_INPUT = "\n\nselected_for_publication\n3\ny\ny\nn\n"

AWS_STUB = """#!/bin/bash
echo "$@" >> "$AWS_LOG"
case "$1 $2" in
  "s3 sync")
    if [[ "$3" == s3://* && "$4" != s3://* ]]; then
      mkdir -p "$4/archive_date=2026-10-01/run_number=3"
      touch "$4/archive_date=2026-10-01/run_number=3/part.parquet"
    fi ;;
  "s3 ls") echo "2026-10-01 00:00:00 1 part.parquet" ;;
  "s3 cp") [[ -z "${AWS_FAIL_CP:-}" ]] || { echo "AccessDenied" >&2; exit 1; } ;;
  "stepfunctions list-state-machines") echo "main-Publication-Output arn:aws:states:::stateMachine:main" ;;
esac
exit 0
"""

UV_STUB = """#!/bin/bash
if [[ " $* " == *" --list "* ]]; then
  echo "run 3"
  exit 0
fi
while [[ $# -gt 0 ]]; do
  [[ "$1" == "--output_dir" ]] && output_dir="$2"
  shift
done
mkdir -p "$output_dir/archive_date=2026-10-01/run_number=3"
touch "$output_dir/archive_date=2026-10-01/run_number=3/part.parquet"
"""


@pytest.fixture(autouse=True)
def require_bash():
    if shutil.which("bash") is None:
        pytest.skip("bash not available")


def run_script(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fail_copy: bool = False
) -> tuple[subprocess.CompletedProcess, list[str]]:
    """Runs a copy of update_run_log.sh in tmp_path with `aws` and `uv` stubbed.

    Args:
        tmp_path (Path): Working root; the script runs from its `scripts/` folder.
        monkeypatch (pytest.MonkeyPatch): Fixture used to set the environment.
        fail_copy (bool): Whether the stubbed `aws s3 cp` should fail.

    Returns:
        tuple[subprocess.CompletedProcess, list[str]]: The finished process and
            the logged `aws` argument lines.
    """
    scripts_dir = tmp_path / "scripts"
    scripts_dir.mkdir()
    shutil.copy(SCRIPT_PATH, scripts_dir / "update_run_log.sh")

    stub_dir = tmp_path / "stubs"
    stub_dir.mkdir()
    for name, body in (("aws", AWS_STUB), ("uv", UV_STUB)):
        stub = stub_dir / name
        stub.write_text(body, newline="\n")
        stub.chmod(stub.stat().st_mode | stat.S_IEXEC)

    aws_log = tmp_path / "aws.log"
    monkeypatch.setenv("PATH", f"{stub_dir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("AWS_LOG", str(aws_log))
    if fail_copy:
        monkeypatch.setenv("AWS_FAIL_CP", "1")

    result = subprocess.run(
        ["bash", str(scripts_dir / "update_run_log.sh")],
        # Bytes, not text: text mode would turn each newline into CRLF on Windows.
        input=SCRIPT_INPUT.encode(),
        capture_output=True,
        timeout=60,
    )
    result.stderr = result.stderr.decode()
    return result, aws_log.read_text().splitlines() if aws_log.exists() else []


class TestCopyRunToLongTermArchive:
    def test_makes_no_write_check_calls(self, tmp_path, monkeypatch):
        result, aws_calls = run_script(tmp_path, monkeypatch)

        assert result.returncode == 0, result.stderr
        assert not any("put-object" in c or "delete-object" in c for c in aws_calls)

    def test_copies_each_dataset_partition_to_archive_bucket(
        self, tmp_path, monkeypatch
    ):
        _, aws_calls = run_script(tmp_path, monkeypatch)

        copy_calls = [c for c in aws_calls if c.startswith("s3 cp")]
        expected = [
            f"s3 cp s3://{DATASETS_BUCKET}/{p}/{PARTITION}/ "
            f"s3://{ARCHIVE_BUCKET}/{p}/{PARTITION}/ "
            "--recursive --only-show-errors --profile run-log-updater"
            for p in DATASET_PREFIXES
        ]
        assert copy_calls == expected

    def test_failed_copy_exits_non_zero_with_permission_hint(
        self, tmp_path, monkeypatch
    ):
        result, _ = run_script(tmp_path, monkeypatch, fail_copy=True)

        assert result.returncode == 1
        assert "Check profile 'run-log-updater' can write there" in result.stderr
