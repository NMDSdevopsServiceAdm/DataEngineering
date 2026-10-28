"""
Works out the release tag and CHANGELOG release for a selected, reconciled run.

Works on local files only; `cut_release_tag.sh` does the S3, git and GitHub work
and runs this as a module from the repo root (`python -m scripts.cut_release_tag`).
"""

import argparse
import calendar
import json
import re
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Optional, Sequence

import polars as pl

from scripts.update_run_log import read_run_log, write_run_partition

from utils.column_names.ind_cqc_pipeline_columns import (
    ArchiveDateRunNumberPartitionKeys as Keys,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    ArchiveRunLogColumns as RunLog,
)

RELEASE_TAG_PATTERN = re.compile(r"v(\d{4})\.(\d{2})\.(\d+)")
UNRELEASED_HEADING = "## [Unreleased]"
UNRELEASED_SUBSECTIONS = ("### Added", "### Changed", "### Improved", "### Fixed")


def select_run_to_tag(run_log_df: pl.DataFrame, run_number: int) -> pl.DataFrame:
    """Finds the run to tag, checking it is ready to be tagged.

    Args:
        run_log_df (pl.DataFrame): The run log, including the partition key columns.
        run_number (int): The run to tag.

    Returns:
        pl.DataFrame: The run's single row.

    Raises:
        ValueError: If the run number matches no row or more than one row, or the
            run is not selected for publication, not reconciled, or already tagged.
    """
    run_df = run_log_df.filter(pl.col(Keys.run_number) == run_number)

    if run_df.height == 0:
        raise ValueError(f"Run number {run_number} not found in the run log.")
    if run_df.height > 1:
        raise ValueError(
            f"Run number {run_number} matches multiple rows in the run log."
        )

    if not run_df[RunLog.selected_for_publication].item():
        raise ValueError(f"Run number {run_number} is not selected_for_publication.")
    if not run_df[RunLog.reconciled].item():
        raise ValueError(f"Run number {run_number} is not reconciled.")
    existing_tag = run_df[RunLog.tag].item()
    if existing_tag is not None:
        raise ValueError(f"Run number {run_number} is already tagged {existing_tag}.")

    return run_df


def get_data_month(archive_date: str) -> date:
    """Finds the month a run's data is as at: the month before it was archived.

    Args:
        archive_date (str): The run's archive date (yyyy-mm-dd).

    Returns:
        date: The first day of the data month.
    """
    first_of_archive_month = date.fromisoformat(archive_date).replace(day=1)
    return (first_of_archive_month - timedelta(days=1)).replace(day=1)


def next_tag(tag_commits: dict[str, str], archive_date: str, commit_sha: str) -> str:
    """Chooses the release tag `vYYYY.MM.N` for a run's data month.

    A release tag already on the commit for the data month is reused, so a rerun
    after a partial failure does not cut a second tag.

    Args:
        tag_commits (dict[str, str]): Every tag in the repo, mapped to its commit SHA.
        archive_date (str): The run's archive date (yyyy-mm-dd).
        commit_sha (str): The run's commit SHA.

    Returns:
        str: The tag to cut, or the existing tag on the commit.
    """
    data_month = get_data_month(archive_date)
    month_tags = {}
    for tag in tag_commits:
        match = RELEASE_TAG_PATTERN.fullmatch(tag)
        if match and (int(match[1]), int(match[2])) == (
            data_month.year,
            data_month.month,
        ):
            month_tags[tag] = int(match[3])

    for tag in month_tags:
        if tag_commits[tag] == commit_sha:
            return tag

    next_suffix = max(month_tags.values(), default=-1) + 1
    return f"v{data_month.year}.{data_month.month:02d}.{next_suffix}"


def tag_message(archive_date: str) -> str:
    """Builds the annotated tag message from the run's data month.

    Args:
        archive_date (str): The run's archive date (yyyy-mm-dd).

    Returns:
        str: The message, e.g. "Codebase used for data as at September 2026".
    """
    data_month = get_data_month(archive_date)
    return f"Codebase used for data as at {calendar.month_name[data_month.month]} {data_month.year}"


def _split_unreleased(
    changelog: str,
) -> tuple[list[str], dict[str, list[str]], list[str]]:
    """Splits a changelog around its Unreleased section.

    Args:
        changelog (str): The changelog text.

    Returns:
        tuple[list[str], dict[str, list[str]], list[str]]: The lines before the
            Unreleased heading, the Unreleased entries by subsection heading, and
            the lines from the first version heading onwards.

    Raises:
        ValueError: If the changelog has no Unreleased section.
    """
    lines = changelog.split("\n")
    if UNRELEASED_HEADING not in lines:
        raise ValueError(f"The changelog has no '{UNRELEASED_HEADING}' section.")

    start = lines.index(UNRELEASED_HEADING)
    end = next(
        (i for i in range(start + 1, len(lines)) if lines[i].startswith("## [")),
        len(lines),
    )

    subsections: dict[str, list[str]] = {}
    entries: list[str] = []
    for line in lines[start + 1 : end]:
        if line.startswith("### "):
            entries = subsections.setdefault(line, [])
        elif line.startswith("- "):
            entries.append(line)
        elif line.strip() and entries:
            # Continuation of a wrapped entry.
            entries[-1] += "\n" + line
    return lines[:start], subsections, lines[end:]


def _render_subsections(subsections: dict[str, list[str]]) -> list[str]:
    """Renders subsections as changelog lines, with two blank lines after an empty one."""
    lines: list[str] = []
    for heading, entries in subsections.items():
        lines += [heading, *entries, ""]
        if not entries:
            lines.append("")
    return lines


def release_changelog(
    changelog_at_commit: str, changelog_at_main: str, tag: str, release_date: date
) -> str:
    """Moves the Unreleased entries present at the run's commit under a new version.

    Entries added to main after the commit stay under Unreleased.

    Args:
        changelog_at_commit (str): The changelog at the run's commit.
        changelog_at_main (str): The changelog at the tip of main.
        tag (str): The release tag, used as the version.
        release_date (date): The date of the release.

    Returns:
        str: The changelog at main with the commit's entries under the new version.

    Raises:
        ValueError: If either changelog has no Unreleased section, or the commit's
            changelog has no Unreleased entries.
    """
    _, commit_subsections, _ = _split_unreleased(changelog_at_commit)
    if not any(commit_subsections.values()):
        raise ValueError("The changelog at the commit has no Unreleased entries.")

    before, main_subsections, after = _split_unreleased(changelog_at_main)

    released: dict[str, list[str]] = {}
    remaining: dict[str, list[str]] = {
        heading: [] for heading in UNRELEASED_SUBSECTIONS
    }
    for heading, entries in main_subsections.items():
        for entry in entries:
            if entry in commit_subsections.get(heading, []):
                released.setdefault(heading, []).append(entry)
            else:
                remaining.setdefault(heading, []).append(entry)

    version_heading = f"## [{tag}] - {release_date.strftime('%d/%m/%Y')}"
    return "\n".join(
        [
            *before,
            UNRELEASED_HEADING,
            "",
            *_render_subsections(remaining),
            version_heading,
            "",
            *_render_subsections(released),
            *after,
        ]
    )


def set_run_tag(run_log_df: pl.DataFrame, run_number: int, tag: str) -> pl.DataFrame:
    """Sets the tag for one run.

    Args:
        run_log_df (pl.DataFrame): The run log, including the partition key columns.
        run_number (int): The run to update.
        tag (str): The release tag.

    Returns:
        pl.DataFrame: The run log with the tag set for the run.
    """
    return run_log_df.with_columns(
        pl.when(pl.col(Keys.run_number) == run_number)
        .then(pl.lit(tag))
        .otherwise(pl.col(RunLog.tag))
        .alias(RunLog.tag)
    )


def _read_tag_commits(tags_file: Path) -> dict[str, str]:
    """Reads `<tag> <commit sha>` lines, one per tag."""
    return dict(line.split() for line in tags_file.read_text().splitlines() if line)


def _parse_arguments(argv: Optional[Sequence[str]]) -> argparse.Namespace:
    """Parses the command line arguments.

    Args:
        argv (Optional[Sequence[str]]): Command line arguments; defaults to `sys.argv`.

    Returns:
        argparse.Namespace: The parsed arguments, with the chosen command in `command`.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    plan = commands.add_parser(
        "plan", help="Print the run's commit SHA, tag and tag message as JSON"
    )
    plan.add_argument("--run_log_dir", type=Path, required=True, help="Local run log")
    plan.add_argument("--run_number", type=int, required=True)
    plan.add_argument(
        "--tags_file",
        type=Path,
        required=True,
        help="Lines of '<tag> <commit sha>' for every tag in the repo",
    )

    changelog = commands.add_parser(
        "changelog", help="Move the commit's Unreleased entries under the new version"
    )
    changelog.add_argument("--changelog_at_commit", type=Path, required=True)
    changelog.add_argument("--changelog_at_main", type=Path, required=True)
    changelog.add_argument("--tag", required=True)
    changelog.add_argument("--release_date", type=date.fromisoformat, required=True)
    changelog.add_argument("--output", type=Path, required=True)

    record_tag = commands.add_parser(
        "record-tag", help="Write the tag into the run's row of a local run log"
    )
    record_tag.add_argument("--run_log_dir", type=Path, required=True)
    record_tag.add_argument("--output_dir", type=Path, required=True)
    record_tag.add_argument("--run_number", type=int, required=True)
    record_tag.add_argument("--tag", required=True)

    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Runs one of the commands the release workflow uses.

    Args:
        argv (Optional[Sequence[str]]): Command line arguments; defaults to `sys.argv`.

    Returns:
        int: 0 on success, 1 if the run or changelog is not ready for release.
    """
    args = _parse_arguments(argv)

    try:
        if args.command == "changelog":
            changelog = release_changelog(
                args.changelog_at_commit.read_text(encoding="utf-8"),
                args.changelog_at_main.read_text(encoding="utf-8"),
                args.tag,
                args.release_date,
            )
            args.output.write_text(changelog, encoding="utf-8", newline="\n")
            return 0

        run_log_df = read_run_log(args.run_log_dir)
        run_df = select_run_to_tag(run_log_df, args.run_number)
        if args.command == "plan":
            archive_date = run_df[Keys.archive_date].item()
            commit_sha = run_df[RunLog.commit_sha].item()
            print(
                json.dumps(
                    {
                        "run_number": args.run_number,
                        "archive_date": archive_date,
                        "commit_sha": commit_sha,
                        "tag": next_tag(
                            _read_tag_commits(args.tags_file), archive_date, commit_sha
                        ),
                        "tag_message": tag_message(archive_date),
                    }
                )
            )
        else:
            updated_df = set_run_tag(run_log_df, args.run_number, args.tag)
            write_run_partition(updated_df, args.run_number, args.output_dir)
    except (ValueError, FileNotFoundError) as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
