import json
from dataclasses import dataclass
from datetime import date
from typing import Optional

import polars as pl
import polars.testing as pl_testing
import pytest

import scripts.cut_release_tag as job
from utils.column_names.ind_cqc_pipeline_columns import (
    ArchiveDateRunNumberPartitionKeys as Keys,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    ArchiveRunLogColumns as RunLog,
)

RUN_LOG_SCHEMA = {
    Keys.archive_date: pl.String,
    Keys.run_number: pl.Int64,
    RunLog.commit_sha: pl.String,
    RunLog.selected_for_publication: pl.Boolean,
    RunLog.reconciled: pl.Boolean,
    RunLog.tag: pl.String,
}

COMMIT_SHA = "sha_of_the_run"


def build_run_log(
    runs: list[tuple[int, bool, bool, Optional[str]]],
) -> pl.DataFrame:
    """Builds a run log with one row per (run_number, selected, reconciled, tag)."""
    return pl.DataFrame(
        [
            ("2026-10-15", run_number, f"sha{run_number}", selected, reconciled, tag)
            for run_number, selected, reconciled, tag in runs
        ],
        schema=RUN_LOG_SCHEMA,
        orient="row",
    )


@dataclass
class SelectRunToTagRaisesTestCase:
    id: str
    runs: list[tuple[int, bool, bool, Optional[str]]]
    run_number: int
    message: str

    def as_pytest_param(self):
        return pytest.param(self.runs, self.run_number, self.message, id=self.id)


select_run_to_tag_raises_cases = [
    SelectRunToTagRaisesTestCase(
        id="raises_when_run_is_not_selected",
        runs=[(1, False, True, None)],
        run_number=1,
        message="not selected_for_publication",
    ),
    SelectRunToTagRaisesTestCase(
        id="raises_when_run_is_already_tagged",
        runs=[(1, True, True, "v2026.09.0")],
        run_number=1,
        message="already tagged v2026.09.0",
    ),
    SelectRunToTagRaisesTestCase(
        id="raises_when_run_number_is_not_found",
        runs=[(1, True, True, None)],
        run_number=99,
        message="99 not found",
    ),
]


class TestSelectRunToTag:
    def test_returns_the_selected_untagged_run(self):
        run_log_df = build_run_log(
            [(1, False, False, None), (2, True, True, None), (3, False, True, None)]
        )

        returned_df = job.select_run_to_tag(run_log_df, 2, {})

        expected_df = run_log_df.filter(pl.col(Keys.run_number) == 2)
        pl_testing.assert_frame_equal(expected_df, returned_df)

    def test_returns_the_run_when_it_is_not_reconciled(self):
        run_log_df = build_run_log([(1, True, False, None)])

        returned_df = job.select_run_to_tag(run_log_df, 1, {})

        pl_testing.assert_frame_equal(run_log_df, returned_df)

    def test_returns_the_run_when_its_tag_is_already_on_its_commit(self):
        run_log_df = build_run_log([(1, True, True, "v2026.09.0")])

        returned_df = job.select_run_to_tag(run_log_df, 1, {"v2026.09.0": "sha1"})

        pl_testing.assert_frame_equal(run_log_df, returned_df)

    @pytest.mark.parametrize(
        "runs, run_number, message",
        [case.as_pytest_param() for case in select_run_to_tag_raises_cases],
    )
    def test_raises_when_run_cannot_be_tagged(self, runs, run_number, message):
        run_log_df = build_run_log(runs)

        with pytest.raises(ValueError, match=message):
            job.select_run_to_tag(run_log_df, run_number, {"v2026.09.0": "other_sha"})


@dataclass
class NextTagTestCase:
    id: str
    tag_commits: dict[str, str]
    archive_date: str
    expected_tag: str

    def as_pytest_param(self):
        return pytest.param(
            self.tag_commits, self.archive_date, self.expected_tag, id=self.id
        )


next_tag_cases = [
    NextTagTestCase(
        id="uses_the_month_before_the_archive_date",
        tag_commits={},
        archive_date="2026-10-15",
        expected_tag="v2026.09.0",
    ),
    NextTagTestCase(
        id="uses_december_of_the_previous_year_for_a_january_archive_date",
        tag_commits={},
        archive_date="2027-01-05",
        expected_tag="v2026.12.0",
    ),
    NextTagTestCase(
        id="starts_at_zero_when_the_month_has_no_tag",
        tag_commits={"v2026.08.1": "other_sha"},
        archive_date="2026-10-15",
        expected_tag="v2026.09.0",
    ),
    NextTagTestCase(
        id="increments_past_the_highest_suffix_for_the_month",
        tag_commits={"v2026.09.0": "sha_a", "v2026.09.2": "sha_b"},
        archive_date="2026-10-15",
        expected_tag="v2026.09.3",
    ),
    NextTagTestCase(
        id="compares_suffixes_numerically",
        tag_commits={"v2026.09.9": "sha_a", "v2026.09.10": "sha_b"},
        archive_date="2026-10-15",
        expected_tag="v2026.09.11",
    ),
    NextTagTestCase(
        id="ignores_tags_for_other_months_and_non_release_tags",
        tag_commits={
            "v2026.09.0": "sha_a",
            "v2026.08.5": COMMIT_SHA,
            "v2026.10.4": "sha_b",
            "v2026.09.1-rc": "sha_c",
            "spike-2000-l2": "sha_d",
        },
        archive_date="2026-10-15",
        expected_tag="v2026.09.1",
    ),
    NextTagTestCase(
        id="reuses_the_release_tag_already_on_the_commit",
        tag_commits={"v2026.09.0": "sha_a", "v2026.09.1": COMMIT_SHA},
        archive_date="2026-10-15",
        expected_tag="v2026.09.1",
    ),
]


class TestNextTag:
    @pytest.mark.parametrize(
        "tag_commits, archive_date, expected_tag",
        [case.as_pytest_param() for case in next_tag_cases],
    )
    def test_returns_expected_tag(self, tag_commits, archive_date, expected_tag):
        returned_tag = job.next_tag(tag_commits, archive_date, COMMIT_SHA)

        assert returned_tag == expected_tag


class TestTagMessage:
    def test_builds_the_tag_message_from_the_data_month(self):
        returned_message = job.tag_message("2026-10-15")

        assert returned_message == "Codebase used for data as at September 2026"


PREFACE = "# Changelog\n\nAll notable changes.\n\n\n"
OLDER_VERSIONS = "## [v2026.08.1] - 11/09/2026\n\n### Added\n- Old entry.\n"
EMPTY_UNRELEASED = "### Added\n\n\n### Changed\n\n\n### Improved\n\n\n### Fixed\n\n\n"
TAG = "v2026.09.1"
RELEASE_DATE = date(2026, 10, 15)


def build_changelog(unreleased_body: str) -> str:
    """Builds a changelog whose Unreleased section holds `unreleased_body`."""
    return PREFACE + "## [Unreleased]\n\n" + unreleased_body + OLDER_VERSIONS


class TestReleaseChangelog:
    def test_moves_the_unreleased_entries_at_the_commit_under_the_new_version(self):
        body = "### Added\n- A.\n\n### Fixed\n- B.\n\n"
        changelog = build_changelog(body)

        returned_changelog = job.release_changelog(
            changelog, changelog, TAG, RELEASE_DATE
        )

        expected_changelog = (
            PREFACE
            + "## [Unreleased]\n\n"
            + EMPTY_UNRELEASED
            + "## [v2026.09.1] - 15/10/2026\n\n### Added\n- A.\n\n### Fixed\n- B.\n\n"
            + OLDER_VERSIONS
        )
        assert returned_changelog == expected_changelog

    def test_leaves_entries_added_after_the_commit_under_unreleased(self):
        at_commit = build_changelog("### Added\n- A.\n\n")
        at_main = build_changelog("### Added\n- A.\n- C.\n\n")

        returned_changelog = job.release_changelog(
            at_commit, at_main, TAG, RELEASE_DATE
        )

        expected_unreleased = "## [Unreleased]\n\n### Added\n- C.\n\n### Changed\n\n\n"
        expected_version = "## [v2026.09.1] - 15/10/2026\n\n### Added\n- A.\n\n"
        assert expected_unreleased in returned_changelog
        assert expected_version in returned_changelog

    def test_keeps_subsection_headings_for_moved_entries(self):
        changelog = build_changelog("### Changed\n- X.\n\n### Improved\n- Y.\n\n")

        returned_changelog = job.release_changelog(
            changelog, changelog, TAG, RELEASE_DATE
        )

        expected_version = (
            "## [v2026.09.1] - 15/10/2026\n\n"
            "### Changed\n- X.\n\n### Improved\n- Y.\n\n"
        )
        assert expected_version + OLDER_VERSIONS in returned_changelog

    def test_resets_unreleased_to_empty_subsections(self):
        changelog = build_changelog("### Added\n- A.\n\n")

        returned_changelog = job.release_changelog(
            changelog, changelog, TAG, RELEASE_DATE
        )

        expected_unreleased = "## [Unreleased]\n\n" + EMPTY_UNRELEASED + "## [v2026"
        assert expected_unreleased in returned_changelog

    def test_writes_the_version_heading_date_as_dd_mm_yyyy(self):
        changelog = build_changelog("### Added\n- A.\n\n")

        returned_changelog = job.release_changelog(
            changelog, changelog, TAG, date(2026, 11, 3)
        )

        assert "## [v2026.09.1] - 03/11/2026\n" in returned_changelog

    def test_leaves_older_versions_unchanged(self):
        changelog = build_changelog("### Added\n- A.\n\n")

        returned_changelog = job.release_changelog(
            changelog, changelog, TAG, RELEASE_DATE
        )

        assert returned_changelog.endswith(OLDER_VERSIONS)

    def test_raises_when_the_commit_has_no_unreleased_entries(self):
        at_commit = build_changelog(EMPTY_UNRELEASED)
        at_main = build_changelog("### Added\n- A.\n\n")

        with pytest.raises(ValueError, match="no Unreleased entries"):
            job.release_changelog(at_commit, at_main, TAG, RELEASE_DATE)


class TestSetRunTag:
    def test_sets_the_tag_on_the_run(self):
        run_log_df = build_run_log([(1, True, True, None), (2, True, True, None)])

        returned_df = job.set_run_tag(run_log_df, 2, TAG)

        returned_run = returned_df.filter(pl.col(Keys.run_number) == 2)
        assert returned_run[RunLog.tag].to_list() == [TAG]

    def test_does_not_change_other_runs(self):
        run_log_df = build_run_log(
            [(1, False, False, None), (2, True, True, None), (3, True, True, "v1")]
        )

        returned_df = job.set_run_tag(run_log_df, 2, TAG)

        other_runs = pl.col(Keys.run_number) != 2
        pl_testing.assert_frame_equal(
            run_log_df.filter(other_runs), returned_df.filter(other_runs)
        )


class TestReleaseChangelogEdgeCases:
    def test_raises_when_none_of_the_commits_entries_are_unreleased_on_main(self):
        at_commit = build_changelog("### Added\n- A.\n\n")
        at_main = build_changelog("### Added\n- B.\n\n")

        with pytest.raises(ValueError, match="nothing to release"):
            job.release_changelog(at_commit, at_main, TAG, RELEASE_DATE)

    def test_raises_when_the_changelog_has_no_unreleased_section(self):
        at_commit = build_changelog("### Added\n- A.\n\n")

        with pytest.raises(ValueError, match=r"no '## \[Unreleased\]' section"):
            job.release_changelog(at_commit, "# Changelog\n", TAG, RELEASE_DATE)

    def test_keeps_a_wrapped_entry_together(self):
        changelog = build_changelog("### Added\n- A wrapped\n  entry.\n\n")

        returned_changelog = job.release_changelog(
            changelog, changelog, TAG, RELEASE_DATE
        )

        assert "### Added\n- A wrapped\n  entry.\n\n" + OLDER_VERSIONS in (
            returned_changelog
        )

    def test_returns_the_same_changelog_for_crlf_input(self):
        changelog = build_changelog("### Added\n- A.\n\n")

        returned_changelog = job.release_changelog(
            changelog.replace("\n", "\r\n"),
            changelog.replace("\n", "\r\n"),
            TAG,
            RELEASE_DATE,
        )

        expected_changelog = job.release_changelog(
            changelog, changelog, TAG, RELEASE_DATE
        )
        assert returned_changelog == expected_changelog


def write_run_log(run_log_dir, rows):
    """Writes one parquet file per (run_number, selected, reconciled, tag) row."""
    for run_number, selected, reconciled, tag in rows:
        partition_dir = (
            run_log_dir / "archive_date=2026-10-15" / f"run_number={run_number}"
        )
        partition_dir.mkdir(parents=True)
        pl.DataFrame(
            {
                RunLog.commit_sha: [f"sha{run_number}"],
                RunLog.selected_for_publication: [selected],
                RunLog.reconciled: [reconciled],
                RunLog.tag: pl.Series([tag], dtype=pl.String),
            }
        ).write_parquet(partition_dir / "file.parquet")


class TestMain:
    def test_plan_prints_the_run_commit_tag_and_message(self, tmp_path, capsys):
        write_run_log(tmp_path / "log", [(2, True, True, None)])
        tags_file = tmp_path / "tags"
        tags_file.write_text("v2026.09.0 abc\n")

        exit_code = job.main(
            [
                "plan",
                "--run_log_dir",
                str(tmp_path / "log"),
                "--run_number",
                "2",
                "--tags_file",
                str(tags_file),
            ]
        )

        assert exit_code == 0
        assert json.loads(capsys.readouterr().out) == {
            "run_number": 2,
            "archive_date": "2026-10-15",
            "commit_sha": "sha2",
            "tag": "v2026.09.1",
            "tag_message": "Codebase used for data as at September 2026",
        }

    def test_plan_prints_the_plan_when_the_run_is_not_reconciled(
        self, tmp_path, capsys
    ):
        write_run_log(tmp_path / "log", [(2, True, False, None)])
        tags_file = tmp_path / "tags"
        tags_file.write_text("")

        exit_code = job.main(
            [
                "plan",
                "--run_log_dir",
                str(tmp_path / "log"),
                "--run_number",
                "2",
                "--tags_file",
                str(tags_file),
            ]
        )

        assert exit_code == 0
        assert json.loads(capsys.readouterr().out)["run_number"] == 2

    def test_plan_returns_one_when_the_run_is_not_selected(self, tmp_path, capsys):
        write_run_log(tmp_path / "log", [(2, False, True, None)])
        tags_file = tmp_path / "tags"
        tags_file.write_text("")

        exit_code = job.main(
            [
                "plan",
                "--run_log_dir",
                str(tmp_path / "log"),
                "--run_number",
                "2",
                "--tags_file",
                str(tags_file),
            ]
        )

        assert exit_code == 1
        assert "not selected_for_publication" in capsys.readouterr().err

    def test_record_tag_writes_the_tag_to_the_runs_partition(self, tmp_path):
        write_run_log(tmp_path / "log", [(1, True, True, None), (2, True, True, None)])

        exit_code = job.main(
            [
                "record-tag",
                "--run_log_dir",
                str(tmp_path / "log"),
                "--output_dir",
                str(tmp_path / "out"),
                "--run_number",
                "2",
                "--tag",
                "v2026.09.1",
            ]
        )

        written_df = pl.read_parquet(
            tmp_path / "out" / "**" / "*.parquet", hive_partitioning=True
        )
        assert exit_code == 0
        assert written_df[Keys.run_number].to_list() == [2]
        assert written_df[RunLog.tag].to_list() == ["v2026.09.1"]
