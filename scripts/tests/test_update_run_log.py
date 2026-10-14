from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import polars as pl
import polars.testing as pl_testing
import pytest

import scripts.update_run_log as job
from utils.column_names.ind_cqc_pipeline_columns import (
    ArchiveDateRunNumberPartitionKeys as Keys,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    ArchiveRunLogColumns as RunLog,
)

RUN_LOG_SCHEMA = {
    Keys.archive_date: pl.String,
    Keys.run_number: pl.Int64,
    RunLog.archive_date_time: pl.Datetime,
    RunLog.max_ascwds_workplace_import_date: pl.Date,
    RunLog.max_cqc_location_import_date: pl.Date,
    RunLog.max_cqc_pir_import_date: pl.Date,
    RunLog.max_ct_care_home_import_date: pl.Date,
    RunLog.max_ct_non_res_import_date: pl.Date,
    RunLog.max_current_ons_import_date: pl.Date,
    RunLog.approved: pl.Boolean,
    RunLog.commit_sha: pl.String,
    RunLog.tag: pl.String,
    RunLog.selected_for_publication: pl.Boolean,
    RunLog.reconciled: pl.Boolean,
    RunLog.locally_checked: pl.Boolean,
}

DEFAULT_VALUES = {
    pl.Boolean: False,
    pl.Int64: 0,
    pl.String: "abc123",
    pl.Datetime: datetime(2026, 9, 22, 9, 0),
    pl.Date: date(2026, 9, 1),
}


def build_run_log(runs: list[tuple[str, int, bool]]) -> pl.DataFrame:
    """Builds a full-width run log with one row per (archive_date, run_number, approved)."""
    rows = []
    for archive_date, run_number, approved in runs:
        row = {
            column: DEFAULT_VALUES[dtype] for column, dtype in RUN_LOG_SCHEMA.items()
        }
        row[Keys.archive_date] = archive_date
        row[Keys.run_number] = run_number
        row[RunLog.approved] = approved
        row[RunLog.tag] = None
        rows.append(row)
    return pl.DataFrame(rows, schema=RUN_LOG_SCHEMA)


@dataclass
class SetRunFlagRaisesTestCase:
    id: str
    runs: list[tuple[str, int, bool]]
    run_number: int
    flag: str
    message: str

    def as_pytest_param(self):
        return pytest.param(
            self.runs, self.run_number, self.flag, self.message, id=self.id
        )


set_run_flag_raises_cases = [
    SetRunFlagRaisesTestCase(
        id="raises_when_run_number_not_found",
        runs=[("2026-09-22", 1, False)],
        run_number=99,
        flag=RunLog.approved,
        message="99",
    ),
    SetRunFlagRaisesTestCase(
        id="raises_when_run_number_matches_multiple_rows",
        runs=[("2026-09-22", 1, False), ("2026-09-29", 1, False)],
        run_number=1,
        flag=RunLog.approved,
        message="multiple",
    ),
    SetRunFlagRaisesTestCase(
        id="raises_when_flag_already_true",
        runs=[("2026-09-22", 1, True)],
        run_number=1,
        flag=RunLog.approved,
        message="already",
    ),
    *[
        SetRunFlagRaisesTestCase(
            id=f"raises_when_flag_is_not_supported_{flag}",
            runs=[("2026-09-22", 1, False)],
            run_number=1,
            flag=flag,
            message="not supported",
        )
        for flag in [
            RunLog.reconciled,
            RunLog.selected_for_publication,
            RunLog.locally_checked,
            RunLog.tag,
            "unknown",
        ]
    ],
]


class TestSetRunFlag:
    def test_sets_approved_true_for_matching_run(self):
        run_log_df = build_run_log([("2026-09-22", 1, False), ("2026-09-29", 2, False)])

        returned_df = job.set_run_flag(run_log_df, 2, RunLog.approved)

        returned_approved = returned_df.filter(pl.col(Keys.run_number) == 2)
        assert returned_approved[RunLog.approved].to_list() == [True]

    def test_changes_only_the_flag_column_in_matching_run(self):
        run_log_df = build_run_log([("2026-09-22", 1, False)])

        returned_df = job.set_run_flag(run_log_df, 1, RunLog.approved)

        expected_df = run_log_df.with_columns(pl.lit(True).alias(RunLog.approved))
        pl_testing.assert_frame_equal(expected_df, returned_df)

    def test_leaves_other_runs_unchanged(self):
        run_log_df = build_run_log(
            [
                ("2026-09-22", 1, False),
                ("2026-09-29", 2, False),
                ("2026-10-06", 3, True),
            ]
        )

        returned_df = job.set_run_flag(run_log_df, 2, RunLog.approved)

        other_runs = pl.col(Keys.run_number) != 2
        pl_testing.assert_frame_equal(
            run_log_df.filter(other_runs), returned_df.filter(other_runs)
        )

    @pytest.mark.parametrize(
        "runs, run_number, flag, message",
        [case.as_pytest_param() for case in set_run_flag_raises_cases],
    )
    def test_raises_for_invalid_update(self, runs, run_number, flag, message):
        run_log_df = build_run_log(runs)

        with pytest.raises(ValueError, match=message):
            job.set_run_flag(run_log_df, run_number, flag)


def write_run_log(run_log_dir: Path, run_log_df: pl.DataFrame) -> None:
    """Writes a run log in the S3 layout: one folder per run, key columns not in files."""
    for row in run_log_df.iter_rows(named=True):
        partition_dir = (
            run_log_dir
            / f"{Keys.archive_date}={row[Keys.archive_date]}"
            / f"{Keys.run_number}={row[Keys.run_number]}"
        )
        partition_dir.mkdir(parents=True)
        run_log_df.filter(pl.col(Keys.run_number) == row[Keys.run_number]).drop(
            Keys.archive_date, Keys.run_number
        ).write_parquet(partition_dir / "file.parquet")


class TestReadRunLog:
    def test_reads_partition_key_columns_from_folder_names(self, tmp_path):
        run_log_df = build_run_log([("2026-09-22", 1, False), ("2026-09-29", 2, True)])
        write_run_log(tmp_path, run_log_df)

        returned_df = job.read_run_log(tmp_path).sort(Keys.run_number)

        pl_testing.assert_frame_equal(run_log_df, returned_df, check_column_order=False)

    def test_raises_when_run_log_dir_has_no_parquet_files(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="parquet"):
            job.read_run_log(tmp_path)


class TestWriteRunPartition:
    def test_writes_only_changed_run_partition_with_same_layout(self, tmp_path):
        run_log_df = build_run_log([("2026-09-22", 1, False), ("2026-09-29", 2, True)])

        job.write_run_partition(run_log_df, 2, tmp_path)

        written_files = [p.relative_to(tmp_path) for p in tmp_path.rglob("*.parquet")]
        expected_folder = (
            Path(f"{Keys.archive_date}=2026-09-29") / f"{Keys.run_number}=2"
        )
        assert len(written_files) == 1
        assert written_files[0].parent == expected_folder

    def test_written_files_exclude_partition_key_columns(self, tmp_path):
        run_log_df = build_run_log([("2026-09-22", 1, True)])

        job.write_run_partition(run_log_df, 1, tmp_path)

        written_file = next(tmp_path.rglob("*.parquet"))
        written_df = pl.read_parquet(written_file, hive_partitioning=False)
        assert written_df.columns == [
            c for c in RUN_LOG_SCHEMA if c not in (Keys.archive_date, Keys.run_number)
        ]


class TestMain:
    @pytest.fixture
    def run_log_dirs(self, tmp_path):
        original_dir = tmp_path / "original"
        write_run_log(
            original_dir,
            build_run_log([("2026-09-22", 1, False), ("2026-09-29", 2, False)]),
        )
        return original_dir, tmp_path / "updated"

    @staticmethod
    def run_main(original_dir, updated_dir, run_number="2", flag=RunLog.approved):
        return job.main(
            [
                "--run_log_dir",
                str(original_dir),
                "--output_dir",
                str(updated_dir),
                "--run_number",
                run_number,
                "--flag",
                flag,
            ]
        )

    def test_writes_updated_run_log_to_output_dir_and_returns_zero(self, run_log_dirs):
        original_dir, updated_dir = run_log_dirs

        exit_code = self.run_main(original_dir, updated_dir)

        assert exit_code == 0
        updated_df = job.read_run_log(updated_dir)
        assert updated_df[Keys.run_number].to_list() == [2]
        assert updated_df[RunLog.approved].to_list() == [True]

    def test_does_not_modify_original_run_log_dir(self, run_log_dirs):
        original_dir, updated_dir = run_log_dirs
        files_before = {p: p.read_bytes() for p in original_dir.rglob("*.parquet")}

        self.run_main(original_dir, updated_dir)

        files_after = {p: p.read_bytes() for p in original_dir.rglob("*.parquet")}
        assert files_after == files_before

    def test_returns_nonzero_on_error(self, run_log_dirs):
        original_dir, updated_dir = run_log_dirs

        exit_code = self.run_main(original_dir, updated_dir, run_number="99")

        assert exit_code != 0

    def test_leaves_output_dir_empty_on_error(self, run_log_dirs):
        original_dir, updated_dir = run_log_dirs

        self.run_main(original_dir, updated_dir, run_number="99")

        assert not updated_dir.exists()

    def test_prints_changed_row(self, run_log_dirs, capsys):
        original_dir, updated_dir = run_log_dirs

        self.run_main(original_dir, updated_dir)

        printed = capsys.readouterr().out
        before_text, after_text = printed.split("After:")
        assert "2026-09-29" in before_text
        assert f"{RunLog.approved}: False" in before_text
        assert f"{RunLog.approved}: True" in after_text
