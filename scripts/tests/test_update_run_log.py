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

MONTH_START = date(2026, 10, 1)

DEFAULT_VALUES = {
    pl.Boolean: False,
    pl.Int64: 0,
    pl.String: "abc123",
    pl.Datetime: datetime(2026, 9, 22, 9, 0),
    pl.Date: date(2026, 9, 1),
}


def build_run_log(
    runs: list[tuple[str, int, bool]], selected_runs: tuple[int, ...] = ()
) -> pl.DataFrame:
    """Builds a full-width run log with one row per (archive_date, run_number, approved).

    The runs in `selected_runs` have `selected_for_publication` True.
    """
    rows = []
    for archive_date, run_number, approved in runs:
        row = {
            column: DEFAULT_VALUES[dtype] for column, dtype in RUN_LOG_SCHEMA.items()
        }
        row[Keys.archive_date] = archive_date
        row[Keys.run_number] = run_number
        row[RunLog.approved] = approved
        row[RunLog.selected_for_publication] = run_number in selected_runs
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
        for flag in [RunLog.tag, "unknown"]
    ],
]


class TestSetRunFlag:
    def test_sets_approved_true_for_matching_run(self):
        run_log_df = build_run_log([("2026-09-22", 1, False), ("2026-09-29", 2, False)])

        returned_df = job.set_run_flag(run_log_df, 2, RunLog.approved, MONTH_START)

        returned_approved = returned_df.filter(pl.col(Keys.run_number) == 2)
        assert returned_approved[RunLog.approved].to_list() == [True]

    def test_changes_only_the_flag_column_in_matching_run(self):
        run_log_df = build_run_log([("2026-09-22", 1, False)])

        returned_df = job.set_run_flag(run_log_df, 1, RunLog.approved, MONTH_START)

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

        returned_df = job.set_run_flag(run_log_df, 2, RunLog.approved, MONTH_START)

        other_runs = pl.col(Keys.run_number) != 2
        pl_testing.assert_frame_equal(
            run_log_df.filter(other_runs), returned_df.filter(other_runs)
        )

    @pytest.mark.parametrize("flag", job.SUPPORTED_FLAGS)
    def test_sets_each_supported_flag_true(self, flag):
        is_approved = flag != RunLog.approved
        run_log_df = build_run_log([("2026-10-06", 1, is_approved)])

        returned_df = job.set_run_flag(run_log_df, 1, flag, MONTH_START)

        assert returned_df[flag].to_list() == [True]

    def test_approved_ignores_the_month_start(self):
        run_log_df = build_run_log([("2026-09-22", 1, False)])

        returned_df = job.set_run_flag(run_log_df, 1, RunLog.approved, MONTH_START)

        assert returned_df[RunLog.approved].to_list() == [True]

    @pytest.mark.parametrize(
        "runs, run_number, flag, message",
        [case.as_pytest_param() for case in set_run_flag_raises_cases],
    )
    def test_raises_for_invalid_update(self, runs, run_number, flag, message):
        run_log_df = build_run_log(runs)

        with pytest.raises(ValueError, match=message):
            job.set_run_flag(run_log_df, run_number, flag, MONTH_START)


@dataclass
class SelectRaisesTestCase:
    id: str
    runs: list[tuple[str, int, bool]]
    message: str
    selected_runs: tuple[int, ...] = ()

    def as_pytest_param(self):
        return pytest.param(self.runs, self.selected_runs, self.message, id=self.id)


select_raises_cases = [
    SelectRaisesTestCase(
        id="raises_when_run_is_not_approved",
        runs=[("2026-10-06", 1, False)],
        message="not approved",
    ),
    SelectRaisesTestCase(
        id="raises_when_run_is_before_the_month_start",
        runs=[("2026-09-22", 1, True)],
        message="before 2026-10-01",
    ),
    SelectRaisesTestCase(
        id="raises_when_run_is_archived_the_day_before_the_month_start",
        runs=[("2026-09-30", 1, True)],
        message="before 2026-10-01",
    ),
    SelectRaisesTestCase(
        id="raises_when_selected_run_is_before_the_month_start",
        runs=[("2026-09-22", 1, True)],
        selected_runs=(1,),
        message="before 2026-10-01",
    ),
    SelectRaisesTestCase(
        id="raises_when_run_is_already_selected",
        runs=[("2026-10-06", 1, True)],
        selected_runs=(1,),
        message="already",
    ),
]


class TestSetRunFlagForSelection:
    @pytest.mark.parametrize(
        "runs, selected_runs, message",
        [case.as_pytest_param() for case in select_raises_cases],
    )
    def test_raises_for_run_that_cannot_be_selected(self, runs, selected_runs, message):
        run_log_df = build_run_log(runs, selected_runs)

        with pytest.raises(ValueError, match=message):
            job.set_run_flag(
                run_log_df, 1, RunLog.selected_for_publication, MONTH_START
            )

    def test_moves_selection_from_another_run_this_month(self):
        run_log_df = build_run_log(
            [("2026-10-01", 1, True), ("2026-10-08", 2, True)], selected_runs=(1,)
        )

        returned_df = job.set_run_flag(
            run_log_df, 2, RunLog.selected_for_publication, MONTH_START
        )

        assert returned_df[RunLog.selected_for_publication].to_list() == [False, True]

    def test_leaves_selected_runs_before_the_month_start_unchanged(self):
        run_log_df = build_run_log(
            [("2026-09-22", 1, True), ("2026-10-08", 2, True)], selected_runs=(1,)
        )

        returned_df = job.set_run_flag(
            run_log_df, 2, RunLog.selected_for_publication, MONTH_START
        )

        assert returned_df[RunLog.selected_for_publication].to_list() == [True, True]

    def test_selects_run_archived_on_the_month_start(self):
        run_log_df = build_run_log([("2026-10-01", 1, True)])

        returned_df = job.set_run_flag(
            run_log_df, 1, RunLog.selected_for_publication, MONTH_START
        )

        assert returned_df[RunLog.selected_for_publication].to_list() == [True]

    def test_clears_every_other_selected_run_this_month(self):
        run_log_df = build_run_log(
            [("2026-10-01", 1, True), ("2026-10-08", 2, True), ("2026-10-12", 3, True)],
            selected_runs=(1, 2),
        )

        returned_df = job.set_run_flag(
            run_log_df, 3, RunLog.selected_for_publication, MONTH_START
        )

        assert returned_df[RunLog.selected_for_publication].to_list() == [
            False,
            False,
            True,
        ]

    def test_moving_selection_changes_only_the_selected_column(self):
        run_log_df = build_run_log(
            [("2026-10-01", 1, True), ("2026-10-08", 2, True)], selected_runs=(1,)
        )

        returned_df = job.set_run_flag(
            run_log_df, 2, RunLog.selected_for_publication, MONTH_START
        )

        other_columns = [
            c for c in run_log_df.columns if c != RunLog.selected_for_publication
        ]
        pl_testing.assert_frame_equal(
            run_log_df.select(other_columns), returned_df.select(other_columns)
        )


class TestGetChangedRunNumbers:
    def test_returns_only_runs_where_the_flag_changed(self):
        run_log_df = build_run_log(
            [
                ("2026-10-01", 1, False),
                ("2026-10-08", 2, False),
                ("2026-10-15", 3, False),
            ]
        )
        updated_df = job.set_run_flag(run_log_df, 2, RunLog.approved, MONTH_START)

        returned_run_numbers = job.get_changed_run_numbers(
            run_log_df, updated_df, RunLog.approved
        )

        assert returned_run_numbers == [2]

    def test_returns_both_runs_when_the_selection_moves(self):
        run_log_df = build_run_log(
            [("2026-10-01", 1, True), ("2026-10-08", 2, True)], selected_runs=(1,)
        )
        updated_df = job.set_run_flag(
            run_log_df, 2, RunLog.selected_for_publication, MONTH_START
        )

        returned_run_numbers = job.get_changed_run_numbers(
            run_log_df, updated_df, RunLog.selected_for_publication
        )

        assert returned_run_numbers == [1, 2]

    def test_returns_run_whose_flag_changed_from_null(self):
        run_log_df = build_run_log(
            [("2026-10-01", 1, True), ("2026-10-08", 2, True)], selected_runs=(1,)
        ).with_columns(
            pl.when(pl.col(Keys.run_number) == 2)
            .then(None)
            .otherwise(pl.col(RunLog.selected_for_publication))
            .alias(RunLog.selected_for_publication)
        )
        updated_df = job.set_run_flag(
            run_log_df, 2, RunLog.selected_for_publication, MONTH_START
        )

        returned_run_numbers = job.get_changed_run_numbers(
            run_log_df, updated_df, RunLog.selected_for_publication
        )

        assert returned_run_numbers == [1, 2]


class TestFormatRunTable:
    def test_lists_runs_from_the_month_start_with_their_flags(self):
        run_log_df = build_run_log(
            [("2026-10-08", 2, True), ("2026-10-01", 1, False)], selected_runs=(2,)
        )

        returned_lines = job.format_run_table(run_log_df, MONTH_START).splitlines()

        assert returned_lines[0].split() == [
            Keys.run_number,
            Keys.archive_date,
            RunLog.approved,
            RunLog.selected_for_publication,
            RunLog.reconciled,
            RunLog.locally_checked,
            RunLog.tag,
        ]
        assert returned_lines[1].split() == [
            "1",
            "2026-10-01",
            "False",
            "False",
            "False",
            "False",
        ]
        assert returned_lines[2].split() == [
            "2",
            "2026-10-08",
            "True",
            "True",
            "False",
            "False",
        ]

    def test_excludes_runs_archived_before_the_month_start(self):
        run_log_df = build_run_log([("2026-09-22", 1, True), ("2026-10-08", 2, True)])

        returned_text = job.format_run_table(run_log_df, MONTH_START)

        assert "2026-09-22" not in returned_text
        assert "2026-10-08" in returned_text

    def test_says_so_when_no_runs_are_in_the_window(self):
        run_log_df = build_run_log([("2026-09-22", 1, True)])

        returned_text = job.format_run_table(run_log_df, MONTH_START)

        assert returned_text == "No runs archived on or after 2026-10-01."


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

    @pytest.fixture
    def selection_dirs(self, tmp_path):
        original_dir = tmp_path / "original"
        write_run_log(
            original_dir,
            build_run_log(
                [
                    ("2026-09-22", 1, True),
                    ("2026-10-01", 2, True),
                    ("2026-10-08", 3, True),
                ],
                selected_runs=(1, 2),
            ),
        )
        return original_dir, tmp_path / "updated"

    @staticmethod
    def run_selection(original_dir, updated_dir):
        return job.main(
            [
                "--run_log_dir",
                str(original_dir),
                "--output_dir",
                str(updated_dir),
                "--run_number",
                "3",
                "--flag",
                RunLog.selected_for_publication,
                "--month_start",
                "2026-10-01",
            ]
        )

    def test_writes_each_changed_partition_when_the_selection_moves(
        self, selection_dirs
    ):
        original_dir, updated_dir = selection_dirs

        exit_code = self.run_selection(original_dir, updated_dir)

        assert exit_code == 0
        updated_df = job.read_run_log(updated_dir).sort(Keys.run_number)
        assert updated_df[Keys.run_number].to_list() == [2, 3]
        assert updated_df[RunLog.selected_for_publication].to_list() == [False, True]

    def test_prints_before_and_after_for_each_changed_run(self, selection_dirs, capsys):
        original_dir, updated_dir = selection_dirs

        self.run_selection(original_dir, updated_dir)

        printed = capsys.readouterr().out
        run_blocks = {
            block.split(":")[0]: block.split("Before:")[1].split("After:")
            for block in printed.split("Run ")[1:]
        }
        selected = RunLog.selected_for_publication
        assert sorted(run_blocks) == ["2", "3"]
        assert f"{selected}: True" in run_blocks["2"][0]
        assert f"{selected}: False" in run_blocks["2"][1]
        assert f"{selected}: False" in run_blocks["3"][0]
        assert f"{selected}: True" in run_blocks["3"][1]

    def test_list_prints_runs_and_returns_zero(self, selection_dirs, capsys):
        original_dir, _ = selection_dirs

        exit_code = job.main(
            [
                "--run_log_dir",
                str(original_dir),
                "--list",
                "--month_start",
                "2026-10-01",
            ]
        )

        printed = capsys.readouterr().out
        assert exit_code == 0
        assert "2026-10-08" in printed
        assert "2026-09-22" not in printed

    def test_leaves_output_dir_empty_when_selection_is_rejected(self, selection_dirs):
        original_dir, updated_dir = selection_dirs

        exit_code = job.main(
            [
                "--run_log_dir",
                str(original_dir),
                "--output_dir",
                str(updated_dir),
                "--run_number",
                "1",
                "--flag",
                RunLog.selected_for_publication,
                "--month_start",
                "2026-10-01",
            ]
        )

        assert exit_code != 0
        assert not updated_dir.exists()
