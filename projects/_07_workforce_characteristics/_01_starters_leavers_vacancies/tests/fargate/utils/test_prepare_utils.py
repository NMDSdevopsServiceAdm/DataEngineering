from datetime import date

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.prepare_utils as job
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_job_roles import (
    AscwdsWorkplaceJobRolesColumns as AWPJobRoles,
)

INDEX_COLS = [AWPClean.establishment_id, AWPClean.ascwds_workplace_import_date]


class TestDiscoverJobRoleCodes:
    def test_discovers_non_contiguous_codes_and_orders_numerically(self):
        schema = {
            AWPClean.establishment_id: pl.String,
            "jr01emp": pl.Int32,
            "jr01strt": pl.Int32,
            "jr01stop": pl.Int32,
            "jr01vacy": pl.Int32,
            "jr45emp": pl.Int32,
            "jr45strt": pl.Int32,
            "jr45stop": pl.Int32,
            "jr45vacy": pl.Int32,
            "jr02emp": pl.Int32,
            "jr02strt": pl.Int32,
            "jr02stop": pl.Int32,
            "jr02vacy": pl.Int32,
        }

        discovered = job.discover_job_role_codes(schema)

        assert [cols.job_role_code for cols in discovered] == ["1", "2", "45"]

    def test_ignores_columns_outside_the_slv_selector(self):
        schema = {
            AWPClean.establishment_id: pl.String,
            "jr01emp": pl.Int32,
            "jr01strt": pl.Int32,
            "jr01stop": pl.Int32,
            "jr01vacy": pl.Int32,
            "jr01temp": pl.Int32,  # excluded: ends with "temp"
            "jr01flag": pl.Int32,  # excluded: not a metric suffix
        }

        discovered = job.discover_job_role_codes(schema)

        assert len(discovered) == 1
        assert discovered[0].job_role_code == "1"

    def test_strips_leading_zeros_from_the_code(self):
        schema = {
            "jr07emp": pl.Int32,
            "jr07strt": pl.Int32,
            "jr07stop": pl.Int32,
            "jr07vacy": pl.Int32,
        }

        discovered = job.discover_job_role_codes(schema)

        assert discovered[0].job_role_code == "7"

    def test_raises_when_a_code_is_missing_one_of_its_metric_columns(self):
        schema = {
            "jr01emp": pl.Int32,
            "jr01strt": pl.Int32,
        }

        with pytest.raises(ValueError, match="missing one or more"):
            job.discover_job_role_codes(schema)

    def test_raises_when_a_column_does_not_match_the_expected_naming_pattern(self):
        schema = {
            "jrABemp": pl.Int32,
        }

        with pytest.raises(ValueError, match="naming pattern"):
            job.discover_job_role_codes(schema)


class TestConvertJobRoleColumnsToRows:
    def test_reshapes_wide_job_role_columns_to_one_row_per_code(self):
        job_role_columns = [
            job.JobRoleCodeColumns(
                job_role_code="1",
                employees="jr01emp",
                starters="jr01strt",
                leavers="jr01stop",
                vacancies="jr01vacy",
            ),
            job.JobRoleCodeColumns(
                job_role_code="45",
                employees="jr45emp",
                starters="jr45strt",
                leavers="jr45stop",
                vacancies="jr45vacy",
            ),
        ]
        workplace_lf = pl.LazyFrame(
            {
                AWPClean.establishment_id: ["1-001", "1-002"],
                AWPClean.ascwds_workplace_import_date: [
                    date(2026, 1, 1),
                    date(2026, 1, 1),
                ],
                "jr01emp": [5, 7],
                "jr01strt": [1, 2],
                "jr01stop": [0, 1],
                "jr01vacy": [2, 0],
                "jr45emp": [10, 3],
                "jr45strt": [0, 1],
                "jr45stop": [1, 0],
                "jr45vacy": [0, 2],
            },
            schema_overrides={
                col: pl.Int32
                for col in (
                    "jr01emp",
                    "jr01strt",
                    "jr01stop",
                    "jr01vacy",
                    "jr45emp",
                    "jr45strt",
                    "jr45stop",
                    "jr45vacy",
                )
            },
        )

        returned_lf = job.convert_job_role_columns_to_rows(
            workplace_lf, INDEX_COLS, job_role_columns
        )

        expected_lf = pl.LazyFrame(
            {
                AWPClean.establishment_id: ["1-001", "1-001", "1-002", "1-002"],
                AWPClean.ascwds_workplace_import_date: [date(2026, 1, 1)] * 4,
                AWPJobRoles.job_role_code: ["1", "45", "1", "45"],
                AWPJobRoles.employees: [5, 10, 7, 3],
                AWPJobRoles.starters: [1, 0, 2, 1],
                AWPJobRoles.leavers: [0, 1, 1, 0],
                AWPJobRoles.vacancies: [2, 0, 0, 2],
            },
            schema_overrides={
                AWPJobRoles.employees: pl.Int32,
                AWPJobRoles.starters: pl.Int32,
                AWPJobRoles.leavers: pl.Int32,
                AWPJobRoles.vacancies: pl.Int32,
            },
        )

        sort_keys = [AWPClean.establishment_id, AWPJobRoles.job_role_code]
        pl_testing.assert_frame_equal(
            returned_lf.sort(sort_keys),
            expected_lf.sort(sort_keys),
            check_column_order=False,
        )

    def test_keeps_a_row_for_a_code_with_all_null_metrics(self):
        job_role_columns = [
            job.JobRoleCodeColumns(
                job_role_code="1",
                employees="jr01emp",
                starters="jr01strt",
                leavers="jr01stop",
                vacancies="jr01vacy",
            ),
        ]
        workplace_lf = pl.LazyFrame(
            {
                AWPClean.establishment_id: ["1-001"],
                AWPClean.ascwds_workplace_import_date: [date(2026, 1, 1)],
                "jr01emp": [None],
                "jr01strt": [None],
                "jr01stop": [None],
                "jr01vacy": [None],
            },
            schema_overrides={
                "jr01emp": pl.Int32,
                "jr01strt": pl.Int32,
                "jr01stop": pl.Int32,
                "jr01vacy": pl.Int32,
            },
        )

        returned_lf = job.convert_job_role_columns_to_rows(
            workplace_lf, INDEX_COLS, job_role_columns
        )

        expected_lf = pl.LazyFrame(
            {
                AWPClean.establishment_id: ["1-001"],
                AWPClean.ascwds_workplace_import_date: [date(2026, 1, 1)],
                AWPJobRoles.job_role_code: ["1"],
                AWPJobRoles.employees: [None],
                AWPJobRoles.starters: [None],
                AWPJobRoles.leavers: [None],
                AWPJobRoles.vacancies: [None],
            },
            schema_overrides={
                AWPJobRoles.employees: pl.Int32,
                AWPJobRoles.starters: pl.Int32,
                AWPJobRoles.leavers: pl.Int32,
                AWPJobRoles.vacancies: pl.Int32,
            },
        )

        pl_testing.assert_frame_equal(
            returned_lf, expected_lf, check_column_order=False
        )


class TestPeakRssKb:
    def test_returns_an_int_or_none(self):
        result = job.peak_rss_kb()

        assert result is None or isinstance(result, int)
