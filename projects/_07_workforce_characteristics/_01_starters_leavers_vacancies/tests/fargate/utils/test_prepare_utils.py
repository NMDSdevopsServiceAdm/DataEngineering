from dataclasses import dataclass

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.prepare_utils as job
from projects._07_workforce_characteristics.unittest_data.polars_slv_test_data import (
    Data,
)
from projects._07_workforce_characteristics.unittest_data.polars_slv_test_schemas import (
    Schemas,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)

PATCH_PATH = "projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.prepare_utils"

INDEX_COLS = [AWPClean.establishment_id, AWPClean.ascwds_workplace_import_date]


class TestReduceToPublishedRoles:
    def test_reduce_to_published_roles(self):
        pass


@dataclass
class DiscoverJobRoleCodesTestCase:
    id: str
    schema: dict
    expected_codes: list[str]

    def as_pytest_param(self):
        return pytest.param(self.schema, self.expected_codes, id=self.id)


discover_job_role_codes_cases = [
    DiscoverJobRoleCodesTestCase(
        id="discovers_non_contiguous_codes_and_orders_numerically",
        schema={
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
        },
        expected_codes=["1", "2", "45"],
    ),
    DiscoverJobRoleCodesTestCase(
        id="ignores_columns_outside_the_slv_selector",
        schema={
            AWPClean.establishment_id: pl.String,
            "jr01emp": pl.Int32,
            "jr01strt": pl.Int32,
            "jr01stop": pl.Int32,
            "jr01vacy": pl.Int32,
            "jr01temp": pl.Int32,  # excluded: ends with "temp"
            "jr01flag": pl.Int32,  # excluded: not a metric suffix
        },
        expected_codes=["1"],
    ),
    DiscoverJobRoleCodesTestCase(
        id="strips_leading_zeros_from_the_code",
        schema={
            "jr07emp": pl.Int32,
            "jr07strt": pl.Int32,
            "jr07stop": pl.Int32,
            "jr07vacy": pl.Int32,
        },
        expected_codes=["7"],
    ),
]


@dataclass
class DiscoverJobRoleCodesRaisesTestCase:
    id: str
    schema: dict
    match: str

    def as_pytest_param(self):
        return pytest.param(self.schema, self.match, id=self.id)


discover_job_role_codes_raises_cases = [
    DiscoverJobRoleCodesRaisesTestCase(
        id="raises_when_no_slv_columns_are_found",
        schema={AWPClean.establishment_id: pl.String},
        match="No SLV job-role columns found",
    ),
    DiscoverJobRoleCodesRaisesTestCase(
        id="raises_when_a_code_is_missing_one_of_its_metric_columns",
        schema={
            "jr01emp": pl.Int32,
            "jr01strt": pl.Int32,
        },
        match="missing one or more",
    ),
    DiscoverJobRoleCodesRaisesTestCase(
        id="raises_when_a_column_does_not_match_the_expected_naming_pattern",
        schema={
            "jrABemp": pl.Int32,
        },
        match="naming pattern",
    ),
    DiscoverJobRoleCodesRaisesTestCase(
        id="raises_when_two_columns_collide_on_the_same_normalised_code",
        schema={
            "jr01emp": pl.Int32,
            "jr01strt": pl.Int32,
            "jr01stop": pl.Int32,
            "jr01vacy": pl.Int32,
            "jr1emp": pl.Int32,
        },
        match="both normalise to job role code",
    ),
]


class TestDiscoverJobRoleCodes:
    @pytest.mark.parametrize(
        "schema,expected_codes",
        [c.as_pytest_param() for c in discover_job_role_codes_cases],
    )
    def test_returns_expected_job_role_codes(self, schema, expected_codes):
        discovered = job.discover_job_role_codes(schema)

        assert [cols.job_role_code for cols in discovered] == expected_codes

    @pytest.mark.parametrize(
        "schema,match",
        [c.as_pytest_param() for c in discover_job_role_codes_raises_cases],
    )
    def test_raises_value_error(self, schema, match):
        with pytest.raises(ValueError, match=match):
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
            Data.wide_two_job_roles_rows, schema=Schemas.wide_two_job_roles_schema
        )

        returned_lf = job.convert_job_role_columns_to_rows(
            workplace_lf, INDEX_COLS, job_role_columns
        )

        expected_lf = pl.LazyFrame(
            Data.expected_long_two_job_roles_rows, schema=Schemas.long_job_roles_schema
        )

        sort_keys = [AWPClean.establishment_id, "job_role_code"]
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
            Data.wide_one_job_role_all_null_rows,
            schema=Schemas.wide_one_job_role_all_null_schema,
        )

        returned_lf = job.convert_job_role_columns_to_rows(
            workplace_lf, INDEX_COLS, job_role_columns
        )

        expected_lf = pl.LazyFrame(
            Data.expected_long_one_job_role_all_null_rows,
            schema=Schemas.long_job_roles_schema,
        )

        pl_testing.assert_frame_equal(
            returned_lf, expected_lf, check_column_order=False
        )

    def test_downcasts_metric_columns_to_int16(self):
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
            Data.wide_one_job_role_all_null_rows,
            schema=Schemas.wide_one_job_role_all_null_schema,
        )

        returned_schema = job.convert_job_role_columns_to_rows(
            workplace_lf, INDEX_COLS, job_role_columns
        ).collect_schema()

        assert returned_schema["employees"] == pl.Int16
        assert returned_schema["starters"] == pl.Int16
        assert returned_schema["leavers"] == pl.Int16
        assert returned_schema["vacancies"] == pl.Int16
