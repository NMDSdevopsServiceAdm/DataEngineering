from datetime import date

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._98_hackathon.fargate.utils.worker_turnover_utils as job
from projects._98_hackathon.unittest_data.polars_hackathon_test_file_data import (
    WorkerTurnoverData as Data,
)
from projects._98_hackathon.unittest_data.polars_hackathon_test_file_data import (
    raw_worker_row,
)
from projects._98_hackathon.unittest_data.polars_hackathon_test_file_schemas import (
    WorkerTurnoverSchemas as Schemas,
)
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)


class TestFilterWorkerTurnoverData:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.filter_worker_turnover_data_cases
        ],
    )
    def test_returns_expected_rows(self, case):
        input_lf = pl.LazyFrame(
            case.input_rows, schema=Schemas.filter_worker_schema, orient="row"
        )
        expected_lf = pl.LazyFrame(
            case.expected_rows, schema=Schemas.filter_worker_schema, orient="row"
        )

        returned_lf = job.filter_worker_turnover_data(input_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestAddMainServiceType:
    @pytest.mark.parametrize(
        "case",
        [pytest.param(case, id=case.id) for case in Data.add_main_service_type_cases],
    )
    def test_returns_expected_rows(self, case):
        input_lf = pl.LazyFrame(
            case.input_rows, schema=Schemas.main_service_input_schema, orient="row"
        )
        expected_lf = pl.LazyFrame(
            case.expected_rows,
            schema=Schemas.main_service_type_schema,
            orient="row",
        )

        returned_lf = job.add_main_service_type(input_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestAddIsLeaverFlag:
    @pytest.mark.parametrize(
        "case",
        [pytest.param(case, id=case.id) for case in Data.add_is_leaver_flag_cases],
    )
    def test_returns_expected_rows(self, case):
        input_lf = pl.LazyFrame(
            case.input_rows, schema=Schemas.is_leaver_input_schema, orient="row"
        )
        expected_lf = pl.LazyFrame(
            case.expected_rows, schema=Schemas.is_leaver_output_schema, orient="row"
        )

        returned_lf = job.add_is_leaver_flag(input_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)


def raw_worker_lf(*rows: dict) -> pl.LazyFrame:
    """Returns raw worker rows as a LazyFrame with all-string columns."""
    return pl.LazyFrame(list(rows), schema={column: pl.String for column in rows[0]})


class TestAddRawWorkerColumns:
    flagged_lf = pl.LazyFrame(
        [("1-001", "1", date(2021, 1, 5))],
        schema=Schemas.raw_worker_join_input_schema,
        orient="row",
    )

    def test_adds_all_modelling_columns_after_input_columns(self):
        raw_lf = raw_worker_lf(raw_worker_row("1", "20210105"))

        returned_lf = job.add_raw_worker_columns(self.flagged_lf, raw_lf)

        assert returned_lf.collect_schema().names() == [
            *Schemas.raw_worker_join_input_schema.names(),
            *job.WORKER_CATEGORICAL_COLUMNS,
            *job.WORKER_NUMERIC_COLUMNS,
            *job.WORKER_STRING_COLUMNS,
        ]

    def test_returns_raw_values_for_worker_with_matching_keys(self):
        raw_lf = raw_worker_lf(
            raw_worker_row(
                "1",
                "20210105",
                **{
                    AWKClean.gender: "1",
                    AWKClean.age: "42",
                    AWKClean.start_date: "2015-03-01",
                },
            )
        )

        returned_df = job.add_raw_worker_columns(self.flagged_lf, raw_lf).collect()

        assert returned_df[AWKClean.gender].to_list() == ["1"]
        assert returned_df[AWKClean.age].to_list() == [42.0]
        assert returned_df[AWKClean.start_date].to_list() == ["2015-03-01"]

    @pytest.mark.parametrize(
        "raw_row",
        [
            pytest.param(
                raw_worker_row("2", "20210105", **{AWKClean.gender: "1"}),
                id="other_worker",
            ),
            pytest.param(
                raw_worker_row("1", "20210105", "1-002", **{AWKClean.gender: "1"}),
                id="other_establishment",
            ),
            pytest.param(
                raw_worker_row("1", "20220105", **{AWKClean.gender: "1"}),
                id="other_import_date",
            ),
        ],
    )
    def test_returns_null_columns_when_keys_do_not_match(self, raw_row):
        returned_df = job.add_raw_worker_columns(
            self.flagged_lf, raw_worker_lf(raw_row)
        ).collect()

        assert returned_df.height == 1
        assert returned_df[AWKClean.gender].to_list() == [None]

    def test_returns_expected_column_types(self):
        raw_lf = raw_worker_lf(raw_worker_row("1", "20210105"))

        schema = job.add_raw_worker_columns(self.flagged_lf, raw_lf).collect_schema()

        assert schema[AWKClean.gender] == pl.Categorical
        assert schema[AWKClean.age] == pl.Float32
        assert schema[AWKClean.gender_change_date] == pl.String

    def test_returns_null_for_unparseable_numeric_value(self):
        raw_lf = raw_worker_lf(
            raw_worker_row("1", "20210105", **{AWKClean.age: "unknown"})
        )

        returned_df = job.add_raw_worker_columns(self.flagged_lf, raw_lf).collect()

        assert returned_df[AWKClean.age].to_list() == [None]
