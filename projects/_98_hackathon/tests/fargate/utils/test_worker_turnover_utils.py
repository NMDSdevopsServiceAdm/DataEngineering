import polars as pl
import polars.testing as pl_testing
import pytest

import projects._98_hackathon.fargate.utils.worker_turnover_utils as job
from projects._98_hackathon.unittest_data.polars_hackathon_test_file_data import (
    WorkerTurnoverData as Data,
)
from projects._98_hackathon.unittest_data.polars_hackathon_test_file_schemas import (
    WorkerTurnoverSchemas as Schemas,
)
from utils.column_names.hackathon_columns import HackathonColumns as HackCols


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


class TestGetLatestSaveDatePerEstablishment:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.get_latest_save_date_per_establishment_cases
        ],
    )
    def test_returns_expected_rows(self, case):
        input_lf = pl.LazyFrame(
            case.input_rows, schema=Schemas.latest_save_date_input_schema, orient="row"
        )
        snapshot_lf = pl.LazyFrame(
            case.snapshot_rows or case.input_rows,
            schema=Schemas.latest_save_date_input_schema,
            orient="row",
        )
        expected_lf = pl.LazyFrame(
            case.expected_rows, schema=Schemas.latest_save_date_schema, orient="row"
        )

        returned_lf = job.get_latest_save_date_per_establishment(input_lf, snapshot_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)


class TestAddEstablishmentActiveFlag:
    @staticmethod
    def build_input_lfs(case):
        worker_lf = pl.LazyFrame(
            case.worker_rows, schema=Schemas.active_input_schema, orient="row"
        )
        save_dates_lf = pl.LazyFrame(
            case.save_date_rows, schema=Schemas.latest_save_date_schema, orient="row"
        )
        return worker_lf, save_dates_lf

    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.add_establishment_active_flag_cases
        ],
    )
    def test_returns_expected_rows(self, case):
        worker_lf, save_dates_lf = self.build_input_lfs(case)
        expected_lf = pl.LazyFrame(
            case.expected_rows, schema=Schemas.active_output_schema, orient="row"
        )

        returned_lf = job.add_establishment_active_flag(
            worker_lf, save_dates_lf, case.n_months
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)

    def test_outputs_integer_gaps_and_boolean_flag(self):
        worker_lf, save_dates_lf = self.build_input_lfs(
            Data.add_establishment_active_flag_cases[0]
        )

        returned_schema = job.add_establishment_active_flag(
            worker_lf, save_dates_lf
        ).collect_schema()

        assert returned_schema[HackCols.months_since_save_start] == pl.Int32
        assert returned_schema[HackCols.months_since_save_end] == pl.Int32
        assert returned_schema[HackCols.establishment_active] == pl.Boolean


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
