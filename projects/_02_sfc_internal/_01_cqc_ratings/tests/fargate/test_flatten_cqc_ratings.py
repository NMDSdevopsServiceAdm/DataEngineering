from dataclasses import dataclass, field
from typing import Any
from unittest.mock import Mock, patch

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._02_sfc_internal._01_cqc_ratings.fargate.flatten_cqc_ratings as job
from projects._02_sfc_internal.unittest_data.polars_sfc_test_file_data import (
    FlattenCQCRatings as Data,
)
from projects._02_sfc_internal.unittest_data.polars_sfc_test_file_schemas import (
    FlattenCQCRatings as Schemas,
)
from utils.column_names.cqc_ratings_columns import CQCRatingsColumns as CQCRatings
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_values.categorical_column_values import (
    CQCRatingsDatasetValues,
)

PATCH_PATH = "projects._02_sfc_internal._01_cqc_ratings.fargate.flatten_cqc_ratings"


class TestMain:
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_sinks_standard_and_benchmark_ratings_to_their_destinations(
        self, scan_parquet_mock: Mock, sink_to_parquet_mock: Mock
    ):
        scan_parquet_mock.side_effect = [
            pl.LazyFrame(
                Data.main_snapshot_rows,
                schema=Schemas.main_snapshot_schema,
                orient="row",
            ),
            pl.LazyFrame(
                Data.main_delta_rows, schema=Schemas.main_delta_schema, orient="row"
            ),
            pl.LazyFrame(
                Data.main_ascwds_workplace_rows,
                schema=Schemas.main_ascwds_workplace_schema,
                orient="row",
            ),
        ]

        job.main(
            "snapshot_source/",
            "delta_source/",
            "ascwds_source/",
            "ratings_dest/",
            "benchmark_dest/",
        )

        scanned_sources = [c.args[0] for c in scan_parquet_mock.call_args_list]
        assert scanned_sources == [
            "snapshot_source/",
            "delta_source/",
            "ascwds_source/",
        ]

        sinks = {c.args[1]: c.args[0] for c in sink_to_parquet_mock.call_args_list}
        assert set(sinks) == {"ratings_dest/", "benchmark_dest/"}

        ratings_df = sinks["ratings_dest/"].collect()
        assert set(ratings_df[CQCL.location_id]) == {"1-001"}
        assert sorted(ratings_df[CQCL.dataset]) == [
            CQCRatingsDatasetValues.pre_saf,
            CQCRatingsDatasetValues.pre_saf,
            CQCRatingsDatasetValues.saf,
        ]
        assert ratings_df[CQCRatings.date].dtype == pl.Date
        assert CQCRatings.latest_rating_flag in ratings_df.columns
        assert CQCRatings.total_rating_value in ratings_df.columns
        assert CQCRatings.location_id_hash in ratings_df.columns

        benchmark_df = sinks["benchmark_dest/"].collect()
        assert benchmark_df.columns == [
            CQCRatings.benchmarks_location_id,
            CQCRatings.benchmarks_establishment_id,
            CQCL.name,
            CQCL.dataset,
            CQCRatings.good_or_outstanding_flag,
            CQCRatings.benchmarks_overall_rating,
            CQCRatings.inspection_date,
        ]
        assert sorted(benchmark_df[CQCL.dataset]) == [
            CQCRatingsDatasetValues.pre_saf,
            CQCRatingsDatasetValues.saf,
        ]
        assert set(benchmark_df[CQCRatings.benchmarks_location_id]) == {"1-001"}
        assert set(benchmark_df[CQCRatings.benchmarks_establishment_id]) == {"100"}
        assert set(benchmark_df[CQCRatings.good_or_outstanding_flag]) == {1}
        assert set(benchmark_df[CQCRatings.benchmarks_overall_rating]) == {"Good"}
        assert benchmark_df[CQCRatings.inspection_date].dtype == pl.Date


class TestCreateStandardRatingsDataset:
    def test_keeps_only_standard_columns_in_order(self):
        input_lf = pl.LazyFrame(
            Data.create_standard_ratings_dataset_rows,
            schema=Schemas.create_standard_ratings_dataset_schema,
            orient="row",
        )

        returned_lf = job.create_standard_ratings_dataset(input_lf)

        expected_lf = pl.LazyFrame(
            Data.expected_create_standard_ratings_dataset_rows,
            schema=Schemas.standard_ratings_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(expected_lf, returned_lf)

    def test_removes_duplicate_rows_left_after_dropping_columns(self):
        input_lf = pl.LazyFrame(
            Data.create_standard_ratings_dataset_duplicate_rows,
            schema=Schemas.create_standard_ratings_dataset_schema,
            orient="row",
        )

        returned_lf = job.create_standard_ratings_dataset(input_lf)

        expected_lf = pl.LazyFrame(
            Data.expected_create_standard_ratings_dataset_duplicate_rows,
            schema=Schemas.standard_ratings_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(expected_lf, returned_lf, check_row_order=False)

    def test_removes_duplicate_rows_containing_nulls(self):
        input_lf = pl.LazyFrame(
            Data.create_standard_ratings_dataset_null_rows,
            schema=Schemas.create_standard_ratings_dataset_schema,
            orient="row",
        )

        returned_lf = job.create_standard_ratings_dataset(input_lf)

        expected_lf = pl.LazyFrame(
            Data.expected_create_standard_ratings_dataset_null_rows,
            schema=Schemas.standard_ratings_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(expected_lf, returned_lf)


@dataclass
class AddLocationIdHashTestCase:
    id: str
    rows: list[Any]
    expected_rows: list[Any]
    schema: pl.Schema = field(default_factory=lambda: Schemas.location_id_hash_schema)
    expected_schema: pl.Schema = field(
        default_factory=lambda: Schemas.expected_location_id_hash_schema
    )

    def as_pytest_param(self):
        return pytest.param(
            self.rows,
            self.expected_rows,
            self.schema,
            self.expected_schema,
            id=self.id,
        )


add_location_id_hash_cases = [
    AddLocationIdHashTestCase(
        id="hashes_short_location_id",
        rows=Data.location_id_hash_short_rows,
        expected_rows=Data.expected_location_id_hash_short_rows,
    ),
    AddLocationIdHashTestCase(
        id="hashes_long_location_id",
        rows=Data.location_id_hash_long_rows,
        expected_rows=Data.expected_location_id_hash_long_rows,
    ),
    AddLocationIdHashTestCase(
        id="gives_repeated_location_ids_the_same_hash",
        rows=Data.location_id_hash_repeated_rows,
        expected_rows=Data.expected_location_id_hash_repeated_rows,
    ),
    AddLocationIdHashTestCase(
        id="keeps_other_columns_unchanged",
        rows=Data.location_id_hash_date_rows,
        expected_rows=Data.expected_location_id_hash_date_rows,
        schema=Schemas.location_id_hash_date_schema,
        expected_schema=Schemas.expected_location_id_hash_date_schema,
    ),
    AddLocationIdHashTestCase(
        id="hashes_categorical_location_id",
        rows=Data.location_id_hash_categorical_rows,
        expected_rows=Data.expected_location_id_hash_categorical_rows,
        schema=Schemas.location_id_hash_categorical_schema,
        expected_schema=Schemas.expected_location_id_hash_categorical_schema,
    ),
    AddLocationIdHashTestCase(
        id="returns_null_hash_when_location_id_is_null",
        rows=Data.location_id_hash_null_rows,
        expected_rows=Data.expected_location_id_hash_null_rows,
    ),
]


class TestAddLocationIdHash:
    @pytest.mark.parametrize(
        "rows, expected_rows, schema, expected_schema",
        [case.as_pytest_param() for case in add_location_id_hash_cases],
    )
    def test_adds_expected_location_id_hash(
        self, rows, expected_rows, schema, expected_schema
    ):
        input_lf = pl.LazyFrame(rows, schema=schema, orient="row")

        returned_lf = job.add_location_id_hash(input_lf)

        expected_lf = pl.LazyFrame(expected_rows, schema=expected_schema, orient="row")
        pl_testing.assert_frame_equal(expected_lf, returned_lf, check_row_order=False)


@dataclass
class SelectRatingsForBenchmarksTestCase:
    id: str
    rows: list[Any]
    expected_rows: list[Any]

    def as_pytest_param(self):
        return pytest.param(self.rows, self.expected_rows, id=self.id)


select_ratings_for_benchmarks_cases = [
    SelectRatingsForBenchmarksTestCase(
        id="keeps_registered_current_rating",
        rows=Data.select_ratings_for_benchmarks_registered_current_rows,
        expected_rows=Data.select_ratings_for_benchmarks_registered_current_rows,
    ),
    SelectRatingsForBenchmarksTestCase(
        id="removes_deregistered_rows",
        rows=Data.select_ratings_for_benchmarks_deregistered_rows,
        expected_rows=Data.expected_select_ratings_for_benchmarks_deregistered_rows,
    ),
    SelectRatingsForBenchmarksTestCase(
        id="removes_historic_rows",
        rows=Data.select_ratings_for_benchmarks_historic_rows,
        expected_rows=Data.expected_select_ratings_for_benchmarks_historic_rows,
    ),
]


class TestSelectRatingsForBenchmarks:
    @pytest.mark.parametrize(
        "rows, expected_rows",
        [case.as_pytest_param() for case in select_ratings_for_benchmarks_cases],
    )
    def test_returns_expected_rows(self, rows, expected_rows):
        schema = Schemas.select_ratings_for_benchmarks_schema
        input_lf = pl.LazyFrame(rows, schema=schema, orient="row")

        returned_lf = job.select_ratings_for_benchmarks(input_lf)

        expected_lf = pl.LazyFrame(expected_rows, schema=schema, orient="row")
        pl_testing.assert_frame_equal(expected_lf, returned_lf)


@dataclass
class AddGoodAndOutstandingFlagColumnTestCase:
    id: str
    rows: list[Any]
    expected_rows: list[Any]

    def as_pytest_param(self):
        return pytest.param(self.rows, self.expected_rows, id=self.id)


add_good_and_outstanding_flag_column_cases = [
    AddGoodAndOutstandingFlagColumnTestCase(
        id="flags_one_when_min_rating_is_good_or_outstanding",
        rows=Data.add_good_and_outstanding_flag_good_rows,
        expected_rows=Data.expected_add_good_and_outstanding_flag_good_rows,
    ),
    AddGoodAndOutstandingFlagColumnTestCase(
        id="flags_zero_when_any_rating_below_good",
        rows=Data.add_good_and_outstanding_flag_below_good_rows,
        expected_rows=Data.expected_add_good_and_outstanding_flag_below_good_rows,
    ),
    AddGoodAndOutstandingFlagColumnTestCase(
        id="applies_flag_to_every_row_of_location",
        rows=Data.add_good_and_outstanding_flag_two_locations_rows,
        expected_rows=Data.expected_add_good_and_outstanding_flag_two_locations_rows,
    ),
    AddGoodAndOutstandingFlagColumnTestCase(
        id="flags_zero_when_overall_rating_value_is_null",
        rows=Data.add_good_and_outstanding_flag_null_rows,
        expected_rows=Data.expected_add_good_and_outstanding_flag_null_rows,
    ),
]


class TestAddGoodAndOutstandingFlagColumn:
    @pytest.mark.parametrize(
        "rows, expected_rows",
        [case.as_pytest_param() for case in add_good_and_outstanding_flag_column_cases],
    )
    def test_adds_expected_flag(self, rows, expected_rows):
        input_lf = pl.LazyFrame(
            rows, schema=Schemas.add_good_and_outstanding_flag_schema, orient="row"
        )

        returned_lf = job.add_good_and_outstanding_flag_column(input_lf)

        expected_lf = pl.LazyFrame(
            expected_rows,
            schema=Schemas.expected_add_good_and_outstanding_flag_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(expected_lf, returned_lf)


@dataclass
class JoinEstablishmentIdsTestCase:
    id: str
    ratings_rows: list[Any]
    ascwds_rows: list[Any]
    expected_rows: list[Any]

    def as_pytest_param(self):
        return pytest.param(
            self.ratings_rows, self.ascwds_rows, self.expected_rows, id=self.id
        )


join_establishment_ids_cases = [
    JoinEstablishmentIdsTestCase(
        id="adds_establishment_id_for_matching_location",
        ratings_rows=Data.join_establishment_ids_matching_ratings_rows,
        ascwds_rows=Data.join_establishment_ids_matching_ascwds_rows,
        expected_rows=Data.expected_join_establishment_ids_matching_rows,
    ),
    JoinEstablishmentIdsTestCase(
        id="null_establishment_id_when_location_not_in_ascwds",
        ratings_rows=Data.join_establishment_ids_unmatched_ratings_rows,
        ascwds_rows=Data.join_establishment_ids_unmatched_ascwds_rows,
        expected_rows=Data.expected_join_establishment_ids_unmatched_rows,
    ),
    JoinEstablishmentIdsTestCase(
        id="does_not_duplicate_rows_when_ascwds_repeats_location_id",
        ratings_rows=Data.join_establishment_ids_repeated_ratings_rows,
        ascwds_rows=Data.join_establishment_ids_repeated_ascwds_rows,
        expected_rows=Data.expected_join_establishment_ids_repeated_rows,
    ),
]


class TestJoinEstablishmentIds:
    @pytest.mark.parametrize(
        "ratings_rows, ascwds_rows, expected_rows",
        [case.as_pytest_param() for case in join_establishment_ids_cases],
    )
    def test_returns_ratings_with_expected_establishment_ids(
        self, ratings_rows, ascwds_rows, expected_rows
    ):
        ratings_lf = pl.LazyFrame(
            ratings_rows,
            schema=Schemas.join_establishment_ids_ratings_schema,
            orient="row",
        )
        ascwds_lf = pl.LazyFrame(
            ascwds_rows,
            schema=Schemas.join_establishment_ids_ascwds_schema,
            orient="row",
        )

        returned_lf = job.join_establishment_ids(ratings_lf, ascwds_lf)

        expected_lf = pl.LazyFrame(
            expected_rows,
            schema=Schemas.expected_join_establishment_ids_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(expected_lf, returned_lf)


@dataclass
class CreateBenchmarkRatingsDatasetTestCase:
    id: str
    rows: list[Any]
    expected_rows: list[Any]

    def as_pytest_param(self):
        return pytest.param(self.rows, self.expected_rows, id=self.id)


create_benchmark_ratings_dataset_cases = [
    CreateBenchmarkRatingsDatasetTestCase(
        id="renames_and_orders_columns",
        rows=Data.create_benchmark_ratings_dataset_rows,
        expected_rows=Data.expected_create_benchmark_ratings_dataset_rows,
    ),
    CreateBenchmarkRatingsDatasetTestCase(
        id="removes_rows_with_null_establishment_id",
        rows=Data.create_benchmark_ratings_dataset_null_establishment_id_rows,
        expected_rows=Data.expected_create_benchmark_ratings_dataset_null_establishment_id_rows,
    ),
    CreateBenchmarkRatingsDatasetTestCase(
        id="removes_rows_with_null_overall_rating",
        rows=Data.create_benchmark_ratings_dataset_null_overall_rating_rows,
        expected_rows=Data.expected_create_benchmark_ratings_dataset_null_overall_rating_rows,
    ),
]


class TestCreateBenchmarkRatingsDataset:
    @pytest.mark.parametrize(
        "rows, expected_rows",
        [case.as_pytest_param() for case in create_benchmark_ratings_dataset_cases],
    )
    def test_returns_expected_benchmark_ratings(self, rows, expected_rows):
        input_lf = pl.LazyFrame(
            rows,
            schema=Schemas.create_benchmark_ratings_dataset_schema,
            orient="row",
        )

        returned_lf = job.create_benchmark_ratings_dataset(input_lf)

        expected_lf = pl.LazyFrame(
            expected_rows,
            schema=Schemas.expected_create_benchmark_ratings_dataset_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(expected_lf, returned_lf)
