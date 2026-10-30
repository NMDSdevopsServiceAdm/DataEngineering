from dataclasses import dataclass
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
    CQCCurrentOrHistoricValues,
    CQCRatingsDatasetValues,
)

PATCH_PATH = "projects._02_sfc_internal._01_cqc_ratings.fargate.flatten_cqc_ratings"


class TestMain:
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_sinks_social_care_ratings_to_both_destinations(
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

        for sunk_lf in sinks.values():
            returned_df = sunk_lf.collect()
            assert set(returned_df[CQCL.location_id]) == {"1-001"}
            assert sorted(returned_df[CQCRatings.current_or_historic]) == [
                CQCCurrentOrHistoricValues.current,
                CQCCurrentOrHistoricValues.historic,
            ]
            assert set(returned_df[CQCL.dataset]) == {CQCRatingsDatasetValues.pre_saf}


class TestCreateStandardRatingsDataset:
    def test_keeps_only_standard_columns_in_order(self):
        input_lf = pl.LazyFrame(
            Data.create_standard_ratings_dataset_data,
            schema=Schemas.create_standard_ratings_dataset_schema,
        )

        returned_lf = job.create_standard_ratings_dataset(input_lf)

        expected_lf = pl.LazyFrame(
            Data.expected_create_standard_ratings_dataset_data,
            schema=Schemas.standard_ratings_schema,
        )
        pl_testing.assert_frame_equal(expected_lf, returned_lf)

    def test_removes_duplicate_rows_left_after_dropping_columns(self):
        input_lf = pl.LazyFrame(
            Data.create_standard_ratings_dataset_duplicate_data,
            schema=Schemas.create_standard_ratings_dataset_schema,
        )

        returned_lf = job.create_standard_ratings_dataset(input_lf)

        expected_lf = pl.LazyFrame(
            Data.expected_create_standard_ratings_dataset_duplicate_data,
            schema=Schemas.standard_ratings_schema,
        )
        pl_testing.assert_frame_equal(expected_lf, returned_lf, check_row_order=False)


@dataclass
class AddLocationIdHashTestCase:
    id: str
    rows: list[Any]
    expected_rows: list[Any]

    def as_pytest_param(self):
        return pytest.param(self.rows, self.expected_rows, id=self.id)


add_location_id_hash_cases = [
    AddLocationIdHashTestCase(
        id="three_digit_location_id",
        rows=Data.location_id_hash_three_digit_rows,
        expected_rows=Data.expected_location_id_hash_three_digit_rows,
    ),
    AddLocationIdHashTestCase(
        id="ten_digit_location_id",
        rows=Data.location_id_hash_ten_digit_rows,
        expected_rows=Data.expected_location_id_hash_ten_digit_rows,
    ),
    AddLocationIdHashTestCase(
        id="eleven_digit_location_id",
        rows=Data.location_id_hash_eleven_digit_rows,
        expected_rows=Data.expected_location_id_hash_eleven_digit_rows,
    ),
    AddLocationIdHashTestCase(
        id="twelve_digit_location_id",
        rows=Data.location_id_hash_twelve_digit_rows,
        expected_rows=Data.expected_location_id_hash_twelve_digit_rows,
    ),
]


class TestAddLocationIdHash:
    @pytest.mark.parametrize(
        "rows, expected_rows",
        [case.as_pytest_param() for case in add_location_id_hash_cases],
    )
    def test_hashes_location_id_to_20_char_sha256_prefix(self, rows, expected_rows):
        input_lf = pl.LazyFrame(
            rows, schema=Schemas.location_id_hash_schema, orient="row"
        )

        returned_lf = job.add_location_id_hash(input_lf)

        expected_lf = pl.LazyFrame(
            expected_rows,
            schema=Schemas.expected_location_id_hash_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(expected_lf, returned_lf)

    def test_gives_repeated_location_ids_the_same_hash(self):
        input_lf = pl.LazyFrame({CQCL.location_id: ["1-123", "1-123"]})

        returned_lf = job.add_location_id_hash(input_lf)

        expected_lf = pl.LazyFrame(
            {
                CQCL.location_id: ["1-123", "1-123"],
                CQCRatings.location_id_hash: ["b022a7e5cc45cf3dc578"] * 2,
            }
        )
        pl_testing.assert_frame_equal(expected_lf, returned_lf)

    def test_keeps_row_count_and_other_columns(self):
        input_lf = pl.LazyFrame(
            {
                CQCL.location_id: ["1-123", "1-123", "1-123456789"],
                "other": [1, 2, 3],
            }
        )

        returned_lf = job.add_location_id_hash(input_lf)

        expected_lf = pl.LazyFrame(
            {
                CQCL.location_id: ["1-123", "1-123", "1-123456789"],
                "other": [1, 2, 3],
                CQCRatings.location_id_hash: [
                    "b022a7e5cc45cf3dc578",
                    "b022a7e5cc45cf3dc578",
                    "4a5a7fdc6afede351ffd",
                ],
            }
        )
        pl_testing.assert_frame_equal(expected_lf, returned_lf, check_row_order=False)

    def test_hashes_categorical_location_id(self):
        input_lf = pl.LazyFrame(
            {CQCL.location_id: ["1-123"]}, schema={CQCL.location_id: pl.Categorical}
        )

        returned_lf = job.add_location_id_hash(input_lf)

        expected_lf = pl.LazyFrame(
            {
                CQCL.location_id: ["1-123"],
                CQCRatings.location_id_hash: ["b022a7e5cc45cf3dc578"],
            },
            schema={
                CQCL.location_id: pl.Categorical,
                CQCRatings.location_id_hash: pl.String,
            },
        )
        pl_testing.assert_frame_equal(expected_lf, returned_lf)

    def test_returns_null_hash_when_location_id_is_null(self):
        input_lf = pl.LazyFrame(
            {CQCL.location_id: [None, "1-123"]}, schema={CQCL.location_id: pl.String}
        )

        returned_lf = job.add_location_id_hash(input_lf)

        expected_lf = pl.LazyFrame(
            {
                CQCL.location_id: [None, "1-123"],
                CQCRatings.location_id_hash: [None, "b022a7e5cc45cf3dc578"],
            },
            schema={
                CQCL.location_id: pl.String,
                CQCRatings.location_id_hash: pl.String,
            },
        )
        pl_testing.assert_frame_equal(expected_lf, returned_lf, check_row_order=False)
