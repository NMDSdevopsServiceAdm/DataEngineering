from unittest.mock import Mock, patch

import polars as pl

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
        assert set(ratings_df[CQCL.location_id]) == {"1-001", "1-003"}
        assert sorted(ratings_df[CQCL.dataset]) == [
            CQCRatingsDatasetValues.pre_saf,
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
