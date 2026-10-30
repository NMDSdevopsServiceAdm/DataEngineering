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
            assert returned_df.columns == [
                CQCL.location_id,
                CQCL.registration_status,
                CQCRatings.date,
                CQCL.assessment_plan_id,
                CQCL.title,
                CQCL.assessment_date,
                CQCL.assessment_plan_status,
                CQCL.name,
                CQCL.source_path,
                CQCL.dataset,
                CQCRatings.current_or_historic,
                CQCRatings.overall_rating,
                CQCRatings.safe_rating,
                CQCRatings.well_led_rating,
                CQCRatings.caring_rating,
                CQCRatings.responsive_rating,
                CQCRatings.effective_rating,
            ]
            assert set(returned_df[CQCL.location_id]) == {"1-001"}
            assert sorted(returned_df[CQCL.dataset]) == [
                CQCRatingsDatasetValues.pre_saf,
                CQCRatingsDatasetValues.pre_saf,
                CQCRatingsDatasetValues.saf,
            ]
            assert returned_df[CQCRatings.date].dtype == pl.Date
