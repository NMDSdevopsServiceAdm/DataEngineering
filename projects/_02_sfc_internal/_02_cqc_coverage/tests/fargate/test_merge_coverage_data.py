from unittest.mock import Mock, patch

import polars as pl

import projects._02_sfc_internal._02_cqc_coverage.fargate.merge_coverage_data as job
from projects._02_sfc_internal.unittest_data.polars_sfc_test_file_data import (
    MergeCoverageData as Data,
)
from projects._02_sfc_internal.unittest_data.polars_sfc_test_file_schemas import (
    MergeCoverageSchema as Schemas,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.reconciliation_columns import (
    ReconciliationColumns as ReconColumn,
)

PATCH_PATH = "projects._02_sfc_internal._02_cqc_coverage.fargate.merge_coverage_data"


class TestMain:
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.cov_utils")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_calls_each_placeholder_step_once_in_order_and_writes_both_destinations(
        self, scan_parquet_mock: Mock, cov_utils_mock: Mock, sink_to_parquet_mock: Mock
    ):
        cqc_location_lf = pl.LazyFrame(
            Data.cqc_location_rows,
            schema=Schemas.cqc_location_schema,
            orient="row",
        )
        ascwds_workplace_lf = pl.LazyFrame(
            Data.ascwds_workplace_rows,
            schema=Schemas.ascwds_workplace_schema,
            orient="row",
        )
        cqc_ratings_lf = pl.LazyFrame(
            Data.cqc_ratings_rows,
            schema=Schemas.cqc_ratings_schema,
            orient="row",
        )
        cqc_providers_lf = pl.LazyFrame(
            Data.cqc_providers_rows,
            schema=Schemas.cqc_providers_schema,
            orient="row",
        )
        scan_parquet_mock.side_effect = [
            cqc_location_lf,
            ascwds_workplace_lf,
            cqc_ratings_lf,
            cqc_providers_lf,
        ]

        deduped_merged_coverage_lf = pl.LazyFrame(
            Data.deduped_merged_coverage_rows,
            schema=Schemas.deduped_merged_coverage_schema,
            orient="row",
        )
        cov_utils_mock.add_removed_by_purge_date_filter_flag.side_effect = lambda lf: lf
        cov_utils_mock.deduplicate_ascwds_workplace_data.side_effect = lambda lf: lf
        cov_utils_mock.join_ascwds_data_into_cqc_location_df.side_effect = (
            lambda cqc_lf, ascwds_lf: cqc_lf
        )
        cov_utils_mock.add_flag_for_in_ascwds.side_effect = lambda lf: lf
        cov_utils_mock.deduplicate_merged_coverage_data.return_value = (
            deduped_merged_coverage_lf
        )
        cov_utils_mock.join_latest_cqc_rating_into_coverage_df.side_effect = (
            lambda lf, ratings_lf: lf
        )
        cov_utils_mock.add_columns_for_locality_manager_dashboard.side_effect = (
            lambda lf: lf
        )
        cov_utils_mock.join_provider_name_into_merged_coverage_df.side_effect = (
            lambda lf, providers_lf: lf
        )

        job.main(
            "cqc_location_source/",
            "ascwds_source/",
            "cqc_ratings_source/",
            "cqc_providers_source/",
            "merged_dest/",
            "reduced_dest/",
        )

        scanned_sources = [c.args[0] for c in scan_parquet_mock.call_args_list]
        assert scanned_sources == [
            "cqc_location_source/",
            "ascwds_source/",
            "cqc_ratings_source/",
            "cqc_providers_source/",
        ]

        cov_utils_mock.add_removed_by_purge_date_filter_flag.assert_called_once()
        cov_utils_mock.deduplicate_ascwds_workplace_data.assert_called_once()
        cov_utils_mock.join_ascwds_data_into_cqc_location_df.assert_called_once()
        cov_utils_mock.add_flag_for_in_ascwds.assert_called_once()
        cov_utils_mock.deduplicate_merged_coverage_data.assert_called_once()
        cov_utils_mock.join_latest_cqc_rating_into_coverage_df.assert_called_once()
        cov_utils_mock.add_columns_for_locality_manager_dashboard.assert_called_once()
        cov_utils_mock.join_provider_name_into_merged_coverage_df.assert_called_once()

        assert sink_to_parquet_mock.call_count == 2

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.cov_utils")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_classifies_parents_and_singles_and_subs_for_real(
        self, scan_parquet_mock: Mock, cov_utils_mock: Mock, sink_to_parquet_mock: Mock
    ):
        scan_parquet_mock.side_effect = [
            pl.LazyFrame(
                Data.cqc_location_rows, schema=Schemas.cqc_location_schema, orient="row"
            ),
            pl.LazyFrame(
                Data.ascwds_workplace_rows,
                schema=Schemas.ascwds_workplace_schema,
                orient="row",
            ),
            pl.LazyFrame(
                Data.cqc_ratings_rows, schema=Schemas.cqc_ratings_schema, orient="row"
            ),
            pl.LazyFrame(
                Data.cqc_providers_rows,
                schema=Schemas.cqc_providers_schema,
                orient="row",
            ),
        ]
        cov_utils_mock.add_removed_by_purge_date_filter_flag.side_effect = lambda lf: lf
        cov_utils_mock.deduplicate_ascwds_workplace_data.side_effect = lambda lf: lf
        cov_utils_mock.join_ascwds_data_into_cqc_location_df.side_effect = (
            lambda cqc_lf, ascwds_lf: cqc_lf
        )
        cov_utils_mock.add_flag_for_in_ascwds.side_effect = lambda lf: lf
        cov_utils_mock.deduplicate_merged_coverage_data.return_value = pl.LazyFrame(
            Data.deduped_merged_coverage_rows,
            schema=Schemas.deduped_merged_coverage_schema,
            orient="row",
        )
        cov_utils_mock.join_latest_cqc_rating_into_coverage_df.side_effect = (
            lambda lf, ratings_lf: lf
        )
        cov_utils_mock.add_columns_for_locality_manager_dashboard.side_effect = (
            lambda lf: lf
        )
        cov_utils_mock.join_provider_name_into_merged_coverage_df.side_effect = (
            lambda lf, providers_lf: lf
        )

        job.main(
            "cqc_location_source/",
            "ascwds_source/",
            "cqc_ratings_source/",
            "cqc_providers_source/",
            "merged_dest/",
            "reduced_dest/",
        )

        merged_args, _ = sink_to_parquet_mock.call_args_list[0]
        merged_df = merged_args[0].collect()

        assert (
            merged_df[ReconColumn.parents_or_singles_and_subs].to_list()
            == Data.expected_parents_or_singles_and_subs
        )

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.cov_utils")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_reduced_destination_only_contains_the_latest_cqc_location_import_date(
        self, scan_parquet_mock: Mock, cov_utils_mock: Mock, sink_to_parquet_mock: Mock
    ):
        scan_parquet_mock.side_effect = [
            pl.LazyFrame(
                Data.cqc_location_rows, schema=Schemas.cqc_location_schema, orient="row"
            ),
            pl.LazyFrame(
                Data.ascwds_workplace_rows,
                schema=Schemas.ascwds_workplace_schema,
                orient="row",
            ),
            pl.LazyFrame(
                Data.cqc_ratings_rows, schema=Schemas.cqc_ratings_schema, orient="row"
            ),
            pl.LazyFrame(
                Data.cqc_providers_rows,
                schema=Schemas.cqc_providers_schema,
                orient="row",
            ),
        ]
        cov_utils_mock.add_removed_by_purge_date_filter_flag.side_effect = lambda lf: lf
        cov_utils_mock.deduplicate_ascwds_workplace_data.side_effect = lambda lf: lf
        cov_utils_mock.join_ascwds_data_into_cqc_location_df.side_effect = (
            lambda cqc_lf, ascwds_lf: cqc_lf
        )
        cov_utils_mock.add_flag_for_in_ascwds.side_effect = lambda lf: lf
        cov_utils_mock.deduplicate_merged_coverage_data.return_value = pl.LazyFrame(
            Data.merged_coverage_with_two_import_dates_rows,
            schema=Schemas.merged_coverage_with_two_import_dates_schema,
            orient="row",
        )
        cov_utils_mock.join_latest_cqc_rating_into_coverage_df.side_effect = (
            lambda lf, ratings_lf: lf
        )
        cov_utils_mock.add_columns_for_locality_manager_dashboard.side_effect = (
            lambda lf: lf
        )
        cov_utils_mock.join_provider_name_into_merged_coverage_df.side_effect = (
            lambda lf, providers_lf: lf
        )

        job.main(
            "cqc_location_source/",
            "ascwds_source/",
            "cqc_ratings_source/",
            "cqc_providers_source/",
            "merged_dest/",
            "reduced_dest/",
        )

        merged_args, _ = sink_to_parquet_mock.call_args_list[0]
        reduced_args, _ = sink_to_parquet_mock.call_args_list[1]

        assert merged_args[1] == "merged_dest/"
        assert reduced_args[1] == "reduced_dest/"
        assert merged_args[0].collect().height == 2
        reduced_df = reduced_args[0].collect()
        assert reduced_df.height == 1
        assert reduced_df[CQCLClean.location_id].to_list() == ["1-002"]
