import polars as pl
from polars import testing as pl_testing

import projects._02_sfc_internal._02_cqc_coverage.fargate.utils.utils as job
from projects._02_sfc_internal.unittest_data.polars_sfc_test_file_data import (
    LmEngagementData,
    MergeCoverageData as Data,
)
from projects._02_sfc_internal.unittest_data.polars_sfc_test_file_schemas import (
    LmEngagementSchema,
    MergeCoverageSchema as Schemas,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.coverage_columns import CoverageColumns
from utils.column_names.cqc_ratings_columns import CQCRatingsColumns

SORT_COLUMNS = [CQCLClean.location_id, CQCLClean.cqc_location_import_date]


class TestAddRemovedByPurgeDateFilterFlag:
    def test_flags_workplaces_active_before_their_purge_date_and_drops_source_columns(
        self,
    ):
        input_lf = pl.LazyFrame(
            Data.add_removed_by_purge_date_filter_flag_rows,
            schema=Schemas.add_removed_by_purge_date_filter_flag_schema,
            orient="row",
        )

        returned_lf = job.add_removed_by_purge_date_filter_flag(input_lf)

        assert returned_lf.collect_schema().names() == [
            AWPClean.establishment_id,
            AWPClean.removed_by_purge_date_filter,
        ]
        returned_flags = (
            returned_lf.collect()
            .get_column(AWPClean.removed_by_purge_date_filter)
            .to_list()
        )
        assert returned_flags == Data.expected_removed_by_purge_date_filter_flags


class TestDeduplicateAscwdsWorkplaceData:
    def test_keeps_one_row_per_import_date_and_location_preferring_latest_update(self):
        input_lf = pl.LazyFrame(
            Data.deduplicate_ascwds_workplace_data_rows,
            schema=Schemas.deduplicate_ascwds_workplace_data_schema,
            orient="row",
        )

        returned_lf = job.deduplicate_ascwds_workplace_data(input_lf)

        returned_establishment_ids = sorted(
            returned_lf.collect().get_column(AWPClean.establishment_id).to_list()
        )
        assert (
            returned_establishment_ids
            == Data.expected_deduplicate_ascwds_workplace_data_establishment_ids
        )


class TestJoinAscwdsDataIntoCqcLocationDf:
    def test_joins_on_location_id_and_the_closest_prior_ascwds_import_date(self):
        cqc_location_lf = pl.LazyFrame(
            Data.join_ascwds_data_cqc_location_rows,
            schema=Schemas.join_ascwds_data_cqc_location_schema,
            orient="row",
        )
        ascwds_workplace_lf = pl.LazyFrame(
            Data.join_ascwds_data_ascwds_workplace_rows,
            schema=Schemas.join_ascwds_data_ascwds_workplace_schema,
            orient="row",
        )

        returned_df = job.join_ascwds_data_into_cqc_location_df(
            cqc_location_lf, ascwds_workplace_lf
        ).collect()

        assert returned_df.get_column(
            AWPClean.ascwds_workplace_import_date
        ).to_list() == [Data.expected_join_ascwds_data_aligned_import_date]
        assert returned_df.get_column(AWPClean.establishment_id).to_list() == [
            Data.expected_join_ascwds_data_establishment_id
        ]


class TestAddFlagForInAscwds:
    def test_flags_locations_with_an_active_establishment_as_in_ascwds(self):
        input_lf = pl.LazyFrame(
            Data.add_flag_for_in_ascwds_rows,
            schema=Schemas.add_flag_for_in_ascwds_schema,
            orient="row",
        )

        returned_lf = job.add_flag_for_in_ascwds(input_lf)

        returned_flags = (
            returned_lf.collect().get_column(CoverageColumns.in_ascwds).to_list()
        )
        assert returned_flags == Data.expected_in_ascwds_flags


class TestDeduplicateMergedCoverageData:
    def test_keeps_one_row_per_location_identity_preferring_in_ascwds(self):
        input_lf = pl.LazyFrame(
            Data.deduplicate_merged_coverage_data_rows,
            schema=Schemas.deduplicate_merged_coverage_data_schema,
            orient="row",
        )

        returned_lf = job.deduplicate_merged_coverage_data(input_lf)

        returned_location_ids = sorted(
            returned_lf.collect().get_column(CQCLClean.location_id).to_list()
        )
        assert (
            returned_location_ids
            == Data.expected_deduplicate_merged_coverage_data_location_ids
        )


class TestJoinLatestCqcRatingIntoCoverageDf:
    def test_joins_only_the_latest_current_rating_per_location(self):
        coverage_lf = pl.LazyFrame(
            Data.join_latest_cqc_rating_coverage_rows,
            schema=Schemas.join_latest_cqc_rating_coverage_schema,
            orient="row",
        )
        ratings_lf = pl.LazyFrame(
            Data.join_latest_cqc_rating_ratings_rows,
            schema=Schemas.join_latest_cqc_rating_ratings_schema,
            orient="row",
        )

        returned_df = (
            job.join_latest_cqc_rating_into_coverage_df(coverage_lf, ratings_lf)
            .sort(CQCLClean.location_id)
            .collect()
        )

        assert (
            returned_df.get_column(CQCRatingsColumns.overall_rating).to_list()
            == Data.expected_join_latest_cqc_rating_overall_ratings
        )
        assert CQCRatingsColumns.latest_rating_flag not in returned_df.columns
        assert CQCRatingsColumns.current_or_historic not in returned_df.columns


class TestJoinProviderNameIntoMergedCoverageDf:
    def test_joins_the_latest_provider_name_and_preserves_column_order(self):
        coverage_lf = pl.LazyFrame(
            Data.join_provider_name_coverage_rows,
            schema=Schemas.join_provider_name_coverage_schema,
            orient="row",
        )
        providers_lf = pl.LazyFrame(
            Data.join_provider_name_providers_rows,
            schema=Schemas.join_provider_name_providers_schema,
            orient="row",
        )
        original_columns = coverage_lf.collect_schema().names()

        returned_df = (
            job.join_provider_name_into_merged_coverage_df(coverage_lf, providers_lf)
            .sort(CQCLClean.location_id)
            .collect()
        )

        assert returned_df.columns == [*original_columns, CQCLClean.provider_name]
        assert (
            returned_df.get_column(CQCLClean.provider_name).to_list()
            == Data.expected_join_provider_names
        )


class TestCalculateLaCoverageMonthly:
    def test_adds_la_monthly_coverage_ratio_per_cssr_and_import_date(self):
        input_lf = pl.LazyFrame(
            LmEngagementData.base_rows,
            schema=LmEngagementSchema.base_schema,
            orient="row",
        )
        expected_lf = pl.LazyFrame(
            LmEngagementData.expected_la_coverage_rows,
            schema=LmEngagementSchema.la_coverage_schema,
            orient="row",
        )

        returned_df = job.calculate_la_coverage_monthly(input_lf).sort(SORT_COLUMNS)

        pl_testing.assert_frame_equal(
            returned_df.collect(), expected_lf.sort(SORT_COLUMNS).collect()
        )


class TestCalculateCoverageMonthlyChange:
    def test_adds_month_on_month_change_in_la_coverage_per_location(self):
        input_lf = pl.LazyFrame(
            LmEngagementData.expected_la_coverage_rows,
            schema=LmEngagementSchema.la_coverage_schema,
            orient="row",
        )
        expected_lf = pl.LazyFrame(
            LmEngagementData.expected_coverage_change_rows,
            schema=LmEngagementSchema.coverage_change_schema,
            orient="row",
        )

        returned_df = job.calculate_coverage_monthly_change(input_lf).sort(SORT_COLUMNS)

        pl_testing.assert_frame_equal(
            returned_df.collect(), expected_lf.sort(SORT_COLUMNS).collect()
        )


class TestCalculateLocationsMonthlyChange:
    def test_adds_net_change_in_ascwds_active_locations_per_cssr(self):
        input_lf = pl.LazyFrame(
            LmEngagementData.expected_coverage_change_rows,
            schema=LmEngagementSchema.coverage_change_schema,
            orient="row",
        )
        expected_lf = pl.LazyFrame(
            LmEngagementData.expected_locations_change_rows,
            schema=LmEngagementSchema.locations_change_schema,
            orient="row",
        )

        returned_df = job.calculate_locations_monthly_change(input_lf).sort(
            SORT_COLUMNS
        )

        pl_testing.assert_frame_equal(
            returned_df.collect(), expected_lf.sort(SORT_COLUMNS).collect()
        )


class TestCalculateNewRegistrations:
    def test_adds_monthly_and_year_to_date_new_registration_counts_per_cssr(self):
        input_lf = pl.LazyFrame(
            LmEngagementData.expected_locations_change_rows,
            schema=LmEngagementSchema.locations_change_schema,
            orient="row",
        )
        expected_lf = pl.LazyFrame(
            LmEngagementData.expected_final_rows,
            schema=LmEngagementSchema.final_schema,
            orient="row",
        )

        returned_df = job.calculate_new_registrations(input_lf).sort(SORT_COLUMNS)

        pl_testing.assert_frame_equal(
            returned_df.collect(), expected_lf.sort(SORT_COLUMNS).collect()
        )


class TestAddColumnsForLocalityManagerDashboard:
    def test_adds_all_locality_manager_dashboard_columns(self):
        input_lf = pl.LazyFrame(
            LmEngagementData.orchestrator_input_rows,
            schema=LmEngagementSchema.orchestrator_input_schema,
            orient="row",
        )
        expected_lf = pl.LazyFrame(
            LmEngagementData.expected_final_rows,
            schema=LmEngagementSchema.final_schema,
            orient="row",
        )

        returned_df = job.add_columns_for_locality_manager_dashboard(input_lf).sort(
            SORT_COLUMNS
        )

        pl_testing.assert_frame_equal(
            returned_df.collect(), expected_lf.sort(SORT_COLUMNS).collect()
        )
