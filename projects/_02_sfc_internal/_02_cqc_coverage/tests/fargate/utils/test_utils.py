import polars as pl

import projects._02_sfc_internal._02_cqc_coverage.fargate.utils.utils as job
from projects._02_sfc_internal.unittest_data.polars_sfc_test_file_data import (
    MergeCoverageData as Data,
)
from projects._02_sfc_internal.unittest_data.polars_sfc_test_file_schemas import (
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
