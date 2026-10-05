import polars as pl

from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AscWdsColumns,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.cleaned_data_files.cqc_provider_cleaned import (
    CqcProviderCleanedColumns as CQCPClean,
)
from utils.column_names.coverage_columns import CoverageColumns
from utils.column_names.cqc_ratings_columns import CQCRatingsColumns
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_names.reconciliation_columns import ReconciliationColumns

from projects._02_sfc_internal._02_cqc_coverage.fargate.utils.utils import (
    YEAR_COLUMN,
)


class ValidateMergeCoverageSchemas:
    cqc_locations_schema = pl.Schema(
        [
            (CQCLClean.cqc_location_import_date, pl.Date),
            (CQCLClean.location_id, pl.String),
            (CQCLClean.provider_id, pl.String),
            (CQCLClean.name, pl.String),
            (CQCLClean.postal_code, pl.String),
            (CQCLClean.care_home, pl.String),
            (CQCLClean.number_of_beds, pl.Int32),
            (Keys.year, pl.String),
            (Keys.month, pl.String),
            (Keys.day, pl.String),
        ]
    )

    merged_coverage_schema = pl.Schema(
        [
            (IndCqcColumns.location_id, pl.String),
            (IndCqcColumns.name, pl.String),
            (IndCqcColumns.cqc_location_import_date, pl.Date),
            (IndCqcColumns.care_home, pl.String),
            (IndCqcColumns.provider_id, pl.String),
            (IndCqcColumns.cqc_sector, pl.String),
            (IndCqcColumns.imputed_registration_date, pl.Date),
            (IndCqcColumns.primary_service_type, pl.String),
            (IndCqcColumns.current_ons_import_date, pl.Date),
            (IndCqcColumns.postcode, pl.String),
            (IndCqcColumns.current_cssr, pl.String),
            (IndCqcColumns.current_region, pl.String),
            (IndCqcColumns.current_rural_urban_indicator_2011, pl.String),
            (CoverageColumns.in_ascwds, pl.Int32),
            (CoverageColumns.la_monthly_coverage, pl.Float64),
            (CoverageColumns.locations_monthly_change, pl.Float64),
            (CoverageColumns.new_registrations_monthly, pl.Int32),
            (CoverageColumns.new_registrations_ytd, pl.Int32),
            (AscWdsColumns.master_update_date, pl.Date),
            (AscWdsColumns.nmds_id, pl.String),
            (IndCqcColumns.dormancy, pl.String),
            (CQCRatingsColumns.overall_rating, pl.String),
            (IndCqcColumns.provider_name, pl.String),
            (ReconciliationColumns.parents_or_singles_and_subs, pl.String),
            (CoverageColumns.coverage_monthly_change, pl.Float64),
            (AscWdsColumns.last_logged_in_date, pl.Date),
            (Keys.year, pl.String),
            (Keys.month, pl.String),
            (Keys.day, pl.String),
        ]
    )


class MergeCoverageSchema:
    cqc_location_schema = pl.Schema([(CQCLClean.cqc_location_import_date, pl.Date)])

    ascwds_workplace_schema = pl.Schema([(AscWdsColumns.establishment_id, pl.String)])

    cqc_ratings_schema = pl.Schema([(CQCRatingsColumns.overall_rating, pl.String)])

    cqc_providers_schema = pl.Schema([(CQCPClean.provider_id, pl.String)])

    deduped_merged_coverage_schema = pl.Schema(
        [
            (AscWdsColumns.is_parent, pl.String),
            (AscWdsColumns.parent_permission, pl.String),
        ]
    )

    add_removed_by_purge_date_filter_flag_schema = pl.Schema(
        [
            (AscWdsColumns.establishment_id, pl.String),
            (AscWdsColumns.workplace_last_active_date, pl.Date),
            (AscWdsColumns.purge_date, pl.Date),
        ]
    )

    deduplicate_ascwds_workplace_data_schema = pl.Schema(
        [
            (AscWdsColumns.ascwds_workplace_import_date, pl.Date),
            (AscWdsColumns.location_id, pl.String),
            (AscWdsColumns.master_update_date, pl.Date),
            (AscWdsColumns.establishment_id, pl.String),
        ]
    )

    join_ascwds_data_cqc_location_schema = pl.Schema(
        [
            (CQCLClean.cqc_location_import_date, pl.Date),
            (CQCLClean.location_id, pl.String),
        ]
    )

    join_ascwds_data_ascwds_workplace_schema = pl.Schema(
        [
            (AscWdsColumns.ascwds_workplace_import_date, pl.Date),
            (AscWdsColumns.location_id, pl.String),
            (AscWdsColumns.establishment_id, pl.String),
        ]
    )

    add_flag_for_in_ascwds_schema = pl.Schema(
        [
            (AscWdsColumns.establishment_id, pl.String),
            (AscWdsColumns.removed_by_purge_date_filter, pl.Boolean),
        ]
    )

    deduplicate_merged_coverage_data_schema = pl.Schema(
        [
            (CQCLClean.cqc_location_import_date, pl.Date),
            (CQCLClean.name, pl.String),
            (CQCLClean.postal_code, pl.String),
            (CQCLClean.care_home, pl.String),
            (CoverageColumns.in_ascwds, pl.Int32),
            (CQCLClean.imputed_registration_date, pl.Date),
            (CQCLClean.location_id, pl.String),
        ]
    )

    join_latest_cqc_rating_coverage_schema = pl.Schema(
        [(CQCLClean.location_id, pl.String)]
    )

    join_latest_cqc_rating_ratings_schema = pl.Schema(
        [
            (CQCLClean.location_id, pl.String),
            (CQCRatingsColumns.overall_rating, pl.String),
            (CQCRatingsColumns.latest_rating_flag, pl.Int32),
            (CQCRatingsColumns.current_or_historic, pl.String),
        ]
    )

    join_provider_name_coverage_schema = pl.Schema(
        [
            (CQCLClean.location_id, pl.String),
            (CQCLClean.provider_id, pl.String),
        ]
    )

    join_provider_name_providers_schema = pl.Schema(
        [
            (CQCPClean.provider_id, pl.String),
            (CQCPClean.name, pl.String),
            (CQCPClean.cqc_provider_import_date, pl.Date),
        ]
    )

    merged_coverage_with_two_import_dates_schema = pl.Schema(
        [
            (CQCLClean.cqc_location_import_date, pl.Date),
            (CQCLClean.location_id, pl.String),
            (AscWdsColumns.is_parent, pl.String),
            (AscWdsColumns.parent_permission, pl.String),
        ]
    )


class LmEngagementSchema:
    base_schema = pl.Schema(
        [
            (CQCLClean.location_id, pl.String),
            (CQCLClean.cqc_location_import_date, pl.Date),
            (CQCLClean.current_cssr, pl.String),
            (CoverageColumns.in_ascwds, pl.Int32),
            (YEAR_COLUMN, pl.Int32),
        ]
    )

    la_coverage_schema = pl.Schema(
        [
            *base_schema.items(),
            (CoverageColumns.la_monthly_coverage, pl.Float32),
        ]
    )

    coverage_change_schema = pl.Schema(
        [
            *la_coverage_schema.items(),
            (CoverageColumns.coverage_monthly_change, pl.Float32),
        ]
    )

    locations_change_schema = pl.Schema(
        [
            *coverage_change_schema.items(),
            (CoverageColumns.in_ascwds_last_month, pl.Int32),
            (CoverageColumns.locations_monthly_change, pl.Int32),
        ]
    )

    final_schema = pl.Schema(
        [
            *coverage_change_schema.items(),
            (CoverageColumns.locations_monthly_change, pl.Int32),
            (CoverageColumns.new_registrations_monthly, pl.Int32),
            (CoverageColumns.new_registrations_ytd, pl.Int32),
        ]
    )

    orchestrator_input_schema = pl.Schema(
        [
            (CQCLClean.location_id, pl.String),
            (CQCLClean.cqc_location_import_date, pl.Date),
            (CQCLClean.current_cssr, pl.String),
            (CoverageColumns.in_ascwds, pl.Int32),
        ]
    )


class ReconciliationSchema:
    ascwds_workplace_schema = {
        AscWdsColumns.ascwds_workplace_import_date: pl.Date,
        AscWdsColumns.establishment_id: pl.String,
        AscWdsColumns.nmds_id: pl.String,
        AscWdsColumns.is_parent: pl.String,
        AscWdsColumns.organisation_id: pl.String,
        AscWdsColumns.parent_permission: pl.String,
        AscWdsColumns.establishment_type: pl.String,
        AscWdsColumns.registration_type: pl.String,
        AscWdsColumns.location_id: pl.String,
        AscWdsColumns.main_service_id: pl.String,
        AscWdsColumns.establishment_name: pl.String,
        AscWdsColumns.region_id: pl.String,
    }

    main_ascwds_workplace_schema = {
        **ascwds_workplace_schema,
        AscWdsColumns.workplace_last_active_date: pl.Date,
        AscWdsColumns.purge_date: pl.Date,
    }

    main_cqc_location_schema = {
        CQCL.location_id: pl.String,
        CQCLClean.cqc_location_import_date: pl.Date,
        CQCL.registration_status: pl.String,
        CQCL.deregistration_date: pl.Date,
    }


class FlattenCQCRatings:
    key_question_rating_struct = pl.Struct(
        {CQCL.name: pl.String, CQCL.rating: pl.String}
    )

    current_ratings_struct = pl.Struct(
        {
            CQCL.overall: pl.Struct(
                {
                    CQCL.report_date: pl.String,
                    CQCL.rating: pl.String,
                    CQCL.key_question_ratings: pl.List(key_question_rating_struct),
                }
            )
        }
    )

    historic_ratings_struct = pl.List(
        pl.Struct(
            {
                CQCL.report_date: pl.String,
                CQCL.overall: pl.Struct(
                    {
                        CQCL.rating: pl.String,
                        CQCL.key_question_ratings: pl.List(key_question_rating_struct),
                    }
                ),
            }
        )
    )

    current_ratings_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String),
            (CQCL.registration_status, pl.String),
            (CQCL.current_ratings, current_ratings_struct),
        ]
    )

    historic_ratings_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String),
            (CQCL.registration_status, pl.String),
            (CQCL.historic_ratings, historic_ratings_struct),
        ]
    )

    flattened_ratings_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String),
            (CQCL.registration_status, pl.String),
            (CQCRatingsColumns.date, pl.String),
            (CQCRatingsColumns.overall_rating, pl.String),
            (CQCRatingsColumns.safe_rating, pl.String),
            (CQCRatingsColumns.well_led_rating, pl.String),
            (CQCRatingsColumns.caring_rating, pl.String),
            (CQCRatingsColumns.responsive_rating, pl.String),
            (CQCRatingsColumns.effective_rating, pl.String),
        ]
    )

    flattened_ratings_with_current_or_historic_schema = pl.Schema(
        [
            *flattened_ratings_schema.items(),
            (CQCRatingsColumns.current_or_historic, pl.String),
        ]
    )
