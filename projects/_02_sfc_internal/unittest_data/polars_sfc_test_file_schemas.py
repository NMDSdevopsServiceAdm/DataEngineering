import polars as pl

from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AscWdsColumns,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.coverage_columns import CoverageColumns
from utils.column_names.cqc_ratings_columns import CQCRatingsColumns
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_names.raw_data_files.ascwds_workplace_columns import (
    AscwdsWorkplaceColumns as AWP,
)
from utils.column_names.raw_data_files.ascwds_workplace_columns import (
    PartitionKeys as AWPKeys,
)
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_names.reconciliation_columns import ReconciliationColumns


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

    assessment_key_question_rating_struct = pl.Struct(
        {CQCL.name: pl.String, CQCL.rating: pl.String, CQCL.status: pl.String}
    )

    assessment_struct = pl.List(
        pl.Struct(
            {
                CQCL.assessment_plan_published_datetime: pl.String,
                CQCL.ratings: pl.Struct(
                    {
                        CQCL.overall: pl.List(
                            pl.Struct(
                                {
                                    CQCL.rating: pl.String,
                                    CQCL.status: pl.String,
                                    CQCL.key_question_ratings: pl.List(
                                        assessment_key_question_rating_struct
                                    ),
                                }
                            )
                        ),
                        CQCL.asg_ratings: pl.List(
                            pl.Struct(
                                {
                                    CQCL.assessment_plan_id: pl.String,
                                    CQCL.title: pl.String,
                                    CQCL.assessment_date: pl.String,
                                    CQCL.assessment_plan_status: pl.String,
                                    CQCL.name: pl.String,
                                    CQCL.rating: pl.String,
                                    CQCL.status: pl.String,
                                    CQCL.key_question_ratings: pl.List(
                                        assessment_key_question_rating_struct
                                    ),
                                }
                            )
                        ),
                    }
                ),
            }
        )
    )

    cqc_locations_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String),
            (CQCL.registration_status, pl.String),
            (CQCL.type, pl.String),
            (CQCL.current_ratings, current_ratings_struct),
            (CQCL.historic_ratings, historic_ratings_struct),
            (CQCL.assessment, assessment_struct),
        ]
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

    assessment_ratings_input_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String),
            (CQCL.registration_status, pl.String),
            (CQCL.assessment, assessment_struct),
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

    assessment_ratings_output_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String),
            (CQCL.registration_status, pl.String),
            (CQCL.assessment_plan_published_datetime, pl.String),
            (CQCL.assessment_plan_id, pl.String),
            (CQCL.title, pl.String),
            (CQCL.assessment_date, pl.String),
            (CQCL.assessment_plan_status, pl.String),
            (CQCL.dataset, pl.String),
            (CQCL.name, pl.String),
            (CQCL.status, pl.String),
            (CQCL.rating, pl.String),
            (CQCL.source_path, pl.String),
            (CQCL.safe, pl.String),
            (CQCL.effective, pl.String),
            (CQCL.caring, pl.String),
            (CQCL.responsive, pl.String),
            (CQCL.well_led, pl.String),
        ]
    )

    merge_assessment_ratings_schema = assessment_ratings_output_schema

    # `CQCRatingsColumns.date` is Date here (not String, unlike
    # `flattened_ratings_with_current_or_historic_schema`) because `merge_cqc_ratings`
    # parses the assessment branch's date into a `pl.Date`, and `pl.concat` requires
    # both sides of the union to already share that type.
    merge_standard_ratings_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String),
            (CQCL.registration_status, pl.String),
            (CQCRatingsColumns.date, pl.Date),
            (CQCRatingsColumns.overall_rating, pl.String),
            (CQCRatingsColumns.safe_rating, pl.String),
            (CQCRatingsColumns.well_led_rating, pl.String),
            (CQCRatingsColumns.caring_rating, pl.String),
            (CQCRatingsColumns.responsive_rating, pl.String),
            (CQCRatingsColumns.effective_rating, pl.String),
            (CQCRatingsColumns.current_or_historic, pl.String),
        ]
    )

    merge_expected_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String),
            (CQCL.registration_status, pl.String),
            (CQCRatingsColumns.date, pl.Date),
            (CQCL.assessment_plan_id, pl.String),
            (CQCL.title, pl.String),
            (CQCL.assessment_date, pl.String),
            (CQCL.assessment_plan_status, pl.String),
            (CQCL.name, pl.String),
            (CQCL.source_path, pl.String),
            (CQCL.dataset, pl.String),
            (CQCRatingsColumns.current_or_historic, pl.String),
            (CQCRatingsColumns.overall_rating, pl.String),
            (CQCRatingsColumns.safe_rating, pl.String),
            (CQCRatingsColumns.well_led_rating, pl.String),
            (CQCRatingsColumns.caring_rating, pl.String),
            (CQCRatingsColumns.responsive_rating, pl.String),
            (CQCRatingsColumns.effective_rating, pl.String),
        ]
    )

    ratings_with_assessment_date_schema = pl.Schema(
        [
            *flattened_ratings_with_current_or_historic_schema.items(),
            (CQCL.assessment_date, pl.String),
        ]
    )

    ratings_with_sequence_schema = pl.Schema(
        [
            *ratings_with_assessment_date_schema.items(),
            (CQCRatingsColumns.reversed_rating_sequence, pl.Int64),
        ]
    )

    full_ratings_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String),
            (CQCL.registration_status, pl.String),
            (CQCRatingsColumns.date, pl.String),
            (CQCL.assessment_plan_id, pl.String),
            (CQCL.title, pl.String),
            (CQCL.assessment_date, pl.String),
            (CQCL.assessment_plan_status, pl.String),
            (CQCL.name, pl.String),
            (CQCL.source_path, pl.String),
            (CQCL.dataset, pl.String),
            (CQCRatingsColumns.latest_rating_flag, pl.Int32),
            (CQCRatingsColumns.current_or_historic, pl.String),
            (CQCRatingsColumns.overall_rating, pl.String),
            (CQCRatingsColumns.safe_rating, pl.String),
            (CQCRatingsColumns.well_led_rating, pl.String),
            (CQCRatingsColumns.caring_rating, pl.String),
            (CQCRatingsColumns.responsive_rating, pl.String),
            (CQCRatingsColumns.effective_rating, pl.String),
            (CQCRatingsColumns.safe_rating_value, pl.Int32),
            (CQCRatingsColumns.well_led_rating_value, pl.Int32),
            (CQCRatingsColumns.caring_rating_value, pl.Int32),
            (CQCRatingsColumns.responsive_rating_value, pl.Int32),
            (CQCRatingsColumns.effective_rating_value, pl.Int32),
            (CQCRatingsColumns.total_rating_value, pl.Int32),
        ]
    )

    numerical_ratings_input_schema = pl.Schema(
        [
            (CQCRatingsColumns.overall_rating, pl.String),
            (CQCRatingsColumns.safe_rating, pl.String),
            (CQCRatingsColumns.well_led_rating, pl.String),
            (CQCRatingsColumns.caring_rating, pl.String),
            (CQCRatingsColumns.responsive_rating, pl.String),
            (CQCRatingsColumns.effective_rating, pl.String),
        ]
    )

    expected_numerical_ratings_schema = pl.Schema(
        [
            *numerical_ratings_input_schema.items(),
            (CQCRatingsColumns.overall_rating_value, pl.Int32),
            (CQCRatingsColumns.safe_rating_value, pl.Int32),
            (CQCRatingsColumns.well_led_rating_value, pl.Int32),
            (CQCRatingsColumns.caring_rating_value, pl.Int32),
            (CQCRatingsColumns.responsive_rating_value, pl.Int32),
            (CQCRatingsColumns.effective_rating_value, pl.Int32),
            (CQCRatingsColumns.total_rating_value, pl.Int32),
        ]
    )

    location_id_hash_schema = pl.Schema([(CQCL.location_id, pl.String)])

    expected_location_id_hash_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String),
            (CQCRatingsColumns.location_id_hash, pl.String),
        ]
    )

    benchmarks_ratings_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String),
            (CQCL.registration_status, pl.String),
            (CQCRatingsColumns.current_or_historic, pl.String),
            (CQCRatingsColumns.overall_rating_value, pl.Int32),
        ]
    )

    good_and_outstanding_input_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String),
            (CQCRatingsColumns.overall_rating_value, pl.Int32),
        ]
    )

    ascwds_workplace_schema_for_join = pl.Schema(
        [
            (AWPKeys.import_date, pl.String),
            (AWPKeys.year, pl.String),
            (AWPKeys.month, pl.String),
            (AWPKeys.day, pl.String),
            (AWP.establishment_id, pl.String),
            (AWP.location_id, pl.String),
        ]
    )

    join_establishment_ids_input_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String),
        ]
    )

    ascwds_join_establishment_ids_schema = pl.Schema(
        [
            (AWP.establishment_id, pl.String),
            (AWP.location_id, pl.String),
        ]
    )

    expected_join_establishment_ids_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String),
            (AWP.establishment_id, pl.String),
        ]
    )

    create_benchmark_ratings_dataset_input_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String),
            (AWP.establishment_id, pl.String),
            (CQCL.name, pl.String),
            (CQCL.dataset, pl.String),
            (CQCRatingsColumns.good_or_outstanding_flag, pl.Int32),
            (CQCRatingsColumns.overall_rating, pl.String),
            (CQCRatingsColumns.date, pl.String),
        ]
    )
