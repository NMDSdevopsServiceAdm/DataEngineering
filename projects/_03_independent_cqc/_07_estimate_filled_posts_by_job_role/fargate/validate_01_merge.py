import sys
from datetime import date

import pointblank as pb
import polars as pl

from polars_utils import utils
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns
from utils.column_values.categorical_columns_by_dataset import (
    EstimatedIndCQCFilledPostsByJobRoleCategoricalValues as CatValues,
)
from utils.value_labels.ascwds_worker.ascwds_worker_jobgroup_dictionary import (
    AscwdsWorkerValueLabelsJobGroup,
)

VALIDATION_COLS_TO_IMPORT = [
    IndCqcColumns.id_per_locationid_import_date,
    IndCqcColumns.location_id,
    IndCqcColumns.cqc_location_import_date,
    IndCqcColumns.main_job_role_clean_labelled,
    IndCqcColumns.primary_service_type,
    IndCqcColumns.estimate_filled_posts_source,
    IndCqcColumns.estimate_filled_posts,
    IndCqcColumns.ascwds_filled_posts_dedup_clean,
    IndCqcColumns.ascwds_job_role_counts,
]

IND_CQC_ESTIMATES_COLS_TO_IMPORT = [
    IndCqcColumns.location_id,
]

CQC_EARLIEST_IMPORT_DATE = date(2013, 3, 1)

EXPECTED_SCHEMA = pb.Schema(
    columns={
        IndCqcColumns.id_per_locationid_import_date: "UInt32",
        IndCqcColumns.location_id: 'Categorical(Categories(name="location", namespace="filled_posts", physical=pl.UInt32))',
        IndCqcColumns.cqc_location_import_date: "Date",
        IndCqcColumns.main_job_role_clean_labelled: "Enum(categories=['advice_guidance_and_advocacy', 'care_worker', 'community_support_and_outreach', 'employment_support', 'nursing_assistant', 'other_care_role', 'senior_care_worker', 'support_worker', 'data_governance_manager', 'deputy_manager', 'first_line_manager', 'it_manager', 'it_service_desk_manager', 'middle_management', 'managers_and_staff_in_care_related_but_not_care_providing_roles', 'registered_manager', 'senior_management', 'supervisor', 'team_leader', 'allied_health_professional', 'occupational_therapist', 'registered_nurse', 'registered_nursing_associate', 'safeguarding_and_reviewing_officer', 'social_worker', 'activities_worker_or_coordinator', 'administrative_or_office_staff_not_care_providing', 'ancillary_staff_not_care_providing', 'assessment_officer', 'care_coordinator', 'childrens_roles', 'data_analyst', 'it_and_digital_support', 'learning_and_development_lead', 'occupational_therapist_assistant', 'other_non_care_related_staff', 'software_developer'])",
        IndCqcColumns.primary_service_type: "Enum(categories=['Care home without nursing', 'Care home with nursing', 'non-residential'])",
        IndCqcColumns.estimate_filled_posts_source: "Enum(categories=['imputed_pir_filled_posts_model', 'ascwds_pir_merged', 'imputed_posts_care_home_model', 'care_home_model', 'imputed_posts_non_res_combined_model', 'non_res_combined_model', 'posts_rolling_average_model'])",
        IndCqcColumns.estimate_filled_posts: "Float32",
        IndCqcColumns.ascwds_filled_posts_dedup_clean: "Float32",
        IndCqcColumns.ascwds_job_role_counts: "Int16",
    }
)


def main(
    bucket_name: str, source_path: str, compare_path: str, reports_path: str
) -> None:
    """Validates a dataset according to a set of provided rules and produces a
        summary report as well as failure outputs.

    Args:
        bucket_name (str): the bucket (name only) in which to source the dataset
            and output the report to (should correspond to workspace / feature
            branch name)
        source_path (str): the source dataset path to be validated
        compare_path (str): the path to the dataset to compare against
        reports_path (str): the output path to write reports to
    """
    source_df = utils.read_parquet(
        source=f"s3://{bucket_name}/{source_path}",
        selected_columns=VALIDATION_COLS_TO_IMPORT,
    )
    compare_df = utils.read_parquet(
        source=f"s3://{bucket_name}/{compare_path}",
        selected_columns=IND_CQC_ESTIMATES_COLS_TO_IMPORT,
    )
    expected_row_count = compare_df.height * len(
        AscwdsWorkerValueLabelsJobGroup.all_roles()
    )

    validation = (
        pb.Validate(
            data=source_df,
            label=f"Key validation of {source_path}",
            thresholds=GLOBAL_THRESHOLDS,
            brief=True,
            actions=GLOBAL_ACTIONS,
        )
        # dataset schema
        .col_schema_match(
            schema=EXPECTED_SCHEMA, brief="Dataset should match the expected schema"
        )
        # dataset size
        .row_count_match(
            expected_row_count,
            brief=f"Expects {expected_row_count} rows",
        )
        # complete columns
        .col_vals_not_null(
            columns=[
                IndCqcColumns.id_per_locationid_import_date,
                IndCqcColumns.location_id,
                IndCqcColumns.cqc_location_import_date,
                IndCqcColumns.primary_service_type,
                IndCqcColumns.main_job_role_clean_labelled,
                IndCqcColumns.estimate_filled_posts_source,
                IndCqcColumns.estimate_filled_posts,
            ],
            brief="Key columns should contain no null values",
        )
        # index columns
        .rows_distinct(
            columns_subset=[
                IndCqcColumns.location_id,
                IndCqcColumns.cqc_location_import_date,
                IndCqcColumns.main_job_role_clean_labelled,
            ],
            brief="Primary key (location_id, cqc_location_import_date, main_job_role_clean_labelled) should be unique",
        )
        .col_vals_expr(
            expr=(
                pl.col(IndCqcColumns.id_per_locationid_import_date)
                .n_unique()
                .over(
                    [
                        IndCqcColumns.location_id,
                        IndCqcColumns.cqc_location_import_date,
                    ]
                )
                == 1
            ),
            brief="id_per_locationid_import_date should be unique per locationid and cqc_location_import_date combination",
        )
        # categorical
        .col_vals_in_set(
            IndCqcColumns.estimate_filled_posts_source,
            CatValues.estimate_filled_posts_source_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.primary_service_type,
            CatValues.primary_service_type_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.main_job_role_clean_labelled,
            CatValues.main_job_role_labels_column_values.categorical_values,
        )
        # distinct values
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.estimate_filled_posts_source,
                CatValues.estimate_filled_posts_source_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.estimate_filled_posts_source} should have exactly {CatValues.estimate_filled_posts_source_column_values.count_of_categorical_values} distinct values",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.primary_service_type,
                CatValues.primary_service_type_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.primary_service_type} should have exactly {CatValues.primary_service_type_column_values.count_of_categorical_values} distinct values",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.main_job_role_clean_labelled,
                CatValues.main_job_role_labels_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.main_job_role_clean_labelled} should have exactly {CatValues.main_job_role_labels_column_values.count_of_categorical_values} distinct values",
        )
        # # numerical
        # .col_vals_gt(
        #     columns=[
        #         IndCqcColumns.estimate_filled_posts,
        #         IndCqcColumns.ascwds_filled_posts_dedup_clean,
        #     ],
        #     value=0,
        #     na_pass=True,
        #     brief="estimate_filled_posts and ascwds_filled_posts_dedup_clean should be > 0 where present",
        # )
        # .col_vals_ge(
        #     columns=IndCqcColumns.ascwds_job_role_counts,
        #     value=0,
        #     na_pass=True,
        #     brief="ascwds_job_role_counts should be >= 0 where present",
        # )
        # .col_vals_le(
        #     columns=IndCqcColumns.ascwds_job_role_counts,
        #     value=pb.col(IndCqcColumns.estimate_filled_posts),
        #     na_pass=True,
        #     brief="ascwds_job_role_counts should be <= estimate_filled_posts where present",
        # )
        # # Date plausibility
        # .col_vals_ge(
        #     columns=IndCqcColumns.cqc_location_import_date,
        #     value=CQC_EARLIEST_IMPORT_DATE,
        #     brief=f"cqc_location_import_date should not be before {CQC_EARLIEST_IMPORT_DATE.strftime('%d/%m/%Y')}",
        # )
        .interrogate()
    )
    vl.write_reports(validation, bucket_name, reports_path)


if __name__ == "__main__":
    print(f"Validation script called with parameters: {sys.argv}")

    args = utils.get_args(
        ("--bucket_name", "S3 bucket for source dataset and validation report"),
        ("--source_path", "The filepath of the dataset to validate"),
        ("--compare_path", "The filepath of the dataset to compare against"),
        ("--reports_path", "The filepath to output reports"),
    )
    print(f"Starting validation for {args.source_path}")

    main(args.bucket_name, args.source_path, args.compare_path, args.reports_path)
    print(f"Validation of {args.source_path} complete")
