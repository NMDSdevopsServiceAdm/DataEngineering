import pointblank as pb
import polars as pl

import projects._03_independent_cqc._01_filled_posts._07_archive.fargate.utils.archive_utils as aUtils
from polars_utils import utils
from polars_utils.column_types import CategoricalColumnTypes
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from utils.column_names.ind_cqc_pipeline_columns import (
    ArchiveDateRunNumberPartitionKeys as ArchiveKeys,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

ESTIMATES_KEY_COLUMNS = [
    IndCQC.location_id,
    IndCQC.cqc_location_import_date,
    IndCQC.main_job_role_clean_labelled,
]

ESTIMATES_EXPECTED_SCHEMA = pb.Schema(
    columns={
        IndCQC.id_per_locationid_import_date: "UInt32",
        IndCQC.location_id: str(CategoricalColumnTypes.LocationCatType),
        IndCQC.cqc_location_import_date: "Date",
        IndCQC.estimate_filled_posts: "Float32",
        IndCQC.primary_service_type: str(CategoricalColumnTypes.PrimaryServiceEnumType),
        IndCQC.main_job_role_clean_labelled: str(CategoricalColumnTypes.JobRoleCatType),
        IndCQC.ascwds_job_role_ratios: "Float32",
        IndCQC.imputed_ascwds_job_role_ratios: "Float32",
        IndCQC.ascwds_job_role_rolling_ratio: "Float32",
        IndCQC.ascwds_job_role_ratios_merged: "Float32",
        IndCQC.ascwds_job_role_ratios_merged_source: str(
            CategoricalColumnTypes.AscwdsJobRoleRatiosMergedSourceEnumType
        ),
        IndCQC.estimate_filled_posts_by_job_role_pre_reallocation: "Float32",
        IndCQC.estimate_filled_posts_by_job_role: "Float32",
        IndCQC.main_job_group_labelled: str(CategoricalColumnTypes.JobGroupCatType),
        IndCQC.job_role_filtering_rule: str(
            CategoricalColumnTypes.JobRoleFilteringRuleCatType
        ),
        ArchiveKeys.archive_date: "Date",
        ArchiveKeys.run_number: "Int64",
    }
)

METADATA_EXPECTED_SCHEMA = pb.Schema(
    columns={
        IndCQC.id_per_locationid_import_date: "UInt32",
        IndCQC.current_cssr: "Categorical",
        IndCQC.current_region: "Categorical",
        IndCQC.current_icb: "Categorical",
        IndCQC.current_rural_urban_indicator_2011: "Categorical",
        IndCQC.current_lsoa21: "Categorical",
        IndCQC.current_msoa21: "Categorical",
        IndCQC.imputed_registration_date: "Date",
        IndCQC.ascwds_filled_posts_dedup_clean: "Float32",
        IndCQC.ascwds_pir_merged: "Float32",
        IndCQC.ascwds_filtering_rule: "Categorical",
        IndCQC.estimate_filled_posts_source: str(
            CategoricalColumnTypes.EstimatesFilledPostSourceEnumType
        ),
        IndCQC.ascwds_filled_posts_source: str(
            CategoricalColumnTypes.AscwdsFilledPostsSourceEnumType
        ),
        IndCQC.care_home_model: "Float32",
        IndCQC.imputed_pir_filled_posts_model: "Float32",
        IndCQC.imputed_posts_care_home_model: "Float32",
        IndCQC.imputed_posts_non_res_combined_model: "Float32",
        IndCQC.non_res_combined_model: "Float32",
        IndCQC.pir_people_directly_employed_dedup: "Int64",
        IndCQC.posts_rolling_average_model: "Float32",
        IndCQC.ct_care_home_total_employed_imputed: "Float32",
        IndCQC.ct_non_res_care_workers_employed_imputed: "Float32",
        IndCQC.care_home_status_count: "Int16",
        ArchiveKeys.archive_date: "Date",
        ArchiveKeys.run_number: "Int64",
    }
)


def main(
    bucket_name: str,
    dataset: str,
    source_path: str,
    compare_path: str,
    other_output_path: str,
    reports_path: str,
) -> None:
    """Validates one output of the archived independent CQC filled posts by job role dataset.

    Args:
        bucket_name (str): the bucket (name only) in which to source the dataset
            and output the report to (should correspond to workspace / feature
            branch name)
        dataset (str): which archive output to validate: "estimates" or "metadata"
        source_path (str): the archived dataset path to validate
        compare_path (str): the pre-archive source path to compare this run's row
            count against
        other_output_path (str): the sibling archive output's path, used to confirm
            both outputs received this run's partition
        reports_path (str): the output path to write reports to

    Raises:
        ValueError: if dataset is not "estimates" or "metadata"
    """
    if dataset == "estimates":
        job_role_estimates_validation(
            bucket_name, source_path, compare_path, other_output_path, reports_path
        )
    elif dataset == "metadata":
        job_role_metadata_validation(
            bucket_name, source_path, compare_path, other_output_path, reports_path
        )
    else:
        raise ValueError(f"Unknown dataset: {dataset}")


def job_role_estimates_validation(
    bucket_name: str,
    source_path: str,
    compare_path: str,
    other_output_path: str,
    reports_path: str,
) -> None:
    source_uri = f"s3://{bucket_name}/{source_path}"
    other_output_uri = f"s3://{bucket_name}/{other_output_path}"

    latest_run_number = aUtils.get_run_number([source_uri])
    latest_partition_df = (
        utils.scan_parquet(source_uri)
        .filter(pl.col(ArchiveKeys.run_number) == latest_run_number)
        .collect()
    )

    compare_df = utils.read_parquet(source=f"s3://{bucket_name}/{compare_path}")
    expected_row_count = compare_df.height

    validation = (
        pb.Validate(
            data=latest_partition_df,
            label=f"Validation of {source_path}",
            thresholds=GLOBAL_THRESHOLDS,
            brief=True,
            actions=GLOBAL_ACTIONS,
        )
        .col_schema_match(
            schema=ESTIMATES_EXPECTED_SCHEMA,
            complete=False,
            brief="Archived estimates columns should match the expected (partial) schema",
        )
        .row_count_match(
            expected_row_count,
            brief=f"Expects {expected_row_count} rows to match the source for this run",
        )
        .rows_distinct(
            columns_subset=ESTIMATES_KEY_COLUMNS,
            brief=(
                "Primary key (location_id, cqc_location_import_date, "
                "main_job_role_clean_labelled) should be unique"
            ),
        )
        .col_vals_not_null(
            columns=[*ESTIMATES_KEY_COLUMNS, IndCQC.estimate_filled_posts],
            brief="Key columns and estimate_filled_posts should contain no null values",
        )
        .specially(
            aUtils.make_run_numbers_agree_validator([source_uri, other_output_uri]),
            brief="This run's partition should exist in both archive outputs",
        )
        .interrogate()
    )
    vl.write_reports(validation, bucket_name, reports_path)


def job_role_metadata_validation(
    bucket_name: str,
    source_path: str,
    compare_path: str,
    other_output_path: str,
    reports_path: str,
) -> None:
    source_uri = f"s3://{bucket_name}/{source_path}"
    other_output_uri = f"s3://{bucket_name}/{other_output_path}"

    latest_run_number = aUtils.get_run_number([source_uri])
    latest_partition_df = (
        utils.scan_parquet(source_uri)
        .filter(pl.col(ArchiveKeys.run_number) == latest_run_number)
        .collect()
    )

    compare_df = utils.read_parquet(source=f"s3://{bucket_name}/{compare_path}")
    expected_row_count = compare_df.height

    validation = (
        pb.Validate(
            data=latest_partition_df,
            label=f"Validation of {source_path}",
            thresholds=GLOBAL_THRESHOLDS,
            brief=True,
            actions=GLOBAL_ACTIONS,
        )
        .col_schema_match(
            schema=METADATA_EXPECTED_SCHEMA,
            complete=False,
            brief="Archived metadata columns should match the expected (partial) schema",
        )
        .row_count_match(
            expected_row_count,
            brief=f"Expects {expected_row_count} rows to match the source for this run",
        )
        .rows_distinct(
            columns_subset=IndCQC.id_per_locationid_import_date,
            brief="id_per_locationid_import_date should be unique",
        )
        .col_vals_not_null(
            columns=IndCQC.id_per_locationid_import_date,
            brief="id_per_locationid_import_date should contain no null values",
        )
        .specially(
            aUtils.make_run_numbers_agree_validator([source_uri, other_output_uri]),
            brief="This run's partition should exist in both archive outputs",
        )
        .interrogate()
    )
    vl.write_reports(validation, bucket_name, reports_path)


if __name__ == "__main__":
    args = utils.get_args(
        ("--bucket_name", "S3 bucket for source dataset and validation report"),
        ("--dataset", "Which archive output to validate: estimates or metadata"),
        ("--source_path", "The filepath of the archived dataset to validate"),
        (
            "--compare_path",
            "The filepath of the pre-archive source to compare this run's row count against",
        ),
        (
            "--other_output_path",
            "The sibling archive output's filepath, to confirm both outputs got this run's partition",
        ),
        ("--reports_path", "The filepath to output reports"),
    )
    print(f"Starting validation for {args.source_path}")

    main(
        args.bucket_name,
        args.dataset,
        args.source_path,
        args.compare_path,
        args.other_output_path,
        args.reports_path,
    )
    print(f"Validation of {args.source_path} complete")
