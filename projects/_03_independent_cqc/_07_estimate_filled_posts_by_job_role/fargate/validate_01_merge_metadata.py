import sys
from datetime import date

import pointblank as pb
from dateutil.relativedelta import relativedelta

from polars_utils import utils
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns
from utils.column_values.categorical_columns_by_dataset import (
    EstimatedIndCQCFilledPostsByJobRoleCategoricalValues as CatValues,
)
from utils.value_labels.ons_pd.onspd_icb import OnspdIcb
from utils.value_labels.ons_pd.onspd_lsoa21 import OnspdLsoa21
from utils.value_labels.ons_pd.onspd_msoa21 import OnspdMsoa21

VALIDATION_COLS_TO_IMPORT = [
    IndCqcColumns.id_per_locationid_import_date,
    IndCqcColumns.name,
    IndCqcColumns.provider_id,
    IndCqcColumns.services_offered,
    IndCqcColumns.primary_service_type_second_level,
    IndCqcColumns.care_home,
    IndCqcColumns.dormancy,
    IndCqcColumns.number_of_beds,
    IndCqcColumns.imputed_registration_date,
    IndCqcColumns.ascwds_workplace_import_date,
    IndCqcColumns.ascwds_filled_posts_dedup_clean,
    IndCqcColumns.ascwds_pir_merged,
    IndCqcColumns.ascwds_filtering_rule,
    IndCqcColumns.current_ons_import_date,
    IndCqcColumns.current_cssr,
    IndCqcColumns.current_region,
    IndCqcColumns.current_icb,
    IndCqcColumns.current_rural_urban_indicator_2011,
    IndCqcColumns.current_lsoa21,
    IndCqcColumns.current_msoa21,
    IndCqcColumns.estimate_filled_posts_source,
    IndCqcColumns.worker_records_bounded,
]

IND_CQC_ESTIMATES_COLS_TO_IMPORT = [
    IndCqcColumns.location_id,
    IndCqcColumns.cqc_location_import_date,
]

ASCWDS_IMPORT_DATE_LIMIT = date(2013, 3, 1)
CQC_REGISTRATION_DATE_LIMIT = date(2010, 9, 13)
ONS_IMPORT_DATE_LIMIT = date.today() - relativedelta(months=13)

EXPECTED_SCHEMA = pb.Schema(
    columns={
        IndCqcColumns.id_per_locationid_import_date: "UInt32",
        IndCqcColumns.name: "String",
        IndCqcColumns.provider_id: "String",
        IndCqcColumns.services_offered: "List(String)",
        IndCqcColumns.primary_service_type_second_level: "Categorical",
        IndCqcColumns.care_home: "Categorical",
        IndCqcColumns.dormancy: "Categorical",
        IndCqcColumns.number_of_beds: "Int16",
        IndCqcColumns.imputed_registration_date: "Date",
        IndCqcColumns.ascwds_workplace_import_date: "Date",
        IndCqcColumns.ascwds_filled_posts_dedup_clean: "Float32",
        IndCqcColumns.ascwds_pir_merged: "Float32",
        IndCqcColumns.ascwds_filtering_rule: "Categorical",
        IndCqcColumns.current_ons_import_date: "Date",
        IndCqcColumns.current_cssr: "Categorical",
        IndCqcColumns.current_region: "Categorical",
        IndCqcColumns.current_icb: "Categorical",
        IndCqcColumns.current_rural_urban_indicator_2011: "Categorical",
        IndCqcColumns.current_lsoa21: "Categorical",
        IndCqcColumns.current_msoa21: "Categorical",
        IndCqcColumns.estimate_filled_posts_source: "Enum(categories=['imputed_pir_filled_posts_model', 'ascwds_pir_merged', 'imputed_posts_care_home_model', 'care_home_model', 'imputed_posts_non_res_combined_model', 'non_res_combined_model', 'posts_rolling_average_model'])",
        IndCqcColumns.worker_records_bounded: "Int16",
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

    print(f"Source df dtypes:\n{source_df.dtypes}")

    validation = (
        pb.Validate(
            data=source_df,
            label=f"Key validation of {source_path}",
            thresholds=GLOBAL_THRESHOLDS,
            brief=True,
            actions=GLOBAL_ACTIONS,
        )
        # Schema check
        .col_schema_match(
            schema=EXPECTED_SCHEMA, brief="Dataset should match the expected schema"
        )
        # Dataset size
        .row_count_match(
            count=compare_df.height,
            brief=(
                f"Source file has {source_df.height} rows but expecting "
                f"{compare_df.height} rows to match estimates dataset"
            ),
        )
        # Uniqueness
        .rows_distinct(
            columns_subset=IndCqcColumns.id_per_locationid_import_date,
            brief="Primary key (id_per_locationid_import_date) should be unique",
        )
        # Completeness (no nulls)
        .col_vals_not_null(
            columns=[
                IndCqcColumns.id_per_locationid_import_date,
                IndCqcColumns.name,
                IndCqcColumns.provider_id,
                IndCqcColumns.services_offered,
                IndCqcColumns.primary_service_type_second_level,
                IndCqcColumns.care_home,
                IndCqcColumns.imputed_registration_date,
                IndCqcColumns.ascwds_workplace_import_date,
                IndCqcColumns.ascwds_filtering_rule,
                IndCqcColumns.current_ons_import_date,
                IndCqcColumns.current_cssr,
                IndCqcColumns.current_region,
                IndCqcColumns.current_icb,
                IndCqcColumns.current_rural_urban_indicator_2011,
                IndCqcColumns.current_lsoa21,
                IndCqcColumns.current_msoa21,
                IndCqcColumns.estimate_filled_posts_source,
            ],
            brief="Required columns should contain no null values",
        )
        # Categorical values
        .col_vals_in_set(
            IndCqcColumns.primary_service_type_second_level,
            CatValues.primary_service_type_second_level_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.care_home,
            CatValues.care_home_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.dormancy,
            [*CatValues.dormancy_column_values.categorical_values, None],
        )
        .col_vals_in_set(
            IndCqcColumns.ascwds_filtering_rule,
            CatValues.ascwds_filtering_rule_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.current_cssr,
            CatValues.current_cssr_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.current_region,
            CatValues.current_region_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.current_icb,
            list(OnspdIcb.labels_dict.values()),
        )
        .col_vals_in_set(
            IndCqcColumns.current_rural_urban_indicator_2011,
            CatValues.current_rui_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.current_lsoa21,
            list(OnspdLsoa21.labels_dict.values()),
        )
        .col_vals_in_set(
            IndCqcColumns.current_msoa21,
            list(OnspdMsoa21.labels_dict.values()),
        )
        .col_vals_in_set(
            IndCqcColumns.estimate_filled_posts_source,
            CatValues.estimate_filled_posts_source_column_values.categorical_values,
        )
        # Distinct value counts
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.primary_service_type_second_level,
                CatValues.primary_service_type_second_level_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.primary_service_type_second_level} should have exactly {CatValues.primary_service_type_second_level_column_values.count_of_categorical_values} distinct values",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.care_home,
                CatValues.care_home_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.care_home} should have exactly {CatValues.care_home_column_values.count_of_categorical_values} distinct values",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.dormancy,
                CatValues.dormancy_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.dormancy} should have exactly {CatValues.dormancy_column_values.count_of_categorical_values} distinct values",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.current_cssr,
                CatValues.current_cssr_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.current_cssr} should have exactly {CatValues.current_cssr_column_values.count_of_categorical_values} distinct values",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.current_region,
                CatValues.current_region_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.current_region} should have exactly {CatValues.current_region_column_values.count_of_categorical_values} distinct values",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.current_rural_urban_indicator_2011,
                CatValues.current_rui_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.current_rural_urban_indicator_2011} should have exactly {CatValues.current_rui_column_values.count_of_categorical_values} distinct values",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.ascwds_filtering_rule,
                CatValues.ascwds_filtering_rule_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.ascwds_filtering_rule} should have exactly {CatValues.ascwds_filtering_rule_column_values.count_of_categorical_values} distinct values",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.estimate_filled_posts_source,
                CatValues.estimate_filled_posts_source_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.estimate_filled_posts_source} should have exactly {CatValues.estimate_filled_posts_source_column_values.count_of_categorical_values} distinct values",
        )
        # Date plausibility
        .col_vals_ge(
            columns=[IndCqcColumns.ascwds_workplace_import_date],
            value=ASCWDS_IMPORT_DATE_LIMIT,
            brief=f"ascwds_workplace_import_date should not be before {ASCWDS_IMPORT_DATE_LIMIT.strftime('%d/%m/%Y')}",
        )
        .col_vals_ge(
            columns=[IndCqcColumns.imputed_registration_date],
            value=CQC_REGISTRATION_DATE_LIMIT,
            brief=f"imputed_registration_date should not be before {CQC_REGISTRATION_DATE_LIMIT.strftime('%d/%m/%Y')}",
        )
        # .col_vals_ge(
        #     columns=[IndCqcColumns.current_ons_import_date],
        #     value=ONS_IMPORT_DATE_LIMIT,
        #     brief=f"current_ons_import_date should not be more than 13 months ago (not before {ONS_IMPORT_DATE_LIMIT.strftime('%d/%m/%Y')})",
        # )
        # Numeric range — strictly positive (nulls allowed)
        .col_vals_gt(
            columns=[
                IndCqcColumns.ascwds_filled_posts_dedup_clean,
                IndCqcColumns.ascwds_pir_merged,
                IndCqcColumns.number_of_beds,
                IndCqcColumns.worker_records_bounded,
            ],
            value=0,
            na_pass=True,
            brief="ascwds_filled_posts_dedup_clean, ascwds_pir_merged, number_of_beds and wkrrecs_bounded should be > 0 where present",
        )
    ).interrogate()

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
