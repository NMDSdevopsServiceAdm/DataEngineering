import os
import sys

os.environ["SPARK_VERSION"] = "3.5"

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType

from utils import utils
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CqclCleaned,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
    DimensionPartitionKeys as DimensionKeys,
)
from utils.column_values.categorical_column_values import (
    LocationType,
    RegistrationStatus,
    Services,
)
from utils.raw_data_adjustments import RecordsToRemoveInLocationsData
from utils.validation.validation_rule_names import RuleNames as RuleName
from utils.validation.validation_rules.locations_api_cleaned_validation_rules import (
    LocationsAPICleanedValidationRules as Rules,
)
from utils.validation.validation_utils import (
    add_column_with_length_of_string,
    raise_exception_if_any_checks_failed,
    validate_dataset,
)

PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

raw_cqc_locations_columns_to_import = [
    Keys.import_date,
    CqclCleaned.location_id,
    CqclCleaned.provider_id,
    CqclCleaned.type,
    CqclCleaned.registration_status,
    CqclCleaned.gac_service_types,
    CqclCleaned.regulated_activities,
]


def main(
    raw_cqc_location_source: str,
    dim_gac_services_source: str,
    dim_postcode_matching_source: str,
    cleaned_cqc_locations_source: str,
    report_destination: str,
):
    raw_location_df = utils.read_from_parquet(
        raw_cqc_location_source,
        selected_columns=raw_cqc_locations_columns_to_import,
    )
    cleaned_cqc_locations_df = utils.read_from_parquet(
        cleaned_cqc_locations_source,
    )
    gac_services_df = utils.read_from_parquet(dim_gac_services_source)
    cleaned_cqc_locations_df = join_dimension(cleaned_cqc_locations_df, gac_services_df)

    postcode_matching_df = utils.read_from_parquet(dim_postcode_matching_source)
    cleaned_cqc_locations_df = join_dimension(
        cleaned_cqc_locations_df, postcode_matching_df
    )

    rules = Rules.rules_to_check

    rules[RuleName.size_of_dataset] = (
        calculate_expected_size_of_cleaned_cqc_locations_dataset(raw_location_df)
    )

    cleaned_cqc_locations_df = add_column_with_length_of_string(
        cleaned_cqc_locations_df, [CqclCleaned.location_id, CqclCleaned.provider_id]
    )

    check_result_df = validate_dataset(cleaned_cqc_locations_df, rules)

    utils.write_to_parquet(check_result_df, report_destination, mode="overwrite")

    if isinstance(check_result_df, DataFrame):
        raise_exception_if_any_checks_failed(check_result_df)


def join_dimension(cqc_df: DataFrame, dimension_df: DataFrame) -> DataFrame:
    """
    Joins the CQC dataframe to one of its dimensions on location id and import_date
    Args:
        cqc_df (DataFrame): CQC dataframe with location id and import date columns
        dimension_df (DataFrame): Dimension dataframe with location id and import date columns

    Returns:
        DataFrame: left joined CQC data with the dimension data

    """
    window_spec_dim = Window.partitionBy(
        CqclCleaned.location_id, DimensionKeys.import_date
    ).orderBy(F.col(DimensionKeys.last_updated).desc())
    current_dimension = dimension_df.withColumn(
        "row_num", F.row_number().over(window_spec_dim)
    ).filter(F.col("row_num") == 1)

    joined_df = cqc_df.join(
        current_dimension.drop(
            DimensionKeys.year,
            DimensionKeys.month,
            DimensionKeys.day,
            DimensionKeys.last_updated,
        ),
        [
            CqclCleaned.location_id,
            CqclCleaned.cqc_location_import_date,
            Keys.import_date,
        ],
        how="left",
    )
    return joined_df


def calculate_expected_size_of_cleaned_cqc_locations_dataset(
    raw_location_df: DataFrame,
) -> int:
    has_a_known_regulated_activity: str = "has_a_known_regulated_activity"
    has_a_known_provider_id: str = "has_a_known_provider_id"

    raw_location_df = identify_if_location_has_a_known_value(
        raw_location_df,
        CqclCleaned.regulated_activities,
        has_a_known_regulated_activity,
    )
    raw_location_df = identify_if_location_has_a_known_value(
        raw_location_df, CqclCleaned.provider_id, has_a_known_provider_id
    )

    expected_size = raw_location_df.where(
        (raw_location_df[CqclCleaned.type] == LocationType.social_care_identifier)
        & (
            raw_location_df[CqclCleaned.registration_status]
            == RegistrationStatus.registered
        )
        & (
            raw_location_df[CqclCleaned.location_id]
            != RecordsToRemoveInLocationsData.dental_practice
        )
        & (
            raw_location_df[CqclCleaned.location_id]
            != RecordsToRemoveInLocationsData.temp_registration
        )
        & (
            (
                raw_location_df[CqclCleaned.gac_service_types][0][
                    CqclCleaned.description
                ]
                != Services.specialist_college_service
            )
            | (F.size(raw_location_df[CqclCleaned.gac_service_types]) != 1)
            | (raw_location_df[CqclCleaned.gac_service_types].isNull())
        )
        & (raw_location_df[has_a_known_regulated_activity] == True)
        & (raw_location_df[has_a_known_provider_id] == True)
    ).count()
    return expected_size


def identify_if_location_has_a_known_value(
    df: DataFrame, col_to_check: str, new_col_name: str
) -> DataFrame:
    """
    Identifies if a location has ever had a known valid response and adds a new column to the DataFrame.

    This function partitions the DataFrame by location ID and checks if there are any known values within each location.
    If the column is an array type, the 'known value' is defined as the array being of size greater than zero.
    Otherwise, a known value is identified as a non-null value.
    If any valid responses are found, it sets the value of the new column to True; otherwise, it sets it to False.

    Args:
        df (DataFrame): The input DataFrame containing location data.
        col_to_check (str): The name of the column to check.
        new_col_name (str): The name of the new column to be added to the DataFrame.

    Returns:
        DataFrame: The DataFrame with the new column indicating the presence of valid responses.
    """
    window_spec = Window.partitionBy(
        CqclCleaned.location_id,
    ).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    col_to_check_is_array = isinstance(df.schema[col_to_check].dataType, ArrayType)

    has_known_array_expr = F.max(F.size(df[col_to_check])).over(window_spec) > 0

    has_non_null_value_expr = (
        F.max(F.when(df[col_to_check].isNotNull(), 1)).over(window_spec) > 0
    )

    if col_to_check_is_array:
        df = df.withColumn(
            new_col_name,
            F.when(has_known_array_expr, True).otherwise(False),
        )
    else:
        df = df.withColumn(
            new_col_name,
            F.when(has_non_null_value_expr, True).otherwise(False),
        )

    return df


if __name__ == "__main__":
    print("Spark job 'validate_locations_api_cleaned_data' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        raw_cqc_location_source,
        gac_dimension_source,
        postcode_dimension_source,
        cleaned_cqc_locations_source,
        report_destination,
    ) = utils.collect_arguments(
        (
            "--raw_cqc_location_source",
            "Source s3 directory for parquet locations api dataset",
        ),
        (
            "--gac_dimension_source",
            "Source of the GAC services dimension data",
        ),
        (
            "--postcode_dimension_source",
            "Source of the postcode matching dimension data",
        ),
        (
            "--cleaned_cqc_locations_source",
            "Source s3 directory for parquet locations api cleaned dataset",
        ),
        (
            "--report_destination",
            "Destination s3 directory for validation report parquet",
        ),
    )
    try:
        main(
            raw_cqc_location_source,
            gac_dimension_source,
            postcode_dimension_source,
            cleaned_cqc_locations_source,
            report_destination,
        )
    finally:
        spark = utils.get_spark()
        if spark.sparkContext._gateway:
            spark.sparkContext._gateway.shutdown_callback_server()
        spark.stop()

    print("Spark job 'validate_locations_api_cleaned_data' complete")
