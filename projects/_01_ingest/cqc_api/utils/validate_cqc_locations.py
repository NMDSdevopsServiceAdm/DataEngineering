import polars as pl

from polars_utils import raw_data_adjustments
from polars_utils.cleaning_utils import column_to_date
from projects._01_ingest.cqc_api.fargate.utils import cleaning_utils as cUtils
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_values.categorical_column_values import (
    LocationType,
    RegistrationStatus,
)


def get_expected_row_count_for_validation_full_clean(df: pl.DataFrame) -> int:
    """
    Returns the expected row count for validation of a fully cleaned dataset.
    This function tries to replicate the cleaning process to get the row count.

    Args:
        df (pl.DataFrame): compare Dataframe to get expect row count from

    Returns:
        int: The expected row count after performing minimum set of cleaning steps.
    """

    df = column_to_date(df, Keys.import_date, CQCLClean.cqc_location_import_date)
    df = cUtils.clean_provider_id_column(df)
    df = cUtils.impute_missing_values(
        df,
        [
            CQCLClean.provider_id,
            CQCLClean.regulated_activities_offered,
        ],
    )
    df = df.filter(
        pl.col(CQCLClean.type) == LocationType.social_care_identifier,
        raw_data_adjustments.is_valid_location(),
        pl.col(CQCLClean.registration_status) == RegistrationStatus.registered,
        pl.col(CQCLClean.provider_id).is_not_null(),
        pl.col(CQCLClean.regulated_activities_offered).is_not_null(),
    )
    df = cUtils.remove_specialist_colleges(df)
    row_count = df.height

    return row_count


def add_list_column_validation_check_flags(
    df: pl.DataFrame, columns: list[str]
) -> pl.DataFrame:
    """
    Adds a new boolean column 'column_validation_passed' indicating whether each list
    in the specified list-type column passes validation based on the following rules:
    - The list value may be null (null values are considered valid) if is
    - Non-null lists must not be empty
    - Non-null lists must not contain any None/null elements

    Additionally, this function adds a second boolean column
    'column_completeness_passed' that indicates whether the original column
    contains no null values (completeness check).

    After creating these validation columns, the original list column is dropped.

    Args:
        df (pl.DataFrame): Input Dataframe with complex columns
        columns (list[str]): The list of list-type columns to validate

    Returns:
        pl.DataFrame: DataFrame with a new bool validation passed column
    """
    expressions = []

    for col in columns:
        validation_expr = (
            (
                pl.col(col).is_null()
                | ((pl.col(col).list.len() > 0) & (~pl.col(col).list.contains(None)))
            )
            .cast(pl.Int64)
            .alias(f"{col}_has_no_empty_or_null")
        )

        completeness_expr = (
            pl.col(col).is_not_null().cast(pl.Int64).alias(f"{col}_is_not_null")
        )

        expressions.extend([validation_expr, completeness_expr])

    df_with_flags = df.with_columns(expressions)

    return df_with_flags.drop(columns)
