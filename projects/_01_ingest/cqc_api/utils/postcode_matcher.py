from pyspark.sql import DataFrame, Window, functions as F
from typing import Dict, Tuple

import utils.cleaning_utils as cUtils
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.cleaned_data_files.ons_cleaned import (
    OnsCleanedColumns as ONSClean,
)
from utils.cqc_location_dictionaries import InvalidPostcodes


def run_postcode_matching(
    locations_df: DataFrame,
    postcode_df: DataFrame,
    truncated_postcode_df: DataFrame,
) -> DataFrame:
    """
    Runs full postcode matching logic and raises error if final validation fails.

    This function consists of 5 iterations of matching postcodes:
        - 1 - Match postcodes where there is an exact match at that point in time.
        - 2 - If not, reassign unmatched potcode with the first successfully matched postcode for that location ID (where available).
        - 3 - (TO DO) If not, replace known postcode issues using the invalid postcode dictionary.
        - 4 - (TO DO) If not, match the postcode based on the first half of the postcode only (truncated postcode).
        - 5 - (TO DO) If not, raise an error to manually investigate any unmatched postcodes.

    If an error isn't raised, return a DataFrame with all of the matched postcodes from steps 1 to 4.

    Args:
        locations_df (DataFrame): DataFrame of workplaces with postcodes.
        postcode_df (DataFrame): ONS postcode directory.
        truncated_postcode_df (DataFrame): Truncated ONS postcode directory (last two characters of postcode removed).

    Returns:
        DataFrame: Fully matched DataFrame.
    """
    locations_df = clean_postcode_column(
        locations_df, CQCL.postal_code, CQCLClean.postcode_cleaned, drop_col=False
    )
    locations_df = truncate_postcode()  # add args

    postcode_df = clean_postcode_column(
        postcode_df, ONSClean.postcode, CQCLClean.postcode_cleaned, drop_col=True
    )
    truncated_postcode_df = clean_postcode_column(
        truncated_postcode_df,
        ONSClean.postcode,
        CQCLClean.postcode_truncated_cleaned,
        drop_col=True,
    )

    locations_df = cUtils.add_aligned_date_column(
        locations_df,
        postcode_df,
        CQCLClean.cqc_location_import_date,
        ONSClean.contemporary_ons_import_date,
    )

    # Step 1 - Match postcodes where there is an exact match at that point in time.
    original_matched_df, original_unmatched_df = join_postcode_data(
        locations_df, postcode_df, CQCLClean.postcode_cleaned
    )

    # Step 2 - Reassign unmatched potcode with the first successfully matched postcode for that location ID (where available).
    reassigned_df = get_first_successful_postcode_match(
        original_unmatched_df, original_matched_df
    )
    reassigned_matched_df, reassigned_unmatched_df = join_postcode_data(
        reassigned_df, postcode_df, CQCLClean.postcode_cleaned
    )

    # Step 3 - Replace known postcode issues using the invalid postcode dictionary.
    amended_postcodes_df = amend_invalid_postcodes(reassigned_unmatched_df)
    amended_matched_df, amended_unmatched_df = join_postcode_data(
        amended_postcodes_df, postcode_df, CQCLClean.postcode_cleaned
    )

    # TODO - Step 4 - Match the postcode based on the first half of the postcode only (truncated postcode).

    # Step 5 - Raise an error to manually investigate any unmatched postcodes.
    raise_error_if_unmatched(final_matched_df, postcode_df.columns[1])

    # Step 6 - Create a final DataFrame with all matched postcodes.
    final_matched_df = (
        original_matched_df.unionByName(reassigned_matched_df).unionByName(
            amended_matched_df
        )
        # .unionByName(truncated_match)
    )

    return final_matched_df


# this function exists already, called normalise_column_values
# remove the function once this is done.
def clean_postcode_column(
    df: DataFrame, postcode_col: str, cleaned_col_name: str, drop_col: bool
) -> DataFrame:
    """
    Creates a clean version of the postcode column, in upper case with spaces removed.

    Args:
        df (DataFrame): CQC locations DataFrame with the postcode column in.
        postcode_col (str): The name of the postcode column.
        cleaned_col_name (str): The name of the cleaned postcode column.
        drop_col (bool): Drop the original column if True, otherwise keep the original column.

    Returns:
        DataFrame: A cleaned postcode column in upper case and blank spaces removed.
    """
    df = df.withColumn(
        cleaned_col_name,
        F.upper(F.regexp_replace(F.col(postcode_col), " ", "")),
    )
    if drop_col:
        df = df.drop(postcode_col)
    return df


def join_postcode_data(
    locations_df: DataFrame,
    postcode_df: DataFrame,
    postcode_col: str,
) -> Tuple[DataFrame, DataFrame]:
    """
    Joins postcode data into the locations DataFrame based on matching postcodes.

    A successful join is determined by the presence of a non-null value in the
    current_cssr column of the ONS postcode directory.

    Args:
        locations_df (DataFrame): Workplace DataFrame with a postcode column.
        postcode_df (DataFrame): ONS Postcode directory DataFrame.
        postcode_col (str): Name of the postcode column to join on.

    Returns:
        Tuple[DataFrame, DataFrame]: Matched and unmatched DataFrames.
    """
    joined_df = locations_df.join(
        postcode_df,
        [ONSClean.contemporary_ons_import_date, postcode_col],
        "left",
    )
    matched_df = joined_df.filter(F.col(ONSClean.current_cssr).isNotNull())

    unmatched_df = joined_df.filter(F.col(ONSClean.current_cssr).isNull())
    unmatched_df = unmatched_df.select(*locations_df.columns)

    return matched_df, unmatched_df


def get_first_successful_postcode_match(
    unmatched_df: DataFrame,
    matched_df: DataFrame,
) -> DataFrame:
    """
    Replace unmatched postcodes with the earliest successfully matched postcode for that location_id.

    Incorrectly matched postcodes are often corrected in the CQC database over time.
    Whilst newly entered postcodes will never be reassigned by this method, locations who have
    updated their postcodes to one which now matches the ONS postcode directory will have their
    historical unmatched postcodes reassigned to their first successfully matched one.

    Args:
        unmatched_df (DataFrame): Unmatched workplaces DataFrame.
        matched_df (DataFrame): Matched workplaces DataFrame.

    Returns:
        DataFrame: Repaired DataFrame with postcodes reassigned from historical data.
    """
    window_spec = Window.partitionBy(CQCLClean.location_id).orderBy(
        CQCLClean.cqc_location_import_date
    )
    row_number: str = "row_number"
    successfully_matched_postcode: str = "successfully_matched_postcode"

    first_matched_df = (
        matched_df.select(
            CQCLClean.location_id,
            CQCLClean.postcode_cleaned,
            CQCLClean.cqc_location_import_date,
        )
        .withColumn(row_number, F.row_number().over(window_spec))
        .filter(F.col(row_number) == 1)
        .withColumnRenamed(CQCLClean.postcode_cleaned, successfully_matched_postcode)
        .drop(row_number, CQCLClean.cqc_location_import_date)
    )

    reassigned_df = unmatched_df.join(first_matched_df, CQCLClean.location_id, "left")

    reassigned_df = reassigned_df.withColumn(
        CQCLClean.postcode_cleaned,
        F.when(
            F.col(successfully_matched_postcode).isNotNull(),
            F.col(successfully_matched_postcode),
        ).otherwise(F.col(CQCLClean.postcode_cleaned)),
    ).drop(successfully_matched_postcode)

    return reassigned_df


def amend_invalid_postcodes(df: DataFrame) -> DataFrame:
    """
    Replace invalid postcodes in the DataFrame using a predefined mapping.

    This function uses a dictionary of known invalid postcodes and their corrections.
    If a postcode in the input DataFrame matches a key in the mapping, it is replaced
    with the corresponding corrected postcode. Otherwise, the original postcode is retained.

    Args:
        df (DataFrame): Input DataFrame containing a column of postcodes.

    Returns:
        DataFrame: A new DataFrame with amended postcodes.
    """
    # post_codes_mapping = InvalidPostcodes.invalid_postcodes_map

    # map_func = F.udf(lambda row: post_codes_mapping.get(row, row))
    # df = df.withColumn(CQCL.postal_code, map_func(F.col(CQCL.postal_code)))
    # return df
    mapping_dict: Dict[str, str] = InvalidPostcodes.invalid_postcodes_map

    mapping_expr = F.create_map([F.lit(x) for kv in mapping_dict.items() for x in kv])

    df = df.withColumn(
        CQCL.postal_code,
        F.coalesce(
            mapping_expr.getItem(F.col(CQCL.postal_code)), F.col(CQCL.postal_code)
        ),
    )
    return df


def raise_error_if_unmatched(df: DataFrame) -> None:
    """
    Raise error if there are any unmatched postcodes left.

    Args:
        df (DataFrame): Final DataFrame.

    Raises:
        TypeError: If unmatched postcodes exist.
    """
    unmatched = df.filter(F.col(CQCL.postal_code).isNull())
    if not unmatched.rdd.isEmpty():
        rows = unmatched.select(CQCL.location_id, CQCL.postal_code).distinct().collect()
        errors = [(r[CQCL.location_id], r[CQCL.postal_code]) for r in rows]
        raise TypeError(f"Unmatched postcodes found: {errors}")
