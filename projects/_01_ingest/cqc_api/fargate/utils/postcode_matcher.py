from typing import Dict, List, Tuple

import polars as pl

import polars_utils.cleaning_utils as cUtils
from projects._01_ingest.cqc_api.utils.postcode_replacement_dictionary import (
    ManualPostcodeCorrections,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_names.cleaned_data_files.ons_cleaned import (
    OnsCleanedColumns as ONSClean,
)


def run_postcode_matching(
    locations_lf: pl.LazyFrame,
    postcode_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Runs full postcode matching logic and raises error if final validation fails.

    This function consists of 5 iterations of matching postcodes:
        - 1 - Match postcodes where there is an exact match at that point in time.
        - 2 - If not, reassign unmatched postcode with the first successfully matched postcode for that location ID (where available).
        - 3 - If not, replace known postcode issues using the invalid postcode dictionary.
        - 4 - If not, match the postcode based on the first half of the postcode only (truncated postcode).
        - 5 - If not, raise an error to manually investigate any unmatched postcodes.

    If an error isn't raised, return a LazyFrame with all of the matched postcodes from steps 1 to 4.

    Args:
        locations_lf (pl.LazyFrame): LazyFrame of workplaces with postcodes.
        postcode_lf (pl.LazyFrame): ONS postcode directory.

    Returns:
        pl.LazyFrame: Fully matched LazyFrame.
    """
    locations_lf = clean_postcode_column(
        locations_lf, CQCL.postal_code, CQCLClean.postcode_cleaned, drop_col=False
    )

    postcode_lf = clean_postcode_column(
        postcode_lf, ONSClean.postcode, CQCLClean.postcode_cleaned, drop_col=True
    )

    locations_lf = cUtils.add_aligned_date_column(
        locations_lf,
        postcode_lf,
        CQCLClean.cqc_location_import_date,
        ONSClean.contemporary_ons_import_date,
    )

    # Step 1 - Match postcodes where there is an exact match at that point in time.
    matched_locations_lf, unmatched_locations_lf = join_postcode_data(
        locations_lf, postcode_lf, CQCLClean.postcode_cleaned
    )

    # Step 2 - Reassign unmatched postcode with the first successfully matched postcode for that location ID (where available).
    reassigned_locations_lf = get_first_successful_postcode_match(
        unmatched_locations_lf, matched_locations_lf
    )
    (
        matched_reassigned_locations_lf,
        unmatched_reassigned_locations_lf,
    ) = join_postcode_data(
        reassigned_locations_lf, postcode_lf, CQCLClean.postcode_cleaned
    )

    # Step 3 - Replace known postcode issues using the invalid postcode dictionary.
    amended_locations_lf = amend_invalid_postcodes(unmatched_reassigned_locations_lf)
    matched_amended_locations_lf, unmatched_amended_locations_lf = join_postcode_data(
        amended_locations_lf, postcode_lf, CQCLClean.postcode_cleaned
    )

    # Step 4 - Match the postcode based on the truncated postcode (excludes the last two characters).
    truncated_postcode_lf = create_truncated_postcode_lf(postcode_lf)
    truncated_locations_lf = truncate_postcode(unmatched_amended_locations_lf)
    (
        matched_truncated_locations_lf,
        unmatched_truncated_locations_lf,
    ) = join_postcode_data(
        truncated_locations_lf, truncated_postcode_lf, CQCLClean.postcode_truncated
    )

    # Step 5 - Raise an error and abort pipeline to manually investigate any unmatched postcodes.
    raise_error_if_unmatched(unmatched_truncated_locations_lf)

    # Step 6 - Create a final LazyFrame with all matched postcodes.
    final_matched_lf = combine_matched_dataframes(
        [
            matched_locations_lf,
            matched_reassigned_locations_lf,
            matched_amended_locations_lf,
            matched_truncated_locations_lf,
        ]
    )

    return final_matched_lf


def clean_postcode_column(
    lf: pl.LazyFrame, postcode_col: str, cleaned_col_name: str, drop_col: bool
) -> pl.LazyFrame:
    """
    Creates a clean version of the postcode column, in upper case with spaces removed.

    Args:
        lf (pl.LazyFrame): CQC locations LazyFrame with the postcode column in.
        postcode_col (str): The name of the postcode column.
        cleaned_col_name (str): The name of the cleaned postcode column.
        drop_col (bool): Drop the original column if True, otherwise keep the original column.

    Returns:
        pl.LazyFrame: A cleaned postcode column in upper case and blank spaces removed.
    """
    lf = lf.with_columns(
        pl.col(postcode_col)
        .str.replace_all(" ", "")
        .str.to_uppercase()
        .alias(cleaned_col_name)
    )

    if drop_col:
        lf = lf.drop(postcode_col)
    return lf


def join_postcode_data(
    locations_lf: pl.LazyFrame,
    postcode_lf: pl.LazyFrame,
    postcode_col: str,
) -> Tuple[pl.LazyFrame, pl.LazyFrame]:
    """
    Joins postcode data into the locations LazyFrame based on matching postcodes.

    A successful join is determined by the presence of a non-null value in the
    current_cssr column of the ONS postcode directory.

    Args:
        locations_lf (pl.LazyFrame): Workplace LazyFrame with a postcode column.
        postcode_lf (pl.LazyFrame): ONS Postcode directory LazyFrame.
        postcode_col (str): Name of the postcode column to join on.

    Returns:
        Tuple[pl.LazyFrame, pl.LazyFrame]: Matched and unmatched LazyFrames.
    """
    joined_lf = locations_lf.join(
        postcode_lf,
        [ONSClean.contemporary_ons_import_date, postcode_col],
        "left",
    )
    matched_lf = joined_lf.filter(pl.col(ONSClean.current_cssr).is_not_null())

    unmatched_lf = joined_lf.filter(pl.col(ONSClean.current_cssr).is_null())
    unmatched_lf = unmatched_lf.select(*locations_lf.columns)

    return matched_lf, unmatched_lf


def get_first_successful_postcode_match(
    unmatched_lf: pl.LazyFrame,
    matched_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Replace unmatched postcodes with the earliest successfully matched postcode for that location_id.

    Incorrectly matched postcodes are often corrected in the CQC database over time.
    Whilst newly entered postcodes will never be reassigned by this method, locations who have
    updated their postcodes to one which now matches the ONS postcode directory will have their
    historical unmatched postcodes reassigned to their first successfully matched one.

    Args:
        unmatched_lf (pl.LazyFrame): Unmatched workplaces LazyFrame.
        matched_lf (pl.LazyFrame): Matched workplaces LazyFrame.

    Returns:
        pl.LazyFrame: Repaired LazyFrame with postcodes reassigned from historical data.
    """
    row_number: str = "row_number"
    successfully_matched_postcode: str = "successfully_matched_postcode"

    first_matched_lf = (
        matched_lf.select(
            CQCLClean.location_id,
            CQCLClean.postcode_cleaned,
            CQCLClean.cqc_location_import_date,
        )
        .with_columns(
            pl.row_index(row_number)  # Note. Warning in documentation for row_index.
            .over(CQCLClean.location_id)
            .sort_by(CQCLClean.cqc_location_import_date),
        )
        .filter(pl.col(row_number) == 0)
        .rename({CQCLClean.postcode_cleaned: successfully_matched_postcode})
        .drop(row_number, CQCLClean.cqc_location_import_date)
    )

    reassigned_lf = unmatched_lf.join(first_matched_lf, CQCLClean.location_id, "left")

    reassigned_lf = reassigned_lf.with_columns(
        pl.when(pl.col(successfully_matched_postcode).is_not_null())
        .then(pl.col(successfully_matched_postcode))
        .otherwise(pl.col(CQCLClean.postcode_cleaned))
        .alias(CQCLClean.postcode_cleaned)
    ).drop(successfully_matched_postcode)

    return reassigned_lf


def amend_invalid_postcodes(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Replace invalid postcodes in the LazyFrame using a predefined mapping.

    This function uses a dictionary of known invalid postcodes and their corrections.
    If a postcode in the input LazyFrame matches a key in the mapping, it is replaced
    with the corresponding corrected postcode. Otherwise, the original postcode is retained.

    Args:
        lf (pl.LazyFrame): Input LazyFrame containing a column of postcodes.

    Returns:
        pl.LazyFrame: A new LazyFrame with amended postcodes.
    """
    mapping_dict: Dict[str, str] = ManualPostcodeCorrections.postcode_corrections_dict

    lf = lf.with_columns(pl.col(CQCLClean.postcode_cleaned).replace(mapping_dict))
    return lf


def truncate_postcode(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Creates a new column which has the last 2 characters of the postcode cleaned column removed

    Args:
        lf (pl.LazyFrame): A LazyFrame containing the full postcode.

    Returns:
        pl.LazyFrame: LazyFrame with the truncated postcode added.
    """
    string_length_exp = pl.col(CQCLClean.postcode_cleaned).str.len_bytes() - 2

    return lf.with_columns(
        pl.col(CQCLClean.postcode_cleaned)
        .str.slice(0, string_length_exp)
        .alias(CQCLClean.postcode_truncated)
    )


def create_truncated_postcode_lf(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Generates a LazyFrame containing one representative row for each truncated postcode.

    This function performs the following steps:
        - 1 - Truncates postcodes by removing the final two characters.
        - 2 - Groups by truncated postcode and a set of geography columns to count frequency.
        - 3 - Identifies the most common combination for each truncated postcode.
        - 4 - Filters the LazyFrame to keep only the first row for each most common combination.

    Args:
        lf (pl.LazyFrame): Input LazyFrame containing cleaned postcodes and geography columns.

    Returns:
        pl.LazyFrame: Filtered LazyFrame containing one representative row per truncated postcode,
        with the most frequently occurring combination of geography fields.
    """
    count_col = "count"
    rank_col = "rank"

    grouping_cols = [
        ONSClean.contemporary_cssr,
        ONSClean.contemporary_sub_icb,
        ONSClean.contemporary_ccg,
        ONSClean.current_cssr,
        ONSClean.current_sub_icb,
    ]

    lf = truncate_postcode(lf)

    count_over = [
        CQCLClean.postcode_truncated,
        ONSClean.contemporary_ons_import_date,
    ] + grouping_cols
    lf = lf.with_columns(pl.len().over(count_over).alias(count_col))

    rank_over = [CQCLClean.postcode_truncated, ONSClean.contemporary_ons_import_date]
    lf = lf.sort(
        by=[count_col] + grouping_cols,
        descending=[True, False, False, False, False, False],
    )

    lf = lf.with_columns(pl.cum_count(count_col).over(rank_over).alias(rank_col))

    lf = lf.filter(pl.col(rank_col) == 1).drop(
        rank_col, count_col, CQCLClean.postcode_cleaned
    )

    return lf


def raise_error_if_unmatched(unmatched_lf: pl.LazyFrame) -> None:
    """
    Raise error if there are any unmatched postcodes left.

    If unmatched_lf is empty then all postcodes have been matched.
    If there is data in unmatched_lf, the pipeline will fail and the
    location ID, name and postcode will be printed.

    Args:
        unmatched_lf (pl.LazyFrame): LazyFrame containing any remaining unmatched locations (if there are any).

    Raises:
        TypeError: If unmatched postcodes exist.
    """
    if unmatched_lf.collect().is_empty():
        return

    rows = (
        unmatched_lf.select(
            CQCL.location_id,
            CQCL.name,
            CQCL.postal_address_line1,
            CQCLClean.postcode_cleaned,
        )
        .unique()
        .sort(CQCLClean.postcode_cleaned)
        .collect()
        .to_dicts()
    )
    errors = [
        (
            r[CQCL.location_id],
            r[CQCL.name],
            r[CQCL.postal_address_line1],
            r[CQCLClean.postcode_cleaned],
        )
        for r in rows
    ]
    raise TypeError(f"Unmatched postcodes found: {errors}")


def combine_matched_dataframes(dataframes: List[pl.LazyFrame]) -> pl.LazyFrame:
    """
    Combines a list of LazyFrames using unionByName.

    allowMissingColumns is set to True to account for some LazyFrames having additional columns.

    Args:
        dataframes (List[pl.LazyFrame]): List of LazyFrames to combine.

    Returns:
        pl.LazyFrame: Unified LazyFrame with all rows.
    """
    return pl.concat(dataframes, how="diagonal")
