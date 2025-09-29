import polars as pl

from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)


def extract_registered_manager_names(df: pl.DataFrame) -> pl.DataFrame:
    """
    Extracts deduplicated registered manager names from the regulated activities column.

    This function:
    - Explodes the regulated activities and contacts arrays.
    - Concatenates given and family names into a full name.
    - Collects unique names per location and import date.
    - Joins the result back into the original DataFrame.

    Args:
        df (pl.DataFrame): Input Polars DataFrame containing `imputed_regulated_activities` array.

    Returns:
        pl.DataFrame: DataFrame with a new column `registered_manager_names` containing unique full names.
    """
    exploded_contacts_df = explode_contacts_information(df)
    contact_names_df = select_and_create_full_name(exploded_contacts_df)
    df_with_reg_man_names = add_registered_manager_names(df, contact_names_df)
    return df_with_reg_man_names


def explode_contacts_information(df: pl.DataFrame) -> pl.DataFrame:
    """
    Explodes the `imputed_regulated_activities` array and then the `contacts` array.

    Args:
        df (pl.DataFrame): Input Polars DataFrame with `imputed_regulated_activities`.

    Returns:
        pl.DataFrame: DataFrame with each contact exploded into its own row in `contacts_exploded`.
    """
    df = df.select(
        [
            CQCLClean.location_id,
            CQCLClean.cqc_location_import_date,
            CQCLClean.imputed_regulated_activities,
        ]
    )

    # Explode regulated activities
    df = df.explode(CQCLClean.imputed_regulated_activities)

    # Explode contacts inside each regulated activity
    df = df.with_columns(
        pl.col(CQCLClean.imputed_regulated_activities)
        .struct.field(CQCLClean.contacts)
        .alias(CQCLClean.contacts_exploded)
    ).explode(CQCLClean.contacts_exploded)

    return df


def select_and_create_full_name(df: pl.DataFrame) -> pl.DataFrame:
    """
    Selects relevant columns and creates a full name column by concatenating given and family names.

    Args:
        df (pl.DataFrame): DataFrame with exploded `contacts_exploded` struct column.

    Returns:
        pl.DataFrame: DataFrame containing:
            - `location_id`
            - `cqc_location_import_date`
            - `contacts_full_name`: full name of the contact
    """
    full_name_col = pl.concat_str(
        [
            pl.col(CQCLClean.contacts_exploded).struct.field(
                CQCLClean.person_given_name
            ),
            pl.lit(" "),
            pl.col(CQCLClean.contacts_exploded).struct.field(
                CQCLClean.person_family_name
            ),
        ]
    ).alias(CQCLClean.contacts_full_name)

    df = df.select(
        [CQCLClean.location_id, CQCLClean.cqc_location_import_date, full_name_col]
    )
    return df


def add_registered_manager_names(
    df: pl.DataFrame, contact_names_df: pl.DataFrame
) -> pl.DataFrame:
    """
    Adds a column of registered manager names to the original DataFrame.

    This function:
    - Groups `contact_names_df` by `location_id` and `cqc_location_import_date`.
    - Collects unique values from `contacts_full_name` into a grouped list of names.
    - Joins the grouped names back into the original DataFrame (`df`).

    Args:
        df (pl.DataFrame): Original DataFrame containing `location_id` and
            `cqc_location_import_date`.
        contact_names_df  (pl.DataFrame): DataFrame containing the `contacts_full_name`,
            `location_id` and `cqc_location_import_date` columns.

    Returns:
        pl.DataFrame: Original DataFrame with an added `registered_manager_names` column
        containing a list of unique manager names.
    """
    grouped_df = contact_names_df.group_by(
        [CQCLClean.location_id, CQCLClean.cqc_location_import_date]
    ).agg(
        pl.col(CQCLClean.contacts_full_name)
        .unique()
        .alias(CQCLClean.registered_manager_names)
    )

    return df.join(
        grouped_df,
        on=[CQCLClean.location_id, CQCLClean.cqc_location_import_date],
        how="left",
    )
