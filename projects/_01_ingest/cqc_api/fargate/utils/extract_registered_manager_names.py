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
    grouped_registered_manager_names_df = group_and_collect_names(contact_names_df)
    df_with_reg_man_names = join_names_column_into_original_df(
        df, grouped_registered_manager_names_df
    )
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


def group_and_collect_names(df: pl.DataFrame) -> pl.DataFrame:
    """
    Groups the DataFrame by location and import date, collecting unique registered manager names.

    Args:
        df (pl.DataFrame): DataFrame with `contacts_full_name` column.

    Returns:
        pl.DataFrame: DataFrame grouped by `location_id` and `cqc_location_import_date` with a new
        column `registered_manager_names` containing unique full names as a list.
    """
    df = df.group_by([CQCLClean.location_id, CQCLClean.cqc_location_import_date]).agg(
        pl.col(CQCLClean.contacts_full_name)
        .unique()
        .alias(CQCLClean.registered_manager_names)
    )
    return df


def join_names_column_into_original_df(
    df: pl.DataFrame, registered_manager_names_df: pl.DataFrame
) -> pl.DataFrame:
    """
    Joins the registered manager names column back into the original DataFrame.

    Args:
        df (pl.DataFrame): Original DataFrame.
        registered_manager_names_df (pl.DataFrame): DataFrame containing
            `location_id`, `cqc_location_import_date`, and `registered_manager_names`.

    Returns:
        pl.DataFrame: Original DataFrame enriched with the `registered_manager_names` column.
    """
    df = df.join(
        registered_manager_names_df,
        on=[CQCLClean.location_id, CQCLClean.cqc_location_import_date],
        how="left",
    )
    return df
