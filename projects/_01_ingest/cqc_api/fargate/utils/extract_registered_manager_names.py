import polars as pl

from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)


def extract_registered_manager_names(df: pl.DataFrame) -> pl.DataFrame:
    """
    Extracts and deduplicates registered manager names from regulated activities.

    This function orchestrates the extraction process by
    - flattening the contacts from `imputed_regulated_activities`
    - creating full names for each contact
    - deduplicating the names.
    - dropping the intermediate flattened contacts column.

    Args:
        df (pl.DataFrame): Input DataFrame containing the `imputed_regulated_activities` column.

    Returns:
        pl.DataFrame: DataFrame with a new column for the deduplicated list of
            registered manager names.
    """
    df = extract_contacts(df)
    df = create_registered_manager_names(df)
    df = df.drop(CQCLClean.all_contacts_flat)
    return df


def extract_contacts(df: pl.DataFrame) -> pl.DataFrame:
    """
    Flattens the contacts from the regulated activities column.

    This function:
    - extracts all contacts from the nested regulated activities array (`all_contacts`)
    - concatenates nested lists of contacts into a single list (`all_contacts_flat`)
    - drops the intermediate `all_contacts` column

    Args:
        df (pl.DataFrame): Input DataFrame containing the `imputed_regulated_activities` column.

    Returns:
        pl.DataFrame: DataFrame with the new `all_contacts_flat` column.
    """
    df = (
        df.with_columns(
            pl.col(CQCLClean.imputed_regulated_activities)
            .list.eval(pl.element().struct.field(CQCLClean.contacts))
            .alias(CQCLClean.all_contacts)
        )
        .with_columns(
            pl.col(CQCLClean.all_contacts)
            .list.concat()
            .alias(CQCLClean.all_contacts_flat)
        )
        .drop(CQCLClean.all_contacts)
    )

    return df


def create_registered_manager_names(df: pl.DataFrame) -> pl.DataFrame:
    """
    Creates full names for all contacts and deduplicates them.

    This function:
    - concatenates the `person_given_name` and `person_family_name` fields from the
      flattened contacts list to create a `registered_manager_names` list per row.
    - removes duplicate names within each list.

    Args:
        df (pl.DataFrame): Input DataFrame containing the `all_contacts_flat` column.

    Returns:
        pl.DataFrame: DataFrame with a new column for the deduplicated list of
            registered manager names.
    """
    df = df.with_columns(
        pl.col(CQCLClean.all_contacts_flat)
        .list.eval(
            pl.element().struct.field(CQCLClean.person_given_name)
            + pl.lit(" ")
            + pl.element().struct.field(CQCLClean.person_family_name)
        )
        .alias(CQCLClean.registered_manager_names)
    ).with_columns(pl.col(CQCLClean.registered_manager_names).list.unique())

    return df
