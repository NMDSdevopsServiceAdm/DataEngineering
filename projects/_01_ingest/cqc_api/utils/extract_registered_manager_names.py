from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)


def extract_registered_manager_names(df: DataFrame) -> DataFrame:
    """
    Extracts registered manager names from the regulated activities column.

    The CQC requires a registered manager for each regulated activity at a location.
    The regulated activities column contains an array of contacts for each activity the location offers.
    This function extracts the names for all contacts (Registered Managers) and adds them into an array column.
    Registered manager names are deduplicated in the array so each name will only appear once in the array.

    Args:
        df (DataFrame): Input DataFrame with imputed_regulated_activities column.

    Returns:
        DataFrame: DataFrame with deduplicated registered manager names in a new column.
    """
    exploded_contacts_df = extract_contacts_information(df)
    contact_names_df = select_and_create_full_name(exploded_contacts_df)
    grouped_registered_manager_names_df = group_and_collect_names(contact_names_df)
    df_with_reg_man_names = join_names_column_into_original_df(
        df, grouped_registered_manager_names_df
    )

    return df_with_reg_man_names


def extract_contacts_information(
    df: DataFrame,
) -> DataFrame:
    """
    Explodes the imputed_regulated_activities array and then the contacts array in the DataFrame.

    Args:
        df (DataFrame): Input DataFrame with imputed_regulated_activities array.

    Returns:
        DataFrame: DataFrame with exploded contacts information.
    """
    df = df.select(
        CQCLClean.location_id,
        CQCLClean.cqc_location_import_date,
        CQCLClean.imputed_regulated_activities,
    )
    exploded_activities_df = df.withColumn(
        CQCLClean.imputed_regulated_activities_exploded,
        F.explode(CQCLClean.imputed_regulated_activities),
    )
    exploded_contacts_df = exploded_activities_df.withColumn(
        CQCLClean.contacts_exploded,
        F.explode(
            exploded_activities_df[CQCLClean.imputed_regulated_activities_exploded][
                CQCLClean.contacts
            ]
        ),
    )
    exploded_contacts_df = exploded_contacts_df.drop(
        CQCLClean.imputed_regulated_activities_exploded
    )
    return exploded_contacts_df


def select_and_create_full_name(df: DataFrame) -> DataFrame:
    """
    Selects relevant columns and creates a full name column by concatenating given and family names.

    Args:
        df (DataFrame): Input DataFrame with exploded contacts information.

    Returns:
        DataFrame: DataFrame with selected columns and full name column.
    """
    given_name: str = df[CQCLClean.contacts_exploded][CQCLClean.person_given_name]
    family_name: str = df[CQCLClean.contacts_exploded][CQCLClean.person_family_name]
    full_name: str = CQCLClean.contacts_full_name

    df = df.select(
        df[CQCLClean.location_id],
        df[CQCLClean.cqc_location_import_date],
        F.concat(given_name, F.lit(" "), family_name).alias(full_name),
    )
    return df


def group_and_collect_names(df: DataFrame) -> DataFrame:
    """
    Groups the DataFrame by location_id and cqc_location_import_date, and collects all the unique names into an array column.

    Args:
        df (DataFrame): Filtered DataFrame.

    Returns:
        DataFrame: Grouped DataFrame with unique registered manager names at each location and time period.
    """
    df = df.groupBy(CQCLClean.location_id, CQCLClean.cqc_location_import_date).agg(
        F.collect_set(CQCLClean.contacts_full_name).alias(
            CQCLClean.registered_manager_names
        )
    )
    return df


def join_names_column_into_original_df(
    df: DataFrame, registered_manager_names_df: DataFrame
) -> DataFrame:
    """
    Joins the DataFrame with the registered manager names column into the original DataFrame.

    Args:
        df (DataFrame): Original DataFrame.
        registered_manager_names_df (DataFrame): Grouped DataFrame with registered manager names column.

    Returns:
        DataFrame: Original DataFrame with registered manager names column joined in.
    """
    df = df.join(
        registered_manager_names_df,
        [CQCLClean.location_id, CQCLClean.cqc_location_import_date],
        "left",
    )
    return df
