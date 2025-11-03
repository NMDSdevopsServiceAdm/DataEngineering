import polars as pl

from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)


def extract_registered_manager_names(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Extracts deduplicated registered manager names from the regulated activities column.

    This function:
    - Explodes the regulated activities and contacts arrays.
    - Concatenates given and family names into a full name.
    - Adds a list of unique names per location and import date into the original LazyFrame.

    Args:
        lf (pl.LazyFrame): Input Polars LazyFrame containing `imputed_regulated_activities` array.

    Returns:
        pl.LazyFrame: LazyFrame with a new column `registered_manager_names` containing unique full names.
    """
    exploded_contacts_lf = explode_contacts_information(lf)
    contact_names_lf = select_and_create_full_name(exploded_contacts_lf)
    lf_with_reg_man_names = add_registered_manager_names(lf, contact_names_lf)
    return lf_with_reg_man_names


def explode_contacts_information(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Explodes the `regulated_activities` array and then the `contacts` array.

    Only rows with non-null contacts are retained.

    Args:
        lf (pl.LazyFrame): Input Polars LazyFrame with `regulated_activities`.

    Returns:
        pl.LazyFrame: LazyFrame with each contact exploded into its own row in `contacts_exploded`.
    """
    lf = lf.select(
        [
            CQCLClean.location_id,
            CQCLClean.cqc_location_import_date,
            CQCLClean.regulated_activities,
        ]
    )

    # Explode regulated activities
    lf = lf.explode(CQCLClean.regulated_activities)

    # Explode contacts inside each regulated activity
    lf = lf.with_columns(
        pl.col(CQCLClean.regulated_activities)
        .struct.field(CQCLClean.contacts)
        .alias(CQCLClean.contacts_exploded)
    ).explode(CQCLClean.contacts_exploded)

    lf = lf.filter(pl.col(CQCLClean.contacts_exploded).is_not_null())

    return lf


def select_and_create_full_name(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Selects relevant columns and creates a full name column by concatenating given and family names.

    Args:
        lf (pl.LazyFrame): LazyFrame with exploded `contacts_exploded` struct column.

    Returns:
        pl.LazyFrame: LazyFrame containing:
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

    lf = lf.select(
        [CQCLClean.location_id, CQCLClean.cqc_location_import_date, full_name_col]
    )
    return lf


def add_registered_manager_names(
    lf: pl.LazyFrame, contact_names_lf: pl.LazyFrame
) -> pl.LazyFrame:
    """
    Adds a column of registered manager names to the original LazyFrame.

    This function:
    - Groups `contact_names_lf` by `location_id` and `cqc_location_import_date`.
    - Collects unique values from `contacts_full_name` into a grouped list of names.
    - Joins the grouped names back into the original LazyFrame (`lf`).

    Args:
        lf (pl.LazyFrame): Original LazyFrame containing `location_id` and
            `cqc_location_import_date`.
        contact_names_lf  (pl.LazyFrame): LazyFrame containing the `contacts_full_name`,
            `location_id` and `cqc_location_import_date` columns.

    Returns:
        pl.LazyFrame: Original LazyFrame with an added `registered_manager_names` column
        containing a list of unique manager names.
    """
    join_keys: list[str] = [CQCLClean.location_id, CQCLClean.cqc_location_import_date]

    grouped_lf = contact_names_lf.group_by(join_keys).agg(
        pl.col(CQCLClean.contacts_full_name)
        .unique()
        .sort()
        .alias(CQCLClean.registered_manager_names)
    )

    return lf.join(grouped_lf, on=join_keys, how="left")
