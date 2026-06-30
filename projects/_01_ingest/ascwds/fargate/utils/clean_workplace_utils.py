import polars as pl

from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)

job_role_cols = [
    value
    for role, value in vars(AWPClean()).items()
    if value.startswith("jr") and "flag" not in value
]

# Organisation IDs used internally by Skills for Care for testing purposes.
# These organisations do not represent real workplaces and are excluded from
# downstream processing.
TEST_ACCOUNTS: set[str] = {
    "305",
    "307",
    "308",
    "309",
    "310",
    "2452",
    "28470",
    "26792",
    "31657",
    "31138",
    "51818",
}

# Establishment IDs known to contain duplicated ASC-WDS submissions.
#
# These records represent the same workplace data uploaded against multiple
# accounts. The issue was raised with the support team but there is no way of
# automatically blocking this going forwards.
#
# There are two sets of workplaces here who submitted the exact same ASCWDS
# files on the same day.
# - Four locations ("48904" to "49968")
# - 18 locations ("50538" to "50870").
DUPLICATE_ESTABLISHMENT_IDS: set[str] = {
    "48904",
    "49966",
    "49967",
    "49968",
    "50538",
    "50561",
    "50590",
    "50596",
    "50598",
    "50621",
    "50623",
    "50624",
    "50627",
    "50629",
    "50639",
    "50640",
    "50767",
    "50769",
    "50770",
    "50771",
    "50869",
    "50870",
}


def valid_workplace_filter() -> pl.Expr:
    """
    Return a filter expression that excludes known invalid workplace records.

    Removes:
        - Internal Skills for Care test organisations.
        - Known duplicate workplace submissions.

    Returns:
        pl.Expr: A Polars expression that can be used to filter a LazyFrame.
    """
    return (
        # exclude test accounts
        ~pl.col(AWPClean.organisation_id).is_in(TEST_ACCOUNTS)
        &
        # exclude duplicate establishments
        ~pl.col(AWPClean.establishment_id).is_in(DUPLICATE_ESTABLISHMENT_IDS)
    )


def merge_job_role_columns(
    lf: pl.LazyFrame, job_role_mapping: dict[str, list[str]], suffix_list: list[str]
) -> pl.LazyFrame:
    """
    Merge legacy job roles into current job roles.

    Over time, the ASC-WDS has stopped collecting specific roles.
    We have decided to sum the values for these legacy roles into
    specific current job roles.
    Null is returned when all contributing columns are null.
    Job role 33 is personal assistant which is dropped from the dataset
    without merging their data into another role.

    Args:
        lf (pl.LazyFrame): Input workplace LazyFrame with legacy job role columns.
        job_role_mapping (dict[str, list[str]]): A dict in which keys are
            current job roles and values are legacy job roles to be merged
            into the key.
        suffix_list (list[str]): A list of workplace job role column suffixes.

    Returns:
        pl.LazyFrame: Input LazyFrame with only current job role columns.
    """
    # Construct a mapping dict for each job role and employment status:
    # {
    #     current role perm: [Current role perm and legacy roles perm to merge into current role],
    #     current role temp: [Current role temp and legacy roles temp to merge into current role],
    #     ...
    # }
    legacy_role_mapping = {}
    for role in job_role_cols:
        for current_role, legacy_roles in job_role_mapping.items():
            for suffix in suffix_list:
                if role == f"{current_role}{suffix}":
                    legacy_role_mapping[role] = [
                        role,
                        *[f"jr{i}{suffix}" for i in legacy_roles],
                    ]
                    break

    # Construct a sum expression in which columns are coalesced with zero (to prevent null + value = null)
    # then when any of the columns in the sum are not null, sum them and assign to a current role.
    # Otherwise current role stays as null.
    expr = []
    for current_role, legacy_roles in legacy_role_mapping.items():
        expr.append(
            (
                pl.when(
                    pl.all_horizontal([pl.col(col).is_null() for col in legacy_roles])
                )
                .then(None)
                .otherwise(pl.sum_horizontal(legacy_roles))
                .alias(current_role)
            )
        )

    lf = lf.with_columns(expr)

    # Drop legacy job roles from dataset.
    cols_to_drop = {f"jr33{suffix}" for suffix in suffix_list}
    for suffix in suffix_list:
        for current_role, legacy_roles in job_role_mapping.items():
            cols_to_drop.update(f"jr{i}{suffix}" for i in legacy_roles)

    lf = lf.drop(*cols_to_drop)

    return lf
