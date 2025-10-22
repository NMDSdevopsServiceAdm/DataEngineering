import polars as pl


def add_aligned_date_column(
    primary_lf: pl.LazyFrame,
    secondary_lf: pl.LazyFrame,
    primary_column: str,
    secondary_column: str,
) -> pl.LazyFrame:
    """
    Adds a column to `primary_lf` containing the closest past matching date from `secondary_lf`.

    Uses `join_asof` to align each primary date with the nearest non-future secondary date.

    After this function, we join the full secondary_lf into primary_lf based on the
    `secondary_column`. Unfortunately this needs to be a two step process as we want the entire
    datasets to be aligned on the same date, as opposed to aligning dates to whenever each
    individual location ID or postcode last existed.

    Args:
        primary_lf (pl.LazyFrame): LazyFrame to which the aligned date column will be added.
        secondary_lf (pl.LazyFrame): LazyFrame with dates for alignment.
        primary_column (str): Column name in `primary_lf` containing date values.
        secondary_column (str): Column name in `secondary_lf` containing date values.

    Returns:
        pl.LazyFrame: The original `primary_lf` with an additional column containing
        aligned dates from `secondary_lf`.
    """
    # Ensure both frames are sorted by date for join_asof
    primary_sorted = primary_lf.sort(primary_column)
    secondary_sorted = (
        secondary_lf.select(secondary_column).unique().sort(secondary_column)
    )

    # Join secondary dates to primary dates using asof join
    primary_lf_with_aligned_dates = primary_sorted.join_asof(
        secondary_sorted,
        left_on=primary_column,
        right_on=secondary_column,
        strategy="backward",  # less than or equal to
        coalesce=False,  # include secondary_column in join
    )

    return primary_lf_with_aligned_dates


def apply_categorical_labels(
    lf: pl.LazyFrame,
    labels: dict,
    columns_to_apply: list,
    add_as_new_column: bool = True,
) -> pl.LazyFrame:
    """
    Replace the values in one or more columns in the given LazyFrame by mapping using a dict.

    The given labels dict must have column names as keys and a dict object as values. The value dict object
    must have the old value as keys and the replacement value as values.
    The given columns_to_apply must be a list of 1 or more column names which are also keys within the given labels dict.

    Args:
        lf (pl.LazyFrame): A LazyFrame in which to replace values.
        labels (dict): A dict object where keys are column names and values are dict object (keys = old value, values = replacement)
        columns_to_apply (list): A list of column names to replace value in.
        add_as_new_column (bool): True will add a new column with replacement values. False will overwrite original values. Defaults to True.

    Returns:
        pl.LazyFrame: A LazyFrame with values replaced.
    """
    for column_name in columns_to_apply:
        labels_dict = labels[column_name]
        if add_as_new_column is True:
            new_column_name = column_name + "_labels"
            lf = lf.with_columns(
                pl.col(column_name).replace(labels_dict).alias(new_column_name)
            )
        elif add_as_new_column is False:
            lf = lf.with_columns(
                pl.col(column_name).replace(labels_dict).alias(column_name)
            )

    return lf
