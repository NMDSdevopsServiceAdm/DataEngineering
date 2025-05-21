from pyspark.sql import DataFrame, functions as F, Window
from pyspark.sql.types import IntegerType, StringType, MapType, DoubleType
from typing import List
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import (
    Dormancy,
    EstimateFilledPostsSource,
    PrimaryServiceType,
)


def add_source_description_to_source_column(
    input_df: DataFrame,
    populated_column_name: str,
    source_column_name: str,
    source_description: str,
) -> DataFrame:
    return input_df.withColumn(
        source_column_name,
        F.when(
            (
                F.col(populated_column_name).isNotNull()
                & F.col(source_column_name).isNull()
            ),
            source_description,
        ).otherwise(F.col(source_column_name)),
    )


# TODO Remove this function if it has been replaced and is no longer used.
def populate_estimate_filled_posts_and_source_in_the_order_of_the_column_list(
    df: DataFrame,
    order_of_models_to_populate_estimate_filled_posts_with: list,
    estimates_column_to_populate: str,
    estimates_source_column_to_populate: str,
) -> DataFrame:
    """
    Populate the estimates and source columns using the hierarchy provided in the list.

    The first item in the list is populated first. Subsequent null values are filled by the following items in the list.

    Args:
        df(DataFrame): A data frame with estimates that need merging into a single column.
        order_of_models_to_populate_estimate_filled_posts_with (list): A list of column names of models.
        estimates_column_to_populate (str): The name of the column to populate with estimates.
        estimates_source_column_to_populate (str): The name of the column to populate with estimate sources.

    Returns:
        DataFrame: A data frame with estimates and estimates source column populated.
    """
    df = df.withColumn(estimates_column_to_populate, F.lit(None).cast(IntegerType()))
    df = df.withColumn(
        estimates_source_column_to_populate, F.lit(None).cast(StringType())
    )

    # TODO - replace for loop with better functionality
    # see https://trello.com/c/94jAj8cd/428-update-how-we-populate-estimates
    for model_name in order_of_models_to_populate_estimate_filled_posts_with:
        df = df.withColumn(
            estimates_column_to_populate,
            F.when(
                (
                    F.col(estimates_column_to_populate).isNull()
                    & (F.col(model_name).isNotNull())
                    & (F.col(model_name) >= 1.0)
                ),
                F.col(model_name),
            ).otherwise(F.col(estimates_column_to_populate)),
        )

        df = add_source_description_to_source_column(
            df,
            estimates_column_to_populate,
            estimates_source_column_to_populate,
            model_name,
        )

    return df


def merge_columns_in_order(
    df: DataFrame,
    ordered_list_of_columns_to_be_merged: List,
    merged_column_name: str,
    merged_column_source_name: str,
) -> DataFrame:
    """
    Merges a given list of columns into a new column and adds another column for the source.

    This function creates a new column using the values from other columns given in a list.
    Values are taken from the given list of columns in the order of the list.
    The given list of columns must all be of the same datatype which can be float or map.
    Float values will only be taken if they are greater than or equal to 1.0.
    Map elements will only be taken if they are not null.
    This function also adds a new column for the source, which is the column name that values were taken from.

    Args:
        df (DataFrame): A dataframe containing multiple columns of job role ratios.
        ordered_list_of_columns_to_be_merged (List): A list of column names in priority order highest to lowest.
        merged_column_name (str): The name to give the new merged column.
        merged_column_source_name (str): The name to give the new merged source column.

    Returns:
        DataFrame: A dataframe with a column for the merged job role ratios.

    Raises:
        ValueError: If the given list of columns are not all 'double' or all 'map' datatypes.
    """
    column_types = list(
        set(
            [
                df.schema[column].dataType
                for column in ordered_list_of_columns_to_be_merged
            ]
        )
    )
    if len(column_types) > 1:
        raise ValueError(
            f"The columns to merge must all have the same datatype. Found {column_types}."
        )

    if isinstance(column_types[0], DoubleType):
        df = df.withColumn(
            merged_column_name,
            F.coalesce(
                *[
                    F.when((F.col(column) >= 1.0), F.col(column))
                    for column in ordered_list_of_columns_to_be_merged
                ]
            ),
        )

        source_column = F.when(
            F.col(ordered_list_of_columns_to_be_merged[0]) >= 1.0,
            ordered_list_of_columns_to_be_merged[0],
        )
        for column_name in ordered_list_of_columns_to_be_merged[1:]:
            source_column = source_column.when(F.col(column_name) >= 1.0, column_name)

    elif isinstance(column_types[0], MapType):
        df = df.withColumn(
            merged_column_name,
            F.coalesce(
                *[F.col(column) for column in ordered_list_of_columns_to_be_merged]
            ),
        )

        source_column = F.when(
            F.col(ordered_list_of_columns_to_be_merged[0]).isNotNull(),
            ordered_list_of_columns_to_be_merged[0],
        )
        for column_name in ordered_list_of_columns_to_be_merged[1:]:
            source_column = source_column.when(
                F.col(column_name).isNotNull(), column_name
            )

    else:
        raise ValueError(
            f"Columns to merge must be either 'double' or 'map' type. Found {column_types}."
        )

    df = df.withColumn(merged_column_source_name, source_column)

    return df


def get_selected_value(
    df: DataFrame,
    window_spec: Window,
    column_with_null_values: str,
    column_with_data: str,
    new_column: str,
    selection: str,
) -> DataFrame:
    """
    Creates a new column with the selected value (first or last) from a given column.

    This function creates a new column by selecting a specified value over a given window on a given dataframe. It will
    only select values in the column with data that have null values in the original column.

    Args:
        df (DataFrame): A dataframe containing the supplied columns.
        window_spec (Window): A window describing how to prepare the dataframe.
        column_with_null_values (str): A column with missing data.
        column_with_data (str): A column with data for all the rows that column_with_null_values has data. This can be column_with_null_values itself.
        new_column (str): The name of the new column containing the resulting selected values.
        selection (str): One of 'first' or 'last'. This determines which pyspark window function will be used.

    Returns:
        DataFrame: A dataframe containing a new column with the selected value populated through each window.

    Raises:
        ValueError: If 'selection' is not one of the two permitted pyspark window functions.
    """
    selection_methods = {"first": F.first, "last": F.last}

    if selection not in selection_methods:
        raise ValueError(
            f"Error: The selection parameter '{selection}' was not found. Please use 'first' or 'last'."
        )

    method = selection_methods[selection]

    df = df.withColumn(
        new_column,
        method(
            F.when(F.col(column_with_null_values).isNotNull(), F.col(column_with_data)),
            ignorenulls=True,
        ).over(window_spec),
    )

    return df


def copy_and_fill_filled_posts_when_becoming_not_dormant(df: DataFrame) -> DataFrame:
    """
    Copy estimate_filled_posts into new column when non-residential location becomes non-dorment.

    At the point dormancy changes from 'Y' to 'N', copy the estimate_filled_posts value from the
    'Y' period into a new column at that 'N' period row. Then copy that value into the previous period
    (where dormancy was 'Y').

    Args:
        df (DataFrame): A dataframe with estimate_filled_posts and dormancy

    Returns:
        DataFrame: A dataframe with a new column estimated_filled_posts_at_point_of_becoming_non_dormant.
    """

    w = Window.partitionBy(IndCQC.location_id).orderBy(IndCQC.cqc_location_import_date)

    new_column = IndCQC.estimated_filled_posts_at_point_of_becoming_non_dormant
    prev_dormancy = F.lag(IndCQC.dormancy).over(w)
    current_dormancy = F.col(IndCQC.dormancy)
    prev_filled_posts = F.lag(IndCQC.estimate_filled_posts).over(w)
    next_estimated_posts = F.lead(new_column).over(w)

    df = df.withColumn(
        new_column,
        F.when(
            (F.col(IndCQC.primary_service_type) == PrimaryServiceType.non_residential)
            & (prev_dormancy == Dormancy.dormant)
            & (current_dormancy == Dormancy.not_dormant),
            prev_filled_posts,
        ),
    )

    df = df.withColumn(
        new_column,
        F.when(next_estimated_posts.isNotNull(), next_estimated_posts).otherwise(
            F.col(new_column)
        ),
    )

    return df


def combine_posts_at_point_of_becoming_non_dormant_and_estimate_filled_posts(
    df: DataFrame,
) -> DataFrame:
    """
    Coalesce the columns estimated_filled_posts_at_point_of_becoming_non_dormant and estimate_filled_posts.

    Args:
        df (DataFrame): A dataframe with estimated_filled_posts_at_point_of_becoming_non_dormant and estimate_filled_posts.

    Returns:
        DataFrame: A dataframe with new column estimate_filled_posts_adjusted_for_dormancy_change.
    """

    df = df.withColumn(
        IndCQC.estimate_filled_posts_adjusted_for_dormancy_change,
        F.coalesce(
            F.col(
                IndCQC.imputed_estimated_filled_posts_at_point_of_becoming_non_dormant
            ),
            F.col(IndCQC.estimate_filled_posts),
        ),
    )

    return df


def flag_dormancy_has_changed_over_time(df: DataFrame) -> DataFrame:
    """
    Adds a column to flag locations where the known dormancy has changed over time.

    If the known dormancy string has changed at any time then all rows for that location
    get bool value of True. If dormancy is null, then flag is also null.

    Args:
        df (DataFrame): A dataframe with column for dormancy

    Returns:
        DataFrame: A dataframe with an addtional bool column to show if dormancy status has changed or not.
    """

    df_count_distinct_dormancy = df.groupBy(IndCQC.location_id).agg(
        F.countDistinct(IndCQC.dormancy).alias(
            IndCQC.flag_dormancy_has_changed_over_time
        )
    )

    df_count_distinct_dormancy = df_count_distinct_dormancy.withColumn(
        IndCQC.flag_dormancy_has_changed_over_time,
        F.when(F.col(IndCQC.flag_dormancy_has_changed_over_time) > 1, True).otherwise(
            False
        ),
    )

    df = df.join(df_count_distinct_dormancy, on=IndCQC.location_id, how="left")

    df = df.withColumn(
        IndCQC.flag_dormancy_has_changed_over_time,
        F.when(
            F.col(IndCQC.dormancy).isNull(),
            F.lit(None),
        ).otherwise(F.col(IndCQC.flag_dormancy_has_changed_over_time)),
    )

    return df


def apply_adjustments_when_dormancy_changes(
    df: DataFrame, expected_change_per_day: float
) -> DataFrame:
    """
    Adds a column with the cqc_location_import_date copied at the row before dormancy changed.

    Args:
        df (DataFrame): A dataframe with a dormancy column.
        expected_change_per_day (float): The expected percentage change in overall filled posts per day.

    Returns:
        DataFrame: A dataframe with an additional date column populated on selected rows only.
    """

    original_columns_list = df.columns

    df = add_column_with_previous_dormancy_status(df)
    df = add_column_with_period_when_dormancy_changed(df)
    df = add_column_with_filled_posts_at_date_when_dormancy_changed(df)
    df = add_column_with_days_since_dormancy_changed(df)

    adjustment_ratio = (
        F.col(IndCQC.number_of_days_since_dormancy_change) * expected_change_per_day
    )

    df = df.withColumn(
        IndCQC.estimate_filled_posts_adjusted_for_dormancy_change,
        F.when(
            (F.col(IndCQC.previous_dormancy_value) == "Y")
            & (F.col(IndCQC.dormancy) == "N"),
            F.col(IndCQC.estimate_filled_posts_at_period_when_dormancy_changed)
            * (1 + adjustment_ratio),
        )
        .when(
            (F.col(IndCQC.previous_dormancy_value) == "N")
            & (F.col(IndCQC.dormancy) == "Y"),
            F.col(IndCQC.estimate_filled_posts_at_period_when_dormancy_changed)
            * (1 - adjustment_ratio),
        )
        .otherwise(F.col(IndCQC.estimate_filled_posts)),
    )

    df = df.select(
        *original_columns_list,
        IndCQC.previous_dormancy_value,
        IndCQC.period_when_dormancy_changed,
        IndCQC.estimate_filled_posts_at_period_when_dormancy_changed,
        IndCQC.number_of_days_since_dormancy_change,
        IndCQC.estimate_filled_posts_adjusted_for_dormancy_change,
    ).sort([IndCQC.location_id, IndCQC.cqc_location_import_date])

    return df


def add_column_with_previous_dormancy_status(df: DataFrame) -> DataFrame:
    """
    Adds a column to show the previous dormancy status.

    This column repeats the locations previous dormancy status from the point at which dormancy changed.
    If dormancy changes from null to value or value to null, then this column shows null.
    Otherwise it shows the previous dormancy status repeated until another change occurs.

    Args:
        df (DataFrame): A dataframe with a dormancy column.

    Returns:
        DataFrame: A dataframe with an additional column showing previous dormancy status.
    """

    w_full = Window.partitionBy(IndCQC.location_id).orderBy(
        IndCQC.cqc_location_import_date
    )
    w_for_filling_down = (
        Window.partitionBy(IndCQC.location_id)
        .orderBy(IndCQC.cqc_location_import_date)
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    df = df.withColumn(
        IndCQC.previous_dormancy_value,
        F.lag(F.col(IndCQC.dormancy), offset=1).over(w_full),
    )
    df = df.withColumn(
        IndCQC.previous_dormancy_value,
        F.when(
            (F.col(IndCQC.dormancy) == F.col(IndCQC.previous_dormancy_value))
            | (F.col(IndCQC.dormancy).isNull()),
            F.lit(None),
        ).otherwise(F.col(IndCQC.previous_dormancy_value)),
    )
    df = df.withColumn(
        IndCQC.previous_dormancy_value,
        F.last(F.col(IndCQC.previous_dormancy_value), ignorenulls=True).over(
            w_for_filling_down
        ),
    )

    return df


def add_column_with_period_when_dormancy_changed(df: DataFrame) -> DataFrame:
    """
    Adds a column to show the cqc_location_import_date when dormancy first changed.

    This column repeats the date previous to the date when dormancy changed.
    For example, if dormancy was "Y" on 01/01/2025 and "N" on 02/01/2025, then this column
    shows 01/01/2025 repeatedly.

    Changing the function to use F.last instead of F.first will cause it to repeat the prior date
    each time dormancy changes.

    Args:
        df (DataFrame): A dataframe with a previous dormancy status column and cqc_location_import_date.

    Returns:
        DataFrame: A dataframe with an additional column showing the date when dormancy first changed.
    """

    w_full = Window.partitionBy(IndCQC.location_id).orderBy(
        IndCQC.cqc_location_import_date
    )
    w_for_filling_down = (
        Window.partitionBy(IndCQC.location_id)
        .orderBy(IndCQC.cqc_location_import_date)
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    df = df.withColumn(
        IndCQC.period_when_dormancy_changed,
        F.when(
            F.lag(F.col(IndCQC.dormancy), offset=1).over(w_full)
            == F.col(IndCQC.previous_dormancy_value),
            F.lag(F.col(IndCQC.cqc_location_import_date), offset=1).over(w_full),
        ).otherwise(F.lit(None)),
    )
    df = df.withColumn(
        IndCQC.period_when_dormancy_changed,
        F.first(F.col(IndCQC.period_when_dormancy_changed), ignorenulls=True).over(
            w_for_filling_down
        ),
    )

    return df


def add_column_with_filled_posts_at_date_when_dormancy_changed(
    df: DataFrame,
) -> DataFrame:
    """
    Adds a column to show the estimated filled posts value at the date in period_when_dormancy_changed.

    Args:
        df (DataFrame): A dataframe with period_when_dormancy_changed column.

    Returns:
        DataFrame: A dataframe with an additional column showing estimated filled posts value at the date in period_when_dormancy_changed.
    """

    df_lookup_filled_posts_value = (
        df.select(
            [
                IndCQC.location_id,
                IndCQC.cqc_location_import_date,
                IndCQC.estimate_filled_posts,
            ]
        )
        .withColumnRenamed(
            IndCQC.cqc_location_import_date, IndCQC.period_when_dormancy_changed
        )
        .withColumnRenamed(
            IndCQC.estimate_filled_posts,
            IndCQC.estimate_filled_posts_at_period_when_dormancy_changed,
        )
    )
    df = df.join(
        df_lookup_filled_posts_value,
        on=([IndCQC.location_id, IndCQC.period_when_dormancy_changed]),
        how="left",
    )

    return df


def add_column_with_days_since_dormancy_changed(df: DataFrame) -> DataFrame:
    """
    Adds a column to show the number of days between cqc_location_import_date and period_when_dormancy_changed.

    Args:
        df (DataFrame): A dataframe with cqc_location_import_date and period_when_dormancy_changed.

    Returns:
        DataFrame: A dataframe with an additional column showing days between dates.
    """

    df = df.withColumn(
        IndCQC.number_of_days_since_dormancy_change,
        (
            (
                F.unix_timestamp(F.col(IndCQC.cqc_location_import_date))
                - F.unix_timestamp(F.col(IndCQC.period_when_dormancy_changed))
            )
            / 86400
        ),
    )

    return df


def flag_location_has_ascwds_value(df: DataFrame) -> DataFrame:
    """
    Adds a column to flag locations where their estimated filled posts source is ascwds_pir_merged at any time.

    If a location has their estimated filled posts source as ascwds_pir_merged at any point in time, then
    all rows for that location get a value of true. The estimated filled posts source cannot be null, so this
    column also cannot be null.

    Args:
        df (DataFrame): A dataframe with column for dormancy

    Returns:
        DataFrame: A dataframe with an addtional bool column to show if dormancy status has changed or not.
    """

    columns_to_select = [IndCQC.location_id, IndCQC.estimate_filled_posts_source]
    df_deduplicated_estimate_source = df.select(*columns_to_select).dropDuplicates()

    df_deduplicated_estimate_source = df_deduplicated_estimate_source.withColumn(
        IndCQC.flag_location_has_ascwds_value,
        F.when(
            F.col(IndCQC.estimate_filled_posts_source)
            == EstimateFilledPostsSource.ascwds_pir_merged,
            1,
        ).otherwise(0),
    )

    df_deduplicated_estimate_source = df_deduplicated_estimate_source.groupBy(
        IndCQC.location_id
    ).agg(
        F.max(F.col(IndCQC.flag_location_has_ascwds_value)).alias(
            IndCQC.flag_location_has_ascwds_value
        )
    )

    df_deduplicated_estimate_source = df_deduplicated_estimate_source.withColumn(
        IndCQC.flag_location_has_ascwds_value,
        F.when(F.col(IndCQC.flag_location_has_ascwds_value) == 1, True).otherwise(
            False
        ),
    )

    df = df.join(df_deduplicated_estimate_source, on=IndCQC.location_id, how="left")

    return df
