import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc


def model_extrapolation(
    lf: pl.LazyFrame,
    column_with_null_values: str,
    model_to_extrapolate_from: str,
    extrapolation_method: str,
) -> pl.LazyFrame:
    """
    Perform extrapolation on a column with null values using specified models.

    This function calculates the first and final submission dates, performs forward and backward extrapolation,
    and combines the extrapolated results into a new column.

    Values before the first known submission in 'column_with_null_values' and after the last known submission are
    extrapolated, either by nominal or ratio method as specified in 'extrapolation_method'.
    The ratio method is based on multiplying the known value by the rate of change of the '<model_column_name>'.
    The nominal method is based on adding/subtracting the nominal change of the '<model_column_name>' to the known value.

    Args:
        lf (pl.LazyFrame): The input LazyFrame containing the data.
        column_with_null_values (str): The name of the column that contains null values to be extrapolated.
        model_to_extrapolate_from (str): The model used for extrapolation.
        extrapolation_method (str): The choice of method. Must be either 'nominal' or 'ratio'.

    Returns:
        pl.LazyFrame: The LazyFrame with the extrapolated values in the 'extrapolation_model' column.

    Raises:
        ValueError: If chosen extrapolation_method does not match 'nominal' or 'ratio'.
    """
    ### ADD RANK COLUMN ###
    lf = lf.with_columns(
        pl.when(pl.col(column_with_null_values).is_not_null())
        .then(pl.col(IndCqc.cqc_location_import_date).rank().over(IndCqc.location_id))
        .otherwise(None)
        .alias("rank")
    )
    ### AGGREGATE COLUMNS ###
    first_submission_expr = (
        pl.col(IndCqc.cqc_location_import_date)
        .min_by("rank")
        .alias(IndCqc.first_submission_time)
    )
    final_submission_expr = (
        pl.col(IndCqc.cqc_location_import_date)
        .max_by("rank")
        .alias(IndCqc.final_submission_time)
    )
    previous_non_null_expr = (
        pl.col(column_with_null_values)
        .min_by("rank")
        .alias(IndCqc.previous_non_null_value)
    )
    first_non_null_expr = (
        pl.col(column_with_null_values)
        .max_by("rank")
        .alias(IndCqc.first_non_null_value)
    )
    previous_model_expr = (
        pl.col(model_to_extrapolate_from)
        .min_by("rank")
        .alias(IndCqc.previous_model_value)
    )
    first_model_expr = (
        pl.col(model_to_extrapolate_from).max_by("rank").alias(IndCqc.first_model_value)
    )

    aggregations_needed = [
        first_submission_expr,
        final_submission_expr,
        previous_model_expr,
        previous_non_null_expr,
        first_model_expr,
        first_non_null_expr,
    ]

    agg_lf = (
        # lf.drop_nulls(column_with_null_values)
        lf.sort([IndCqc.location_id, IndCqc.cqc_location_import_date])
        .group_by(IndCqc.location_id)
        .agg(aggregations_needed)
    )

    lf = lf.join(
        agg_lf,
        on=IndCqc.location_id,
        how="left",
    )

    ### CALCULATE EXTRAPOLATION ###

    ratio_forwards_expr = pl.when(
        pl.col(IndCqc.cqc_location_import_date) > pl.col(IndCqc.first_submission_time)
    ).then(
        pl.col(IndCqc.previous_non_null_value)
        .mul(pl.col(model_to_extrapolate_from))
        .truediv(pl.col(IndCqc.previous_model_value))
    )
    nominal_forwards_expr = pl.when(
        pl.col(IndCqc.cqc_location_import_date) > pl.col(IndCqc.first_submission_time)
    ).then(
        pl.col(IndCqc.previous_non_null_value)
        .add(pl.col(model_to_extrapolate_from))
        .sub(pl.col(IndCqc.previous_model_value))
    )
    ratio_backwards_expr = pl.when(
        pl.col(IndCqc.cqc_location_import_date) < pl.col(IndCqc.first_submission_time)
    ).then(
        pl.col(IndCqc.first_non_null_value)
        .mul(pl.col(model_to_extrapolate_from))
        .truediv(pl.col(IndCqc.first_model_value))
    )

    nominal_backwards_expr = pl.when(
        pl.col(IndCqc.cqc_location_import_date) < pl.col(IndCqc.first_submission_time)
    ).then(
        pl.col(IndCqc.first_non_null_value)
        .add(pl.col(model_to_extrapolate_from))
        .sub(pl.col(IndCqc.first_model_value))
    )
    after_final_submission_expr = pl.col(IndCqc.cqc_location_import_date) > pl.col(
        IndCqc.final_submission_time
    )
    before_first_submission_expr = pl.col(IndCqc.cqc_location_import_date) < pl.col(
        IndCqc.first_submission_time
    )

    if extrapolation_method == "ratio":
        lf = lf.with_columns(
            ratio_forwards_expr.alias(IndCqc.extrapolation_forwards)
        )  # Used in interpolation
        combine_extrapolation_expr = (
            pl.when(after_final_submission_expr)
            .then(IndCqc.extrapolation_forwards)
            .when(before_first_submission_expr)
            .then(ratio_backwards_expr)
        )

    elif extrapolation_method == "nominal":
        lf = lf.with_columns(
            nominal_forwards_expr.alias(IndCqc.extrapolation_forwards)
        )  # Used in interpolation
        combine_extrapolation_expr = (
            pl.when(after_final_submission_expr)
            .then(IndCqc.extrapolation_forwards)
            .when(before_first_submission_expr)
            .then(nominal_backwards_expr)
        )

    else:
        raise ValueError("Error: method must be either 'ratio' or 'nominal'.")

    lf = lf.with_columns(combine_extrapolation_expr.alias(IndCqc.extrapolation_model))

    return lf.drop(
        IndCqc.first_submission_time,
        IndCqc.final_submission_time,
        "rank",
        IndCqc.previous_non_null_value,
        IndCqc.previous_model_value,
        IndCqc.first_non_null_value,
        IndCqc.first_model_value,
    )
