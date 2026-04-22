import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc

rank_col: str = "rank"


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
    The function adds two columns: 'extrapolation_model' and 'extrapolation_forwards'. The interpolation model uses
    'extrapolation_forwards' in its computations.

    Args:
        lf (pl.LazyFrame): The input LazyFrame containing the data.
        column_with_null_values (str): The name of the column that contains null values to be extrapolated.
        model_to_extrapolate_from (str): The model used for extrapolation.
        extrapolation_method (str): The choice of method. Must be either 'nominal' or 'ratio'.

    Returns:
        pl.LazyFrame: The LazyFrame with the extrapolated values in the 'extrapolation_model' and 'extrapolation_forwards' columns.

    Raises:
        ValueError: If chosen extrapolation_method does not match 'nominal' or 'ratio'.
    """
    expr = PolarsExpressionsForExtrapolationBusinessLogic(
        column_with_null_values, model_to_extrapolate_from
    )
    ### ADD RANK COLUMN ###
    lf = lf.with_columns(
        pl.when(pl.col(column_with_null_values).is_not_null())
        .then(pl.col(IndCqc.cqc_location_import_date).rank().over(IndCqc.location_id))
        .otherwise(None)
        .alias(rank_col)
    )
    ### AGGREGATE COLUMNS ###
    aggregations_needed = [
        expr.first_submission,
        expr.final_submission,
        expr.previous_model,
        expr.previous_non_null,
        expr.first_model,
        expr.first_non_null,
    ]

    agg_lf = (
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

    if extrapolation_method == "ratio":
        lf = lf.with_columns(
            expr.ratio_forwards.alias(IndCqc.extrapolation_forwards)
        )  # Used in interpolation
        lf = lf.with_columns(
            expr.combine_ratio_extrapolation.alias(IndCqc.extrapolation_model)
        )

    elif extrapolation_method == "nominal":
        lf = lf.with_columns(
            expr.nominal_forwards.alias(IndCqc.extrapolation_forwards)
        )  # Used in interpolation
        lf = lf.with_columns(
            expr.combine_nominal_extrapolation.alias(IndCqc.extrapolation_model)
        )

    else:
        raise ValueError("Error: method must be either 'ratio' or 'nominal'.")

    return lf.drop(
        IndCqc.first_submission_time,
        IndCqc.final_submission_time,
        rank_col,
        IndCqc.previous_non_null_value,
        IndCqc.previous_model_value,
        IndCqc.first_non_null_value,
        IndCqc.first_model_value,
    )


class PolarsExpressionsForExtrapolationBusinessLogic:
    """Define polars logic for extrapolation values and concepts

    This class creates various polars expressions required for computing extrapolation.

    Args:
        column_with_null_values (str): The name of the column that contains null values to be extrapolated.
        model_to_extrapolate_from (str): The model used for extrapolation.
    """

    def __init__(
        self,
        column_with_null_values: str,
        model_to_extrapolate_from: str,
    ):
        ### AGGREGATION EXPRESSIONS ###
        # The cqc_location_import_date of the first non-null value per location_id in column_with_non_null_values
        self.first_submission = (
            pl.col(IndCqc.cqc_location_import_date)
            .min_by(rank_col)
            .alias(IndCqc.first_submission_time)
        )

        # The cqc_location_import_date of the final non-null value per location_id in column_with_non_null_values
        self.final_submission = (
            pl.col(IndCqc.cqc_location_import_date)
            .max_by(rank_col)
            .alias(IndCqc.final_submission_time)
        )

        # The previous non-null value in column_with_non_null_values per location_id
        self.previous_non_null = (
            pl.col(column_with_null_values)
            .min_by(rank_col)
            .alias(IndCqc.previous_non_null_value)
        )

        # The first non-null value in column_with_non_null_values per location_id
        self.first_non_null = (
            pl.col(column_with_null_values)
            .max_by(rank_col)
            .alias(IndCqc.first_non_null_value)
        )

        # The previous non-null value in model_to_extrapolate_from per location_id
        self.previous_model = (
            pl.col(model_to_extrapolate_from)
            .min_by(rank_col)
            .alias(IndCqc.previous_model_value)
        )

        # The first non-null value in model_to_extrapolate_from per location_id
        self.first_model = (
            pl.col(model_to_extrapolate_from)
            .max_by(rank_col)
            .alias(IndCqc.first_model_value)
        )

        ### DATE COMPARISONS ###
        # A cqc_location_import_date after the final submission
        self.after_final_submission = pl.col(IndCqc.cqc_location_import_date) > pl.col(
            IndCqc.final_submission_time
        )

        # A cqc_location_import_date before the first submission
        self.before_first_submission = pl.col(IndCqc.cqc_location_import_date) < pl.col(
            IndCqc.first_submission_time
        )

        # A cqc_location_import_date after the first submission
        self.after_first_submission = pl.col(IndCqc.cqc_location_import_date) > pl.col(
            IndCqc.first_submission_time
        )

        ### CALCULATION ###
        # The formula for calculating forwards ratio extrapolation and the conditions in which to apply it
        self.ratio_forwards = pl.when(self.after_first_submission).then(
            pl.col(IndCqc.previous_non_null_value)
            .mul(pl.col(model_to_extrapolate_from))
            .truediv(pl.col(IndCqc.previous_model_value))
        )

        # The formula for calculating forwards nominal extrapolation and the conditions in which to apply it
        self.nominal_forwards = pl.when(self.after_first_submission).then(
            pl.col(IndCqc.previous_non_null_value)
            .add(pl.col(model_to_extrapolate_from))
            .sub(pl.col(IndCqc.previous_model_value))
        )

        # The formula for calculating backwards ratio extrapolation and the conditions in which to apply it
        self.ratio_backwards = pl.when(self.before_first_submission).then(
            pl.col(IndCqc.first_non_null_value)
            .mul(pl.col(model_to_extrapolate_from))
            .truediv(pl.col(IndCqc.first_model_value))
        )

        # The formula for calculating backwards nominal extrapolation and the conditions in which to apply it
        self.nominal_backwards = pl.when(self.before_first_submission).then(
            pl.col(IndCqc.first_non_null_value)
            .add(pl.col(model_to_extrapolate_from))
            .sub(pl.col(IndCqc.first_model_value))
        )

        # The logic for applying ratio extrapolation
        self.combine_ratio_extrapolation = (
            pl.when(self.after_final_submission)
            .then(self.ratio_forwards)
            .when(self.before_first_submission)
            .then(self.ratio_backwards)
        )

        # The logic for applying nominal extrapolation
        self.combine_nominal_extrapolation = (
            pl.when(self.after_final_submission)
            .then(self.nominal_forwards)
            .when(self.before_first_submission)
            .then(self.nominal_backwards)
        )
