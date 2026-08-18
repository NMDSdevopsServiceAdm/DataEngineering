import polars as pl

from polars_utils.expressions import is_care_home, is_not_care_home
from projects._03_independent_cqc.utils.imputation.extrapolation import (
    model_extrapolation,
)
from projects._03_independent_cqc.utils.imputation.interpolation import (
    model_interpolation,
)
from utils.column_names.ind_cqc_pipeline_columns import Imputation
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc
from utils.column_values.categorical_column_values import CareHome


def model_imputation(
    lf: pl.LazyFrame,
    column_with_null_values: str,
    model_column_name: str,
    imputed_column_name: str,
    care_home: bool,
    extrapolation_method: str,
) -> pl.LazyFrame:
    """
    Create a new column of imputed values based on known values and null values
    being extrapolated and interpolated.

    Eligibility for imputation is flagged per row (based on the care_home
    status of the location and only for locations who have at least one
    non-null value). Extrapolation and interpolation run across the whole
    LazyFrame, grouped by `[location_id, care_home]` rather than
    `location_id` alone — a location's `care_home` status can change between
    import dates (it's re-derived from `primary_service_type` on every
    import), so grouping by `location_id` alone would mix a location's
    differently-flagged periods into the same calculation. The final imputed
    value is only kept where the eligibility flag is set.

    The imputation model is carried out in two steps, extrapolation and
    interpolation, which both populate null values based on the rate of change
    of values in '<model_column_name>'.

    Args:
        lf (pl.LazyFrame): The input LazyFrame containing the column_with_null_values.
        column_with_null_values (str): The name of the column containing null
            values to be imputed.
        model_column_name (str): The name of the column containing the model
            values used for imputation.
        imputed_column_name (str): The name of the new imputated column.
        care_home (bool): True if imputation is for care homes, False if it is
            for non residential.
        extrapolation_method (str): The choice of method.
            Must be either 'nominal' or 'ratio'.

    Returns:
        pl.LazyFrame: The LazyFrame with the added column imputed_column_name.
    """
    # lf = flag_rows_eligible_for_imputation(lf, column_with_null_values, care_home)
    group_columns = [IndCqc.location_id, IndCqc.care_home]

    lf = model_extrapolation(
        lf,
        column_with_null_values,
        model_column_name,
        extrapolation_method,
        group_columns=group_columns,
    )
    lf = model_interpolation(
        lf,
        column_with_null_values,
        method="trend",
        group_columns=group_columns,
    )

    lf = lf.with_columns(
        pl.when(pl.col(IndCqc.care_home) == care_home)
        .then(
            pl.coalesce(
                column_with_null_values,
                IndCqc.extrapolation_model,
                IndCqc.interpolation_model,
            )
        )
        .cast(pl.Float32)
        .alias(imputed_column_name)
    ).drop(
        IndCqc.extrapolation_forwards,
        IndCqc.extrapolation_model,
        IndCqc.interpolation_model,
        Imputation.eligible_for_imputation,
    )

    return lf


def flag_rows_eligible_for_imputation(
    lf: pl.LazyFrame, column_with_null_values: str, care_home: bool
) -> pl.LazyFrame:
    """
    Adds a boolean column flagging rows eligible for imputation.

    A row is eligible when its care_home status matches `care_home` and its
    location has at least one non-null value in `column_with_null_values`.
    The flag is added as a column on the full input LazyFrame (via a single
    broadcast left-join) rather than splitting the LazyFrame in two, so that
    repeated calls to `model_imputation` don't fork and re-concatenate the
    query plan on every call.

    Args:
        lf (pl.LazyFrame): The input LazyFrame.
        column_with_null_values (str): The name of the column to check for
            non-null values.
        care_home (bool): True if imputation is for care homes, False if it is
            for non residential.

    Returns:
        pl.LazyFrame: The input LazyFrame with an added boolean column,
            `Imputation.eligible_for_imputation`.
    """
    if care_home:
        care_home_filter_expr: pl.Expr = is_care_home()
    else:
        care_home_filter_expr: pl.Expr = is_not_care_home()

    groups_with_values = (
        lf.filter(pl.col(column_with_null_values).is_not_null())
        .filter(care_home_filter_expr)
        .select([IndCqc.location_id, IndCqc.care_home])
        .unique()
        .with_columns(pl.lit(True).alias(Imputation.eligible_for_imputation))
    )

    return lf.join(
        groups_with_values,
        on=[IndCqc.location_id, IndCqc.care_home],
        how="left",
    ).with_columns(pl.col(Imputation.eligible_for_imputation).fill_null(False))
