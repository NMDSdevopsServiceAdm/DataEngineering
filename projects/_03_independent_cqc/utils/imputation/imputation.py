from typing import Tuple

import polars as pl

from projects._03_independent_cqc.utils.imputation.extrapolation import (
    model_extrapolation,
)
from projects._03_independent_cqc.utils.imputation.interpolation import (
    model_interpolation,
)
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

    This function first splits the dataset into two, one which is relevant for
    imputation (based on the care_home status of the location and only for
    locations who have at least one non-null value) and another which includes
    all other rows not relevant to imputation.

    The imputation model is carried out in two steps, extrapolation and
    interpolation, which both populate null values based on the rate of change
    of values in '<model_column_name>'.

    Args:
        lf (pl.LazyFrame): The input LazyFrame containing the columns to be
            extrapolated and interpolated.
        column_with_null_values (str): The name of the column containing null
            values to be extrapolated and interpolated.
        model_column_name (str): The name of the column containing the model
            values used for extrapolation and interpolation.
        imputed_column_name (str): The name of the new imputated column.
        care_home (bool): True if imputation is for care homes, False if it is
            for non residential.
        extrapolation_method (str): The choice of method.
            Must be either 'nominal' or 'ratio'.

    Returns:
        pl.LazyFrame: The LazyFrame with the added column for imputed values.
    """
    imputed_lf, non_imputed_lf = split_dataset_for_imputation(
        lf, column_with_null_values, care_home
    )

    imputed_lf = model_extrapolation(
        imputed_lf,
        column_with_null_values,
        model_column_name,
        extrapolation_method,
    )
    imputed_lf = model_interpolation(
        imputed_lf,
        column_with_null_values,
        method="trend",
    )

    imputed_lf = imputed_lf.with_columns(
        pl.coalesce(
            column_with_null_values,
            IndCqc.extrapolation_model,
            IndCqc.interpolation_model,
        )
        .cast(pl.Float32)
        .alias(imputed_column_name)
    ).drop(
        IndCqc.extrapolation_forwards,
        IndCqc.extrapolation_model,
        IndCqc.interpolation_model,
    )

    return pl.concat([imputed_lf, non_imputed_lf], how="diagonal")


def split_dataset_for_imputation(
    lf: pl.LazyFrame, column_with_null_values: str, care_home: bool
) -> Tuple[pl.LazyFrame, pl.LazyFrame]:
    """
    Splits the LazyFrame into two based on whether or not the rows meet the
    criteria for imputation.

    Splits the LazyFrame into two based on the presence of non-null values in a
    specified column and whether the care_home column matches the provided
    argument.

    Args:
        lf (pl.LazyFrame): The input LazyFrame.
        column_with_null_values (str): The name of the column to check for
            non-null values.
        care_home (bool): True if imputation is for care homes, False if it is
            for non residential.

    Returns:
        Tuple[pl.LazyFrame, pl.LazyFrame]: A tuple containing two LazyFrames:
            - imputation_lf: LazyFrame with rows meeting the criteria for imputation.
            - non_imputation_lf: LazyFrame with rows not meeting the criteria.
    """
    if care_home:
        care_home_filter_value: str = CareHome.care_home
    else:
        care_home_filter_value: str = CareHome.not_care_home

    locs_with_values_expr = (
        pl.col(column_with_null_values)
        .count()
        .over([IndCqc.location_id, IndCqc.care_home])
        > 0
    ) & (pl.col(IndCqc.care_home) == care_home_filter_value)

    temp_bool = "temp_bool"
    lf = lf.with_columns(locs_with_values_expr.alias(temp_bool))

    imputation_lf = lf.filter(pl.col(temp_bool) == True).drop(temp_bool)
    non_imputation_lf = lf.filter(pl.col(temp_bool) == False).drop(temp_bool)

    return (imputation_lf, non_imputation_lf)
