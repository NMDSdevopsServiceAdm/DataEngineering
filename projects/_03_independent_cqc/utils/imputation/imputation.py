from typing import List, Optional

import polars as pl

from polars_utils.expressions import is_care_home, is_not_care_home
from projects._03_independent_cqc.utils.imputation.extrapolation import (
    model_extrapolation,
)
from projects._03_independent_cqc.utils.imputation.interpolation import (
    model_interpolation,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc


def model_imputation(
    lf: pl.LazyFrame,
    column_with_null_values: str,
    model_column_name: str,
    imputed_column_name: str,
    care_home: Optional[bool],
    extrapolation_method: str,
    group_columns: Optional[List[str]] = None,
) -> pl.LazyFrame:
    """
    Create a new column of imputed values: the known values, with nulls filled by
    extrapolation and interpolation.

    Extrapolation and interpolation run across the whole LazyFrame, grouped by
    `group_columns`, and fill nulls by following the change in
    '<model_column_name>'. The known and filled values are coalesced into
    'imputed_column_name' for the rows selected by `care_home`; other rows are null.

    Args:
        lf (pl.LazyFrame): The input LazyFrame containing the column_with_null_values.
        column_with_null_values (str): The name of the column containing null
            values to be imputed.
        model_column_name (str): The name of the column containing the model
            values used for imputation.
        imputed_column_name (str): The name of the new imputed column.
        care_home (Optional[bool]): True to impute care homes only, False to
            impute non residential only, None to impute every row.
        extrapolation_method (str): The choice of method.
            Must be either 'nominal' or 'ratio'.
        group_columns (Optional[List[str]]): The columns to group by. Defaults
            to `[location_id, care_home]`.

    Returns:
        pl.LazyFrame: The LazyFrame with the added column imputed_column_name.
    """
    group_columns = group_columns or [IndCqc.location_id, IndCqc.care_home]

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

    imputed_value = pl.coalesce(
        column_with_null_values,
        IndCqc.extrapolation_model,
        IndCqc.interpolation_model,
    )
    if care_home is not None:
        care_home_filter_expr = is_care_home() if care_home else is_not_care_home()
        imputed_value = pl.when(care_home_filter_expr).then(imputed_value)

    return lf.with_columns(
        imputed_value.cast(pl.Float32).alias(imputed_column_name)
    ).drop(
        IndCqc.extrapolation_forwards,
        IndCqc.extrapolation_model,
        IndCqc.interpolation_model,
    )
