from typing import Optional

from pyspark.sql import DataFrame, Column, Window, functions as F


def calculate_new_column(
    df: DataFrame,
    new_col: str,
    col_1: str,
    method: str,
    col_2: str,
    when_clause: Optional[Column] = None,
) -> DataFrame:
    """
    Adds a new column to a DataFrame based on a specified mathematical operation between two columns.

    There is also the option to apply a custom condition (when_clause) to determine when the calculation should be performed.

    Args:
        df (DataFrame): Input DataFrame.
        new_col (str): Name of the new column to be created.
        col_1 (str): Name of the first operand column.
        method (str): Calculation to perform: "plus", "minus", "multiplied by", "divided by, "average" or "absolute difference".
        col_2 (str): Name of the second operand column.
        when_clause (Optional[Column]): Optional Boolean condition for applying the calculation.

    Returns:
        DataFrame: New DataFrame with the calculated column.

    Raises:
        ValueError: If the method is not recognised.
    """
    c1 = F.col(col_1)
    c2 = F.col(col_2)

    if method == "plus":
        calculation = c1 + c2
    elif method == "minus":
        calculation = c1 - c2
    elif method == "multiplied by":
        calculation = c1 * c2
    elif method == "divided by":
        calculation = c1 / c2
    elif method == "average":
        calculation = (c1 + c2) / 2
    elif method == "absolute difference":
        calculation = F.abs(c1 - c2)
    else:
        raise ValueError(f"Invalid method: {method}")

    base_condition = c1.isNotNull() & c2.isNotNull()

    if method == "divided by":
        base_condition = base_condition & (c2 != 0)

    if when_clause is not None:
        condition = base_condition & when_clause
    else:
        condition = base_condition

    return df.withColumn(new_col, F.when(condition, calculation).otherwise(None))


def calculate_windowed_column(
    df: DataFrame,
    window: Window,
    new_col: str,
    input_column: str,
    aggregation_function: str,
) -> DataFrame:
    """
    This function adds a column containing the specified function (mean, min or max) over the given window.

    Args:
        df (DataFrame): The input DataFrame.
        window (Window): The window specification to use.
        new_col (str): The name of the new column to be added.
        input_column (str): The name of the input column to be aggregated.
        aggregation_function (str): The function to use in the calculation ('avg', 'count', 'max', 'min' or 'sum').

    Returns:
        DataFrame: A dataframe with an additional column containing the specified aggregation function over the given window.

    Raises:
        ValueError: If chosen function does not match 'avg', 'count', 'max', 'min' or 'sum'.
    """
    functions = {
        "avg": F.avg,
        "count": F.count,
        "max": F.max,
        "min": F.min,
        "sum": F.sum,
    }

    if aggregation_function not in functions:
        raise ValueError(
            f"Error: The aggregation function '{aggregation_function}' was not found. Please use 'avg', 'count', 'max', 'min' or 'sum'."
        )

    method = functions[aggregation_function]

    return df.withColumn(new_col, method(F.col(input_column)).over(window))
