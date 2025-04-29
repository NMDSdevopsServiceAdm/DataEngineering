from typing import Optional

from pyspark.sql import DataFrame, Column, functions as F


def calculate(
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
        method (str): Calculation to perform: "plus", "minus", "multiplied by" or "divided by".
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
