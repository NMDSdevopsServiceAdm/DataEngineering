import polars as pl


def clean_postcode_column(
    df: pl.DataFrame, postcode_col: str, cleaned_col_name: str, drop_col: bool
) -> pl.DataFrame:
    """
    Creates a clean version of the postcode column, in upper case with spaces removed.

    Args:
        df (pl.DataFrame): CQC locations DataFrame with the postcode column in.
        postcode_col (str): The name of the postcode column.
        cleaned_col_name (str): The name of the cleaned postcode column.
        drop_col (bool): Drop the original column if True, otherwise keep the original column.

    Returns:
        pl.DataFrame: A cleaned postcode column in upper case and blank spaces removed.
    """
    df = df.with_columns(
        pl.col(postcode_col)
        .str.replace_all(" ", "")
        .str.to_uppercase()
        .alias(cleaned_col_name)
    )

    if drop_col:
        df = df.drop(postcode_col)
    return df
