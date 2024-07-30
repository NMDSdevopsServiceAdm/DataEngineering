from pyspark.sql import DataFrame


def add_filtering_rule_column(df: DataFrame) -> DataFrame:
    """
    Add column which flags if data is present or missing.

    This function adds a new column which will eventually contain the filtering rules. When it is added, the data in ascwds_filled_posts_clean is identical to ascwds_filled_posts, so the only values will be "populated" and "missing data".

    Args:
        df (DataFrame): A dataframe containing ascwds_filled_posts_clean before any filters have been applied to the column.

    Returns:
        (DataFrame) : A dataframe with an additional column that states whether data is present or missing before filters are applied.
    """
    return df


def update_filtering_rule(df: DataFrame, rule_name: str) -> DataFrame:
    """
    Update filtering rule for rows where data was present but is now missing.

    This function adds updates the filtering rule where it was listed as "populated" but the current filtering rule has just nullified the data in ascwds_filled_posts_clean. The new values will be the name of the filter applied.

    Args:
        df (DataFrame): A dataframe containing ascwds_filled_posts_clean and ascwds_filtering_rule after a new rules has been applied.
        rule_name (str): The name of the rule that has just been applied.

    Returns:
        (DataFrame) : A dataframe with the ascwds_filtering_rule column updated.
    """
    return df
