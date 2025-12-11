import random

import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


def split_train_test(
    df: pl.DataFrame, fraction: float = 0.8, seed: int = 42
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Deterministic group-based train/test split using a reproducible seed.

    Args:
        df (pl.DataFrame): Input DataFrame (with multiple rows per identifier).
        fraction (float): Proportion of groups to assign to training (default 0.8).
        seed (int): Random seed to ensure reproducibility (default 42).

    Returns:
        tuple[pl.DataFrame, pl.DataFrame]: A tuple of two dataframes; train_df and test_df.
    """
    identifier_col = IndCQC.location_id

    unique_ids = df.select(identifier_col).unique().to_series().to_list()

    rng = random.Random(seed)
    rng.shuffle(unique_ids)

    cutoff = int(len(unique_ids) * fraction)
    train_ids = set(unique_ids[:cutoff])

    train_df = df.filter(pl.col(identifier_col).is_in(train_ids))
    test_df = df.filter(~pl.col(identifier_col).is_in(train_ids))

    return train_df, test_df
