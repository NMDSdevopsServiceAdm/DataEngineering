import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import CareHome

posts_col = pl.col(IndCQC.ascwds_filled_posts_dedup_clean)
people_col = pl.col(IndCQC.pir_people_directly_employed_dedup)


def convert_pir_to_filled_posts(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Converts PIR people to filled posts using a global ratio.

    The ratio is calculated using a filtered subset of valid rows and then
    applied only to non-care home locations where PIR people is present.

    Args:
        lf (pl.LazyFrame): input dataframe with PIR people and ASC-WDS
            filled posts.

    Returns:
        pl.LazyFrame: input dataframe with estimated PIR filled posts.
    """
    ratio = compute_global_ratio(lf)
    print(f"PIR people to filled posts ratio: {ratio:.4f}")

    return lf.with_columns(
        pl.when(
            (pl.col(IndCQC.care_home) == CareHome.not_care_home)
            & people_col.is_not_null()
            & (people_col > 0)
        )
        .then(people_col * ratio)
        .alias(IndCQC.pir_filled_posts_model)
    )


def compute_global_ratio(lf: pl.LazyFrame) -> float:
    """
    Computes the global ratio of filled posts to PIR people using only valid rows.
    """
    row = (
        lf.filter(
            (pl.col(IndCQC.care_home) == CareHome.not_care_home)
            & people_col.is_not_null()
            & (people_col > 0)
            & posts_col.is_not_null()
            & (posts_col > 0)
        )
        .select(
            [
                posts_col.sum().alias("posts_sum"),
                people_col.sum().alias("people_sum"),
            ]
        )
        .collect()
        .row(0)
    )

    posts_sum, people_sum = row

    if people_sum == 0 or posts_sum is None:
        raise ValueError("No valid rows available to compute PIR ratio.")

    return posts_sum / people_sum
