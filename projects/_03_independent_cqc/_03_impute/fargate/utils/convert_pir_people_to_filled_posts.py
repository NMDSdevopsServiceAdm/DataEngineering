import polars as pl

from polars_utils.expressions import is_not_care_home
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

posts_col = pl.col(IndCQC.ascwds_filled_posts_dedup_clean)
people_col = pl.col(IndCQC.pir_people_directly_employed_dedup)


def convert_pir_to_filled_posts(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Converts PIR people to filled posts using a global ratio.

    The ratio is calculated using only valid rows and then applied only to
    non-care home locations where PIR people is present.

    Args:
        lf (pl.LazyFrame): input dataframe with PIR people and ASC-WDS
            filled posts.

    Returns:
        pl.LazyFrame: input dataframe with estimated PIR filled posts.
    """
    ratio = compute_global_ratio()

    return lf.with_columns(
        pl.when(is_not_care_home() & people_col.is_not_null() & (people_col > 0))
        .then(people_col * ratio)
        .alias(IndCQC.pir_filled_posts_model)
    )


def compute_global_ratio() -> pl.Expr:
    """
    Builds a lazy expression for the global ratio of filled posts to PIR
    people using only valid rows.

    Valid rows are defined as locations which:
        - are non-care home
        - have non-null and greater than zero values for both PIR people and
          ASC-WDS filled posts
        - have a ratio of filled posts to PIR people greater than or equal to
          0.75. In theory, the number of filled posts should always be greater
          than or equal to the number of people. We filter out rows where this
          ratio is less than 0.75 to exclude potentially poor quality data
          whilst allowing for some variance in the relationship.

    Invalid rows are masked to null rather than filtered out, so the
    resulting expression can be evaluated as part of the same lazy chain as
    the frame it will be applied to, rather than requiring an early collect.

    Returns:
        pl.Expr: A scalar expression for the global ratio, which broadcasts
            across all rows when used in a `with_columns`/`when` context.
    """
    quality_filter_expr = posts_col.truediv(people_col) >= 0.75

    valid_row_expr = (
        is_not_care_home()
        & people_col.is_not_null()
        & (people_col > 0)
        & posts_col.is_not_null()
        & (posts_col > 0)
        & quality_filter_expr
    )

    valid_posts_sum = pl.when(valid_row_expr).then(posts_col).otherwise(None).sum()
    valid_people_sum = pl.when(valid_row_expr).then(people_col).otherwise(None).sum()

    return valid_posts_sum.truediv(valid_people_sum)
