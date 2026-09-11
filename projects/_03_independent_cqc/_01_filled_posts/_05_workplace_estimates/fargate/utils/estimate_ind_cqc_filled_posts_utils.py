import polars as pl

from polars_utils.column_types import CategoricalColumnTypes as CatColType
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


def estimate_filled_posts_cast_expr() -> pl.Expr:
    """Expression casting estimate_filled_posts to Float32."""
    return pl.col(IndCQC.estimate_filled_posts).cast(pl.Float32)


def estimate_filled_posts_source_cast_expr() -> pl.Expr:
    """Expression casting estimate_filled_posts_source to its Enum type."""
    return pl.col(IndCQC.estimate_filled_posts_source).cast(
        CatColType.EstimatesFilledPostSourceEnumType
    )
