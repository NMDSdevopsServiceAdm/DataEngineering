from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


def add_source_description_to_source_column(
    input_df: DataFrame,
    populated_column_name: str,
    source_column_name: str,
    source_description: str,
) -> DataFrame:
    return input_df.withColumn(
        source_column_name,
        F.when(
            (
                F.col(populated_column_name).isNotNull()
                & F.col(source_column_name).isNull()
            ),
            source_description,
        ).otherwise(F.col(source_column_name)),
    )


def populate_estimate_filled_posts_and_source_in_the_order_of_the_column_list(
    df: DataFrame,
    order_of_models_to_populate_estimate_filled_posts_with: list,
) -> DataFrame:
    df = df.withColumn(IndCQC.estimate_filled_posts, F.lit(None).cast(IntegerType()))
    df = df.withColumn(
        IndCQC.estimate_filled_posts_source, F.lit(None).cast(StringType())
    )

    # TODO - replace for loop with better functionality
    # see https://trello.com/c/94jAj8cd/428-update-how-we-populate-estimates
    for model_name in order_of_models_to_populate_estimate_filled_posts_with:
        df = df.withColumn(
            IndCQC.estimate_filled_posts,
            F.when(
                (
                    F.col(IndCQC.estimate_filled_posts).isNull()
                    & (F.col(model_name).isNotNull())
                    & (F.col(model_name) >= 1.0)
                ),
                F.col(model_name),
            ).otherwise(F.col(IndCQC.estimate_filled_posts)),
        )

        df = add_source_description_to_source_column(
            df,
            IndCQC.estimate_filled_posts,
            IndCQC.estimate_filled_posts_source,
            model_name,
        )

    return df
