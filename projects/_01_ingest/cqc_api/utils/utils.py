from pyspark.sql import DataFrame, functions as F

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

from utils.column_values.categorical_column_values import (
    SpecialistGeneralistOther,
)


def classify_specialisms(
    df: DataFrame,
    new_column_name: str,
    specialism: str,
) -> DataFrame:
    df = df.withColumn(
        new_column_name,
        F.when(
            F.array_contains(F.col(IndCQC.specialisms_offered), specialism)
            & (F.size(F.col(IndCQC.specialisms_offered)) == 1),
            SpecialistGeneralistOther.specialist,
        )
        .when(
            F.array_contains(F.col(IndCQC.specialisms_offered), specialism)
            & (F.size(F.col(IndCQC.specialisms_offered)) > 1),
            SpecialistGeneralistOther.generalist,
        )
        .otherwise(SpecialistGeneralistOther.other),
    )

    return df
