from pyspark.sql import DataFrame, functions as F

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

from utils.column_values.categorical_column_values import (
    SpecialistGeneralistOther,
    Specialisms,
)


def classify_specialisms(
    df: DataFrame,
    dementia_column_name: str,
    lda_column_name: str,
    mh_column_name: str,
) -> DataFrame:
    df = df.withColumn(
        dementia_column_name,
        F.when(
            F.array_contains(F.col(IndCQC.specialisms_offered), Specialisms.dementia)
            & (F.size(F.col(IndCQC.specialisms_offered)) == 1),
            SpecialistGeneralistOther.specialist,
        )
        .when(
            F.array_contains(F.col(IndCQC.specialisms_offered), Specialisms.dementia)
            & (F.size(F.col(IndCQC.specialisms_offered)) > 1),
            SpecialistGeneralistOther.generalist,
        )
        .otherwise(SpecialistGeneralistOther.other),
    )

    df = df.withColumn(
        lda_column_name,
        F.when(
            F.array_contains(
                F.col(IndCQC.specialisms_offered), Specialisms.learning_disabilities
            )
            & (F.size(F.col(IndCQC.specialisms_offered)) == 1),
            SpecialistGeneralistOther.specialist,
        )
        .when(
            F.array_contains(
                F.col(IndCQC.specialisms_offered), Specialisms.learning_disabilities
            )
            & (F.size(F.col(IndCQC.specialisms_offered)) > 1),
            SpecialistGeneralistOther.generalist,
        )
        .otherwise(SpecialistGeneralistOther.other),
    )

    df = df.withColumn(
        mh_column_name,
        F.when(
            F.array_contains(
                F.col(IndCQC.specialisms_offered), Specialisms.mental_health
            )
            & (F.size(F.col(IndCQC.specialisms_offered)) == 1),
            SpecialistGeneralistOther.specialist,
        )
        .when(
            F.array_contains(
                F.col(IndCQC.specialisms_offered), Specialisms.mental_health
            )
            & (F.size(F.col(IndCQC.specialisms_offered)) > 1),
            SpecialistGeneralistOther.generalist,
        )
        .otherwise(SpecialistGeneralistOther.other),
    )

    return df
