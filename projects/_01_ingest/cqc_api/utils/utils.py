from pyspark.sql import DataFrame, functions as F

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

from utils.column_values.categorical_column_values import (
    SpecialistGeneralistOther,
)


def classify_specialisms(
    df: DataFrame,
    specialism: str,
) -> DataFrame:
    """
    Classifies the specialisms offered and creates a new column for that specialism to indicate if it's specialist/generalist/other
    Specialist - when the named specialism is the only one offered by the location.
    Generalist - when the named specialism is offered alongside others.
    Other - the named specialism is not offered.

    Args:
        df (DataFrame): CQC registered locations DataFrame with the specialisms offered column.
        specialism (str): Unique specialism name we want to create new column for.

    Returns:
        DataFrame: Updated dataFrame with new column for specified specialism

    """
    new_column_name: str = f"specialist_generalist_other_{specialism}"
    new_column_name = new_column_name.replace(" ", "_").lower()
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
