from dataclasses import dataclass

from utils.column_values.categorical_columns_by_dataset import (
    FeatureEngineeringCategoricalValues as CatValues,
)


@dataclass
class FeatureEngineeringValueLabelsSpecialisms:
    """The possible values of the specialisms feature in the independent CQC estimates pipeline"""

    column_name: str = CatValues.specialisms_column_values.column_name

    labels_dict = {
        "specialism_adults_over_65": CatValues.specialisms_column_values.adults_over_65,
        "specialism_adults_under_65": CatValues.specialisms_column_values.adults_under_65,
        "specialism_children": CatValues.specialisms_column_values.children,
        "specialism_dementia": CatValues.specialisms_column_values.dementia,
        "specialism_learning_disabilities": CatValues.specialisms_column_values.learning_disabilities,
        "specialism_mental_health": CatValues.specialisms_column_values.mental_health,
        "specialism_physical_disabilities": CatValues.specialisms_column_values.physical_disabilities,
        "specialism_whole_population": CatValues.specialisms_column_values.whole_population,
    }
