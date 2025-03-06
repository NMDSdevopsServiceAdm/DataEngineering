from dataclasses import dataclass

from utils.column_values.categorical_columns_by_dataset import (
    FeatureEngineeringCategoricalValues as CatValues,
)


@dataclass
class FeatureEngineeringValueLabelsSpecialisms:
    """The possible values of the services feature in the independent CQC estimates pipeline"""

    column_name: str = CatValues.specialisms_column_values.column_name

    labels_dict = {
        "specialism_adults_over_65": CatValues.specialisms_column_values.adults_over_65,
        "specialism_adults_under_65": CatValues.specialisms_column_values.adults_under_65,
        "specialism_children": CatValues.specialisms_column_values.children,
        "specialism_dementia": CatValues.specialisms_column_values.dementia,
        "specialism_detained_under_mental_health_act": CatValues.specialisms_column_values.detained_under_mental_health_act,
        "specialism_eating_disorders": CatValues.specialisms_column_values.eating_disorders,
        "specialism_learning_disabilities": CatValues.specialisms_column_values.learning_disabilities,
        "specialism_mental_health": CatValues.specialisms_column_values.mental_health,
        "specialism_physical_disabilities": CatValues.specialisms_column_values.physical_disabilities,
        "specialism_sensory_impairment": CatValues.specialisms_column_values.sensory_impairment,
        "specialism_substance_misuse": CatValues.specialisms_column_values.substance_misuse,
        "specialism_whole_population": CatValues.specialisms_column_values.whole_population,
    }
