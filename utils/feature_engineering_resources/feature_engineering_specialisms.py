from dataclasses import dataclass

from utils.column_values.categorical_columns_by_dataset import (
    FeatureEngineeringCategoricalValues as CatValues,
)


@dataclass
class FeatureEngineeringValueLabelsSpecialisms:
    """The possible values of the specialisms feature in the independent CQC estimates pipeline"""

    column_name: str = CatValues.specialisms_column_values.column_name

    non_res_model_labels_dict = {
        "specialism_adults_over_65": "Caring for adults over 65 yrs",
        "specialism_adults_under_65": "Caring for adults under 65 yrs",
        "specialism_children": "Caring for children",
        "specialism_dementia": "Dementia",
        # "specialism_detained_under_mental_health_act": "Caring for people whose rights are restricted under the Mental Health Act",
        # "specialism_eating_disorders": "Eating disorders",
        "specialism_learning_disabilities": "Learning disabilities",
        "specialism_mental_health": "Mental health conditions",
        "specialism_physical_disabilities": "Physical disabilities",
        # "specialism_sensory_impairment": "Sensory impairment",
        # "specialism_substance_misuse": "Substance misuse problems",
        "specialism_whole_population": "Services for everyone",
    }
