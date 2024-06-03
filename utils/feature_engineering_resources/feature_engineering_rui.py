from dataclasses import dataclass

from utils.column_values.categorical_columns_by_dataset import (
    FeatureEngineeringCategoricalValues as CatValues,
)


@dataclass
class FeatureEngineeringValueLabelsRUI:
    """The possible values of the current rui feature in the independent CQC estimates pipeline"""

    column_name: str = CatValues.current_rui_column_values.column_name

    labels_dict = {
        "indicator_1": CatValues.current_rui_column_values.rural_hamlet_sparse,
        "indicator_2": CatValues.current_rui_column_values.rural_hamlet,
        "indicator_3": CatValues.current_rui_column_values.rural_village,
        "indicator_4": CatValues.current_rui_column_values.rural_town_sparse,
        "indicator_5": CatValues.current_rui_column_values.rural_town,
        "indicator_6": CatValues.current_rui_column_values.urban_city_sparse,
        "indicator_7": CatValues.current_rui_column_values.urban_city,
        "indicator_8": CatValues.current_rui_column_values.urban_major,
        "indicator_9": CatValues.current_rui_column_values.urban_minor,
        "indicator_10": CatValues.current_rui_column_values.rural_village_sparse,
    }
