from dataclasses import dataclass

from utils.column_values.categorical_columns_by_dataset import (
    FeatureEngineeringCategoricalValues as CatValues,
)


@dataclass
class FeatureEngineeringValueLabelsRuralUrban:
    """The possible values of the current rui feature in the independent CQC estimates pipeline"""

    column_name: str = CatValues.current_rui_column_values.column_name

    care_home_labels_dict = {
        "rui_rural_hamlet": CatValues.current_rui_column_values.rural_hamlet,
        "rui_rural_hamlet_sparse": CatValues.current_rui_column_values.rural_hamlet_sparse,
        "rui_rural_town": CatValues.current_rui_column_values.rural_town,
        "rui_rural_town_sparse": CatValues.current_rui_column_values.rural_town_sparse,
        "rui_rural_village": CatValues.current_rui_column_values.rural_village,
        "rui_rural_village_sparse": CatValues.current_rui_column_values.rural_village_sparse,
        "rui_urban_city": CatValues.current_rui_column_values.urban_city,
        "rui_urban_city_sparse": CatValues.current_rui_column_values.urban_city_sparse,
        "rui_urban_major": CatValues.current_rui_column_values.urban_major,
        "rui_urban_minor": CatValues.current_rui_column_values.urban_minor,
    }

    non_res_model_labels_dict = {
        # "rui_rural_hamlet": CatValues.current_rui_column_values.rural_hamlet,
        # "rui_rural_hamlet_sparse": CatValues.current_rui_column_values.rural_hamlet_sparse,
        # "rui_rural_town": CatValues.current_rui_column_values.rural_town,
        # "rui_rural_town_sparse": CatValues.current_rui_column_values.rural_town_sparse,
        # "rui_rural_village": CatValues.current_rui_column_values.rural_village,
        # "rui_rural_village_sparse": CatValues.current_rui_column_values.rural_village_sparse,
        "rui_urban_city": CatValues.current_rui_column_values.urban_city,
        # "rui_urban_city_sparse": CatValues.current_rui_column_values.urban_city_sparse,
        "rui_urban_major": CatValues.current_rui_column_values.urban_major,
        "rui_urban_minor": CatValues.current_rui_column_values.urban_minor,
    }
