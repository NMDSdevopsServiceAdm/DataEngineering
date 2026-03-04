from dataclasses import dataclass

from utils.column_names.cleaned_data_files.ons_cleaned import OnsCleanedColumns
from utils.column_values.categorical_column_values import RUI


@dataclass
class OnspdRuralUrbanIndicator2011:
    """The possible values of the rural urban indicator 2011 column in ONS Postcode Directory data"""

    column_name: str = OnsCleanedColumns.rural_urban_indicator_2011

    labels_dict = {
        "1": RUI.urban_major,
        "2": RUI.urban_minor,
        "3": RUI.urban_city,
        "4": RUI.urban_city_sparse,
        "5": RUI.rural_town,
        "6": RUI.rural_town_sparse,
        "7": RUI.rural_village,
        "8": RUI.rural_village_sparse,
        "9": RUI.rural_hamlet,
        "10": RUI.rural_hamlet_sparse,
    }
