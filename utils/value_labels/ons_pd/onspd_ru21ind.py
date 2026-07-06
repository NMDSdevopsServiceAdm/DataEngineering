from dataclasses import dataclass

from utils.column_names.cleaned_data_files.ons_cleaned import OnsCleanedColumns
from utils.column_values.categorical_column_values import RUI2021


@dataclass
class OnspdRuralUrbanIndicator2021:
    """The possible values of the rural urban indicator 2021 column in ONS Postcode Directory data"""

    column_name: str = OnsCleanedColumns.rural_urban_indicator_2021

    labels_dict = {
        "1": RUI2021.rural_larger_further,
        "2": RUI2021.rural_larger_nearer,
        "3": RUI2021.rural_smaller_further,
        "4": RUI2021.rural_smaller_nearer,
        "5": RUI2021.urban_further,
        "6": RUI2021.urban_nearer,
    }
