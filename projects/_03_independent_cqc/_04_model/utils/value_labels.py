from dataclasses import dataclass

from utils.column_values.categorical_columns_by_dataset import (
    FeatureEngineeringCategoricalValues as CatValues,
)


@dataclass
class RegionLabels:
    """The possible values of the current_region feature in the independent CQC estimates pipeline"""

    column_name: str = CatValues.current_region_column_values.column_name

    labels_dict = {
        "region_east_midlands": CatValues.current_region_column_values.east_midlands,
        "region_eastern": CatValues.current_region_column_values.eastern,
        "region_london": CatValues.current_region_column_values.london,
        "region_north_east": CatValues.current_region_column_values.north_east,
        "region_north_west": CatValues.current_region_column_values.north_west,
        "region_south_east": CatValues.current_region_column_values.south_east,
        "region_south_west": CatValues.current_region_column_values.south_west,
        "region_west_midlands": CatValues.current_region_column_values.west_midlands,
        "region_yorkshire_and_the_humber": CatValues.current_region_column_values.yorkshire_and_the_humber,
    }


@dataclass
class RelatedLocationLabels:
    """The possible values of the related_location feature in the independent CQC estimates pipeline"""

    column_name: str = CatValues.related_location_column_values.column_name

    labels_dict = {
        "no_related_location": CatValues.related_location_column_values.no_related_location,
    }


@dataclass
class RuralUrbanLabels:
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

    non_res_labels_dict = {
        "rui_urban_city": CatValues.current_rui_column_values.urban_city,
        "rui_urban_major": CatValues.current_rui_column_values.urban_major,
        "rui_urban_minor": CatValues.current_rui_column_values.urban_minor,
        "rui_sparse": "Sparse setting",
    }


@dataclass
class ServicesLabels:
    """The possible values of the services feature in the independent CQC estimates pipeline"""

    column_name: str = CatValues.services_column_values.column_name

    care_home_labels_dict = {
        "service_care_home_with_nursing": CatValues.services_column_values.care_home_service_with_nursing,
        "service_care_home_without_nursing": CatValues.services_column_values.care_home_service_without_nursing,
        "service_domiciliary": CatValues.services_column_values.domiciliary_care_service,
        "service_extra_care_housing": CatValues.services_column_values.extra_care_housing_services,
        "service_shared_lives": CatValues.services_column_values.shared_lives,
        "service_specialist_college": CatValues.services_column_values.specialist_college_service,
        "service_supported_living": CatValues.services_column_values.supported_living_service,
    }

    non_res_labels_dict = {
        "service_domiciliary": CatValues.services_column_values.domiciliary_care_service,
        "service_extra_care_housing": CatValues.services_column_values.extra_care_housing_services,
        "service_shared_lives": CatValues.services_column_values.shared_lives,
        "service_supported_living": CatValues.services_column_values.supported_living_service,
    }


@dataclass
class SpecialismsLabels:
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
