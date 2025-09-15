# from enum import Enum, EnumType

# import pointblank as pb

# from polars_utils.logger import get_logger
# from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
#     CqcLocationCleanedColumns as CQCLClean,
# )
# from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
# from utils.column_names.raw_data_files.cqc_location_api_columns import (
#     NewCqcLocationApiColumns as CQCL,
# )
# from utils.column_names.validation_table_columns import (
#     Validation,
# )
# from utils.column_values.categorical_columns_by_dataset import (
#     LocationsApiCleanedCategoricalValues as CatValues,
# )

# logger = get_logger(__name__)


# class Rules(list[str], Enum):
#     pass


# class LocationsCategorical(tuple[str, list[str]], Enum):
#     care_home = (
#         CQCLClean.care_home,
#         CatValues.care_home_column_values.categorical_values,
#     )
#     cqc_sector = (
#         CQCLClean.cqc_sector,
#         CatValues.sector_column_values.categorical_values,
#     )
#     registration_status = (
#         CQCLClean.registration_status,
#         CatValues.registration_status_column_values.categorical_values,
#     )
#     dormancy = (CQCLClean.dormancy, CatValues.dormancy_column_values.categorical_values)
#     primary_service_type = (
#         CQCLClean.primary_service_type,
#         CatValues.primary_service_type_column_values.categorical_values,
#     )
#     contemporary_cssr = (
#         CQCLClean.contemporary_cssr,
#         CatValues.contemporary_cssr_column_values.categorical_values,
#     )
#     contemporary_region = (
#         CQCLClean.contemporary_region,
#         CatValues.contemporary_region_column_values.categorical_values,
#     )
#     current_cssr = (
#         CQCLClean.current_cssr,
#         CatValues.current_cssr_column_values.categorical_values,
#     )
#     current_region = (
#         CQCLClean.current_region,
#         CatValues.current_region_column_values.categorical_values,
#     )
#     current_rural_urban_ind_11 = (
#         CQCLClean.current_rural_urban_ind_11,
#         CatValues.current_rui_column_values.categorical_values,
#     )
#     related_location = (
#         CQCLClean.related_location,
#         CatValues.related_location_column_values.categorical_values,
#     )

#     def __init__(self, column, set_values):
#         self.column = column
#         self.set_values = set_values


# class LocationsMinValues(tuple[str, int], Enum):
#     number_of_beds = (CQCLClean.number_of_beds, 0)
#     time_registered = (CQCLClean.time_registered, 1)
#     location_id_length = (Validation.location_id_length, 3)
#     provider_id_length = (Validation.provider_id_length, 3)

#     def __init__(self, column, min_value):
#         self.column = column
#         self.min_value = min_value


# class LocationsMaxValues(tuple[str, int], Enum):
#     number_of_beds = (CQCLClean.number_of_beds, 500)
#     location_id_length = (Validation.location_id_length, 14)
#     provider_id_length = (Validation.provider_id_length, 14)

#     def __init__(self, column, max_value):
#         self.column = column
#         self.min_value = max_value


# class LocationsTuples(Rules):
#     col_vals_ge: LocationsMinValues
#     col_vals_le: LocationsMaxValues


# class LocationsRawLists(Rules):
#     cols_not_null = [CQCL.location_id, Keys.import_date, CQCL.name]
#     rows_distinct = [CQCL.location_id, Keys.import_date]


# class LocationsRaw(Enum):
#     lists: LocationsRawLists


# class LocationsMaps(Enum):
#     col_vals_in_set: LocationsCategorical


# class LocationsCleaned(Enum):
#     lists: LocationsRawLists
#     tuples: LocationsTuples
#     maps: LocationsMaps


# class ValidationDatasets(Enum):
#     locations_raw = LocationsRaw
#     locations_cleaned = LocationsCleaned


# class Validator(pb.Validate):
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)

#     def add_checks(self, rules_parent: EnumType) -> "Validator":
#         for data_types, enum_class in rules_parent.__members__.items():
#             match data_types:
#                 case "lists":
#                     self = self.add_list_rules(enum_class)

#                 case _:
#                     logger.warning(f"Unrecognised rule set {data_types}")
#         return self

#     def add_list_rules(self, rule_set: EnumType) -> "Validator":
#         for rule_type, values in rule_set.__members__.items():
#             func = getattr(self, rule_type)
#             self = func(values)
#         return self

#     def add_tuple_rules(self, rule_set: EnumType) -> "Validator":
#         for rule_type, values in rule_set.__members__.items():
#             func = getattr(self, rule_type)
#             self = func(*values)
#         return self

#     def add_map_rules(self, rule_set: EnumType) -> "Validator":
#         for rule_type, values in rule_set.__members__.items():
#             func = getattr(self, rule_type)
#             self = func()
#         return self
