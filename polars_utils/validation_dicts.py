# from enum import Enum, EnumType

# import pointblank as pb

# from polars_utils.logger import get_logger
# from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
# from utils.column_names.raw_data_files.cqc_location_api_columns import (
#     NewCqcLocationApiColumns as CQCL,
# )

# logger = get_logger(__name__)


# class Rules(list[str], Enum):
#     pass


# class ListRules(Rules):
#     cols_not_null = [CQCL.location_id, Keys.import_date, CQCL.name]
#     rows_distinct = [CQCL.location_id, Keys.import_date]


# class LocationsRaw(Enum):
#     lists: ListRules


# class ValidationDatasets(Enum):
#     locations_raw = LocationsRaw


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
