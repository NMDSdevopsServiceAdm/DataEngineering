from dataclasses import dataclass


@dataclass
class RuleNames:
    size_of_dataset: str = "size_of_dataset"
    complete_columns: str = "complete_columns"
    index_columns: str = "index_columns"
    min_values: str = "min_values"
    max_values: str = "max_values"
    categorical_values_in_columns: str = "categorical_values_in_columns"
    distinct_values: str = "distinct_values"
