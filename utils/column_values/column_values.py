from dataclasses import dataclass, asdict


@dataclass
class ColumnValues:
    column_name: str

    def __post_init__(self):
        self.categorical_values = self.list_values()
        self.count_of_categorical_values = self.count_values()

    def list_values(self) -> list:
        dict_values = asdict(self)
        dict_values.pop("column_name")
        list_values = list(dict_values.values())
        return list_values

    def count_values(self) -> int:
        count = len(self.categorical_values)
        return count
