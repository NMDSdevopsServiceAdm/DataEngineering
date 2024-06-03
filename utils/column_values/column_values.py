from dataclasses import dataclass


@dataclass
class ColumnValues:
    column_name: str
    values_list: list
    values_count: int

    def list_values(self):
        self.values_list = list(self.__annotations__.keys())

    def count_values(self):
        self.values_count = len(self.list_values())
