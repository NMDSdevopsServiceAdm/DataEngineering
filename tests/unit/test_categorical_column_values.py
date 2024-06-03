import unittest

from utils.column_values.categorical_column_values import ColumnValues, Dormancy


class ColumnValuesTests(unittest.TestCase):
    def setUp(self) -> None:
        return super().setUp()


class ListValuesTests(ColumnValuesTests):
    def setUp(self) -> None:
        super().setUp()
        self.expected_categorical_values = [Dormancy.dormant, Dormancy.not_dormant]
        self.expected_filtered_categorical_values = [Dormancy.dormant]

    def test_list_values_initialises_a_list_of_values_for_the_column_when_no_values_to_remove_are_given(
        self,
    ):
        test_object = Dormancy("test_column")
        self.assertEqual(
            test_object.categorical_values, self.expected_categorical_values
        )

    def test_list_values_initialises_a_list_of_values_for_the_column_when_no_values_to_remove_are_given(
        self,
    ):
        test_object = Dormancy("test_column", value_to_remove="N")
        self.assertEqual(
            test_object.categorical_values, self.expected_filtered_categorical_values
        )


class CountValuesTests(ColumnValuesTests):
    def setUp(self) -> None:
        super().setUp()
        self.expected_count = 2
        self.expected_filtered_count = 1
        self.expected_count_with_null_vales = 3
        self.expected_filtered_count_with_null_values = 4

    def test_count_values_initialises_a_count_of_values_for_the_column_when_no_values_to_remove_are_given_and_no_null_values_are_included(
        self,
    ):
        test_object = Dormancy("test_column")
        self.assertEqual(test_object.categorical_values, self.expected_count)

    def test_count_values_initialises_a_count_of_values_for_the_column_when_values_to_remove_are_given_and_no_null_values_are_included(
        self,
    ):
        test_object = Dormancy("test_column", value_to_remove="N")
        self.assertEqual(test_object.categorical_values, self.expected_filtered_count)

    def test_count_values_initialises_a_count_of_values_for_the_column_when_no_values_to_remove_are_given_and_null_values_are_included(
        self,
    ):
        test_object = Dormancy("test_column", contains_null_values=True)
        self.assertEqual(
            test_object.categorical_values, self.expected_count_with_null_vales
        )

    def test_count_values_initialises_a_count_of_values_for_the_column_when_values_to_remove_are_given_and_null_values_are_included(
        self,
    ):
        test_object = Dormancy(
            "test_column", value_to_remove="N", contains_null_values=True
        )
        self.assertEqual(
            test_object.categorical_values,
            self.expected_filtered_count_with_null_values,
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
