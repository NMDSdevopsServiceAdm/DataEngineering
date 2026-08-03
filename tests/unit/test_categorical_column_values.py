import unittest

from utils.column_values.categorical_column_values import Dormancy, PublishedJobRoleID


class ListValuesTests(unittest.TestCase):
    def setUp(self) -> None:
        self.expected_categorical_values = [Dormancy.dormant, Dormancy.not_dormant]
        self.expected_str_filtered_categorical_values = [Dormancy.dormant]
        self.expected_list_filtered_categorical_values = []

    def test_list_values_initialises_a_list_of_values_for_the_column_when_no_values_to_remove_are_given(
        self,
    ):
        test_object = Dormancy("test_column")
        self.assertEqual(
            test_object.categorical_values, self.expected_categorical_values
        )

    def test_list_values_initialises_a_list_of_values_for_the_column_when_string_value_to_remove_is_given(
        self,
    ):
        test_object = Dormancy("test_column", value_to_remove="N")

        self.assertEqual(
            test_object.categorical_values,
            self.expected_str_filtered_categorical_values,
        )

    def test_list_values_initialises_a_list_of_values_for_the_column_when_list_of_values_to_remove_are_given(
        self,
    ):
        test_object = Dormancy("test_column", value_to_remove=["N", "Y"])
        self.assertEqual(
            test_object.categorical_values,
            self.expected_list_filtered_categorical_values,
        )


class CountValuesTests(unittest.TestCase):
    def setUp(self) -> None:
        self.expected_count = 2
        self.expected_filtered_count = 1
        self.expected_count_with_null_vales = 3
        self.expected_filtered_count_with_null_values = 2

    def test_count_values_initialises_a_count_of_values_for_the_column_when_no_values_to_remove_are_given_and_no_null_values_are_included(
        self,
    ):
        test_object = Dormancy("test_column")
        self.assertEqual(test_object.count_of_categorical_values, self.expected_count)

    def test_count_values_initialises_a_count_of_values_for_the_column_when_values_to_remove_are_given_and_no_null_values_are_included(
        self,
    ):
        test_object = Dormancy("test_column", value_to_remove="N")
        self.assertEqual(
            test_object.count_of_categorical_values, self.expected_filtered_count
        )

    def test_count_values_initialises_a_count_of_values_for_the_column_when_no_values_to_remove_are_given_and_null_values_are_included(
        self,
    ):
        test_object = Dormancy("test_column", contains_null_values=True)
        self.assertEqual(
            test_object.count_of_categorical_values, self.expected_count_with_null_vales
        )

    def test_count_values_initialises_a_count_of_values_for_the_column_when_values_to_remove_are_given_and_null_values_are_included(
        self,
    ):
        test_object = Dormancy(
            "test_column", value_to_remove="N", contains_null_values=True
        )
        self.assertEqual(
            test_object.count_of_categorical_values,
            self.expected_filtered_count_with_null_values,
        )


class TestPublishedJobRoleID:
    def test_pins_the_published_job_role_id_values(self):
        test_object = PublishedJobRoleID("test_column")
        expected_values = [
            "1",
            "4",
            "6",
            "7",
            "8",
            "9",
            "15",
            "16",
            "17",
            "43",
            "52",
            "1001",
            "1002",
            "1003",
            "1004",
        ]
        assert test_object.categorical_values == expected_values


if __name__ == "__main__":
    unittest.main(warnings="ignore")
