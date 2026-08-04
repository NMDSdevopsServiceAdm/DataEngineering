import unittest

from utils.column_values.categorical_column_values import (
    Dormancy,
    PublishedJobRoleLabels,
)


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


class TestPublishedJobRoleLabels:
    def test_pins_the_published_job_role_label_values(self):
        test_object = PublishedJobRoleLabels("test_column")
        expected_values = [
            "senior_management",
            "registered_manager",
            "social_worker",
            "senior_care_worker",
            "care_worker",
            "community_support_and_outreach",
            "occupational_therapist",
            "registered_nurse",
            "allied_health_professional",
            "deputy_manager",
            "support_worker",
            "other_managers",
            "other_regulated_professions",
            "other_direct_care",
            "other",
        ]
        assert test_object.categorical_values == expected_values


if __name__ == "__main__":
    unittest.main(warnings="ignore")
