import unittest
import warnings

from utils.utils import get_spark
from utils.features import helper as job
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)
from tests.test_file_data import ModelFeatures as Data
from tests.test_file_schemas import ModelFeatures as Schemas


class LocationsFeatureEngineeringTests(unittest.TestCase):
    def setUp(self):
        self.spark = get_spark()

        warnings.simplefilter("ignore", ResourceWarning)


class CalculateTimeRegisteredForTests(LocationsFeatureEngineeringTests):
    def setUp(self) -> None:
        super().setUp()

    def test_calculate_time_registered_returns_zero_when_dates_are_on_the_same_day(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.calculate_time_registered_same_day_rows,
            Schemas.calculate_time_registered_for_schema,
        )
        returned_df = job.calculate_time_registered_for(test_df)

        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_time_registered_same_day_rows,
            Schemas.expected_calculate_time_registered_for_schema,
        )
        returned_data = returned_df.collect()
        expected_data = expected_df.collect()

        self.assertEqual(returned_data, expected_data)

    def test_calculate_time_registered_returns_zero_when_dates_are_exact_years_apart(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.calculate_time_registered_exact_years_apart_rows,
            Schemas.calculate_time_registered_for_schema,
        )
        returned_df = job.calculate_time_registered_for(test_df)

        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_time_registered_exact_years_apart_rows,
            Schemas.expected_calculate_time_registered_for_schema,
        )
        returned_data = returned_df.sort(IndCQC.location_id).collect()
        expected_data = expected_df.collect()

        self.assertEqual(returned_data, expected_data)

    def test_calculate_time_registered_returns_zero_when_dates_are_one_day_less_than_a_full_year_apart(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.calculate_time_registered_one_day_less_than_a_full_year_apart_rows,
            Schemas.calculate_time_registered_for_schema,
        )
        returned_df = job.calculate_time_registered_for(test_df)

        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_time_registered_one_day_less_than_a_full_year_apart_rows,
            Schemas.expected_calculate_time_registered_for_schema,
        )
        returned_data = returned_df.sort(IndCQC.location_id).collect()
        expected_data = expected_df.collect()

        self.assertEqual(returned_data, expected_data)

    def test_calculate_time_registered_returns_zero_when_dates_are_one_day_more_than_a_full_year_apart(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.calculate_time_registered_one_day_more_than_a_full_year_apart_rows,
            Schemas.calculate_time_registered_for_schema,
        )
        returned_df = job.calculate_time_registered_for(test_df)

        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_time_registered_one_day_more_than_a_full_year_apart_rows,
            Schemas.expected_calculate_time_registered_for_schema,
        )
        returned_data = returned_df.sort(IndCQC.location_id).collect()
        expected_data = expected_df.collect()

        self.assertEqual(returned_data, expected_data)


class ConvertCategoricalVariableToBinaryVariablesBasedOnADictionaryTests(
    LocationsFeatureEngineeringTests
):
    def setUp(self) -> None:
        super().setUp()

    def test_convert_categorical_variable_to_binary_variables_based_on_a_dictionary(
        self,
    ):
        cols = ["location", "rui_2011"]
        rows = [
            (
                "1-0001",
                "Rural hamlet and isolated dwellings in a sparse setting",
            )
        ]
        df = self.spark.createDataFrame(rows, cols)
        result = job.convert_categorical_variable_to_binary_variables_based_on_a_dictionary(
            df=df,
            categorical_col_name="rui_2011",
            lookup_dict={
                "indicator_1": "Rural hamlet and isolated dwellings in a sparse setting"
            },
        )
        result_rows = result.collect()
        self.assertEqual(result_rows[0].indicator_1, 1)


class AddArrayColumnCountTests(LocationsFeatureEngineeringTests):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.add_array_column_count_with_one_element_rows,
            Schemas.add_array_column_count_schema,
        )
        self.returned_df = job.add_array_column_count(
            df=test_df,
            new_col_name=IndCQC.service_count,
            col_to_check=IndCQC.gac_service_types,
        )

    def test_add_array_column_count_adds_new_column(self):
        self.assertTrue(IndCQC.service_count in self.returned_df.columns)

    def test_add_array_column_count_returns_expected_data_when_one_element_in_array(
        self,
    ):
        expected_df = self.spark.createDataFrame(
            Data.expected_add_array_column_count_with_one_element_rows,
            Schemas.expected_add_array_column_count_schema,
        )
        self.assertEqual(self.returned_df.collect(), expected_df.collect())

    def test_add_array_column_count_returns_expected_data_when_multiple_elements_in_array(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.add_array_column_count_with_multiple_elements_rows,
            Schemas.add_array_column_count_schema,
        )
        returned_df = job.add_array_column_count(
            df=test_df,
            new_col_name=IndCQC.service_count,
            col_to_check=IndCQC.gac_service_types,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_add_array_column_count_with_multiple_elements_rows,
            Schemas.expected_add_array_column_count_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_add_array_column_count_returns_zero_when_array_is_empty(self):
        test_df = self.spark.createDataFrame(
            Data.add_array_column_count_with_empty_array_rows,
            Schemas.add_array_column_count_schema,
        )
        returned_df = job.add_array_column_count(
            df=test_df,
            new_col_name=IndCQC.service_count,
            col_to_check=IndCQC.gac_service_types,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_add_array_column_count_with_empty_array_rows,
            Schemas.expected_add_array_column_count_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_add_array_column_count_returns_zero_when_array_is_null(self):
        test_df = self.spark.createDataFrame(
            Data.add_array_column_count_with_null_value_rows,
            Schemas.add_array_column_count_schema,
        )
        returned_df = job.add_array_column_count(
            df=test_df,
            new_col_name=IndCQC.service_count,
            col_to_check=IndCQC.gac_service_types,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_add_array_column_count_with_null_value_rows,
            Schemas.expected_add_array_column_count_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())


class VectoriseDataframeTests(LocationsFeatureEngineeringTests):
    def setUp(self) -> None:
        super().setUp()

    def test_vectorise_dataframe(self):
        list_for_vectorisation = ["col_1", "col_2", "col_3"]

        df = self.spark.createDataFrame(
            Data.vectorise_input_rows, Schemas.vectorise_schema
        )

        output_df = job.vectorise_dataframe(
            df=df, list_for_vectorisation=list_for_vectorisation
        )
        output_data = (
            output_df.sort(IndCQC.location_id).select(IndCQC.features).collect()
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_vectorised_feature_rows,
            Schemas.expected_vectorised_feature_schema,
        )
        expected_data = (
            expected_df.sort(IndCQC.location_id).select(IndCQC.features).collect()
        )

        self.assertEqual(output_data, expected_data)


class CapIntegerAtMaxValueTests(LocationsFeatureEngineeringTests):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.cap_integer_at_max_value_rows, Schemas.cap_integer_at_max_value_schema
        )

        self.returned_df = job.cap_integer_at_max_value(
            df=test_df,
            col_name=IndCQC.service_count,
            max_value=2,
            new_col_name=IndCQC.service_count_capped,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_cap_integer_at_max_value_rows,
            Schemas.expected_cap_integer_at_max_value_schema,
        )
        self.returned_data = self.returned_df.sort(IndCQC.location_id).collect()
        self.expected_data = self.expected_df.collect()

    def test_cap_integer_at_max_value_returns_expected_columns(self):
        self.assertTrue(self.returned_df.columns, self.expected_df.columns)

    def test_cap_integer_at_max_value_returns_expected_data(self):
        self.assertEqual(self.returned_data, self.expected_data)


class AddDateIndexColumnTests(LocationsFeatureEngineeringTests):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.add_date_index_column_rows, Schemas.add_date_index_column_schema
        )
        self.returned_df = job.add_date_index_column(test_df)
        self.expected_df = self.spark.createDataFrame(
            Data.expected_add_date_index_column_rows,
            Schemas.expected_add_date_index_column_schema,
        )
        self.returned_data = self.returned_df.sort(IndCQC.location_id).collect()
        self.expected_data = self.expected_df.collect()

    def test_add_date_index_column_returns_expected_columns(self):
        self.assertTrue(self.returned_df.columns, self.expected_df.columns)

    def test_add_date_index_column_returns_expected_data(self):
        self.assertEqual(self.returned_data, self.expected_data)
