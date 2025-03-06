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
        returned_data = returned_df.sort(IndCQC.location_id).collect()
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


class AddArrayColumnCountToDataTests(LocationsFeatureEngineeringTests):
    def setUp(self) -> None:
        super().setUp()

    def test_add_array_column_count_to_data(self):
        cols = ["location", "services"]
        new_col_name = "service_count"
        rows = [("1", ["service_1", "service_2", "service_3"]), ("2", ["service_1"])]
        df = self.spark.createDataFrame(rows, cols)

        result = job.add_array_column_count_to_data(
            df=df, new_col_name=new_col_name, col_to_check="services"
        )
        rows = result.collect()

        self.assertTrue(
            rows[0].asDict()
            == {
                "location": "1",
                "services": ["service_1", "service_2", "service_3"],
                "service_count": 3,
            }
        )
        self.assertTrue(
            rows[1].asDict()
            == {"location": "2", "services": ["service_1"], "service_count": 1}
        )


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
