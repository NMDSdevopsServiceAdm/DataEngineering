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


class AddTimeRegisteredForIntoDfTests(LocationsFeatureEngineeringTests):
    def setUp(self) -> None:
        super().setUp()

    def test_add_time_registered_into_df(self):
        test_df = self.spark.createDataFrame(
            Data.add_time_registered_rows, Schemas.add_time_registered_schema
        )
        returned_df = job.add_time_registered_into_df(df=test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_add_time_registered_rows,
            Schemas.expected_add_time_registered_schema,
        )

        self.assertEqual(expected_df.collect(), returned_df.collect())


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
