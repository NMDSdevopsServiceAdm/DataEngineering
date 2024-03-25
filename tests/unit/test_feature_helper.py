from datetime import date
import unittest
import warnings

from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

from utils.utils import get_spark
from utils.features.helper import (
    add_date_diff_into_df,
    convert_categorical_variable_to_binary_variables_based_on_a_dictionary,
    add_service_count_to_data,
    vectorise_dataframe,
)


class LocationsFeatureEngineeringTests(unittest.TestCase):
    def setUp(self):
        self.spark = get_spark()

        warnings.simplefilter("ignore", ResourceWarning)
        return super().setUp()

    def test_add_date_diff_into_df(self):
        date_col = "import_date"
        date_diff = "date_diff"
        df = self.spark.createDataFrame(
            [[date(2013, 1, 10)], [date(2023, 1, 10)]], [date_col]
        )
        result = add_date_diff_into_df(
            df=df, new_col_name=date_diff, import_date_col=date_col
        )
        expected_max_date = date(2023, 1, 10)
        actual_max_date = result.agg(F.max(date_col)).first()[0]

        expected_diff_between_max_date_and_other_date = 3652
        actual_diff = (
            result.filter(F.col(date_col) == date(2013, 1, 10))
            .select(F.col(date_diff))
            .collect()
        )

        self.assertEqual(actual_max_date, expected_max_date)
        self.assertEqual(
            actual_diff[0].date_diff, expected_diff_between_max_date_and_other_date
        )

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
        result = convert_categorical_variable_to_binary_variables_based_on_a_dictionary(
            df=df,
            categorical_col_name="rui_2011",
            lookup_dict={
                "indicator_1": "Rural hamlet and isolated dwellings in a sparse setting"
            },
        )
        result_rows = result.collect()
        self.assertEqual(result_rows[0].indicator_1, 1)

    def test_add_service_count_to_data(self):
        cols = ["location", "services"]
        new_col_name = "service_count"
        rows = [("1", ["service_1", "service_2", "service_3"]), ("2", ["service_1"])]
        df = self.spark.createDataFrame(rows, cols)

        result = add_service_count_to_data(
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

    def test_vectorisation(self):
        val_1 = "Gateshead"
        val_2 = "London"
        val_3 = "Leeds"

        col_list = {val_1, val_2, val_3}

        cols = ["locationid", "region"]
        rows = [("1", val_1)]
        df = self.spark.createDataFrame(rows, cols)

        exploded_df, regions = explode_column_from_distinct_values(
            df=df, column_name="region", col_prefix="ons_", col_list_set=col_list
        )

        result = vectorise_dataframe(df=exploded_df, list_for_vectorisation=regions)

        # vector to array is a feature of spark 3.0.0
        rows = result.withColumn(
            "features", vector_to_array(F.col("features"))
        ).collect()

        result = rows[0].features
        result.sort()

        expected_vector_list = [0.0, 0.0, 1.0]

        self.assertEqual(expected_vector_list, result)
