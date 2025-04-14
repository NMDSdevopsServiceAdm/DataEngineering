from datetime import date
from pathlib import Path
import shutil
import unittest
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    DateType,
)

from tests.test_file_data import UtilsData, CQCPirCleanedData
from tests.test_file_schemas import UtilsSchema, CQCPIRCleanSchema

from utils import utils
from utils.column_names.cleaned_data_files.cqc_pir_cleaned import (
    CqcPIRCleanedColumns,
)

from utils.column_names.raw_data_files.cqc_provider_api_columns import (
    CqcProviderApiColumns as CQCColNames,
)


class UtilsTests(unittest.TestCase):
    test_csv_path = "tests/test_data/example_csv.csv"
    tmp_dir = "tmp-out"
    TEST_ASCWDS_WORKPLACE_FILE = "tests/test_data/tmp-workplace"
    example_csv_for_schema_tests_with_datetype = (
        "tests/test_data/example_csv_for_schema_tests_with_datetype.csv"
    )
    example_parquet_path = "tests/test_data/example_parquet.parquet"

    def setUp(self):
        self.spark = utils.get_spark()
        self.df = self.spark.read.csv(self.test_csv_path, header=True)
        self.pir_cleaned_test_df: DataFrame = self.spark.createDataFrame(
            data=CQCPirCleanedData.subset_for_latest_submission_date_before_filter,
            schema=CQCPIRCleanSchema.clean_subset_for_grouping_by,
        )
        self.test_grouping_list = [
            F.col(CqcPIRCleanedColumns.location_id),
            F.col(CqcPIRCleanedColumns.care_home),
            F.col(CqcPIRCleanedColumns.cqc_pir_import_date),
        ]
        self.pir_cleaned_test_date_column = F.col(
            CqcPIRCleanedColumns.pir_submission_date_as_date
        )

    def tearDown(self):
        try:
            shutil.rmtree(self.tmp_dir)
            shutil.rmtree(self.TEST_ASCWDS_WORKPLACE_FILE)
        except OSError:
            pass  # Ignore dir does not exist


class GeneralUtilsTests(UtilsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_get_model_name_returns_model_name(self):
        path_to_model = (
            "s3://sfc-bucket/models/care_home_jobs_prediction/1.0.0/subfolder/"
        )
        model_name = utils.get_model_name(path_to_model)
        expected_model_name = "care_home_jobs_prediction"

        self.assertEqual(expected_model_name, model_name)

    def test_read_from_parquet_imports_all_rows(self):
        df = utils.read_from_parquet(self.example_parquet_path)

        self.assertEqual(df.count(), 2270)

    def test_read_from_parquet_imports_all_columns_when_column_list_is_None(self):
        df = utils.read_from_parquet(self.example_parquet_path)

        self.assertCountEqual(
            df.columns,
            [
                CQCColNames.postal_address_line1,
                CQCColNames.companies_house_number,
                CQCColNames.constituency,
                CQCColNames.postal_address_county,
                CQCColNames.deregistration_date,
                CQCColNames.inspection_directorate,
                CQCColNames.onspd_latitude,
                CQCColNames.local_authority,
                CQCColNames.location_ids,
                CQCColNames.onspd_longitude,
                CQCColNames.name,
                CQCColNames.organisation_type,
                CQCColNames.ownership_type,
                CQCColNames.main_phone_number,
                CQCColNames.postal_code,
                CQCColNames.provider_id,
                CQCColNames.region,
                CQCColNames.registration_date,
                CQCColNames.registration_status,
                CQCColNames.postal_address_town_city,
                CQCColNames.type,
                CQCColNames.uprn,
            ],
        )

    def test_read_from_parquet_only_imports_selected_columns(self):
        column_list = [
            CQCColNames.provider_id,
            CQCColNames.name,
            CQCColNames.registration_status,
        ]

        df = utils.read_from_parquet(
            self.example_parquet_path, selected_columns=column_list
        )

        self.assertCountEqual(
            df.columns,
            [
                CQCColNames.provider_id,
                CQCColNames.name,
                CQCColNames.registration_status,
            ],
        )

    def test_write(self):
        df = utils.read_csv(self.test_csv_path)
        utils.write_to_parquet(df, self.tmp_dir)

        self.assertTrue(Path("tmp-out").is_dir())
        self.assertTrue(Path("tmp-out/_SUCCESS").exists())

    def test_format_date_fields(self):
        self.assertEqual(self.df.select("date_col").first()[0], "28/11/1993")
        formatted_df = utils.format_date_fields(self.df, raw_date_format="dd/MM/yyyy")
        self.assertEqual(type(formatted_df.select("date_col").first()[0]), date)
        self.assertEqual(formatted_df.select("date_col").first()[0], date(1993, 11, 28))

    def test_format_date_fields_can_handle_timestamps_as_strings(self):
        test_rows = [
            ("loc 1", "2011-01-19 00:00:00"),
            ("loc 2", "2011-01-19"),
        ]
        test_schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("date_column", StringType(), True),
            ]
        )
        test_df = self.spark.createDataFrame(test_rows, test_schema)
        returned_df = utils.format_date_fields(test_df, raw_date_format="yyyy-MM-dd")
        expected_rows = [
            (
                "loc 1",
                date(2011, 1, 19),
            ),
            ("loc 2", date(2011, 1, 19)),
        ]
        expected_schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("date_column", DateType(), True),
            ]
        )
        expected_data = self.spark.createDataFrame(
            expected_rows, expected_schema
        ).collect()
        returned_data = returned_df.collect()
        self.assertEqual(expected_data, returned_data)

    def test_create_unix_timestamp_variable_from_date_column(self):
        column_schema = StructType(
            [
                StructField("locationid", StringType(), False),
                StructField("snapshot_date", StringType(), False),
            ]
        )
        row = [
            ("1-000000001", "2023-01-01"),
        ]
        df = self.spark.createDataFrame(row, schema=column_schema)
        df = utils.create_unix_timestamp_variable_from_date_column(
            df, "snapshot_date", "yyyy-MM-dd", "snapshot_date_unix_conv"
        )
        self.assertEqual(
            df.columns, ["locationid", "snapshot_date", "snapshot_date_unix_conv"]
        )

        df = df.orderBy("locationid").collect()
        self.assertEqual(df[0]["snapshot_date_unix_conv"], 1672531200)

    def test_convert_days_to_unix_time(self):
        self.assertEqual(utils.convert_days_to_unix_time(1), 86400)
        self.assertEqual(utils.convert_days_to_unix_time(90), 7776000)


class LatestDatefieldForGroupingTests(UtilsTests):
    def setup(self) -> None:
        super(LatestDatefieldForGroupingTests, self).setUp()

    def test_latest_datefield_for_grouping_raises_error_for_non_list_of_columns(
        self,
    ):
        bad_grouping_list = [
            "location_id",
            F.col(CqcPIRCleanedColumns.care_home),
            F.col(CqcPIRCleanedColumns.cqc_pir_import_date),
        ]

        with self.assertRaises(TypeError) as context:
            utils.latest_datefield_for_grouping(
                self.pir_cleaned_test_df,
                bad_grouping_list,
                self.pir_cleaned_test_date_column,
            )

        self.assertTrue(
            "List items must be of pyspark.sql.Column type" in str(context.exception),
        )

    def test_latest_datefield_for_grouping_raises_error_for_non_column_param(
        self,
    ):
        bad_date_column = CqcPIRCleanedColumns.pir_submission_date_as_date

        with self.assertRaises(TypeError) as context:
            utils.latest_datefield_for_grouping(
                self.pir_cleaned_test_df, self.test_grouping_list, bad_date_column
            )

        self.assertTrue(
            "Column must be of pyspark.sql.Column type" in str(context.exception),
        )

    def test_latest_datefield_for_grouping_returns_latest_date_df_correctly(
        self,
    ):
        after_df = utils.latest_datefield_for_grouping(
            self.pir_cleaned_test_df,
            self.test_grouping_list,
            self.pir_cleaned_test_date_column,
        )

        # Ensure earlier submission date row exists before and is removed
        self.assertTrue(
            self.pir_cleaned_test_df.selectExpr(
                'ANY(cqc_pir_submission_date="2023-05-12") as date_present'
            )
            .collect()[0]
            .date_present
        )
        self.assertFalse(
            after_df.selectExpr(
                'ANY(cqc_pir_submission_date="2023-05-12") as date_present'
            )
            .collect()[0]
            .date_present
        )
        # Ensure different carehome indicator doesn't count as duplicate
        self.assertTrue(
            self.pir_cleaned_test_df.selectExpr('ANY(carehome="N") as non_care_home')
            .collect()[0]
            .non_care_home
        )
        self.assertTrue(
            after_df.selectExpr('ANY(carehome="N") as non_care_home')
            .collect()[0]
            .non_care_home
        )
        # No other rows are removed
        self.assertEqual(after_df.count(), 5)

    def test_normalise_column_values(self):
        rows = [("lower_case"), ("with spaces "), ("uppe r_ca se")]
        test_df = self.spark.createDataFrame(rows, StringType())

        returned_df = utils.normalise_column_values(test_df, "value")

        self.assertEqual(returned_df.collect()[0][0], "LOWER_CASE")
        self.assertEqual(returned_df.collect()[1][0], "WITHSPACES")
        self.assertEqual(returned_df.collect()[2][0], "UPPER_CASE")


class FilterDataframeToMaximumValueTests(UtilsTests):
    def setUp(self) -> None:
        super().setUp()

        self.df = self.spark.createDataFrame(
            UtilsData.filter_to_max_value_rows,
            UtilsSchema.filter_to_max_value_schema,
        )

    def test_filter_df_to_maximum_value_in_column_filters_correctly_with_date(self):
        returned_df = utils.filter_df_to_maximum_value_in_column(
            self.df, "date_type_column"
        )

        expected_df = self.spark.createDataFrame(
            UtilsData.expected_filter_to_max_date_rows,
            UtilsSchema.filter_to_max_value_schema,
        )

        returned_data = returned_df.sort("ID").collect()
        expected_data = expected_df.sort("ID").collect()

        self.assertEqual(expected_data, returned_data)

    def test_filter_df_to_maximum_value_in_column_filters_correctly_with_string(self):
        returned_df = utils.filter_df_to_maximum_value_in_column(
            self.df, "import_date_style_col"
        )

        expected_df = self.spark.createDataFrame(
            UtilsData.expected_filter_to_max_string_rows,
            UtilsSchema.filter_to_max_value_schema,
        )

        returned_data = returned_df.sort("ID").collect()
        expected_data = expected_df.sort("ID").collect()

        self.assertEqual(expected_data, returned_data)


class SelectRowsWithValueTests(UtilsTests):
    def setUp(self) -> None:
        super().setUp()

        self.df = self.spark.createDataFrame(
            UtilsData.select_rows_with_value_rows,
            UtilsSchema.select_rows_with_value_schema,
        )

        self.returned_df = utils.select_rows_with_value(
            self.df, "value_to_filter_on", "keep"
        )

        self.returned_ids = (
            self.returned_df.select("id").rdd.flatMap(lambda x: x).collect()
        )

    def test_select_rows_with_value_selects_rows_with_value(self):
        self.assertTrue("id_1" in self.returned_ids)

    def test_select_rows_with_value_drops_other_rows(self):
        self.assertFalse("id_2" in self.returned_ids)

    def test_select_rows_with_value_does_not_change_columns(self):
        self.assertEqual(
            self.returned_df.schema, UtilsSchema.select_rows_with_value_schema
        )


class SelectRowsWithNonNullValueTests(UtilsTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_df = self.spark.createDataFrame(
            UtilsData.select_rows_with_non_null_values_rows,
            UtilsSchema.select_rows_with_non_null_values_schema,
        )

    def test_select_rows_returns_expected_non_null_rows(self):
        returned_df = utils.select_rows_with_non_null_value(
            self.test_df, "column_with_nulls"
        )
        expected_df = self.spark.createDataFrame(
            UtilsData.expected_select_rows_with_non_null_values_rows,
            UtilsSchema.select_rows_with_non_null_values_schema,
        )

        returned_data = returned_df.sort("id").collect()
        expected_data = expected_df.collect()

        self.assertEqual(returned_data, expected_data)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
