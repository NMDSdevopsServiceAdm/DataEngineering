import unittest
from datetime import date, datetime
from enum import Enum
from pathlib import Path

import boto3
from botocore.stub import Stubber
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from tests.base_test import SparkBaseTest
from tests.test_file_data import UtilsData
from tests.test_file_schemas import UtilsSchema
from utils import utils
from utils.column_names.cleaned_data_files.cqc_pir_cleaned import CqcPIRCleanedColumns
from utils.column_names.raw_data_files.cqc_provider_api_columns import (
    CqcProviderApiColumns as CQCColNames,
)


class StubberType(Enum):
    client = "client"
    resource = "resource"


class StubberClass:
    __s3_client = None
    __s3_resource = None
    __stubber = None
    __type = ""

    def __init__(self, type):
        self.__type = type
        self.decide_type()

    def decide_type(self):
        if self.__type == StubberType.client:
            self.build_client()
            self.build_stubber_client()

        if self.__type == StubberType.resource:
            self.build_resource()
            self.build_stubber_resource()

    def get_s3_client(self):
        return self.__s3_client

    def get_s3_resource(self):
        return self.__s3_resource

    def get_stubber(self):
        return self.__stubber

    def build_client(self):
        self.__s3_client = boto3.client("s3")

    def build_resource(self):
        self.__s3_resource = boto3.resource("s3")

    def build_stubber_client(self):
        self.__stubber = Stubber(self.__s3_client)

    def build_stubber_resource(self):
        self.__stubber = Stubber(self.__s3_resource.meta.client)

    def add_response(self, stubbed_method, data, params):
        self.__stubber.add_response(stubbed_method, data, params)
        self.__stubber.activate()


class UtilsTests(SparkBaseTest):
    test_csv_path = "tests/test_data/example_csv.csv"
    test_csv_custom_delim_path = "tests/test_data/example_csv_custom_delimiter.csv"
    TEST_ASCWDS_WORKPLACE_FILE = "tests/test_data/tmp-workplace"
    example_csv_for_schema_tests = "tests/test_data/example_csv_for_schema_tests.csv"
    example_csv_for_schema_tests_extra_column = (
        "tests/test_data/example_csv_for_schema_tests_extra_column.csv"
    )
    example_csv_for_schema_tests_with_datetype = (
        "tests/test_data/example_csv_for_schema_tests_with_datetype.csv"
    )
    example_parquet_path = "tests/test_data/example_parquet.parquet"

    def setUp(self):
        self.df = self.spark.read.csv(self.test_csv_path, header=True)
        self.df_with_extra_col = self.spark.read.csv(
            self.example_csv_for_schema_tests_extra_column, header=True
        )
        self.pir_cleaned_test_df: DataFrame = self.spark.createDataFrame(
            data=UtilsData.cqc_pir_rows,
            schema=UtilsSchema.cqc_pir_schema,
        )
        self.test_grouping_list = [
            F.col(CqcPIRCleanedColumns.location_id),
            F.col(CqcPIRCleanedColumns.care_home),
            F.col(CqcPIRCleanedColumns.cqc_pir_import_date),
        ]
        self.pir_cleaned_test_date_column = F.col(
            CqcPIRCleanedColumns.pir_submission_date_as_date
        )


class GeneralUtilsTests(UtilsTests):
    def test_generate_s3_datasets_dir_date_path_changes_version_when_version_number_is_passed(
        self,
    ):
        dec_first_21 = datetime(2021, 12, 1)
        version_number = "2.0.0"
        dir_path = utils.generate_s3_datasets_dir_date_path(
            "s3://sfc-main-datasets",
            "test_domain",
            "test_dateset",
            dec_first_21,
            version_number,
        )
        self.assertEqual(
            dir_path,
            "s3://sfc-main-datasets/domain=test_domain/dataset=test_dateset/version=2.0.0/year=2021/month=12/day=01/import_date=20211201/",
        )

    def test_generate_s3_datasets_dir_date_path_uses_version_one_when_no_version_number_is_passed(
        self,
    ):
        dec_first_21 = datetime(2021, 12, 1)
        dir_path = utils.generate_s3_datasets_dir_date_path(
            "s3://sfc-main-datasets", "test_domain", "test_dateset", dec_first_21
        )
        self.assertEqual(
            dir_path,
            "s3://sfc-main-datasets/domain=test_domain/dataset=test_dateset/version=1.0.0/year=2021/month=12/day=01/import_date=20211201/",
        )

    def test_read_csv(self):
        df = utils.read_csv(self.test_csv_path)
        self.assertEqual(df.columns, ["col_a", "col_b", "col_c", "date_col"])
        self.assertEqual(df.count(), 3)

    def test_read_csv_with_defined_schema(self):
        schema = StructType(
            [
                StructField("string_field", StringType(), True),
                StructField("integer_field", IntegerType(), True),
                StructField("float_field", FloatType(), True),
            ]
        )

        df = utils.read_csv_with_defined_schema(
            self.example_csv_for_schema_tests, schema
        )
        self.assertEqual(df.columns[0], "string_field")
        self.assertEqual(df.columns[1], "integer_field")
        self.assertEqual(df.columns[2], "float_field")
        row_one = df.collect()[0]
        assert isinstance(row_one.string_field, str)
        assert isinstance(row_one.integer_field, int)
        assert isinstance(row_one.float_field, float)

    def test_read_csv_with_defined_schema_with_null_values_in_csv(self):
        schema = StructType(
            [
                StructField("string_field", StringType(), False),
                StructField("integer_field", IntegerType(), False),
                StructField("float_field", FloatType(), False),
            ]
        )

        df = utils.read_csv_with_defined_schema(
            self.example_csv_for_schema_tests, schema
        )
        self.assertEqual(df.columns[0], "string_field")
        self.assertEqual(df.columns[1], "integer_field")
        self.assertEqual(df.columns[2], "float_field")
        row_two = df.collect()[1]
        assert isinstance(row_two.string_field, type(None))
        assert isinstance(row_two.integer_field, type(None))
        assert isinstance(row_two.float_field, type(None))

    def test_read_csv_with_defined_schema_with_column_missing_in_csv(self):
        schema = StructType(
            [
                StructField("string_field", StringType(), False),
                StructField("integer_field", IntegerType(), False),
                StructField("float_field", FloatType(), False),
                StructField("missing_field", StringType(), True),
            ]
        )

        df = utils.read_csv_with_defined_schema(
            self.example_csv_for_schema_tests, schema
        )
        self.assertEqual(df.columns[0], "string_field")
        self.assertEqual(df.columns[1], "integer_field")
        self.assertEqual(df.columns[2], "float_field")
        self.assertEqual(df.columns[3], "missing_field")

    def test_read_csv_with_defined_schema_with_extra_column_in_csv(self):
        schema = StructType(
            [
                StructField("string_field", StringType(), False),
                StructField("integer_field", IntegerType(), False),
                StructField("float_field", FloatType(), False),
            ]
        )

        df_with_no_schema = self.df_with_extra_col

        df = utils.read_csv_with_defined_schema(
            self.example_csv_for_schema_tests_extra_column, schema
        )
        self.assertEqual(df.columns[0], "string_field")
        self.assertEqual(df.columns[1], "integer_field")
        self.assertEqual(df.columns[2], "float_field")

        self.assertTrue(len(df.columns) < len(df_with_no_schema.columns))

    def test_read_csv_with_defined_schema_where_there_is_incorrect_value_type(self):
        schema = StructType(
            [
                StructField("string_field", IntegerType(), False),
                StructField("integer_field", StringType(), False),
                StructField("float_field", FloatType(), False),
            ]
        )

        df = utils.read_csv_with_defined_schema(
            self.example_csv_for_schema_tests, schema
        )

        row_one = df.collect()[0]
        assert isinstance(row_one.string_field, type(None))
        assert isinstance(row_one.integer_field, str)
        assert isinstance(row_one.float_field, float)

    def test_read_with_custom_delimiter(self):
        df = utils.read_csv(self.test_csv_custom_delim_path, "|")

        self.assertEqual(df.columns, ["col_a", "col_b", "col_c"])
        self.assertEqual(df.count(), 3)

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

    def test_read_from_parquet_applies_provided_schema(self):
        schema = StructType(
            [
                StructField(CQCColNames.provider_id, StringType(), True),
                StructField(CQCColNames.name, StringType(), True),
                StructField(CQCColNames.postal_code, StringType(), True),
            ]
        )
        df = utils.read_from_parquet(
            self.example_parquet_path,
            schema=schema,
        )
        self.assertCountEqual(
            df.columns,
            [
                CQCColNames.provider_id,
                CQCColNames.name,
                CQCColNames.postal_code,
            ],
        )

    def test_read_from_parquet_with_schema_and_column_list(self):
        schema = StructType(
            [
                StructField(CQCColNames.name, StringType(), True),
                StructField(CQCColNames.provider_id, StringType(), True),
                StructField(CQCColNames.region, StringType(), True),
            ]
        )
        column_list = [CQCColNames.name, CQCColNames.region]

        df = utils.read_from_parquet(
            self.example_parquet_path, selected_columns=column_list, schema=schema
        )

        self.assertCountEqual(df.columns, column_list)

    def test_read_from_parquet_with_schema_extra_column_not_in_parquet_ignored(self):
        schema = StructType(
            [
                StructField(CQCColNames.name, StringType(), True),
                StructField("extra_col", StringType(), True),
            ]
        )

        df = utils.read_from_parquet(self.example_parquet_path, schema=schema)

        self.assertCountEqual(df.columns, [CQCColNames.name, "extra_col"])
        null_count = df.filter(df["extra_col"].isNotNull()).count()
        self.assertEqual(null_count, 0)

    def test_read_from_parquet_with_empty_schema_imports_all_columns(self):
        schema = StructType([])

        df = utils.read_from_parquet(self.example_parquet_path, schema=schema)

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

    def test_write(self):
        df = utils.read_csv(self.test_csv_path)
        parquet_dir = self.get_temp_path("test_parquet")
        utils.write_to_parquet(df, parquet_dir)

        self.assertTrue(Path(parquet_dir).is_dir())
        self.assertTrue(Path(parquet_dir).joinpath("_SUCCESS").exists())

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


if __name__ == "__main__":
    unittest.main(warnings="ignore")
