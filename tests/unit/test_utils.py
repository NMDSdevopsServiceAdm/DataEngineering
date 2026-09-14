import unittest
from enum import Enum
from pathlib import Path

import boto3
from botocore.stub import Stubber
from pyspark.sql.types import StringType, StructField, StructType

from tests.base_test import SparkBaseTest
from tests.test_file_data import UtilsData
from tests.test_file_schemas import UtilsSchema
from utils import utils
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
    example_parquet_path = "tests/test_data/example_parquet.parquet"

    def setUp(self):
        self.df = self.spark.createDataFrame(
            [
                ("1", "a", "tuna", "28/11/1993"),
                ("2", "b", "salmon", "12/12/2012"),
                ("3", "c", "cod", "18/1/2021"),
            ],
            ["col_a", "col_b", "col_c", "date_col"],
        )


class GeneralUtilsTests(UtilsTests):
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
        parquet_dir = self.get_temp_path("test_parquet")
        utils.write_to_parquet(self.df, parquet_dir)

        self.assertTrue(Path(parquet_dir).is_dir())
        self.assertTrue(Path(parquet_dir).joinpath("_SUCCESS").exists())


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
