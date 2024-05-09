import unittest
import warnings
from unittest.mock import ANY, Mock, patch

from utils import utils

from tests.test_file_data import PAFilledPostsByICBArea as TestData
from tests.test_file_schemas import PAFilledPostsByICBAreaSchema as TestSchema

from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DPColNames,
)
from utils.column_names.cleaned_data_files.ons_cleaned_values import (
    OnsCleanedColumns as ONSClean,
)

import jobs.split_pa_filled_posts_into_icb_areas as job


class SplitPAFilledPostsIntoICBAreas(unittest.TestCase):
    TEST_ONS_SOURCE = "some/directory"
    TEST_PA_SOURCE = "some/directory"
    TEST_DESTINATION = "some/directory"

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_sample_ons_rows = self.spark.createDataFrame(
            TestData.ons_sample_contemporary_rows,
            schema=TestSchema.ons_sample_contemporary_schema,
        )
        self.test_sample_pa_filled_post_rows = self.spark.createDataFrame(
            TestData.pa_sample_filled_post_rows,
            schema=TestSchema.pa_sample_filled_post_schema,
        )


class MainTests(SplitPAFilledPostsIntoICBAreas):
    def setUp(self) -> None:
        super().setUp()

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main(self, read_from_parquet_mock: Mock, write_to_parquet_mock: Mock):
        read_from_parquet_mock.side_effect = [
            self.test_sample_ons_rows,
            self.test_sample_pa_filled_post_rows,
        ]
        job.main(self.TEST_ONS_SOURCE, self.TEST_PA_SOURCE, self.TEST_DESTINATION)
        self.assertEqual(read_from_parquet_mock.call_count, 2)
        write_to_parquet_mock.assert_called_once_with(
            ANY,
            self.TEST_DESTINATION,
            mode="overwrite",
            partitionKeys=[DPColNames.YEAR],
        )


class CountPostcodesPerListOfColumns(SplitPAFilledPostsIntoICBAreas):
    def setUp(self) -> None:
        super().setUp()

        self.returned_postcode_count_by_la = job.count_postcodes_per_list_of_columns(
            self.test_sample_ons_rows,
            [ONSClean.contemporary_ons_import_date, ONSClean.contemporary_cssr],
            DPColNames.COUNT_OF_DISTINCT_POSTCODES_PER_LA,
        ).sort([ONSClean.postcode, ONSClean.contemporary_ons_import_date])

        self.returned_postcode_count_by_la_and_icb = (
            job.count_postcodes_per_list_of_columns(
                self.test_sample_ons_rows,
                [
                    ONSClean.contemporary_ons_import_date,
                    ONSClean.contemporary_cssr,
                    ONSClean.contemporary_icb,
                ],
                DPColNames.COUNT_OF_DISTINCT_POSTCODES_PER_HYBRID_AREA,
            ).sort([ONSClean.postcode, ONSClean.contemporary_ons_import_date])
        )

    def test_count_postcodes_per_list_of_columns_adds_new_column_with_given_name(
        self,
    ):
        self.assertTrue(
            DPColNames.COUNT_OF_DISTINCT_POSTCODES_PER_LA
            in self.returned_postcode_count_by_la.columns
        )

    def test_count_postcodes_per_list_of_columns_has_expected_values_when_grouped_by_import_date_and_la(
        self,
    ):
        expected_postcode_count_per_la_rows = self.spark.createDataFrame(
            TestData.expected_postcode_count_per_la_rows,
            schema=TestSchema.expected_postcode_count_per_la_schema,
        ).sort([ONSClean.postcode, ONSClean.contemporary_ons_import_date])

        self.assertEqual(
            self.returned_postcode_count_by_la.collect(),
            expected_postcode_count_per_la_rows.collect(),
        )

    def test_count_postcodes_per_list_of_columns_has_expected_values_when_grouped_by_import_date_and_la_and_icb(
        self,
    ):
        expected_postcode_count_per_la_icb_rows = self.spark.createDataFrame(
            TestData.expected_postcode_count_per_la_icb_rows,
            schema=TestSchema.expected_postcode_count_per_la_icb_schema,
        ).sort([ONSClean.postcode, ONSClean.contemporary_ons_import_date])

        self.assertEqual(
            self.returned_postcode_count_by_la_and_icb.collect(),
            expected_postcode_count_per_la_icb_rows.collect(),
        )


class CreateRatioBetweenColumns(SplitPAFilledPostsIntoICBAreas):
    def setUp(self) -> None:
        super().setUp()

        self.sample_df = self.spark.createDataFrame(
            TestData.sample_rows_with_la_and_hybrid_area_postcode_counts,
            schema=TestSchema.sample_rows_with_la_and_hybrid_area_postcode_counts_schema,
        )

        self.returned_df = (
            job.create_ratio_between_hybrid_area_and_la_area_postcode_counts(
                self.sample_df,
            )
        )

        self.expected_df = self.spark.createDataFrame(
            TestData.expected_ratio_between_hybrid_area_and_la_area_postcodes_rows,
            schema=TestSchema.expected_ratio_between_hybrid_area_and_la_area_postcodes_schema,
        )

        self.returned_rows = self.returned_df.sort("GroupID").collect()
        self.expected_rows = self.expected_df.sort("GroupID").collect()

    def test_create_ratio_between_hybrid_area_and_la_area_postcode_counts_has_expected_columns(
        self,
    ):
        self.assertEqual(
            self.returned_df.columns,
            self.expected_df.columns,
        )

    def test_create_ratio_between_hybrid_area_and_la_area_postcode_counts_has_expected_length(
        self,
    ):
        self.assertEqual(len(self.returned_rows), len(self.expected_rows))

    def test_create_ratio_between_hybrid_area_and_la_area_postcode_counts_has_expected_values_when_given_hybrid_and_la_counts(
        self,
    ):
        for i in range(len(self.returned_rows)):
            self.assertAlmostEqual(
                self.returned_rows[i][
                    DPColNames.PROPORTION_OF_ICB_POSTCODES_IN_LA_AREA
                ],
                self.expected_rows[i][
                    DPColNames.PROPORTION_OF_ICB_POSTCODES_IN_LA_AREA
                ],
                3,
                "rows are not almost equal",
            )
