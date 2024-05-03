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
        self.expected_postcode_count_per_la_rows = self.spark.createDataFrame(
            TestData.expected_postcode_count_per_la_rows,
            schema=TestSchema.expected_postcode_count_per_la_schema,
        )
        self.expected_postcode_count_per_la_icb_rows = self.spark.createDataFrame(
            TestData.expected_postcode_count_per_la_icb_rows,
            schema=TestSchema.expected_postcode_count_per_la_icb_schema,
        )
        self.test_sample_rows_with_la_and_hybrid_area_postcode_counts = self.spark.createDataFrame(
            TestData.sample_rows_with_la_and_hybrid_area_postcode_counts,
            schema=TestSchema.sample_rows_with_la_and_hybrid_area_postcode_counts_schema,
        )
        self.test_expected_ratio_between_hybrid_area_and_la_area_postcodes_rows = self.spark.createDataFrame(
            TestData.expected_ratio_between_hybrid_area_and_la_area_postcodes_rows,
            schema=TestSchema.expected_ratio_between_hybrid_area_and_la_area_postcodes_schema,
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

    def test_count_postcodes_per_list_of_columns_adds_new_column_with_given_name_when_given_la_name(
        self,
    ):
        self.assertTrue(
            DPColNames.COUNT_OF_DISTINCT_POSTCODES_PER_LA
            in job.count_postcodes_per_list_of_columns(
                self.test_sample_ons_rows,
                [ONSClean.contemporary_ons_import_date, ONSClean.contemporary_cssr],
                DPColNames.COUNT_OF_DISTINCT_POSTCODES_PER_LA,
            ).columns
        )

    def test_count_postcodes_per_list_of_columns_adds_new_column_with_given_name_when_given_la_icb_name(
        self,
    ):
        self.assertTrue(
            DPColNames.COUNT_OF_DISTINCT_POSTCODES_PER_HYBRID_AREA
            in job.count_postcodes_per_list_of_columns(
                self.test_sample_ons_rows,
                [
                    ONSClean.contemporary_ons_import_date,
                    ONSClean.contemporary_cssr,
                    ONSClean.contemporary_icb,
                ],
                DPColNames.COUNT_OF_DISTINCT_POSTCODES_PER_HYBRID_AREA,
            ).columns
        )

    def test_count_postcodes_per_list_of_columns_has_expected_values_when_grouped_by_la_only(
        self,
    ):
        self.assertEqual(
            job.count_postcodes_per_list_of_columns(
                self.test_sample_ons_rows,
                [ONSClean.contemporary_ons_import_date, ONSClean.contemporary_cssr],
                DPColNames.COUNT_OF_DISTINCT_POSTCODES_PER_LA,
            )
            .sort([ONSClean.postcode, ONSClean.contemporary_ons_import_date])
            .collect(),
            self.expected_postcode_count_per_la_rows.sort(
                [ONSClean.postcode, ONSClean.contemporary_ons_import_date]
            ).collect(),
        )

    def test_count_postcodes_per_list_of_columns_has_expected_values_when_grouped_by_la_and_icb(
        self,
    ):
        self.assertEqual(
            job.count_postcodes_per_list_of_columns(
                self.test_sample_ons_rows,
                [
                    ONSClean.contemporary_ons_import_date,
                    ONSClean.contemporary_cssr,
                    ONSClean.contemporary_icb,
                ],
                DPColNames.COUNT_OF_DISTINCT_POSTCODES_PER_HYBRID_AREA,
            )
            .sort([ONSClean.postcode, ONSClean.contemporary_ons_import_date])
            .collect(),
            self.expected_postcode_count_per_la_icb_rows.sort(
                [ONSClean.postcode, ONSClean.contemporary_ons_import_date]
            ).collect(),
        )


class CreateRatioBetweenColumns(SplitPAFilledPostsIntoICBAreas):
    def setUp(self) -> None:
        super().setUp()

    def test_create_ratio_between_columns_adds_new_column_with_given_name_when_given_hybrid_and_la_counts(
        self,
    ):
        self.assertTrue(
            DPColNames.RATIO_HYBRID_AREA_TO_LA_AREA_POSTCODES
            in job.create_ratio_between_columns(
                self.test_sample_rows_with_la_and_hybrid_area_postcode_counts,
                DPColNames.COUNT_OF_DISTINCT_POSTCODES_PER_HYBRID_AREA,
                DPColNames.COUNT_OF_DISTINCT_POSTCODES_PER_LA,
                DPColNames.RATIO_HYBRID_AREA_TO_LA_AREA_POSTCODES,
            ).columns
        )

    def test_create_ratio_between_columns_has_expected_values_when_given_hybrid_and_la_counts(
        self,
    ):
        returned_rows = (
            job.create_ratio_between_columns(
                self.test_sample_rows_with_la_and_hybrid_area_postcode_counts,
                DPColNames.COUNT_OF_DISTINCT_POSTCODES_PER_HYBRID_AREA,
                DPColNames.COUNT_OF_DISTINCT_POSTCODES_PER_LA,
                DPColNames.RATIO_HYBRID_AREA_TO_LA_AREA_POSTCODES,
            )
            .sort([ONSClean.postcode, ONSClean.contemporary_ons_import_date])
            .collect()
        )

        expected_rows = self.test_expected_ratio_between_hybrid_area_and_la_area_postcodes_rows.sort(
            [ONSClean.postcode, ONSClean.contemporary_ons_import_date]
        ).collect()

        self.assertAlmostEqual(
            returned_rows[0][DPColNames.RATIO_HYBRID_AREA_TO_LA_AREA_POSTCODES],
            expected_rows[0][DPColNames.RATIO_HYBRID_AREA_TO_LA_AREA_POSTCODES],
            3,
            "rows are not almost equal",
        )
        self.assertAlmostEqual(
            returned_rows[1][DPColNames.RATIO_HYBRID_AREA_TO_LA_AREA_POSTCODES],
            expected_rows[1][DPColNames.RATIO_HYBRID_AREA_TO_LA_AREA_POSTCODES],
            3,
            "rows are not almost equal",
        )
        self.assertAlmostEqual(
            returned_rows[2][DPColNames.RATIO_HYBRID_AREA_TO_LA_AREA_POSTCODES],
            expected_rows[2][DPColNames.RATIO_HYBRID_AREA_TO_LA_AREA_POSTCODES],
            3,
            "rows are not almost equal",
        )
        self.assertAlmostEqual(
            returned_rows[3][DPColNames.RATIO_HYBRID_AREA_TO_LA_AREA_POSTCODES],
            expected_rows[3][DPColNames.RATIO_HYBRID_AREA_TO_LA_AREA_POSTCODES],
            3,
            "rows are not almost equal",
        )
        self.assertAlmostEqual(
            returned_rows[4][DPColNames.RATIO_HYBRID_AREA_TO_LA_AREA_POSTCODES],
            expected_rows[4][DPColNames.RATIO_HYBRID_AREA_TO_LA_AREA_POSTCODES],
            3,
            "rows are not almost equal",
        )
        self.assertAlmostEqual(
            returned_rows[5][DPColNames.RATIO_HYBRID_AREA_TO_LA_AREA_POSTCODES],
            expected_rows[5][DPColNames.RATIO_HYBRID_AREA_TO_LA_AREA_POSTCODES],
            3,
            "rows are not almost equal",
        )
        self.assertAlmostEqual(
            returned_rows[6][DPColNames.RATIO_HYBRID_AREA_TO_LA_AREA_POSTCODES],
            expected_rows[6][DPColNames.RATIO_HYBRID_AREA_TO_LA_AREA_POSTCODES],
            3,
            "rows are not almost equal",
        )
        self.assertAlmostEqual(
            returned_rows[7][DPColNames.RATIO_HYBRID_AREA_TO_LA_AREA_POSTCODES],
            expected_rows[7][DPColNames.RATIO_HYBRID_AREA_TO_LA_AREA_POSTCODES],
            3,
            "rows are not almost equal",
        )
        self.assertAlmostEqual(
            returned_rows[8][DPColNames.RATIO_HYBRID_AREA_TO_LA_AREA_POSTCODES],
            expected_rows[8][DPColNames.RATIO_HYBRID_AREA_TO_LA_AREA_POSTCODES],
            3,
            "rows are not almost equal",
        )
        self.assertAlmostEqual(
            returned_rows[9][DPColNames.RATIO_HYBRID_AREA_TO_LA_AREA_POSTCODES],
            expected_rows[9][DPColNames.RATIO_HYBRID_AREA_TO_LA_AREA_POSTCODES],
            3,
            "rows are not almost equal",
        )
        self.assertAlmostEqual(
            returned_rows[10][DPColNames.RATIO_HYBRID_AREA_TO_LA_AREA_POSTCODES],
            expected_rows[10][DPColNames.RATIO_HYBRID_AREA_TO_LA_AREA_POSTCODES],
            3,
            "rows are not almost equal",
        )
        self.assertAlmostEqual(
            returned_rows[11][DPColNames.RATIO_HYBRID_AREA_TO_LA_AREA_POSTCODES],
            expected_rows[11][DPColNames.RATIO_HYBRID_AREA_TO_LA_AREA_POSTCODES],
            3,
            "rows are not almost equal",
        )
        self.assertAlmostEqual(
            returned_rows[12][DPColNames.RATIO_HYBRID_AREA_TO_LA_AREA_POSTCODES],
            expected_rows[12][DPColNames.RATIO_HYBRID_AREA_TO_LA_AREA_POSTCODES],
            3,
            "rows are not almost equal",
        )
        self.assertAlmostEqual(
            returned_rows[13][DPColNames.RATIO_HYBRID_AREA_TO_LA_AREA_POSTCODES],
            expected_rows[13][DPColNames.RATIO_HYBRID_AREA_TO_LA_AREA_POSTCODES],
            3,
            "rows are not almost equal",
        )
