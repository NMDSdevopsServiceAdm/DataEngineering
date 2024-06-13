import unittest
import warnings
from unittest.mock import ANY, Mock, patch

from utils import utils

from tests.test_file_data import PAFilledPostsByIcbArea as TestData
from tests.test_file_schemas import PAFilledPostsByIcbAreaSchema as TestSchema

from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DPColNames,
)
from utils.column_names.cleaned_data_files.ons_cleaned import (
    OnsCleanedColumns as ONSClean,
)

import jobs.split_pa_filled_posts_into_icb_areas as job


class SplitPAFilledPostsIntoIcbAreas(unittest.TestCase):
    TEST_ONS_SOURCE = "some/directory"
    TEST_PA_SOURCE = "some/directory"
    TEST_DESTINATION = "some/directory"

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.sample_ons_contemporary_df = self.spark.createDataFrame(
            TestData.sample_ons_contemporary_rows,
            schema=TestSchema.sample_ons_contemporary_schema,
        )
        self.sample_pa_filled_posts_df = self.spark.createDataFrame(
            TestData.sample_pa_filled_posts_rows,
            schema=TestSchema.sample_pa_filled_posts_schema,
        )


class MainTests(SplitPAFilledPostsIntoIcbAreas):
    def setUp(self) -> None:
        super().setUp()

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main(self, read_from_parquet_mock: Mock, write_to_parquet_mock: Mock):
        read_from_parquet_mock.side_effect = [
            self.sample_ons_contemporary_df,
            self.sample_pa_filled_posts_df,
        ]
        job.main(self.TEST_ONS_SOURCE, self.TEST_PA_SOURCE, self.TEST_DESTINATION)
        self.assertEqual(read_from_parquet_mock.call_count, 2)
        write_to_parquet_mock.assert_called_once_with(
            ANY,
            self.TEST_DESTINATION,
            mode="overwrite",
            partitionKeys=[DPColNames.YEAR],
        )


class CountPostcodesPerListOfColumns(SplitPAFilledPostsIntoIcbAreas):
    def setUp(self) -> None:
        super().setUp()

        self.returned_postcode_count_by_la_df = job.count_postcodes_per_list_of_columns(
            self.sample_ons_contemporary_df,
            [ONSClean.contemporary_ons_import_date, ONSClean.contemporary_cssr],
            DPColNames.COUNT_OF_DISTINCT_POSTCODES_PER_LA,
        ).sort([ONSClean.postcode, ONSClean.contemporary_ons_import_date])

        self.returned_postcode_count_by_la_and_icb_df = (
            job.count_postcodes_per_list_of_columns(
                self.sample_ons_contemporary_df,
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
            in self.returned_postcode_count_by_la_df.columns
        )

    def test_count_postcodes_per_list_of_columns_has_expected_values_when_grouped_by_import_date_and_la(
        self,
    ):
        expected_df = self.spark.createDataFrame(
            TestData.expected_postcode_count_per_la_rows,
            schema=TestSchema.expected_postcode_count_per_la_schema,
        ).sort([ONSClean.postcode, ONSClean.contemporary_ons_import_date])

        self.assertEqual(
            self.returned_postcode_count_by_la_df.collect(),
            expected_df.collect(),
        )

    def test_count_postcodes_per_list_of_columns_has_expected_values_when_grouped_by_import_date_and_la_and_icb(
        self,
    ):
        expected_df = self.spark.createDataFrame(
            TestData.expected_postcode_count_per_la_icb_rows,
            schema=TestSchema.expected_postcode_count_per_la_icb_schema,
        ).sort([ONSClean.postcode, ONSClean.contemporary_ons_import_date])

        self.assertEqual(
            self.returned_postcode_count_by_la_and_icb_df.collect(),
            expected_df.collect(),
        )


class CreateRatioBetweenColumns(SplitPAFilledPostsIntoIcbAreas):
    def setUp(self) -> None:
        super().setUp()

        self.sample_df = self.spark.createDataFrame(
            TestData.sample_rows_with_la_and_hybrid_area_postcode_counts,
            schema=TestSchema.sample_rows_with_la_and_hybrid_area_postcode_counts_schema,
        )

        self.returned_df = (
            job.create_proportion_between_hybrid_area_and_la_area_postcode_counts(
                self.sample_df,
            )
        )

        self.expected_df = self.spark.createDataFrame(
            TestData.expected_ratio_between_hybrid_area_and_la_area_postcodes_rows,
            schema=TestSchema.expected_ratio_between_hybrid_area_and_la_area_postcodes_schema,
        )

        self.returned_rows = self.returned_df.collect()
        self.expected_rows = self.expected_df.collect()

    def test_create_proportion_between_hybrid_area_and_la_area_postcode_counts_has_expected_columns(
        self,
    ):
        self.assertEqual(
            self.returned_df.columns,
            self.expected_df.columns,
        )

    def test_create_proportion_between_hybrid_area_and_la_area_postcode_counts_has_expected_length(
        self,
    ):
        self.assertEqual(len(self.returned_rows), len(self.expected_rows))

    def test_create_proportion_between_hybrid_area_and_la_area_postcode_counts_has_expected_values_when_given_hybrid_and_la_counts(
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


class DeduplicateRatioBetweenAreaCounts(SplitPAFilledPostsIntoIcbAreas):
    def setUp(self) -> None:
        super().setUp()

        self.sample_df = self.spark.createDataFrame(
            TestData.full_rows_with_la_and_hybrid_area_postcode_counts,
            schema=TestSchema.full_rows_with_la_and_hybrid_area_postcode_counts_schema,
        )

        self.returned_df = (
            job.deduplicate_proportion_between_hybrid_area_and_la_area_postcode_counts(
                self.sample_df
            )
        )

        self.expected_df = self.spark.createDataFrame(
            TestData.expected_deduplicated_import_date_hybrid_and_la_and_ratio_rows,
            schema=TestSchema.expected_deduplicated_import_date_hybrid_and_la_and_ratio_schema,
        )

    def test_deduplicate_proportion_between_hybrid_area_and_la_area_postcode_counts_returns_expected_columns(
        self,
    ):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)

    def test_deduplicate_proportion_between_hybrid_area_and_la_area_postcode_counts_returns_expected_data(
        self,
    ):
        returned_rows = self.returned_df.sort(
            ONSClean.contemporary_ons_import_date,
            ONSClean.contemporary_cssr,
            ONSClean.contemporary_icb,
        ).collect()

        expected_rows = self.expected_df.sort(
            ONSClean.contemporary_ons_import_date,
            ONSClean.contemporary_cssr,
            ONSClean.contemporary_icb,
        ).collect()

        self.assertEqual(
            returned_rows,
            expected_rows,
        )


class CreateDateColumnFromYearInPaFilledPosts(SplitPAFilledPostsIntoIcbAreas):
    def setUp(self) -> None:
        super().setUp()

        self.sample_pa_filled_posts_df = self.spark.createDataFrame(
            TestData.sample_pa_filled_posts_rows,
            schema=TestSchema.sample_pa_filled_posts_schema,
        )

        self.returned_create_date_column_from_year_in_pa_estimates_df = (
            job.create_date_column_from_year_in_pa_estimates(
                self.sample_pa_filled_posts_df
            )
        )

        self.expected_create_date_column_from_year_in_pa_estimates_df = self.spark.createDataFrame(
            TestData.expected_create_date_column_from_year_in_pa_estimates_rows,
            schema=TestSchema.expected_create_date_column_from_year_in_pa_estimates_schema,
        )

    def test_create_date_column_from_year_in_pa_estimates_has_expected_values(
        self,
    ):
        returned_rows = (
            self.returned_create_date_column_from_year_in_pa_estimates_df.collect()
        )
        expected_rows = (
            self.expected_create_date_column_from_year_in_pa_estimates_df.collect()
        )
        self.assertEqual(returned_rows, expected_rows)


class JoinPaFilledPostsToHybridAreaProportions(SplitPAFilledPostsIntoIcbAreas):
    def setUp(self) -> None:
        super().setUp()

        self.sample_hybrid_area_proportions_df = self.spark.createDataFrame(
            TestData.sample_postcode_proportions_before_joining_pa_filled_posts_rows,
            schema=TestSchema.sample_postcode_proportions_before_joining_pa_filled_posts_schema,
        )

        self.sample_pa_filled_posts_df = self.spark.createDataFrame(
            TestData.sample_pa_filled_posts_prepared_for_joining_to_postcode_proportions_rows,
            schema=TestSchema.sample_pa_filled_posts_prepared_for_joining_to_postcode_proportions_schema,
        )

        self.returned_join_pa_filled_posts_to_hybrid_area_proportions_df = (
            job.join_pa_filled_posts_to_hybrid_area_proportions(
                self.sample_hybrid_area_proportions_df,
                self.sample_pa_filled_posts_df,
            )
        )

        self.expected_join_pa_filled_posts_to_hybrid_area_proportions_df = self.spark.createDataFrame(
            TestData.expected_postcode_proportions_after_joining_pa_filled_posts_rows,
            schema=TestSchema.expected_postcode_proportions_after_joining_pa_filled_posts_schema,
        )

    def test_join_pa_filled_posts_to_hybrid_area_proportions_adds_2_columns(
        self,
    ):
        self.assertEqual(
            len(
                self.returned_join_pa_filled_posts_to_hybrid_area_proportions_df.columns
            ),
            len(self.sample_hybrid_area_proportions_df.columns) + 2,
        )

    def test_join_pa_filled_posts_to_hybrid_area_proportions_does_not_add_any_rows(
        self,
    ):
        self.assertEqual(
            self.returned_join_pa_filled_posts_to_hybrid_area_proportions_df.count(),
            self.sample_hybrid_area_proportions_df.count(),
        )

    def test_join_pa_filled_posts_to_hybrid_area_proportions_has_no_duplicate_columns(
        self,
    ):
        self.assertEqual(
            sorted(
                self.returned_join_pa_filled_posts_to_hybrid_area_proportions_df.columns
            ),
            sorted(
                list(
                    set(
                        self.returned_join_pa_filled_posts_to_hybrid_area_proportions_df.columns
                    )
                )
            ),
        )

    def test_join_pa_filled_posts_to_hybrid_area_proportions_has_expected_values(
        self,
    ):
        returned_sorted_join_pa_filled_posts_to_hybrid_area_proportions_df = (
            self.returned_join_pa_filled_posts_to_hybrid_area_proportions_df.select(
                self.expected_join_pa_filled_posts_to_hybrid_area_proportions_df.columns
            )
        )

        sort_by_list = [
            ONSClean.contemporary_ons_import_date,
            ONSClean.contemporary_cssr,
            ONSClean.contemporary_icb,
        ]

        returned_rows = (
            returned_sorted_join_pa_filled_posts_to_hybrid_area_proportions_df.sort(
                sort_by_list
            ).collect()
        )
        expected_rows = (
            self.expected_join_pa_filled_posts_to_hybrid_area_proportions_df.sort(
                sort_by_list
            ).collect()
        )
        self.assertEqual(returned_rows, expected_rows)


class ApplyIcbProportionsToPAEstimates(SplitPAFilledPostsIntoIcbAreas):
    def setUp(self) -> None:
        super().setUp()

        self.sample_apply_icb_proportions_to_pa_filled_posts_df = (
            self.spark.createDataFrame(
                TestData.sample_proportions_and_pa_filled_posts_rows,
                schema=TestSchema.sample_proportions_and_pa_filled_posts_schema,
            )
        )

        self.returned_apply_icb_proportions_to_pa_filled_posts_df = (
            job.apply_icb_proportions_to_pa_filled_posts(
                self.sample_apply_icb_proportions_to_pa_filled_posts_df
            )
        )

        self.expected_apply_icb_proportions_to_pa_filled_posts_df = self.spark.createDataFrame(
            TestData.expected_pa_filled_posts_after_applying_proportions_rows,
            schema=TestSchema.expected_pa_filled_posts_after_applying_proportions_schema,
        )

    def test_apply_icb_proportions_to_pa_filled_posts_drops_given_column(
        self,
    ):
        self.assertIn(
            DPColNames.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS,
            self.sample_apply_icb_proportions_to_pa_filled_posts_df.columns,
        )
        self.assertNotIn(
            DPColNames.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS,
            self.returned_apply_icb_proportions_to_pa_filled_posts_df.columns,
        )

    def test_apply_icb_proportions_to_pa_filled_posts_adds_given_column_name(
        self,
    ):
        self.assertTrue(
            DPColNames.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS_PER_HYBRID_AREA
            in self.returned_apply_icb_proportions_to_pa_filled_posts_df.columns
        )

    def test_apply_icb_proportions_to_pa_filled_posts_has_expected_values(
        self,
    ):
        returned_rows = (
            self.returned_apply_icb_proportions_to_pa_filled_posts_df.collect()
        )
        expected_rows = (
            self.expected_apply_icb_proportions_to_pa_filled_posts_df.collect()
        )

        for i in range(len(returned_rows)):
            self.assertAlmostEqual(
                returned_rows[i][
                    DPColNames.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS_PER_HYBRID_AREA
                ],
                expected_rows[i][
                    DPColNames.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS_PER_HYBRID_AREA
                ],
                3,
                "rows are not almost equal",
            )
