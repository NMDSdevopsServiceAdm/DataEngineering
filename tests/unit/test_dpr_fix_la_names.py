import unittest

from utils import utils

from tests.test_file_data import PAFilledPostsByIcbArea as TestData
from tests.test_file_schemas import PAFilledPostsByIcbAreaSchema as TestSchema

import utils.direct_payments_utils.prepare_direct_payments.fix_la_names as job


class ChangeLaNamesToMatchOnsCleanedLaNames(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

        self.sample_df = self.spark.createDataFrame(
            TestData.sample_la_name_rows, schema=TestSchema.sample_la_name_schema
        )

        self.returned_df = job.change_la_names_to_match_ons_cleaned(self.sample_df)

        self.expected_df = self.spark.createDataFrame(
            TestData.expected_la_names_with_correct_spelling_rows,
            schema=TestSchema.expected_la_names_with_correct_spelling_schema,
        )

    def test_change_la_names_to_match_ons_cleaned_does_not_add_any_columns(self):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)

    def test_change_la_names_to_match_ons_cleaned_does_not_add_any_rows(self):
        self.assertEqual(self.returned_df.count(), self.expected_df.count())

    def test_change_la_names_to_match_ons_cleaned_has_expected_values(self):
        returned_rows = self.returned_df.collect()
        expected_rows = self.expected_df.collect()

        self.assertEqual(returned_rows, expected_rows)
