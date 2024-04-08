import unittest
import warnings
from datetime import date
from unittest.mock import Mock, patch
from pyspark.sql import functions as F

import jobs.reconciliation as job
from utils import utils

from tests.test_file_data import ReconciliationData as Data
from tests.test_file_schemas import ReconciliationSchema as Schemas
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    CqcLocationApiColumns as CQCL,
)
from utils.reconciliation_utils.reconciliation_values import (
    ReconciliationColumns as ReconColumn,
)


class ReconciliationTests(unittest.TestCase):
    TEST_CQC_LOCATION_API_SOURCE = "some/source"
    TEST_ASCWDS_WORKPLACE_SOURCE = "another/source"
    TEST_SINGLE_SUB_DESTINATION = "some/destination"
    TEST_PARENT_DESTINATION = "another/destination"

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_cqc_location_api_df = self.spark.createDataFrame(
            Data.input_cqc_location_api_rows,
            Schemas.input_cqc_location_api_schema,
        )
        self.test_clean_ascwds_workplace_df = self.spark.createDataFrame(
            Data.input_ascwds_workplace_rows,
            Schemas.input_ascwds_workplace_schema,
        )

        warnings.simplefilter("ignore", ResourceWarning)


class MainTests(ReconciliationTests):
    def setUp(self) -> None:
        super().setUp()

    @patch("utils.utils.read_from_parquet")
    def test_main_run(
        self,
        read_from_parquet_patch: Mock,
    ):
        read_from_parquet_patch.side_effect = [
            self.test_cqc_location_api_df,
            self.test_clean_ascwds_workplace_df,
        ]

        job.main(
            self.TEST_CQC_LOCATION_API_SOURCE,
            self.TEST_ASCWDS_WORKPLACE_SOURCE,
            self.TEST_SINGLE_SUB_DESTINATION,
            self.TEST_PARENT_DESTINATION,
        )

        self.assertEqual(read_from_parquet_patch.call_count, 2)


class PrepareMostRecentCqcLocationDataTests(ReconciliationTests):
    def setUp(self) -> None:
        super().setUp()

    def test_prepare_most_recent_cqc_location_df_returns_expected_dataframe(self):
        returned_df = job.prepare_most_recent_cqc_location_df(
            self.test_cqc_location_api_df
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_prepared_most_recent_cqc_location_rows,
            Schemas.expected_prepared_most_recent_cqc_location_schema,
        )
        returned_data = returned_df.sort(CQCL.location_id).collect()
        expected_data = expected_df.sort(CQCL.location_id).collect()

        self.assertEqual(returned_data, expected_data)


class CollectDatesToUseTests(ReconciliationTests):
    def setUp(self) -> None:
        super().setUp()

    def test_collect_dates_to_use_return_correct_value(self):
        df = self.spark.createDataFrame(
            Data.dates_to_use_rows, Schemas.dates_to_use_schema
        )
        (
            first_of_most_recent_month,
            first_of_previous_month,
        ) = job.collect_dates_to_use(df)

        self.assertEqual(first_of_most_recent_month, date(2024, 3, 1))
        self.assertEqual(first_of_previous_month, date(2024, 2, 1))


class CreateReconciliationOutputForSingleAndSubAccountsTests(ReconciliationTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_singles_and_subs_df = self.spark.createDataFrame(
            Data.singles_and_subs_rows,
            Schemas.singles_and_subs_schema,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_singles_and_subs_rows,
            Schemas.expected_singles_and_subs_schema,
        )
        self.returned_df = job.add_singles_and_sub_description_column(
            self.test_singles_and_subs_df,
        )

    @unittest.skip("to do")
    def test(self):
        pass


class AddSinglesAndSubDescriptionColumnTests(ReconciliationTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_add_singles_and_subs_description_df = self.spark.createDataFrame(
            Data.add_singles_and_subs_description_rows,
            Schemas.add_singles_and_subs_description_schema,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_singles_and_subs_description_rows,
            Schemas.expected_singles_and_subs_description_schema,
        )
        self.returned_df = job.add_singles_and_sub_description_column(
            self.test_add_singles_and_subs_description_df,
        )

    def test_add_singles_and_subs_description_column_adds_a_column(self):
        returned_columns = len(self.returned_df.columns)
        expected_columns = (
            len(self.test_add_singles_and_subs_description_df.columns) + 1
        )
        self.assertEqual(returned_columns, expected_columns)

    def test_add_singles_and_subs_description_column_adds_a_column_with_expected_values(
        self,
    ):
        returned_data = self.returned_df.sort(CQCL.location_id).collect()
        expected_data = self.expected_df.sort(CQCL.location_id).collect()
        self.assertEqual(returned_data, expected_data)


class CreateMissingColumnsRequiredForOutputTests(ReconciliationTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_create_missing_columns_df = self.spark.createDataFrame(
            Data.create_missing_columns_rows,
            Schemas.create_missing_columns_schema,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_create_missing_columns_rows,
            Schemas.expected_create_missing_columns_schema,
        )
        self.returned_df = job.create_missing_columns_required_for_output(
            self.test_create_missing_columns_df,
        )
        self.returned_df.show()
        self.returned_df.printSchema()

    def test_create_missing_columns_returns_expected_columns(self):
        self.assertEqual(
            sorted(self.returned_df.columns), sorted(self.expected_df.columns)
        )

    def test_create_missing_columns_returns_expected_rows(self):
        self.assertEqual(self.returned_df.count(), self.expected_df.count())

    def test_create_missing_columns_returns_expected_values_in_new_columns(self):
        returned_data = self.returned_df.collect()
        expected_data = self.expected_df.collect()
        self.assertEqual(
            returned_data[0][ReconColumn.nmds], expected_data[0][ReconColumn.nmds]
        )
        self.assertEqual(
            returned_data[0][ReconColumn.workplace_id],
            expected_data[0][ReconColumn.workplace_id],
        )
        self.assertEqual(
            returned_data[0][ReconColumn.requester_name],
            expected_data[0][ReconColumn.requester_name],
        )
        self.assertEqual(
            returned_data[0][ReconColumn.requester_name_2],
            expected_data[0][ReconColumn.requester_name_2],
        )
        self.assertEqual(
            returned_data[0][ReconColumn.status], expected_data[0][ReconColumn.status]
        )
        self.assertEqual(
            returned_data[0][ReconColumn.technician],
            expected_data[0][ReconColumn.technician],
        )
        self.assertEqual(
            returned_data[0][ReconColumn.manual_call_log],
            expected_data[0][ReconColumn.manual_call_log],
        )
        self.assertEqual(
            returned_data[0][ReconColumn.mode], expected_data[0][ReconColumn.mode]
        )
        self.assertEqual(
            returned_data[0][ReconColumn.priority],
            expected_data[0][ReconColumn.priority],
        )
        self.assertEqual(
            returned_data[0][ReconColumn.category],
            expected_data[0][ReconColumn.category],
        )
        self.assertEqual(
            returned_data[0][ReconColumn.sub_category],
            expected_data[0][ReconColumn.sub_category],
        )
        self.assertEqual(
            returned_data[0][ReconColumn.is_requester_named],
            expected_data[0][ReconColumn.is_requester_named],
        )
        self.assertEqual(
            returned_data[0][ReconColumn.security_question],
            expected_data[0][ReconColumn.security_question],
        )
        self.assertEqual(
            returned_data[0][ReconColumn.website], expected_data[0][ReconColumn.website]
        )
        self.assertEqual(
            returned_data[0][ReconColumn.item], expected_data[0][ReconColumn.item]
        )
        self.assertEqual(
            returned_data[0][ReconColumn.phone], expected_data[0][ReconColumn.phone]
        )


class FinalColumnSelectionTests(ReconciliationTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_final_column_selection_df = self.spark.createDataFrame(
            Data.final_column_selection_rows,
            Schemas.final_column_selection_schema,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_final_column_selection_rows,
            Schemas.expected_final_column_selection_schema,
        )
        self.returned_df = job.final_column_selection(
            self.test_final_column_selection_df,
        )

    def test_final_column_selection_contains_correct_columns(self):
        print(self.returned_df.columns)
        print(self.expected_df.columns)
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)

    def test_final_column_selection_sorts_data_correctly(self):
        returned_data = self.returned_df.select(
            ReconColumn.nmds, ReconColumn.description
        ).collect()
        expected_data = self.expected_df.select(
            ReconColumn.nmds, ReconColumn.description
        ).collect()
        self.assertEqual(returned_data, expected_data)

    def test_final_column_selection_returns_expected_rows(self):
        self.assertEqual(self.returned_df.count(), self.expected_df.count())
