import unittest

from projects._02_sfc_internal.unittest_data.sfc_test_file_data import (
    LmEngagementUtilsData as Data,
)
from projects._02_sfc_internal.unittest_data.sfc_test_file_schemas import (
    LmEngagementUtilsSchemas as Schemas,
)
from utils import utils
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
import projects._02_sfc_internal.cqc_coverage.utils.lm_engagement_utils as job


class SetupForTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = utils.get_spark()
        (
            self.w,
            self.agg_w,
            self.ytd_w,
        ) = job.create_windows_for_lm_engagement_calculations()


class AddColumnsForLocalityManagerDashboardTests(SetupForTests):
    def setUp(self) -> None:
        super().setUp()

    def test_add_columns_for_locality_manager_dashboard_returns_correct_values(self):
        test_df = self.spark.createDataFrame(
            Data.add_columns_for_locality_manager_dashboard_rows,
            Schemas.add_columns_for_locality_manager_dashboard_schema,
        )

        returned_df = job.add_columns_for_locality_manager_dashboard(test_df)

        expected_df = self.spark.createDataFrame(
            Data.expected_add_columns_for_locality_manager_dashboard_rows,
            Schemas.expected_add_columns_for_locality_manager_dashboard_schema,
        )
        self.assertEqual(
            returned_df.sort(
                CQCLClean.location_id,
                CQCLClean.cqc_location_import_date,
                CQCLClean.current_cssr,
            ).collect(),
            expected_df.sort(
                CQCLClean.location_id,
                CQCLClean.cqc_location_import_date,
                CQCLClean.current_cssr,
            ).collect(),
        )

    def test_calculate_la_coverage_monthly_returns_correct_values(self):
        test_df = self.spark.createDataFrame(
            Data.add_columns_for_locality_manager_dashboard_rows,
            Schemas.add_columns_for_locality_manager_dashboard_schema,
        )

        returned_df = job.calculate_la_coverage_monthly(test_df, self.agg_w)

        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_la_coverage_monthly_rows,
            Schemas.expected_calculate_la_coverage_monthly_schema,
        )
        self.assertEqual(
            returned_df.sort(
                CQCLClean.location_id,
                CQCLClean.cqc_location_import_date,
                CQCLClean.current_cssr,
            ).collect(),
            expected_df.sort(
                CQCLClean.location_id,
                CQCLClean.cqc_location_import_date,
                CQCLClean.current_cssr,
            ).collect(),
        )

    def test_calculate_coverage_monthly_change_returns_correct_values(self):
        test_df = self.spark.createDataFrame(
            Data.calculate_coverage_monthly_change_rows,
            Schemas.calculate_coverage_monthly_change_schema,
        )

        returned_df = job.calculate_coverage_monthly_change(test_df, self.w)

        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_coverage_monthly_change_rows,
            Schemas.expected_calculate_coverage_monthly_change_schema,
        )

        self.assertEqual(
            returned_df.sort(
                CQCLClean.location_id,
                CQCLClean.cqc_location_import_date,
                CQCLClean.current_cssr,
            ).collect(),
            expected_df.sort(
                CQCLClean.location_id,
                CQCLClean.cqc_location_import_date,
                CQCLClean.current_cssr,
            ).collect(),
        )

    def test_calculate_locations_monthly_change_returns_correct_values(self):
        test_df = self.spark.createDataFrame(
            Data.calculate_locations_monthly_change_rows,
            Schemas.calculate_locations_monthly_change_schema,
        )

        returned_df = job.calculate_locations_monthly_change(
            test_df, self.w, self.agg_w
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_locations_monthly_change_rows,
            Schemas.expected_calculate_locations_monthly_change_schema,
        )

        self.assertEqual(
            returned_df.sort(
                CQCLClean.location_id,
                CQCLClean.cqc_location_import_date,
                CQCLClean.current_cssr,
            ).collect(),
            expected_df.sort(
                CQCLClean.location_id,
                CQCLClean.cqc_location_import_date,
                CQCLClean.current_cssr,
            ).collect(),
        )

    def test_calculate_new_registrations_returns_correct_values(self):
        test_df = self.spark.createDataFrame(
            Data.calculate_new_registrations_rows,
            Schemas.calculate_new_registrations_schema,
        )

        returned_df = job.calculate_new_registrations(test_df, self.agg_w, self.ytd_w)

        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_new_registrations_rows,
            Schemas.expected_calculate_new_registrations_schema,
        )

        self.assertEqual(
            returned_df.sort(
                CQCLClean.location_id,
                CQCLClean.cqc_location_import_date,
                CQCLClean.current_cssr,
            ).collect(),
            expected_df.sort(
                CQCLClean.location_id,
                CQCLClean.cqc_location_import_date,
                CQCLClean.current_cssr,
            ).collect(),
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
