import unittest

from pyspark.sql import Window

from tests.test_file_data import LmEngagementUtilsData as Data
from tests.test_file_schemas import LmEngagementUtilsSchemas as Schemas
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
import utils.coverage_utils.lm_engagement_utils as job


class SetupForTests(unittest.TestCase):

    def setUp(self) -> None:
        self.spark = utils.get_spark()


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
        returned_df.sort(
            CQCLClean.location_id,
            CQCLClean.cqc_location_import_date,
            CQCLClean.current_cssr,
        ).show()
        expected_df.sort(
            CQCLClean.location_id,
            CQCLClean.cqc_location_import_date,
            CQCLClean.current_cssr,
        ).show()
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
        w = (
            Window.partitionBy(
                CQCLClean.current_cssr, CQCLClean.cqc_location_import_date
            )
            .orderBy(CQCLClean.cqc_location_import_date)
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        )

        returned_df = job.calculate_la_coverage_monthly(test_df, w)

        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_la_coverage_monthly_rows,
            Schemas.expected_calculate_la_coverage_monthly_schema,
        )
        returned_df.sort(
            CQCLClean.location_id,
            CQCLClean.cqc_location_import_date,
            CQCLClean.current_cssr,
        ).show()
        expected_df.sort(
            CQCLClean.location_id,
            CQCLClean.cqc_location_import_date,
            CQCLClean.current_cssr,
        ).show()
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
        w = Window.partitionBy(CQCLClean.location_id).orderBy(
            CQCLClean.cqc_location_import_date
        )

        returned_df = job.calculate_coverage_monthly_change(test_df, w)

        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_coverage_monthly_change_rows,
            Schemas.expected_calculate_coverage_monthly_change_schema,
        )
        returned_df.sort(
            CQCLClean.location_id,
            CQCLClean.cqc_location_import_date,
            CQCLClean.current_cssr,
        ).show()
        expected_df.sort(
            CQCLClean.location_id,
            CQCLClean.cqc_location_import_date,
            CQCLClean.current_cssr,
        ).show()
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
        w = Window.partitionBy(CQCLClean.location_id).orderBy(
            CQCLClean.cqc_location_import_date
        )
        agg_w = (
            Window.partitionBy(
                CQCLClean.current_cssr, CQCLClean.cqc_location_import_date
            )
            .orderBy(CQCLClean.cqc_location_import_date)
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        )

        returned_df = job.calculate_locations_monthly_change(test_df, w, agg_w)

        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_locations_monthly_change_rows,
            Schemas.expected_calculate_locations_monthly_change_schema,
        )
        returned_df.sort(
            CQCLClean.location_id,
            CQCLClean.cqc_location_import_date,
            CQCLClean.current_cssr,
        ).show()
        expected_df.sort(
            CQCLClean.location_id,
            CQCLClean.cqc_location_import_date,
            CQCLClean.current_cssr,
        ).show()
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
        w = Window.partitionBy(CQCLClean.location_id).orderBy(
            CQCLClean.cqc_location_import_date
        )
        agg_w = (
            Window.partitionBy(
                CQCLClean.current_cssr, CQCLClean.cqc_location_import_date
            )
            .orderBy(CQCLClean.cqc_location_import_date)
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        )
        ytd_w = (
            Window.partitionBy(CQCLClean.current_cssr, Keys.year)
            .orderBy(CQCLClean.cqc_location_import_date)
            .rangeBetween(Window.unboundedPreceding, Window.currentRow)
        )

        returned_df = job.calculate_new_registrations(test_df, agg_w, ytd_w)

        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_new_registrations_rows,
            Schemas.expected_calculate_new_registrations_schema,
        )
        returned_df.sort(
            CQCLClean.location_id,
            CQCLClean.cqc_location_import_date,
            CQCLClean.current_cssr,
        ).show()
        expected_df.sort(
            CQCLClean.location_id,
            CQCLClean.cqc_location_import_date,
            CQCLClean.current_cssr,
        ).show()
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
