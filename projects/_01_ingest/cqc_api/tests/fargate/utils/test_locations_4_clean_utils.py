import unittest

import polars as pl
import polars.testing as pl_testing

from projects._01_ingest.cqc_api.fargate.utils import locations_4_clean_utils as job
from projects._01_ingest.unittest_data.polars_ingest_test_file_data import (
    LocationsCleanUtilsData as Data,
)
from projects._01_ingest.unittest_data.polars_ingest_test_file_schema import (
    LocationsCleanUtilsSchema as Schemas,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)

PATCH_PATH = "projects._01_ingest.cqc_api.fargate.utils.locations_4_clean_utils"


class CleanProviderIdColumnTests(unittest.TestCase):
    def test_does_not_change_valid_ids(self):
        """Input with provider ids which are all populated and less than 14 characters"""
        lf = pl.LazyFrame(
            data=Data.clean_provider_id_column_rows,
            schema=Schemas.clean_provider_id_column_schema,
        )

        returned_lf = job.clean_provider_id_column(lf)

        pl_testing.assert_frame_equal(returned_lf, lf)

    def test_removes_long_provider_ids(self):
        """Input with provider ids which are longer than 14 characters"""
        lf = pl.LazyFrame(
            data=Data.long_provider_id_column_rows,
            schema=Schemas.clean_provider_id_column_schema,
        )
        expected_lf = pl.LazyFrame(
            data=Data.expected_long_provider_id_column_rows,
            schema=Schemas.clean_provider_id_column_schema,
        )

        returned_lf = job.clean_provider_id_column(lf)

        pl_testing.assert_frame_equal(expected_lf, returned_lf)


class ImputeMissingValuesTests(unittest.TestCase):
    def test_single_column_imputation(self):
        """Tests imputation for a single column with null values."""

        lf = pl.LazyFrame(
            Data.impute_missing_values_single_col_rows,
            schema=Schemas.impute_missing_values_schema,
        )
        expected_lf = pl.LazyFrame(
            Data.expected_impute_missing_values_single_col_rows,
            schema=Schemas.impute_missing_values_schema,
        )

        result_lf = job.impute_missing_values(lf, [CQCLClean.provider_id])

        pl_testing.assert_frame_equal(result_lf, expected_lf)

    def test_multiple_columns_independent_imputation(self):
        """Ensures multiple columns are imputed independently and correctly."""

        lf = pl.LazyFrame(
            Data.impute_missing_values_multiple_cols_rows,
            schema=Schemas.impute_missing_values_schema,
        )
        expected_lf = pl.LazyFrame(
            Data.expected_impute_missing_values_multiple_cols_rows,
            schema=Schemas.impute_missing_values_schema,
        )

        result_lf = job.impute_missing_values(
            lf, [CQCLClean.provider_id, CQCLClean.services_offered]
        )

        pl_testing.assert_frame_equal(result_lf, expected_lf)

    def test_imputation_partitions_correctly(self):
        """Verifies imputation does not leak values across location_id partitions."""

        lf = pl.LazyFrame(
            Data.impute_missing_values_imputation_partitions_rows,
            schema=Schemas.impute_missing_values_schema,
        )
        expected_lf = pl.LazyFrame(
            Data.expected_impute_missing_values_imputation_partitions_rows,
            schema=Schemas.impute_missing_values_schema,
        )

        result_lf = job.impute_missing_values(lf, [CQCLClean.provider_id])

        pl_testing.assert_frame_equal(result_lf, expected_lf)

    def test_out_of_order_dates_are_ordered_before_imputation(self):
        """Ensures imputation follows chronological order based on date column."""

        lf = pl.LazyFrame(
            Data.impute_missing_values_out_of_order_dates_rows,
            schema=Schemas.impute_missing_values_schema,
        )
        expected_lf = pl.LazyFrame(
            Data.expected_impute_missing_values_out_of_order_dates_rows,
            schema=Schemas.impute_missing_values_schema,
        )

        result_lf = job.impute_missing_values(lf, [CQCLClean.provider_id])

        pl_testing.assert_frame_equal(result_lf, expected_lf)

    def test_fully_null_column_remains_null_after_imputation(self):
        """If a column is entirely null, imputed column should remain all nulls."""

        lf = pl.LazyFrame(
            Data.impute_missing_values_fully_null_rows,
            schema=Schemas.impute_missing_values_schema,
        )
        expected_lf = pl.LazyFrame(
            Data.expected_impute_missing_values_fully_null_rows,
            schema=Schemas.impute_missing_values_schema,
        )

        result_lf = job.impute_missing_values(lf, [CQCLClean.provider_id])

        pl_testing.assert_frame_equal(result_lf, expected_lf)

    def test_multiple_partitions_with_varied_missing_patterns(self):
        """Tests complex case with several partitions, ensuring each is filled correctly."""

        lf = pl.LazyFrame(
            Data.impute_missing_values_multiple_partitions_and_missing_data_rows,
            schema=Schemas.impute_missing_values_schema,
        )
        expected_lf = pl.LazyFrame(
            Data.expected_impute_missing_values_multiple_partitions_and_missing_data_rows,
            schema=Schemas.impute_missing_values_schema,
        )

        result_lf = job.impute_missing_values(
            lf, [CQCLClean.provider_id, CQCLClean.services_offered]
        )

        pl_testing.assert_frame_equal(result_lf, expected_lf)

    def test_overwrites_empty_lists(self):
        """Tests empty lists are replaced by correct poplulated list"""

        lf = pl.LazyFrame(
            Data.impute_missing_values_overwrites_empty_lists_rows,
            schema=Schemas.impute_missing_values_schema,
        )
        expected_lf = pl.LazyFrame(
            Data.expected_impute_missing_values_overwrites_empty_lists_rows,
            schema=Schemas.impute_missing_values_schema,
        )

        result_lf = job.impute_missing_values(
            lf, [CQCLClean.provider_id, CQCLClean.services_offered]
        )

        pl_testing.assert_frame_equal(result_lf, expected_lf)


class AssignCqcSectorTests(unittest.TestCase):
    def test_assigns_local_authority(self):
        lf = pl.LazyFrame(
            data=Data.assign_cqc_sector,
            schema=Schemas.assign_cqc_sector_schema,
        )
        local_authority_provider_ids = lf.collect()[CQCLClean.provider_id].to_list()

        result_lf = job.assign_cqc_sector(lf, local_authority_provider_ids)

        expected_lf = pl.LazyFrame(
            data=Data.expected_assign_cqc_sector_local_authority,
            schema=Schemas.expected_assign_cqc_sector_schema,
        )
        pl_testing.assert_frame_equal(expected_lf, result_lf)

    def test_assigns_independent(self):
        lf = pl.LazyFrame(
            data=Data.assign_cqc_sector,
            schema=Schemas.assign_cqc_sector_schema,
        )
        local_authority_provider_ids = ["non-matching-id-1", "non-matching-id-2"]

        result_lf = job.assign_cqc_sector(lf, local_authority_provider_ids)

        expected_lf = pl.LazyFrame(
            data=Data.expected_assign_cqc_sector_independent,
            schema=Schemas.expected_assign_cqc_sector_schema,
        )
        pl_testing.assert_frame_equal(expected_lf, result_lf)


class AllocatePrimaryServiceTests(unittest.TestCase):
    def test_allocate_primary_service_type(self):
        test_primary_service_lf = pl.LazyFrame(
            data=Data.primary_service_type_rows,
            schema=Schemas.primary_service_type_schema,
            orient="row",
        )
        expected_lf = pl.LazyFrame(
            data=Data.expected_primary_service_type_rows,
            schema=Schemas.expected_primary_service_type_schema,
        )
        returned_lf = job.allocate_primary_service_type(test_primary_service_lf)

        self.assertTrue(CQCLClean.primary_service_type in returned_lf.columns)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class RealignCareHomeColumnWthPrimaryServiceTests(unittest.TestCase):
    def test_care_home_values_match_expected_data(self):
        test_realign_carehome_column_lf = pl.LazyFrame(
            data=Data.realign_carehome_column_rows,
            schema=Schemas.realign_carehome_column_schema,
            orient="row",
        )
        returned_lf = job.realign_carehome_column_with_primary_service(
            test_realign_carehome_column_lf
        )
        expected_lf = pl.LazyFrame(
            data=Data.expected_realign_carehome_column_rows,
            schema=Schemas.realign_carehome_column_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class AddColumnRelatedLocationTests(unittest.TestCase):
    def test_add_related_location_column_returns_correct_values(self):
        test_lf = pl.LazyFrame(
            data=Data.add_related_location_column_rows,
            schema=Schemas.add_related_location_column_schema,
        )
        expected_lf = pl.LazyFrame(
            data=Data.expected_add_related_location_column_rows,
            schema=Schemas.expected_add_related_location_column_schema,
        )
        returned_lf = job.add_related_location_column(test_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class CleanAndImputeRegistrationDateColumnTests(unittest.TestCase):
    def test_clean_and_impute_registration_date_returns_reg_date_when_reg_date_before_or_equal_to_import_date(
        self,
    ):
        test_lf = pl.LazyFrame(
            data=Data.clean_and_impute_registration_date_when_reg_date_before_or_equal_to_import_date_rows,
            schema=Schemas.clean_and_impute_registration_date_schema,
        )
        returned_lf = job.clean_and_impute_registration_date(test_lf)
        expected_lf = pl.LazyFrame(
            data=Data.expected_clean_and_impute_registration_date_when_reg_date_before_or_equal_to_import_date_rows,
            schema=Schemas.expected_clean_and_impute_registration_date_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_clean_and_impute_registration_date_returns_import_date_when_reg_date_null(
        self,
    ):
        test_lf = pl.LazyFrame(
            data=Data.clean_and_impute_registration_date_when_reg_date_null_rows,
            schema=Schemas.clean_and_impute_registration_date_schema,
        )
        returned_lf = job.clean_and_impute_registration_date(test_lf)
        expected_lf = pl.LazyFrame(
            data=Data.expected_clean_and_impute_registration_date_when_reg_date_null_rows,
            schema=Schemas.expected_clean_and_impute_registration_date_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_clean_and_impute_registration_date_returns_import_date_when_reg_date_after_import_date_and_only_one_reg_date(
        self,
    ):
        test_lf = pl.LazyFrame(
            data=Data.clean_and_impute_registration_date_when_reg_date_after_import_date_and_only_one_reg_date_rows,
            schema=Schemas.clean_and_impute_registration_date_schema,
        )
        returned_lf = job.clean_and_impute_registration_date(test_lf)
        expected_lf = pl.LazyFrame(
            data=Data.expected_clean_and_impute_registration_date_when_reg_date_after_import_date_and_only_one_reg_date_rows,
            schema=Schemas.expected_clean_and_impute_registration_date_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_clean_and_impute_registration_date_returns_earliest_reg_date_when_reg_date_after_import_date_and_has_other_acceptable_reg_date(
        self,
    ):
        test_lf = pl.LazyFrame(
            data=Data.clean_and_impute_registration_date_when_reg_date_after_import_date_and_has_other_acceptable_reg_date_rows,
            schema=Schemas.clean_and_impute_registration_date_schema,
        )
        returned_lf = job.clean_and_impute_registration_date(test_lf)
        expected_lf = pl.LazyFrame(
            data=Data.expected_clean_and_impute_registration_date_when_reg_date_after_import_date_and_has_other_acceptable_reg_date_rows,
            schema=Schemas.expected_clean_and_impute_registration_date_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_clean_and_impute_registration_date_returns_earliest_import_date_when_reg_date_after_import_date_and_import_date_out_of_order(
        self,
    ):
        test_lf = pl.LazyFrame(
            data=Data.clean_and_impute_registration_date_when_reg_date_after_import_date_and_import_date_out_of_order_rows,
            schema=Schemas.clean_and_impute_registration_date_schema,
        )
        returned_lf = job.clean_and_impute_registration_date(test_lf)
        expected_lf = pl.LazyFrame(
            data=Data.expected_clean_and_impute_registration_date_when_reg_date_after_import_date_and_import_date_out_of_order_rows,
            schema=Schemas.expected_clean_and_impute_registration_date_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_clean_and_impute_registration_date_returns_expected_data_when_given_mixed_scenarios(
        self,
    ):
        test_lf = pl.LazyFrame(
            data=Data.clean_and_impute_registration_date_when_given_mixed_scenarios_rows,
            schema=Schemas.clean_and_impute_registration_date_schema,
        )
        returned_lf = job.clean_and_impute_registration_date(test_lf)
        expected_lf = pl.LazyFrame(
            data=Data.expected_clean_and_impute_registration_date_when_given_mixed_scenarios_rows,
            schema=Schemas.expected_clean_and_impute_registration_date_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class RemoveSpecialistCollegesTests(unittest.TestCase):
    def test_remove_specialist_colleges_removes_only_specialist_colleges(self):
        test_lf = pl.LazyFrame(
            data=Data.remove_specialist_colleges_rows,
            schema=Schemas.remove_specialist_colleges_schema,
        )
        returned_lf = job.remove_specialist_colleges(test_lf)
        expected_lf = pl.LazyFrame(
            data=Data.expected_remove_specialist_colleges_rows,
            schema=Schemas.remove_specialist_colleges_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)
