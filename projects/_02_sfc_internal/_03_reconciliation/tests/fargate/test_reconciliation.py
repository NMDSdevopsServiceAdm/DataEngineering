from dataclasses import dataclass
from datetime import date
from unittest.mock import Mock, call, patch

import polars as pl
import pytest
from polars import testing as pl_testing

import projects._02_sfc_internal._03_reconciliation.fargate.reconciliation as job
from projects._02_sfc_internal.unittest_data.polars_sfc_test_file_data import (
    ReconciliationData as Data,
)
from projects._02_sfc_internal.unittest_data.polars_sfc_test_file_schemas import (
    ReconciliationSchema as Schemas,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_names.reconciliation_columns import (
    ReconciliationColumns as ReconColumn,
)
from utils.column_values.categorical_column_values import (
    ParentsOrSinglesAndSubs,
    RegistrationStatus,
    SingleSubDescription,
    Subject,
)

PATCH_PATH = "projects._02_sfc_internal._03_reconciliation.fargate.reconciliation"


class TestMain:
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_writes_both_reports_and_applies_purge_filter_before_processing(
        self, scan_parquet_mock: Mock, sink_to_parquet_mock: Mock
    ):
        ascwds_workplace_lf = pl.LazyFrame(
            Data.main_ascwds_workplace_rows,
            schema=Schemas.main_ascwds_workplace_schema,
            orient="row",
        )
        cqc_location_lf = pl.LazyFrame(
            Data.main_cqc_location_rows,
            schema=Schemas.main_cqc_location_schema,
            orient="row",
        )
        scan_parquet_mock.side_effect = [cqc_location_lf, ascwds_workplace_lf]

        job.main(
            "cqc_source/",
            "ascwds_source/",
            "single_and_subs_dest/",
            "parents_dest/",
        )

        scan_parquet_mock.assert_has_calls(
            [
                call("cqc_source/"),
                call("ascwds_source/"),
            ]
        )
        assert sink_to_parquet_mock.call_count == 2

        single_and_sub_args, _ = sink_to_parquet_mock.call_args_list[0]
        parents_args, _ = sink_to_parquet_mock.call_args_list[1]

        assert single_and_sub_args[1] == "single_and_subs_dest/"
        assert parents_args[1] == "parents_dest/"

        single_and_sub_df = single_and_sub_args[0].collect()
        assert (
            single_and_sub_df[ReconColumn.nmds].to_list()
            == Data.main_expected_single_and_subs_nmds_ids
        )

        parents_df = parents_args[0].collect()
        assert parents_df.height == 0


@dataclass
class MonthBoundariesTestCase:
    id: str
    max_import_date: date
    expected_most_recent_month: date
    expected_previous_month: date

    def as_pytest_param(self):
        return pytest.param(
            self.max_import_date,
            self.expected_most_recent_month,
            self.expected_previous_month,
            id=self.id,
        )


month_boundary_cases = [
    MonthBoundariesTestCase(
        id="mid_year_uses_previous_calendar_month",
        max_import_date=date(2024, 3, 28),
        expected_most_recent_month=date(2024, 3, 1),
        expected_previous_month=date(2024, 2, 1),
    ),
    MonthBoundariesTestCase(
        id="january_rolls_back_to_december_of_previous_year",
        max_import_date=date(2024, 1, 10),
        expected_most_recent_month=date(2024, 1, 1),
        expected_previous_month=date(2023, 12, 1),
    ),
]


class TestGetReconciliationMonthBoundaries:
    @pytest.mark.parametrize(
        "max_import_date, expected_most_recent_month, expected_previous_month",
        [c.as_pytest_param() for c in month_boundary_cases],
    )
    def test_returns_first_day_of_most_recent_and_previous_month(
        self, max_import_date, expected_most_recent_month, expected_previous_month
    ):
        cqc_location_lf = pl.LazyFrame(
            {CQCLClean.cqc_location_import_date: [max_import_date]},
            schema={CQCLClean.cqc_location_import_date: pl.Date},
        )

        (
            first_of_most_recent_month,
            first_of_previous_month,
        ) = job.get_reconciliation_month_boundaries(cqc_location_lf)

        assert first_of_most_recent_month == expected_most_recent_month
        assert first_of_previous_month == expected_previous_month


class TestPrepareLatestCleanedAscwdsWorkforceData:
    def input_lf(self) -> pl.LazyFrame:
        return pl.LazyFrame(
            Data.ascwds_workplace_rows,
            schema=Schemas.ascwds_workplace_schema,
            orient="row",
        )

    def test_filters_to_cqc_registered_non_head_office_accounts_at_latest_import_date(
        self,
    ):
        cqc_registered_lf, _ = job.prepare_latest_cleaned_ascwds_workforce_data(
            self.input_lf()
        )

        returned_ids = set(
            cqc_registered_lf.collect()[AWPClean.establishment_id].to_list()
        )
        assert returned_ids == {"100", "103", "104", "201", "202"}

    def test_returns_only_parent_accounts_in_the_parent_lookup(self):
        _, parent_accounts_lf = job.prepare_latest_cleaned_ascwds_workforce_data(
            self.input_lf()
        )
        parent_accounts_df = parent_accounts_lf.collect()

        assert parent_accounts_df[AWPClean.establishment_id].to_list() == ["201"]
        assert set(parent_accounts_df.columns) == {
            AWPClean.nmds_id,
            AWPClean.establishment_id,
            AWPClean.establishment_name,
            AWPClean.organisation_id,
            AWPClean.establishment_type,
            AWPClean.region_id,
        }

    def test_applies_region_id_labels(self):
        cqc_registered_lf, _ = job.prepare_latest_cleaned_ascwds_workforce_data(
            self.input_lf()
        )

        region = (
            cqc_registered_lf.filter(pl.col(AWPClean.establishment_id) == "100")
            .collect()[AWPClean.region_id]
            .item()
        )
        assert region == "I - Eastern"

    def test_applies_region_id_labels_for_the_not_known_sentinel(self):
        cqc_registered_lf, _ = job.prepare_latest_cleaned_ascwds_workforce_data(
            self.input_lf()
        )

        region = (
            cqc_registered_lf.filter(pl.col(AWPClean.establishment_id) == "104")
            .collect()[AWPClean.region_id]
            .item()
        )
        assert region == "Not known"


FIRST_OF_MOST_RECENT_MONTH = date(2024, 4, 1)
FIRST_OF_PREVIOUS_MONTH = date(2024, 3, 1)


@dataclass
class RelevantToReconciliationTestCase:
    id: str
    registration_status: str | None
    deregistration_date: date | None
    classification: str
    expected_included: bool

    def as_pytest_param(self):
        return pytest.param(
            self.registration_status,
            self.deregistration_date,
            self.classification,
            self.expected_included,
            id=self.id,
        )


relevant_to_reconciliation_cases = [
    RelevantToReconciliationTestCase(
        "null_registration_status_and_parent_is_kept",
        None,
        date(2024, 3, 31),
        ParentsOrSinglesAndSubs.parents,
        True,
    ),
    RelevantToReconciliationTestCase(
        "null_registration_status_and_singles_and_subs_is_kept",
        None,
        date(2024, 3, 31),
        ParentsOrSinglesAndSubs.singles_and_subs,
        True,
    ),
    RelevantToReconciliationTestCase(
        "null_registration_status_parent_at_start_of_previous_month_is_kept",
        None,
        date(2024, 3, 1),
        ParentsOrSinglesAndSubs.parents,
        True,
    ),
    RelevantToReconciliationTestCase(
        "null_registration_status_singles_and_subs_at_start_of_previous_month_is_kept",
        None,
        date(2024, 3, 1),
        ParentsOrSinglesAndSubs.singles_and_subs,
        True,
    ),
    RelevantToReconciliationTestCase(
        "null_registration_status_parent_before_previous_month_is_kept",
        None,
        date(2024, 2, 29),
        ParentsOrSinglesAndSubs.parents,
        True,
    ),
    RelevantToReconciliationTestCase(
        "null_registration_status_singles_and_subs_before_previous_month_is_kept",
        None,
        date(2024, 2, 29),
        ParentsOrSinglesAndSubs.singles_and_subs,
        True,
    ),
    RelevantToReconciliationTestCase(
        "null_registration_status_parent_at_start_of_current_month_is_kept",
        None,
        date(2024, 4, 1),
        ParentsOrSinglesAndSubs.parents,
        True,
    ),
    RelevantToReconciliationTestCase(
        "null_registration_status_singles_and_subs_at_start_of_current_month_is_kept",
        None,
        date(2024, 4, 1),
        ParentsOrSinglesAndSubs.singles_and_subs,
        True,
    ),
    RelevantToReconciliationTestCase(
        "registered_parent_is_removed",
        RegistrationStatus.registered,
        date(2024, 3, 31),
        ParentsOrSinglesAndSubs.parents,
        False,
    ),
    RelevantToReconciliationTestCase(
        "registered_singles_and_subs_is_removed",
        RegistrationStatus.registered,
        date(2024, 3, 31),
        ParentsOrSinglesAndSubs.singles_and_subs,
        False,
    ),
    RelevantToReconciliationTestCase(
        "registered_parent_at_start_of_previous_month_is_removed",
        RegistrationStatus.registered,
        date(2024, 3, 1),
        ParentsOrSinglesAndSubs.parents,
        False,
    ),
    RelevantToReconciliationTestCase(
        "registered_singles_and_subs_at_start_of_previous_month_is_removed",
        RegistrationStatus.registered,
        date(2024, 3, 1),
        ParentsOrSinglesAndSubs.singles_and_subs,
        False,
    ),
    RelevantToReconciliationTestCase(
        "registered_parent_before_previous_month_is_removed",
        RegistrationStatus.registered,
        date(2024, 2, 29),
        ParentsOrSinglesAndSubs.parents,
        False,
    ),
    RelevantToReconciliationTestCase(
        "registered_singles_and_subs_before_previous_month_is_removed",
        RegistrationStatus.registered,
        date(2024, 2, 29),
        ParentsOrSinglesAndSubs.singles_and_subs,
        False,
    ),
    RelevantToReconciliationTestCase(
        "registered_parent_at_start_of_current_month_is_removed",
        RegistrationStatus.registered,
        date(2024, 4, 1),
        ParentsOrSinglesAndSubs.parents,
        False,
    ),
    RelevantToReconciliationTestCase(
        "registered_singles_and_subs_at_start_of_current_month_is_removed",
        RegistrationStatus.registered,
        date(2024, 4, 1),
        ParentsOrSinglesAndSubs.singles_and_subs,
        False,
    ),
    RelevantToReconciliationTestCase(
        "deregistered_parent_before_current_month_is_kept",
        RegistrationStatus.deregistered,
        date(2024, 3, 31),
        ParentsOrSinglesAndSubs.parents,
        True,
    ),
    RelevantToReconciliationTestCase(
        "deregistered_singles_and_subs_within_previous_month_is_kept",
        RegistrationStatus.deregistered,
        date(2024, 3, 31),
        ParentsOrSinglesAndSubs.singles_and_subs,
        True,
    ),
    RelevantToReconciliationTestCase(
        "deregistered_parent_at_start_of_previous_month_is_kept",
        RegistrationStatus.deregistered,
        date(2024, 3, 1),
        ParentsOrSinglesAndSubs.parents,
        True,
    ),
    RelevantToReconciliationTestCase(
        "deregistered_singles_and_subs_at_start_of_previous_month_is_kept",
        RegistrationStatus.deregistered,
        date(2024, 3, 1),
        ParentsOrSinglesAndSubs.singles_and_subs,
        True,
    ),
    RelevantToReconciliationTestCase(
        "deregistered_parent_before_previous_month_is_kept",
        RegistrationStatus.deregistered,
        date(2024, 2, 29),
        ParentsOrSinglesAndSubs.parents,
        True,
    ),
    RelevantToReconciliationTestCase(
        "deregistered_singles_and_subs_before_previous_month_is_removed",
        RegistrationStatus.deregistered,
        date(2024, 2, 29),
        ParentsOrSinglesAndSubs.singles_and_subs,
        False,
    ),
    RelevantToReconciliationTestCase(
        "deregistered_parent_at_start_of_current_month_is_removed",
        RegistrationStatus.deregistered,
        date(2024, 4, 1),
        ParentsOrSinglesAndSubs.parents,
        False,
    ),
    RelevantToReconciliationTestCase(
        "deregistered_singles_and_subs_at_start_of_current_month_is_removed",
        RegistrationStatus.deregistered,
        date(2024, 4, 1),
        ParentsOrSinglesAndSubs.singles_and_subs,
        False,
    ),
]


class TestFilterToLocationsRelevantToReconciliationProcess:
    @pytest.mark.parametrize(
        "registration_status, deregistration_date, classification, expected_included",
        [c.as_pytest_param() for c in relevant_to_reconciliation_cases],
    )
    def test_filters_rows_according_to_registration_and_deregistration_rules(
        self,
        registration_status,
        deregistration_date,
        classification,
        expected_included,
    ):
        input_lf = pl.LazyFrame(
            {
                CQCL.registration_status: [registration_status],
                CQCL.deregistration_date: [deregistration_date],
                ReconColumn.parents_or_singles_and_subs: [classification],
            },
            schema={
                CQCL.registration_status: pl.String,
                CQCL.deregistration_date: pl.Date,
                ReconColumn.parents_or_singles_and_subs: pl.String,
            },
        )

        returned_df = job.filter_to_locations_relevant_to_reconciliation_process(
            input_lf, FIRST_OF_MOST_RECENT_MONTH, FIRST_OF_PREVIOUS_MONTH
        ).collect()

        assert (returned_df.height == 1) == expected_included


class TestBuildSingleAndSubsOutput:
    def test_returns_only_singles_and_subs_rows_with_description_and_subject(self):
        input_lf = pl.LazyFrame(
            {
                AWPClean.nmds_id: ["10", "20"],
                AWPClean.establishment_type: ["type_a", "type_b"],
                AWPClean.region_id: ["region_a", "region_b"],
                AWPClean.establishment_name: ["Name A", "Name B"],
                ReconColumn.parents_or_singles_and_subs: [
                    ParentsOrSinglesAndSubs.singles_and_subs,
                    ParentsOrSinglesAndSubs.parents,
                ],
                CQCL.deregistration_date: [date(2024, 3, 15), None],
            }
        )

        returned_df = job.build_single_and_subs_output(input_lf).collect()

        assert returned_df[ReconColumn.nmds].to_list() == ["10"]
        assert returned_df[ReconColumn.subject].to_list() == [
            Subject.single_sub_subject_value
        ]
        assert returned_df[ReconColumn.description].to_list() == [
            SingleSubDescription.single_sub_deregistered_description
        ]

    def test_uses_reg_type_description_when_not_deregistered(self):
        input_lf = pl.LazyFrame(
            {
                AWPClean.nmds_id: ["30"],
                AWPClean.establishment_type: ["type_a"],
                AWPClean.region_id: ["region_a"],
                AWPClean.establishment_name: ["Name C"],
                ReconColumn.parents_or_singles_and_subs: [
                    ParentsOrSinglesAndSubs.singles_and_subs
                ],
                CQCL.deregistration_date: [None],
            },
            schema={
                AWPClean.nmds_id: pl.String,
                AWPClean.establishment_type: pl.String,
                AWPClean.region_id: pl.String,
                AWPClean.establishment_name: pl.String,
                ReconColumn.parents_or_singles_and_subs: pl.String,
                CQCL.deregistration_date: pl.Date,
            },
        )

        returned_df = job.build_single_and_subs_output(input_lf).collect()

        assert returned_df[ReconColumn.description].to_list() == [
            SingleSubDescription.single_sub_reg_type_description
        ]


class TestBuildParentsOutput:
    def test_builds_one_row_per_parent_account_with_issue_columns_and_subject(self):
        ascwds_parent_accounts_lf = pl.LazyFrame(
            {
                AWPClean.nmds_id: ["P1"],
                AWPClean.establishment_id: ["E1"],
                AWPClean.establishment_name: ["Parent Name"],
                AWPClean.organisation_id: ["org1"],
                AWPClean.establishment_type: ["type_a"],
                AWPClean.region_id: ["region_a"],
            }
        )
        reconciliation_lf = pl.LazyFrame(
            {
                AWPClean.organisation_id: ["org1", "org1", "org1", "org2"],
                AWPClean.nmds_id: ["S1", "S2", "S3", "X1"],
                ReconColumn.parents_or_singles_and_subs: [
                    ParentsOrSinglesAndSubs.parents,
                    ParentsOrSinglesAndSubs.parents,
                    ParentsOrSinglesAndSubs.parents,
                    ParentsOrSinglesAndSubs.singles_and_subs,
                ],
                CQCL.deregistration_date: [
                    date(2024, 3, 10),
                    date(2024, 2, 1),
                    None,
                    date(2024, 3, 10),
                ],
            }
        )

        returned_df = job.build_parents_output(
            ascwds_parent_accounts_lf, reconciliation_lf, date(2024, 3, 1)
        ).collect()

        assert returned_df.height == 1
        assert returned_df[ReconColumn.nmds].to_list() == ["P1"]
        assert returned_df[ReconColumn.subject].to_list() == [
            Subject.parent_subject_value
        ]
        assert returned_df[ReconColumn.description].item() == (
            "new_potential_subs: S1 old_potential_subs: S2 "
            "missing_or_incorrect_potential_subs: S3 "
        )

    def test_excludes_parent_accounts_with_no_outstanding_issues(self):
        ascwds_parent_accounts_lf = pl.LazyFrame(
            {
                AWPClean.nmds_id: ["P2"],
                AWPClean.establishment_id: ["E2"],
                AWPClean.establishment_name: ["Parent Name 2"],
                AWPClean.organisation_id: ["org2"],
                AWPClean.establishment_type: ["type_a"],
                AWPClean.region_id: ["region_a"],
            }
        )
        reconciliation_lf = pl.LazyFrame(
            {
                AWPClean.organisation_id: [],
                AWPClean.nmds_id: [],
                ReconColumn.parents_or_singles_and_subs: [],
                CQCL.deregistration_date: [],
            },
            schema={
                AWPClean.organisation_id: pl.String,
                AWPClean.nmds_id: pl.String,
                ReconColumn.parents_or_singles_and_subs: pl.String,
                CQCL.deregistration_date: pl.Date,
            },
        )

        returned_df = job.build_parents_output(
            ascwds_parent_accounts_lf, reconciliation_lf, date(2024, 3, 1)
        ).collect()

        assert returned_df.height == 0


class TestJoinNmdsIdsIntoParentAccounts:
    def test_returns_one_row_per_organisation_with_sorted_comma_separated_ids(self):
        lf_with_issues = pl.LazyFrame(
            {
                AWPClean.organisation_id: ["org1", "org1", "org1", "org2"],
                AWPClean.nmds_id: ["loc 3", "loc 1", "loc 2", "loc 4"],
            }
        )
        parent_accounts_lf = pl.LazyFrame(
            {
                AWPClean.organisation_id: ["org1", "org2", "org3"],
                "other_column": ["a", "b", "c"],
            }
        )

        returned_df = job.join_nmds_ids_into_parent_accounts(
            lf_with_issues, "new_column", parent_accounts_lf
        ).collect()

        returned = dict(
            zip(
                returned_df[AWPClean.organisation_id].to_list(),
                returned_df["new_column"].to_list(),
            )
        )
        assert returned == {
            "org1": "new_column: loc 1, loc 2, loc 3",
            "org2": "new_column: loc 4",
            "org3": None,
        }
        assert returned_df["new_column"].dtype == pl.String


@dataclass
class DescriptionConcatTestCase:
    id: str
    new_potential_subs: str | None
    old_potential_subs: str | None
    missing_or_incorrect_potential_subs: str | None
    expected_description: str

    def as_pytest_param(self):
        return pytest.param(
            self.new_potential_subs,
            self.old_potential_subs,
            self.missing_or_incorrect_potential_subs,
            self.expected_description,
            id=self.id,
        )


description_concat_cases = [
    DescriptionConcatTestCase("all_null_gives_empty_description", None, None, None, ""),
    DescriptionConcatTestCase(
        "only_missing_is_not_null", None, None, "missing", "missing "
    ),
    DescriptionConcatTestCase("only_old_is_not_null", None, "old", None, "old "),
    DescriptionConcatTestCase(
        "old_and_missing_are_not_null", None, "old", "missing", "old missing "
    ),
    DescriptionConcatTestCase("only_new_is_not_null", "new", None, None, "new "),
    DescriptionConcatTestCase(
        "new_and_missing_are_not_null", "new", None, "missing", "new missing "
    ),
    DescriptionConcatTestCase(
        "new_and_old_are_not_null", "new", "old", None, "new old "
    ),
    DescriptionConcatTestCase(
        "new_old_and_missing_are_all_not_null",
        "new",
        "old",
        "missing",
        "new old missing ",
    ),
]


class TestCreateDescriptionColumnForParentAccounts:
    @pytest.mark.parametrize(
        "new_potential_subs, old_potential_subs, missing_or_incorrect_potential_subs, "
        "expected_description",
        [c.as_pytest_param() for c in description_concat_cases],
    )
    def test_concatenates_non_null_segments_with_trailing_spaces(
        self,
        new_potential_subs,
        old_potential_subs,
        missing_or_incorrect_potential_subs,
        expected_description,
    ):
        input_lf = pl.LazyFrame(
            {
                ReconColumn.new_potential_subs: [new_potential_subs],
                ReconColumn.old_potential_subs: [old_potential_subs],
                ReconColumn.missing_or_incorrect_potential_subs: [
                    missing_or_incorrect_potential_subs
                ],
            },
            schema={
                ReconColumn.new_potential_subs: pl.String,
                ReconColumn.old_potential_subs: pl.String,
                ReconColumn.missing_or_incorrect_potential_subs: pl.String,
            },
        )

        returned_df = job.create_description_column_for_parent_accounts(
            input_lf
        ).collect()

        assert returned_df[ReconColumn.description].item() == expected_description


class TestCreateMissingColumnsRequiredForOutput:
    def test_adds_expected_fixed_and_derived_columns(self):
        input_lf = pl.LazyFrame(
            {
                AWPClean.nmds_id: ["id_1"],
                AWPClean.establishment_type: ["care_home"],
                AWPClean.region_id: ["region"],
                AWPClean.establishment_name: ["Care Home Name"],
            }
        )

        returned_df = job.create_missing_columns_required_for_output(input_lf).collect()
        row = returned_df.row(0, named=True)

        assert row[ReconColumn.sector] == "care_home"
        assert row[ReconColumn.sfc_region] == "region"
        assert row[ReconColumn.name] == "Care Home Name"
        assert row[ReconColumn.nmds] == "id_1"
        assert row[ReconColumn.workplace_id] == "id_1"
        assert row[ReconColumn.requester_name] == "id_1 Care Home Name"
        assert row[ReconColumn.requester_name_2] == "id_1 Care Home Name"
        assert row[ReconColumn.status] == "Open"
        assert row[ReconColumn.technician] == "_"
        assert row[ReconColumn.manual_call_log] == "No"
        assert row[ReconColumn.mode] == "Internal"
        assert row[ReconColumn.priority] == "Priority 5"
        assert row[ReconColumn.category] == "CQC work"
        assert row[ReconColumn.sub_category] == "CQC work"
        assert row[ReconColumn.is_requester_named] == "Yes"
        assert row[ReconColumn.security_question] == "N/A"
        assert row[ReconColumn.website] == "ASC-WDS"
        assert row[ReconColumn.item] == "CQC work"
        assert row[ReconColumn.phone] == 0


class TestFinalColumnSelection:
    def test_selects_expected_columns_in_order(self):
        column_names = [
            ReconColumn.subject,
            ReconColumn.nmds,
            ReconColumn.name,
            ReconColumn.description,
            ReconColumn.requester_name_2,
            ReconColumn.requester_name,
            ReconColumn.sector,
            ReconColumn.status,
            ReconColumn.technician,
            ReconColumn.sfc_region,
            ReconColumn.manual_call_log,
            ReconColumn.mode,
            ReconColumn.priority,
            ReconColumn.category,
            ReconColumn.sub_category,
            ReconColumn.is_requester_named,
            ReconColumn.security_question,
            ReconColumn.website,
            ReconColumn.item,
            ReconColumn.workplace_id,
        ]
        input_lf = pl.LazyFrame(
            {**{col: ["value"] for col in column_names}, "extra_column": ["extra"]}
        ).with_columns(pl.lit(0).alias(ReconColumn.phone))

        returned_columns = job.final_column_selection(input_lf).collect_schema().names()

        assert returned_columns == [
            ReconColumn.subject,
            ReconColumn.nmds,
            ReconColumn.name,
            ReconColumn.description,
            ReconColumn.requester_name_2,
            ReconColumn.requester_name,
            ReconColumn.sector,
            ReconColumn.status,
            ReconColumn.technician,
            ReconColumn.sfc_region,
            ReconColumn.manual_call_log,
            ReconColumn.mode,
            ReconColumn.priority,
            ReconColumn.category,
            ReconColumn.sub_category,
            ReconColumn.is_requester_named,
            ReconColumn.security_question,
            ReconColumn.website,
            ReconColumn.item,
            ReconColumn.phone,
            ReconColumn.workplace_id,
        ]

    def test_sorts_by_description_then_nmds(self):
        other_columns = [
            ReconColumn.subject,
            ReconColumn.name,
            ReconColumn.requester_name_2,
            ReconColumn.requester_name,
            ReconColumn.sector,
            ReconColumn.status,
            ReconColumn.technician,
            ReconColumn.sfc_region,
            ReconColumn.manual_call_log,
            ReconColumn.mode,
            ReconColumn.priority,
            ReconColumn.category,
            ReconColumn.sub_category,
            ReconColumn.is_requester_named,
            ReconColumn.security_question,
            ReconColumn.website,
            ReconColumn.item,
            ReconColumn.workplace_id,
        ]
        input_lf = pl.LazyFrame(
            {
                **{col: [""] * 4 for col in other_columns},
                ReconColumn.nmds: ["nmds_1", "nmds_2", "nmds_2", "nmds_1"],
                ReconColumn.description: ["desc_a", "desc_b", "desc_a", "desc_b"],
            }
        ).with_columns(pl.lit(0).alias(ReconColumn.phone))

        returned_df = job.final_column_selection(input_lf).collect()

        expected_df = pl.LazyFrame(
            {
                ReconColumn.nmds: ["nmds_1", "nmds_2", "nmds_1", "nmds_2"],
                ReconColumn.description: ["desc_a", "desc_a", "desc_b", "desc_b"],
            }
        ).collect()

        pl_testing.assert_frame_equal(
            returned_df.select(ReconColumn.nmds, ReconColumn.description), expected_df
        )
