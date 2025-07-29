from dataclasses import dataclass
from datetime import date

from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_values.categorical_column_values import (
    CQCCurrentOrHistoricValues,
    CQCRatingsValues,
    CQCCurrentOrHistoricValues,
    IsParent,
    LocationType,
    ParentsOrSinglesAndSubs,
    RegistrationStatus,
    SingleSubDescription,
)


@dataclass
class ReconciliationData:
    # fmt: off
    input_ascwds_workplace_rows = [
        (date(2024, 4, 1), "100", "A100", "No", "100", "Workplace has ownership", "Private sector", "Not regulated", None, "10", "Est Name 00", "1"),  # Single - not CQC regtype - INCLUDED
        (date(2024, 4, 1), "101", "A101", "No", "101", "Workplace has ownership", "Private sector", "CQC regulated", "1-001", "10", "Est Name 01", "1"),  # Single - ID matches - EXCLUDED
        (date(2024, 4, 1), "102", "A102", "No", "102", "Workplace has ownership", "Private sector", "CQC regulated", "1-902", "10", "Est Name 02", "2"),  # Single - ID matches dereg - EXCLUDED as deregistered before previous month
        (date(2024, 4, 1), "103", "A103", "No", "103", "Workplace has ownership", "Private sector", "CQC regulated", "1-903", "10", "Est Name 03", "3"),  # Single - ID matches dereg - INCLUDED
        (date(2024, 4, 1), "104", "A104", "No", "104", "Workplace has ownership", "Private sector", "CQC regulated", "1-501", "10", "Est Name 04", "4"),  # Single - ID doesn't exist in CQC - INCLUDED
        (date(2024, 4, 1), "105", "A105", "No", "105", "Workplace has ownership", "Private sector", "CQC regulated", None, "10", "Est Name 05", "5"),  # Single - missing CQC ID - INCLUDED
        (date(2024, 4, 1), "106", "A106", "No", "206", "Workplace has ownership", "Private sector", "CQC regulated", "1-002", "10", "Est Name 06", "6"),  # Sub - ID matches - EXCLUDED
        (date(2024, 4, 1), "107", "A107", "No", "207", "Workplace has ownership", "Private sector", "CQC regulated", "1-912", "10", "Est Name 07", "7"),  # Sub - ID matches dereg - EXCLUDED as deregistered before previous month
        (date(2024, 4, 1), "108", "A108", "No", "208", "Workplace has ownership", "Private sector", "CQC regulated", "1-913", "10", "Est Name 08", "8"),  # Sub - ID matches dereg - INCLUDED
        (date(2024, 4, 1), "109", "A109", "No", "209", "Workplace has ownership", "Private sector", "CQC regulated", "1-502", "10", "Est Name 09", "9"),  # Sub - ID doesn't exist in CQC - INCLUDED
        (date(2024, 4, 1), "110", "A110", "No", "210", "Workplace has ownership", "Private sector", "CQC regulated", None, "10", "Est Name 10", "9"),  # Sub - missing CQC ID - INCLUDED
        (date(2024, 4, 1), "111", "A111", "No", "211", "Workplace has ownership", "Private sector", "CQC regulated", "1-995", "10", "Est Name 11", "9"),  # Sub - ID dereg but in current month - EXCLUDED
        (date(2024, 4, 1), "112", "A112", "No", "212", "Workplace has ownership", "Private sector", "CQC regulated", "1-913", "72", "Est Name 08", "8"),  # Sub - ID matches dereg - INCLUDED (keep head office for incorect ID)
        (date(2024, 4, 1), "201", "A201", "Yes", "201", "Workplace has ownership", "Private sector", "Not regulated", None, "10", "Parent 01", "1"),  # Parent - has issues - INCLUDED
        (date(2024, 4, 1), "202", "A202", "No", "201", "Parent has ownership", "Private sector", "CQC regulated", "1-003", "10", "Est Name 22", "2"),  # Parent - ID matches - EXCLUDED
        (date(2024, 4, 1), "203", "A203", "No", "201", "Parent has ownership", "Private sector", "CQC regulated", "1-922", "10", "Est Name 23", "3"),  # Parent - ID matches dereg - INCLUDED (deregistered before previous month)
        (date(2024, 4, 1), "204", "A204", "No", "201", "Parent has ownership", "Private sector", "CQC regulated", "1-923", "10", "Est Name 24", "4"),  # Parent - ID matches dereg - INCLUDED (deregistered in previous month)
        (date(2024, 4, 1), "205", "A205", "No", "201", "Parent has ownership", "Private sector", "CQC regulated", "1-503", "10", "Est Name 25", "5"),  # Parent - ID doesn't exist in CQC - INCLUDED
        (date(2024, 4, 1), "206", "A206", "No", "201", "Parent has ownership", "Private sector", "CQC regulated", None, "10", "Est Name 26", "6"),  # Parent - missing CQC ID - INCLUDED
        (date(2024, 4, 1), "206", "A206", "No", "201", "Parent has ownership", "Private sector", "CQC regulated", None, "72", "Est Name 26", "6"),  # Parent - head office - EXCLUDED
        (date(2024, 4, 1), "301", "A301", "Yes", "301", "Workplace has ownership", "Private sector", "CQC regulated", "1-004", "10", "Parent 02", "1"),  # Parent - no issues - EXCLUDED
    ]
    # fmt: on

    input_cqc_location_api_rows = [
        ("20240101", "1-901", "Deregistered", "2024-01-01"),
        ("20240401", "1-001", "Registered", None),
        ("20240401", "1-002", "Registered", None),
        ("20240401", "1-003", "Registered", None),
        ("20240401", "1-004", "Registered", None),
        ("20240401", "1-902", "Deregistered", "2024-01-01"),
        ("20240401", "1-903", "Deregistered", "2024-03-01"),
        ("20240401", "1-904", "Deregistered", "2024-03-01"),
        ("20240401", "1-912", "Deregistered", "2024-01-01"),
        ("20240401", "1-913", "Deregistered", "2024-03-01"),
        ("20240401", "1-922", "Deregistered", "2024-01-01"),
        ("20240401", "1-923", "Deregistered", "2024-03-01"),
        ("20240401", "1-995", "Deregistered", "2024-04-01"),
    ]


@dataclass
class ReconciliationUtilsData:
    input_ascwds_workplace_rows = ReconciliationData.input_ascwds_workplace_rows
    input_cqc_location_api_rows = ReconciliationData.input_cqc_location_api_rows

    dates_to_use_mid_month_rows = [
        ("1-001", date(2024, 3, 28)),
        ("1-002", date(2023, 1, 1)),
    ]
    dates_to_use_first_month_rows = [
        ("1-001", date(2024, 4, 1)),
        ("1-002", date(2023, 1, 1)),
    ]

    expected_prepared_most_recent_cqc_location_rows = [
        ("1-001", "Registered", None, date(2024, 4, 1)),
        ("1-002", "Registered", None, date(2024, 4, 1)),
        ("1-003", "Registered", None, date(2024, 4, 1)),
        ("1-004", "Registered", None, date(2024, 4, 1)),
        ("1-902", "Deregistered", date(2024, 1, 1), date(2024, 4, 1)),
        ("1-903", "Deregistered", date(2024, 3, 1), date(2024, 4, 1)),
        ("1-904", "Deregistered", date(2024, 3, 1), date(2024, 4, 1)),
        ("1-912", "Deregistered", date(2024, 1, 1), date(2024, 4, 1)),
        ("1-913", "Deregistered", date(2024, 3, 1), date(2024, 4, 1)),
        ("1-922", "Deregistered", date(2024, 1, 1), date(2024, 4, 1)),
        ("1-923", "Deregistered", date(2024, 3, 1), date(2024, 4, 1)),
        ("1-995", "Deregistered", date(2024, 4, 1), date(2024, 4, 1)),
    ]

    dates_to_use_rows = [
        ("1-001", date(2024, 3, 28)),
        ("1-002", date(2023, 1, 1)),
    ]

    regtype_rows = [
        ("1", "Not regulated"),
        ("2", "CQC regulated"),
        ("3", None),
    ]

    remove_head_office_accounts_rows = [
        ("1", "1-001", "Head office services"),
        ("2", "1-002", "any non-head office service"),
        ("3", None, "any non-head office service"),
        ("4", None, "Head office services"),
    ]

    first_of_most_recent_month = date(2024, 4, 1)
    first_of_previous_month = date(2024, 3, 1)

    # fmt: off
    filter_to_relevant_rows = [
        ("loc_1", None, date(2024, 3, 31), ParentsOrSinglesAndSubs.parents),  # keep
        ("loc_2", None, date(2024, 3, 31), ParentsOrSinglesAndSubs.singles_and_subs),  # keep
        ("loc_3", None, date(2024, 3, 1), ParentsOrSinglesAndSubs.parents),  # keep
        ("loc_4", None, date(2024, 3, 1), ParentsOrSinglesAndSubs.singles_and_subs),  # keep
        ("loc_5", None, date(2024, 2, 29), ParentsOrSinglesAndSubs.parents),  # keep
        ("loc_6", None, date(2024, 2, 29), ParentsOrSinglesAndSubs.singles_and_subs),  # keep
        ("loc_7", None, date(2024, 4, 1), ParentsOrSinglesAndSubs.parents),  # keep
        ("loc_8", None, date(2024, 4, 1), ParentsOrSinglesAndSubs.singles_and_subs),  # keep
        ("loc_9", RegistrationStatus.registered, date(2024, 3, 31), ParentsOrSinglesAndSubs.parents),  # remove
        ("loc_10", RegistrationStatus.registered, date(2024, 3, 31), ParentsOrSinglesAndSubs.singles_and_subs),  # remove
        ("loc_11", RegistrationStatus.registered, date(2024, 3, 1), ParentsOrSinglesAndSubs.parents),  # remove
        ("loc_12", RegistrationStatus.registered, date(2024, 3, 1), ParentsOrSinglesAndSubs.singles_and_subs),  # remove
        ("loc_13", RegistrationStatus.registered, date(2024, 2, 29), ParentsOrSinglesAndSubs.parents),  # remove
        ("loc_14", RegistrationStatus.registered, date(2024, 2, 29), ParentsOrSinglesAndSubs.singles_and_subs),  # remove
        ("loc_15", RegistrationStatus.registered, date(2024, 4, 1), ParentsOrSinglesAndSubs.parents),  # remove
        ("loc_16", RegistrationStatus.registered, date(2024, 4, 1), ParentsOrSinglesAndSubs.singles_and_subs),  # remove
        ("loc_17", RegistrationStatus.deregistered, date(2024, 3, 31), ParentsOrSinglesAndSubs.parents),  # keep
        ("loc_18", RegistrationStatus.deregistered, date(2024, 3, 31), ParentsOrSinglesAndSubs.singles_and_subs),  # keep
        ("loc_19", RegistrationStatus.deregistered, date(2024, 3, 1), ParentsOrSinglesAndSubs.parents),  # keep
        ("loc_20", RegistrationStatus.deregistered, date(2024, 3, 1), ParentsOrSinglesAndSubs.singles_and_subs),  # keep
        ("loc_21", RegistrationStatus.deregistered, date(2024, 2, 29), ParentsOrSinglesAndSubs.parents),  # keep
        ("loc_22", RegistrationStatus.deregistered, date(2024, 2, 29), ParentsOrSinglesAndSubs.singles_and_subs),  # remove
        ("loc_23", RegistrationStatus.deregistered, date(2024, 4, 1), ParentsOrSinglesAndSubs.parents),  # remove
        ("loc_24", RegistrationStatus.deregistered, date(2024, 4, 1), ParentsOrSinglesAndSubs.singles_and_subs),  # remove
    ]
    # fmt: on

    parents_or_singles_and_subs_rows = [
        ("1", "Yes", "Parent has ownership"),
        ("2", "Yes", "Workplace has ownership"),
        ("3", "No", "Workplace has ownership"),
        ("4", "No", "Parent has ownership"),
    ]
    # fmt: off
    expected_parents_or_singles_and_subs_rows = [
        ("1", "Yes", "Parent has ownership", ParentsOrSinglesAndSubs.parents),
        ("2", "Yes", "Workplace has ownership", ParentsOrSinglesAndSubs.parents),
        ("3", "No", "Workplace has ownership", ParentsOrSinglesAndSubs.singles_and_subs),
        ("4", "No", "Parent has ownership", ParentsOrSinglesAndSubs.parents),
    ]
    # fmt: on

    add_singles_and_subs_description_rows = [
        ("loc_1", date(2024, 3, 28)),
        ("loc_2", None),
    ]
    # fmt: off
    expected_singles_and_subs_description_rows = [
        ("loc_1", date(2024, 3, 28), SingleSubDescription.single_sub_deregistered_description),
        ("loc_2", None, SingleSubDescription.single_sub_reg_type_description),
    ]
    # fmt: on

    create_missing_columns_rows = [
        ("id_1", "care_home", "region", "Care Home Name"),
    ]

    expected_create_missing_columns_rows = [
        (
            "id_1",
            "care_home",
            "region",
            "Care Home Name",
            "id_1",
            "id_1",
            "id_1 Care Home Name",
            "id_1 Care Home Name",
            "Open",
            "_",
            "No",
            "Internal",
            "Priority 5",
            "CQC work",
            "CQC work",
            "Yes",
            "N/A",
            "ASC-WDS",
            "CQC work",
            0,
        ),
    ]

    # fmt: off
    final_column_selection_rows = [
        ("extra_col", "", "", "", "", "", "", "", "", 0, "", "", "nmds_1", "", "desc_a", "", "", "", "", "", "", ""),
        ("extra_col", "", "", "", "", "", "", "", "", 0, "", "", "nmds_2", "", "desc_b", "", "", "", "", "", "", ""),
        ("extra_col", "", "", "", "", "", "", "", "", 0, "", "", "nmds_2", "", "desc_a", "", "", "", "", "", "", ""),
        ("extra_col", "", "", "", "", "", "", "", "", 0, "", "", "nmds_1", "", "desc_b", "", "", "", "", "", "", ""),
    ]
    expected_final_column_selection_rows = [
        ("", "nmds_1", "", "desc_a", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 0, ""),
        ("", "nmds_2", "", "desc_a", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 0, ""),
        ("", "nmds_1", "", "desc_b", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 0, ""),
        ("", "nmds_2", "", "desc_b", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 0, ""),
    ]
    # fmt: on

    add_subject_column_rows = [("loc_1",)]
    expected_add_subject_column_rows = [("loc_1", "test_subject")]

    new_issues_rows = [
        ("org 1", "loc 1", ""),
        ("org 1", "loc 2", ""),
        ("org 1", "loc 3", ""),
        ("org 2", "loc 4", ""),
        ("org 2", "loc 5", ""),
        ("org 3", "loc 6", ""),
        ("org 5", "loc 7", ""),
    ]

    unique_rows = [
        ("org 1", ""),
        ("org 2", ""),
        ("org 3", ""),
        ("org 6", ""),
    ]

    expected_join_array_of_nmdsids_rows = [
        ("org 1", "", "new_column: loc 2, loc 3, loc 1"),
        ("org 2", "", "new_column: loc 5, loc 4"),
        ("org 3", "", "new_column: loc 6"),
        ("org 6", "", None),
    ]

    new_column = "new_column"

    create_parents_description_rows = [
        ("org 1", None, None, None),
        ("org 2", None, None, "missing"),
        ("org 3", None, "old", None),
        ("org 4", None, "old", "missing"),
        ("org 5", "new", None, None),
        ("org 6", "new", None, "missing"),
        ("org 7", "new", "old", None),
        ("org 8", "new", "old", "missing"),
    ]
    expected_create_parents_description_rows = [
        ("org 1", None, None, None, ""),
        ("org 2", None, None, "missing", "missing "),
        ("org 3", None, "old", None, "old "),
        ("org 4", None, "old", "missing", "old missing "),
        ("org 5", "new", None, None, "new "),
        ("org 6", "new", None, "missing", "new missing "),
        ("org 7", "new", "old", None, "new old "),
        ("org 8", "new", "old", "missing", "new old missing "),
    ]

    get_ascwds_parent_accounts_rows = [
        (
            "nmds_1",
            "estab_1",
            "name",
            "org_1",
            "type",
            "region_id",
            IsParent.is_parent,
            "other",
        ),
        (
            "nmds_2",
            "estab_2",
            "name",
            "org_2",
            "type",
            "region_id",
            IsParent.is_not_parent,
            "other",
        ),
        ("nmds_3", "estab_3", "name", "org_3", "type", "region_id", None, "other"),
    ]
    expected_get_ascwds_parent_accounts_rows = [
        ("nmds_1", "estab_1", "name", "org_1", "type", "region_id"),
    ]

    cqc_data_for_join_rows = [
        ("loc_1", "name"),
        ("loc_2", "name"),
    ]
    ascwds_data_for_join_rows = [
        ("loc_1", "estab_1"),
        ("loc_3", "estab_2"),
    ]
    expected_data_for_join_rows = [
        ("loc_1", "estab_1", "name"),
        ("loc_3", "estab_2", None),
    ]


@dataclass
class MergeCoverageData:
    # fmt: off
    clean_cqc_location_for_merge_rows = [
        (date(2024, 1, 1), "1-000000001", "Name 1", "AB1 2CD", "Independent", "Y", 10),
        (date(2024, 1, 1), "1-000000002", "Name 2", "EF3 4GH", "Independent", "N", None),
        (date(2024, 1, 1), "1-000000003", "Name 3", "IJ5 6KL", "Independent", "N", None),
        (date(2024, 2, 1), "1-000000001", "Name 1", "AB1 2CD", "Independent", "Y", 10),
        (date(2024, 2, 1), "1-000000002", "Name 2", "EF3 4GH", "Independent", "N", None),
        (date(2024, 2, 1), "1-000000003", "Name 3", "IJ5 6KL", "Independent", "N", None),
        (date(2024, 3, 1), "1-000000001", "Name 1", "AB1 2CD", "Independent", "Y", 10),
        (date(2024, 3, 1), "1-000000002", "Name 2", "EF3 4GH", "Independent", "N", None),
        (date(2024, 3, 1), "1-000000003", "Name 3", "IJ5 6KL", "Independent", "N", None),
    ]
    # fmt: on

    clean_ascwds_workplace_for_merge_rows = [
        (date(2024, 1, 1), "1-000000001", date(2024, 1, 1), "1", 1),
        (date(2024, 1, 1), "1-000000003", date(2024, 1, 1), "3", 2),
        (date(2024, 1, 5), "1-000000001", date(2024, 1, 1), "1", 3),
        (date(2024, 1, 9), "1-000000001", date(2024, 1, 1), "1", 4),
        (date(2024, 1, 9), "1-000000003", date(2024, 1, 1), "3", 5),
        (date(2024, 3, 1), "1-000000003", date(2024, 1, 1), "4", 6),
    ]

    # fmt: off
    expected_cqc_and_ascwds_merged_rows = [
        ("1-000000001", date(2024, 1, 1), date(2024, 1, 1), "Name 1", "AB1 2CD", "Independent", "Y", 10, date(2024, 1, 1), "1", 1),
        ("1-000000002", date(2024, 1, 1), date(2024, 1, 1), "Name 2", "EF3 4GH", "Independent", "N", None, None, None, None),
        ("1-000000003", date(2024, 1, 1), date(2024, 1, 1), "Name 3", "IJ5 6KL", "Independent", "N", None, date(2024, 1, 1), "3", 2),
        ("1-000000001", date(2024, 1, 9), date(2024, 2, 1), "Name 1", "AB1 2CD", "Independent", "Y", 10, date(2024, 1, 1), "1", 4),
        ("1-000000002", date(2024, 1, 9), date(2024, 2, 1), "Name 2", "EF3 4GH", "Independent", "N", None, None, None, None),
        ("1-000000003", date(2024, 1, 9), date(2024, 2, 1), "Name 3", "IJ5 6KL", "Independent", "N", None, date(2024, 1, 1), "3", 5),
        ("1-000000001", date(2024, 3, 1), date(2024, 3, 1), "Name 1", "AB1 2CD", "Independent", "Y", 10, None, None, None),
        ("1-000000002", date(2024, 3, 1), date(2024, 3, 1), "Name 2", "EF3 4GH", "Independent", "N", None, None, None, None),
        ("1-000000003", date(2024, 3, 1), date(2024, 3, 1), "Name 3", "IJ5 6KL", "Independent", "N", None, date(2024, 1, 1), "4", 6),
    ]
    # fmt: on

    sample_in_ascwds_rows = [
        (None,),
        ("1",),
    ]

    expected_in_ascwds_rows = [
        (None, 0),
        ("1", 1),
    ]

    sample_cqc_locations_rows = [("1-000000001",), ("1-000000002",)]

    sample_cqc_ratings_for_merge_rows = [
        ("1-000000001", "2024-01-01", "Good", 0, CQCCurrentOrHistoricValues.historic),
        ("1-000000001", "2024-01-02", "Good", 1, CQCCurrentOrHistoricValues.current),
        ("1-000000001", None, "Good", None, None),
        ("1-000000002", "2024-01-01", None, 1, CQCCurrentOrHistoricValues.current),
        ("1-000000002", "2024-01-01", None, 1, CQCCurrentOrHistoricValues.historic),
        (
            "1-000000002",
            "2024-01-01",
            None,
            1,
            CQCCurrentOrHistoricValues.historic,
        ),  # CQC ratings data will contain duplicates so this needs to be handled correctly
    ]

    # fmt: off
    expected_cqc_locations_and_latest_cqc_rating_rows = [
        ("1-000000001", "2024-01-02", "Good",),
        ("1-000000002", "2024-01-01", None,),
    ]
    # fmt: on


@dataclass
class ValidateMergedCoverageData:
    cqc_locations_rows = [
        (date(2024, 1, 1), "1-001", "Name", "AB1 2CD", "Y", 10, "2024", "01", "01"),
        (
            date(2024, 1, 1),
            "1-002",
            "Name",
            "EF3 4GH",
            "N",
            None,
            "2024",
            "01",
            "01",
        ),
        (date(2024, 2, 1), "1-001", "Name", "AB1 2CD", "Y", 10, "2024", "02", "01"),
        (
            date(2024, 2, 1),
            "1-002",
            "Name",
            "EF3 4GH",
            "N",
            None,
            "2024",
            "02",
            "01",
        ),
    ]

    merged_coverage_rows = [
        ("1-001", date(2024, 1, 1), date(2024, 1, 1), "Name", "AB1 2CD", "Y"),
        ("1-002", date(2024, 1, 1), date(2024, 1, 1), "Name", "EF3 4GH", "N"),
        ("1-001", date(2024, 1, 9), date(2024, 1, 1), "Name", "AB1 2CD", "Y"),
        ("1-002", date(2024, 1, 9), date(2024, 1, 1), "Name", "EF3 4GH", "N"),
    ]
    calculate_expected_size_rows = [
        ("loc 1", date(2024, 1, 1), "name", "AB1 2CD", "Y", "2024", "01", "01"),
        ("loc 1", date(2024, 1, 8), "name", "AB1 2CD", "Y", "2024", "01", "08"),
        ("loc 2", date(2024, 1, 1), "name", "AB1 2CD", "Y", "2024", "01", "01"),
    ]


@dataclass
class FlattenCQCRatings:
    test_cqc_locations_rows = [
        (
            "loc_1",
            RegistrationStatus.registered,
            LocationType.social_care_identifier,
            "20240101",
            "2024",
            "01",
            "01",
            {
                CQCL.overall: {
                    CQCL.organisation_id: None,
                    CQCL.rating: "Overall rating Excellent",
                    CQCL.report_date: "report_date",
                    CQCL.report_link_id: None,
                    CQCL.use_of_resources: {
                        CQCL.organisation_id: None,
                        CQCL.summary: None,
                        CQCL.use_of_resources_rating: None,
                        CQCL.combined_quality_summary: None,
                        CQCL.combined_quality_rating: None,
                        CQCL.report_date: None,
                        CQCL.report_link_id: None,
                    },
                    CQCL.key_question_ratings: [
                        {
                            CQCL.name: "Safe",
                            CQCL.rating: "Safe rating Good",
                            CQCL.report_date: None,
                            CQCL.organisation_id: None,
                            CQCL.report_link_id: None,
                        },
                        {
                            CQCL.name: "Well-led",
                            CQCL.rating: "Well-led rating Good",
                            CQCL.report_date: None,
                            CQCL.organisation_id: None,
                            CQCL.report_link_id: None,
                        },
                        {
                            CQCL.name: "Caring",
                            CQCL.rating: "Caring rating Good",
                            CQCL.report_date: None,
                            CQCL.organisation_id: None,
                            CQCL.report_link_id: None,
                        },
                        {
                            CQCL.name: "Responsive",
                            CQCL.rating: "Responsive rating Good",
                            CQCL.report_date: None,
                            CQCL.organisation_id: None,
                            CQCL.report_link_id: None,
                        },
                        {
                            CQCL.name: "Effective",
                            CQCL.rating: "Effective rating Good",
                            CQCL.report_date: None,
                            CQCL.organisation_id: None,
                            CQCL.report_link_id: None,
                        },
                    ],
                },
                CQCL.service_ratings: [
                    {
                        CQCL.name: None,
                        CQCL.rating: None,
                        CQCL.report_date: None,
                        CQCL.organisation_id: None,
                        CQCL.report_link_id: None,
                        CQCL.key_question_ratings: [
                            {
                                CQCL.name: None,
                                CQCL.rating: None,
                            },
                        ],
                    },
                ],
            },
            [
                {
                    CQCL.report_date: "report_date",
                    CQCL.report_link_id: None,
                    CQCL.organisation_id: None,
                    CQCL.service_ratings: [
                        {
                            CQCL.name: None,
                            CQCL.rating: None,
                            CQCL.key_question_ratings: [
                                {
                                    CQCL.name: None,
                                    CQCL.rating: None,
                                },
                            ],
                        },
                    ],
                    CQCL.overall: {
                        CQCL.rating: "Overall rating Excellent",
                        CQCL.use_of_resources: {
                            CQCL.combined_quality_rating: None,
                            CQCL.combined_quality_summary: None,
                            CQCL.use_of_resources_rating: None,
                            CQCL.use_of_resources_summary: None,
                        },
                        CQCL.key_question_ratings: [
                            {CQCL.name: "Safe", CQCL.rating: "Safe rating Good"},
                            {
                                CQCL.name: "Well-led",
                                CQCL.rating: "Well-led rating Good",
                            },
                            {CQCL.name: "Caring", CQCL.rating: "Caring rating Good"},
                            {
                                CQCL.name: "Responsive",
                                CQCL.rating: "Responsive rating Good",
                            },
                            {
                                CQCL.name: "Effective",
                                CQCL.rating: "Effective rating Good",
                            },
                        ],
                    },
                },
            ],
        ),
    ]
    test_ascwds_workplace_rows = [("loc_1", "estab_1", "20240101", "2021", "01", "01")]
    filter_to_first_import_of_most_recent_month_rows = [
        ("loc_1", "20240101", "2024", "01", "01"),
        ("loc_2", "20231201", "2023", "12", "01"),
    ]
    filter_to_first_import_of_most_recent_month_when_two_imports_in_most_recent_month_rows = [
        ("loc_1", "20240101", "2024", "01", "01"),
        ("loc_2", "20231201", "2023", "12", "01"),
        ("loc_3", "20240104", "2024", "01", "04"),
    ]
    filter_to_first_import_of_most_recent_month_when_earliest_date_is_not_first_of_month_rows = [
        ("loc_1", "20240102", "2024", "01", "02"),
        ("loc_2", "20231201", "2023", "12", "01"),
        ("loc_3", "20240104", "2024", "01", "04"),
    ]
    expected_filter_to_first_import_of_most_recent_month_rows = [
        ("loc_1", "20240101", "2024", "01", "01"),
    ]
    expected_filter_to_first_import_of_most_recent_month_when_earliest_date_is_not_first_of_month_rows = [
        ("loc_1", "20240102", "2024", "01", "02"),
    ]

    flatten_current_ratings_rows = [
        (
            "loc_1",
            "registered",
            {
                CQCL.assessment: {
                    CQCL.ratings: {
                        CQCL.asg_ratings: {
                            CQCL.organisation_id: None,
                            CQCL.assessment_date: "assessment_date",
                            CQCL.rating: "Overall rating Excellent",
                            CQCL.report_date: "report_date",
                            CQCL.report_link_id: None,
                            CQCL.use_of_resources: {
                                CQCL.organisation_id: None,
                                CQCL.summary: None,
                                CQCL.use_of_resources_rating: None,
                                CQCL.combined_quality_summary: None,
                                CQCL.combined_quality_rating: None,
                                CQCL.report_date: None,
                                CQCL.report_link_id: None,
                            },
                            CQCL.key_question_ratings: [
                                {
                                    CQCL.name: "Caring",
                                    CQCL.rating: "Caring rating Good",
                                    CQCL.report_date: None,
                                    CQCL.organisation_id: None,
                                    CQCL.report_link_id: None,
                                },
                                {
                                    CQCL.name: "Effective",
                                    CQCL.rating: "Effective rating Good",
                                    CQCL.report_date: None,
                                    CQCL.organisation_id: None,
                                    CQCL.report_link_id: None,
                                },
                                {
                                    CQCL.name: "Responsive",
                                    CQCL.rating: "Responsive rating Good",
                                    CQCL.report_date: None,
                                    CQCL.organisation_id: None,
                                    CQCL.report_link_id: None,
                                },
                                {
                                    CQCL.name: "Safe",
                                    CQCL.rating: "Safe rating Good",
                                    CQCL.report_date: None,
                                    CQCL.organisation_id: None,
                                    CQCL.report_link_id: None,
                                },
                                {
                                    CQCL.name: "Well-led",
                                    CQCL.rating: "Well-led rating Good",
                                    CQCL.report_date: None,
                                    CQCL.organisation_id: None,
                                    CQCL.report_link_id: None,
                                },
                            ],
                        },
                    },
                    CQCL.service_ratings: [
                        {
                            CQCL.name: None,
                            CQCL.rating: None,
                            CQCL.report_date: None,
                            CQCL.organisation_id: None,
                            CQCL.report_link_id: None,
                            CQCL.key_question_ratings: [
                                {
                                    CQCL.name: None,
                                    CQCL.rating: None,
                                },
                            ],
                        },
                    ],
                },
            },
        ),
    ]

    flatten_historic_ratings_rows = [
        (
            "loc_1",
            "registered",
            [
                {
                    CQCL.report_date: "report_date",
                    CQCL.report_link_id: None,
                    CQCL.organisation_id: None,
                    CQCL.service_ratings: [
                        {
                            CQCL.name: None,
                            CQCL.rating: None,
                            CQCL.key_question_ratings: [
                                {
                                    CQCL.name: None,
                                    CQCL.rating: None,
                                },
                            ],
                        },
                    ],
                    CQCL.overall: {
                        CQCL.rating: "Overall rating Excellent",
                        CQCL.use_of_resources: {
                            CQCL.combined_quality_rating: None,
                            CQCL.combined_quality_summary: None,
                            CQCL.use_of_resources_rating: None,
                            CQCL.use_of_resources_summary: None,
                        },
                        CQCL.key_question_ratings: [
                            {CQCL.name: "Safe", CQCL.rating: "Safe rating Good"},
                            {
                                CQCL.name: "Well-led",
                                CQCL.rating: "Well-led rating Good",
                            },
                            {CQCL.name: "Caring", CQCL.rating: "Caring rating Good"},
                            {
                                CQCL.name: "Responsive",
                                CQCL.rating: "Responsive rating Good",
                            },
                            {
                                CQCL.name: "Effective",
                                CQCL.rating: "Effective rating Good",
                            },
                        ],
                    },
                },
            ],
        ),
    ]

    expected_flatten_ratings_rows = [
        (
            "loc_1",
            "registered",
            "report_date",
            "Overall rating Excellent",
            "Safe rating Good",
            "Well-led rating Good",
            "Caring rating Good",
            "Responsive rating Good",
            "Effective rating Good",
        )
    ]
    recode_unknown_to_null_rows = [
        (
            "loc_1",
            "registered",
            "report_date",
            "Excellent",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
        ),
        (
            "loc_2",
            "registered",
            "report_date",
            "Inspected but not rated",
            "No published rating",
            "Insufficient evidence to rate",
            "Good",
            "Good",
            "Good",
        ),
        (
            "loc_3",
            "No published rating",
            "",
            "Excellent",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
        ),
        (
            "loc_4",
            "deregistered",
            "report_date",
            "Inspected but not rated",
            "Inspected but not rated",
            "Inspected but not rated",
            "Inspected but not rated",
            "Inspected but not rated",
            "Inspected but not rated",
        ),
        (
            "loc_4",
            "deregistered",
            "report_date",
            "Inspected but not rated",
            "Inspected but not rated",
            "Inspected but not rated",
            "Inspected but not rated",
            "Inspected but not rated",
            "Insufficient evidence to rate",
        ),
    ]
    expected_recode_unknown_to_null_rows = [
        (
            "loc_1",
            "registered",
            "report_date",
            "Excellent",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
        ),
        (
            "loc_2",
            "registered",
            "report_date",
            None,
            None,
            None,
            "Good",
            "Good",
            "Good",
        ),
        (
            "loc_3",
            "No published rating",
            "",
            "Excellent",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
        ),
        (
            "loc_4",
            "deregistered",
            "report_date",
            None,
            None,
            None,
            None,
            None,
            None,
        ),
    ]

    add_current_or_historic_rows = [
        ("loc_1",),
    ]
    expected_add_current_rows = [
        ("loc_1", CQCCurrentOrHistoricValues.current),
    ]
    expected_add_historic_rows = [
        ("loc_1", CQCCurrentOrHistoricValues.historic),
    ]

    remove_blank_rows_rows = [
        (
            "loc_1",
            "Registered",
            "20240101",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
        ),
        (
            "loc_2",
            "Registered",
            "20240101",
            None,
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
        ),
        (
            "loc_3",
            "Registered",
            "20240101",
            "Good",
            None,
            "Good",
            "Good",
            "Good",
            "Good",
        ),
        (
            "loc_4",
            "Registered",
            "20240101",
            "Good",
            "Good",
            None,
            "Good",
            "Good",
            "Good",
        ),
        (
            "loc_5",
            "Registered",
            "20240101",
            "Good",
            "Good",
            "Good",
            None,
            "Good",
            "Good",
        ),
        (
            "loc_6",
            "Registered",
            "20240101",
            "Good",
            "Good",
            "Good",
            "Good",
            None,
            "Good",
        ),
        (
            "loc_7",
            "Registered",
            "20240101",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            None,
        ),
        ("loc_8", "Registered", "20240101", None, None, None, None, None, None),
        (
            "loc_1",
            "Registered",
            "20240101",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
        ),
    ]
    expected_remove_blank_rows_rows = [
        (
            "loc_1",
            "Registered",
            "20240101",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
        ),
        (
            "loc_2",
            "Registered",
            "20240101",
            None,
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
        ),
        (
            "loc_3",
            "Registered",
            "20240101",
            "Good",
            None,
            "Good",
            "Good",
            "Good",
            "Good",
        ),
        (
            "loc_4",
            "Registered",
            "20240101",
            "Good",
            "Good",
            None,
            "Good",
            "Good",
            "Good",
        ),
        (
            "loc_5",
            "Registered",
            "20240101",
            "Good",
            "Good",
            "Good",
            None,
            "Good",
            "Good",
        ),
        (
            "loc_6",
            "Registered",
            "20240101",
            "Good",
            "Good",
            "Good",
            "Good",
            None,
            "Good",
        ),
        (
            "loc_7",
            "Registered",
            "20240101",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            None,
        ),
    ]

    add_rating_sequence_rows = [
        ("loc_1", "2024-01-01"),
        ("loc_1", "2024-01-02"),
        ("loc_2", "2024-01-01"),
        ("loc_2", "2024-02-01"),
        ("loc_3", "2023-01-01"),
        ("loc_3", "2024-01-01"),
    ]
    expected_add_rating_sequence_rows = [
        ("loc_1", "2024-01-02", 2),
        ("loc_2", "2024-01-01", 1),
        ("loc_2", "2024-02-01", 2),
        ("loc_1", "2024-01-01", 1),
        ("loc_3", "2023-01-01", 1),
        ("loc_3", "2024-01-01", 2),
    ]
    expected_reversed_add_rating_sequence_rows = [
        ("loc_1", "2024-01-02", 1),
        ("loc_2", "2024-01-01", 2),
        ("loc_2", "2024-02-01", 1),
        ("loc_1", "2024-01-01", 2),
        ("loc_3", "2023-01-01", 2),
        ("loc_3", "2024-01-01", 1),
    ]

    add_latest_rating_flag_rows = [
        ("loc_1", 1),
        ("loc_2", 1),
        ("loc_2", 2),
    ]
    expected_add_latest_rating_flag_rows = [
        ("loc_1", 1, 1),
        ("loc_2", 1, 1),
        ("loc_2", 2, 0),
    ]

    create_standard_rating_dataset_rows = [
        (
            "loc_1",
            "Registered",
            "2024-01-01",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Current",
            1,
            1,
            1,
            3,
            3,
            3,
            3,
            3,
            15,
        ),
        (
            "loc_1",
            "Registered",
            "2024-01-01",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Current",
            1,
            1,
            1,
            3,
            3,
            3,
            3,
            3,
            15,
        ),
        (
            "loc_1",
            "Degistered",
            "2024-01-01",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Current",
            1,
            1,
            1,
            3,
            3,
            3,
            3,
            3,
            15,
        ),
        (
            "loc_1",
            "Registered",
            "2024-01-01",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Historic",
            1,
            1,
            1,
            3,
            3,
            3,
            3,
            3,
            15,
        ),
        (
            "loc_1",
            "Registered",
            "2024-01-01",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Current",
            1,
            0,
            1,
            3,
            3,
            3,
            3,
            3,
            15,
        ),
    ]
    expected_create_standard_rating_dataset_rows = [
        (
            "loc_1",
            "2024-01-01",
            "Current",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            1,
            1,
            3,
            3,
            3,
            3,
            3,
            15,
        ),
        (
            "loc_1",
            "2024-01-01",
            "Historic",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            1,
            1,
            3,
            3,
            3,
            3,
            3,
            15,
        ),
    ]
    select_ratings_for_benchmarks_rows = [
        ("loc_1", RegistrationStatus.registered, CQCCurrentOrHistoricValues.current),
        ("loc_2", RegistrationStatus.registered, CQCCurrentOrHistoricValues.historic),
        ("loc_3", RegistrationStatus.deregistered, CQCCurrentOrHistoricValues.current),
        ("loc_4", RegistrationStatus.deregistered, CQCCurrentOrHistoricValues.historic),
    ]
    expected_select_ratings_for_benchmarks_rows = [
        ("loc_1", RegistrationStatus.registered, CQCCurrentOrHistoricValues.current),
    ]

    add_good_or_outstanding_flag_rows = [
        ("loc_1", CQCRatingsValues.outstanding),
        ("loc_2", CQCRatingsValues.good),
        ("loc_3", "other rating"),
        ("loc_1", None),
    ]
    expected_add_good_or_outstanding_flag_rows = [
        ("loc_1", CQCRatingsValues.outstanding, 1),
        ("loc_2", CQCRatingsValues.good, 1),
        ("loc_3", "other rating", 0),
        ("loc_1", None, 0),
    ]
    ratings_join_establishment_ids_rows = [
        ("loc_1", "ratings data"),
        ("loc_3", "ratings data"),
    ]

    ascwds_join_establishment_ids_rows = [
        ("loc_1", "estab_1", "20240101"),
        ("loc_2", "estab_2", "20240101"),
    ]
    expected_join_establishment_ids_rows = [
        ("loc_1", "ratings data", "estab_1"),
        ("loc_3", "ratings data", None),
    ]
    create_benchmark_ratings_dataset_rows = [
        ("loc_1", "estab_1", 1, "Good", "2024-01-01", ""),
        ("loc_2", "estab_2", 0, "Requires improvement", "2024-01-01", ""),
        ("loc_3", None, 1, "Good", "2024-01-01", ""),
        ("loc_4", "estab_2", 0, None, "2024-01-01", ""),
        ("loc_5", None, 0, None, "2024-01-01", ""),
    ]
    expected_create_benchmark_ratings_dataset_rows = [
        ("loc_1", "estab_1", 1, "Good", "2024-01-01"),
        ("loc_2", "estab_2", 0, "Requires improvement", "2024-01-01"),
    ]

    add_numerical_ratings_rows = [
        (
            "loc 1",
            CQCRatingsValues.good,
            CQCRatingsValues.outstanding,
            CQCRatingsValues.requires_improvement,
            CQCRatingsValues.inadequate,
            CQCRatingsValues.good,
            None,
        ),
    ]
    expected_add_numerical_ratings_rows = [
        (
            "loc 1",
            CQCRatingsValues.good,
            CQCRatingsValues.outstanding,
            CQCRatingsValues.requires_improvement,
            CQCRatingsValues.inadequate,
            CQCRatingsValues.good,
            None,
            4,
            2,
            1,
            3,
            0,
            10,
        ),
    ]

    location_id_hash_rows = [
        ("1-123",),
    ]
    expected_location_id_hash_rows = [
        ("1-123", "b022a7e5cc45cf3dc578"),
    ]
    location_id_hash_ten_digit_rows = [
        ("1-123456789",),
    ]
    expected_location_id_hash_ten_digit_rows = [
        ("1-123456789", "4a5a7fdc6afede351ffd"),
    ]
    location_id_hash_eleven_digit_rows = [
        ("1-1234567890",),
    ]
    expected_location_id_hash_eleven_digit_rows = [
        ("1-1234567890", "133d74f156c4fba255e9"),
    ]
    location_id_hash_twelve_digit_rows = [
        ("1-12345678901",),
    ]
    expected_location_id_hash_twelve_digit_rows = [
        ("1-12345678901", "cf16d3a6b6648d845fda"),
    ]


@dataclass
class LmEngagementUtilsData:
    # fmt: off
    add_columns_for_locality_manager_dashboard_rows = [
        ("loc 1", date(2024, 1, 1), "cssr 1", 1, "2024"), # in ascwds on both dates
        ("loc 1", date(2024, 2, 1), "cssr 1", 1, "2024"),
        ("loc 2", date(2024, 1, 1), "cssr 2", 0, "2024"), # joins ascwds
        ("loc 2", date(2024, 2, 1), "cssr 2", 1, "2024"),
        ("loc 3", date(2024, 1, 1), "cssr 3", 1, "2024"), # leaves ascwds
        ("loc 3", date(2024, 2, 1), "cssr 3", 0, "2024"),
        ("loc 4", date(2024, 1, 1), "cssr 4", 0, "2024"), # multiple locations in one cssr
        ("loc 4", date(2024, 2, 1), "cssr 4", 1, "2024"),
        ("loc 4", date(2024, 3, 1), "cssr 4", 1, "2024"),
        ("loc 5", date(2024, 1, 1), "cssr 4", 0, "2024"),
        ("loc 5", date(2024, 2, 1), "cssr 4", 1, "2024"),
        ("loc 5", date(2024, 3, 1), "cssr 4", 1, "2024"),
        ("loc 6", date(2024, 1, 1), "cssr 4", 0, "2024"),
        ("loc 6", date(2024, 2, 1), "cssr 4", 1, "2024"),
        ("loc 6", date(2024, 3, 1), "cssr 4", 1, "2024"),
        ("loc 7", date(2024, 2, 1), "cssr 4", 0, "2024"),
        ("loc 7", date(2024, 1, 1), "cssr 4", 0, "2024"),
        ("loc 7", date(2024, 3, 1), "cssr 4", 1, "2024"),
    ]
    expected_add_columns_for_locality_manager_dashboard_rows = [
        ("loc 1", date(2024, 1, 1), "cssr 1", 1, "2024", 1.0, None, 1, 1, 1),
        ("loc 1", date(2024, 2, 1), "cssr 1", 1, "2024", 1.0, 0.0, 0, 0, 1),
        ("loc 2", date(2024, 1, 1), "cssr 2", 0, "2024", 0.0, None, 0, 0, 0),
        ("loc 2", date(2024, 2, 1), "cssr 2", 1, "2024", 1.0, 1.0, 1, 1, 1),
        ("loc 3", date(2024, 1, 1), "cssr 3", 1, "2024", 1.0, None, 1, 1, 1),
        ("loc 3", date(2024, 2, 1), "cssr 3", 0, "2024", 0.0, -1.0, -1, 0, 1),
        ("loc 4", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0, None, 0, 0, 0),
        ("loc 4", date(2024, 2, 1), "cssr 4", 1, "2024", 0.75, 0.75, 3, 3, 3),
        ("loc 4", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0, 0.25, 1, 1, 4),
        ("loc 5", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0, None, 0, 0, 0),
        ("loc 5", date(2024, 2, 1), "cssr 4", 1, "2024", 0.75, 0.75, 3, 3, 3),
        ("loc 5", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0, 0.25, 1, 1, 4),
        ("loc 6", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0, None, 0, 0, 0),
        ("loc 6", date(2024, 2, 1), "cssr 4", 1, "2024", 0.75, 0.75, 3, 3, 3),
        ("loc 6", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0, 0.25, 1, 1, 4),
        ("loc 7", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0, None, 0, 0, 0),
        ("loc 7", date(2024, 2, 1), "cssr 4", 0, "2024", 0.75, 0.75, 3, 3, 3),
        ("loc 7", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0, 0.25, 1, 1, 4),
    ]

    expected_calculate_la_coverage_monthly_rows = [
        ("loc 1", date(2024, 1, 1), "cssr 1", 1, "2024", 1.0),
        ("loc 1", date(2024, 2, 1), "cssr 1", 1, "2024", 1.0),
        ("loc 2", date(2024, 1, 1), "cssr 2", 0, "2024", 0.0),
        ("loc 2", date(2024, 2, 1), "cssr 2", 1, "2024", 1.0),
        ("loc 3", date(2024, 1, 1), "cssr 3", 1, "2024", 1.0),
        ("loc 3", date(2024, 2, 1), "cssr 3", 0, "2024", 0.0),
        ("loc 4", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0),
        ("loc 4", date(2024, 2, 1), "cssr 4", 1, "2024", 0.75),
        ("loc 4", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0),
        ("loc 5", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0),
        ("loc 5", date(2024, 2, 1), "cssr 4", 1, "2024", 0.75),
        ("loc 5", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0),
        ("loc 6", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0),
        ("loc 6", date(2024, 2, 1), "cssr 4", 1, "2024", 0.75),
        ("loc 6", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0),
        ("loc 7", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0),
        ("loc 7", date(2024, 2, 1), "cssr 4", 0, "2024", 0.75),
        ("loc 7", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0),
    ]

    calculate_coverage_monthly_change_rows = expected_calculate_la_coverage_monthly_rows

    expected_calculate_coverage_monthly_change_rows = [
        ("loc 1", date(2024, 1, 1), "cssr 1", 1, "2024", 1.0, None),
        ("loc 1", date(2024, 2, 1), "cssr 1", 1, "2024", 1.0, 0.0),
        ("loc 2", date(2024, 1, 1), "cssr 2", 0, "2024", 0.0, None),
        ("loc 2", date(2024, 2, 1), "cssr 2", 1, "2024", 1.0, 1.0),
        ("loc 3", date(2024, 1, 1), "cssr 3", 1, "2024", 1.0, None),
        ("loc 3", date(2024, 2, 1), "cssr 3", 0, "2024", 0.0, -1.0),
        ("loc 4", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0, None),
        ("loc 4", date(2024, 2, 1), "cssr 4", 1, "2024", 0.75, 0.75),
        ("loc 4", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0, 0.25),
        ("loc 5", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0, None),
        ("loc 5", date(2024, 2, 1), "cssr 4", 1, "2024", 0.75, 0.75),
        ("loc 5", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0, 0.25),
        ("loc 6", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0, None),
        ("loc 6", date(2024, 2, 1), "cssr 4", 1, "2024", 0.75, 0.75),
        ("loc 6", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0, 0.25),
        ("loc 7", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0, None),
        ("loc 7", date(2024, 2, 1), "cssr 4", 0, "2024", 0.75, 0.75),
        ("loc 7", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0, 0.25),
    ]
    calculate_locations_monthly_change_rows = expected_calculate_coverage_monthly_change_rows

    expected_calculate_locations_monthly_change_rows = [
        ("loc 1", date(2024, 1, 1), "cssr 1", 1, "2024", 1.0, None, 0, 1),
        ("loc 1", date(2024, 2, 1), "cssr 1", 1, "2024", 1.0, 0.0, 1, 0),
        ("loc 2", date(2024, 1, 1), "cssr 2", 0, "2024", 0.0, None, 0, 0),
        ("loc 2", date(2024, 2, 1), "cssr 2", 1, "2024", 1.0, 1.0, 0, 1),
        ("loc 3", date(2024, 1, 1), "cssr 3", 1, "2024", 1.0, None, 0, 1),
        ("loc 3", date(2024, 2, 1), "cssr 3", 0, "2024", 0.0, -1.0, 1, -1),
        ("loc 4", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0, None, 0, 0),
        ("loc 4", date(2024, 2, 1), "cssr 4", 1, "2024", 0.75, 0.75, 0, 3),
        ("loc 4", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0, 0.25, 1, 1),
        ("loc 5", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0, None, 0, 0),
        ("loc 5", date(2024, 2, 1), "cssr 4", 1, "2024", 0.75, 0.75, 0, 3),
        ("loc 5", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0, 0.25, 1, 1),
        ("loc 6", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0, None, 0, 0),
        ("loc 6", date(2024, 2, 1), "cssr 4", 1, "2024", 0.75, 0.75, 0, 3),
        ("loc 6", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0, 0.25, 1, 1),
        ("loc 7", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0, None, 0, 0),
        ("loc 7", date(2024, 2, 1), "cssr 4", 0, "2024", 0.75, 0.75, 0, 3),
        ("loc 7", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0, 0.25, 0, 1),
    ]

    calculate_new_registrations_rows = expected_calculate_locations_monthly_change_rows

    expected_calculate_new_registrations_rows = expected_add_columns_for_locality_manager_dashboard_rows
    # fmt: on
