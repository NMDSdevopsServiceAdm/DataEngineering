from dataclasses import dataclass, fields

import pytest

from utils.column_values.ascwds_labelled_vocab import (
    APPRENTICE,
    CARE_CERTIFICATE_STATUS,
    DISABLED,
    EMPLOYMENT_STATUS,
    ESTABLISHMENT_TYPE_CODE_TO_LABEL,
    ETHNICITY,
    GENDER,
    HOURLY_OR_ANNUAL_PAY,
    IS_PARENT_CODE_TO_LABEL,
    LEVEL_SOCIAL_CARE_QUALIFICATION_HELD,
    MAIN_JOB_ROLE,
    MAIN_SERVICE_ID_CODE_TO_LABEL,
    PARENT_PERMISSION_CODE_TO_LABEL,
    REGION_ID_CODE_TO_LABEL,
    REGISTRATION_TYPE_CODE_TO_LABEL,
    SOCIAL_CARE_QUAL_HELD,
    SOURCE_OF_RECRUITMENT,
    ZERO_HOURS,
    ColumnValues,
    EmploymentStatusID,
    EmploymentStatusLabels,
    EstablishmentType,
    IsParent,
    MainJobRoleID,
    MainJobRoleLabels,
    MainServiceID,
    PairedVocab,
    ParentPermission,
    PublishedJobRoleLabels,
    RegionID,
    RegistrationType,
)


class TestMainJobRoleVocab:
    def test_code_to_label_returns_every_labelled_code(self):
        expected_count = len(fields(MainJobRoleLabels)) - len(fields(ColumnValues))
        assert len(MAIN_JOB_ROLE.code_to_label()) == expected_count

    def test_code_to_label_maps_a_known_code_to_its_label(self):
        assert MAIN_JOB_ROLE.code_to_label()["1"] == "Senior management"

    def test_unlabelled_codes_returns_technician_and_care_navigator(self):
        assert MAIN_JOB_ROLE.unlabelled_codes() == {
            "technician": "22",
            "care_navigator": "41",
        }


class TestEmploymentStatusVocab:
    def test_code_to_label_maps_every_code_to_its_label(self):
        assert EMPLOYMENT_STATUS.code_to_label() == {
            EmploymentStatusID.permanent: EmploymentStatusLabels.permanent,
            EmploymentStatusID.temporary: EmploymentStatusLabels.temporary,
            EmploymentStatusID.bank_or_pool: EmploymentStatusLabels.bank_or_pool,
            EmploymentStatusID.agency: EmploymentStatusLabels.agency,
            EmploymentStatusID.other: EmploymentStatusLabels.other,
        }

    def test_unlabelled_codes_returns_an_empty_dict(self):
        assert EMPLOYMENT_STATUS.unlabelled_codes() == {}


class TestPairedVocabValidation:
    def test_raises_when_label_side_has_a_field_the_code_side_lacks(self):
        with pytest.raises(ValueError, match="IsParent"):
            PairedVocab(MainJobRoleID, IsParent)

    def test_does_not_raise_when_code_side_has_a_field_the_label_side_lacks(self):
        # main_job_role's technician/care_navigator exist only on MainJobRoleID -
        # that's the allowed direction (see unlabelled_codes()), so this must not raise.
        PairedVocab(MainJobRoleID, MainJobRoleLabels)


class TestPublishedJobRoleLabels:
    def test_pins_the_published_job_role_label_values(self):
        test_object = PublishedJobRoleLabels("test_column")
        expected_values = [
            "Senior management",
            "Registered manager",
            "Social worker",
            "Senior care worker",
            "Care worker",
            "Community support and outreach work",
            "Occupational therapist",
            "Registered nurse",
            "Allied health professional",
            "Deputy manager",
            "Support worker",
            "Other managers",
            "Other regulated professions",
            "Other direct care",
            "Other",
        ]
        assert test_object.categorical_values == expected_values


@dataclass
class WorkplaceCodeToLabelDictTestCase:
    id: str
    code_to_label: dict[str, str]
    column_values_class: type
    expected_code_count: int
    # A hand-written, independent restatement of specific code->label pairs,
    # to catch a transposed code<->label mistake in the transcription that
    # set-membership and count checks alone wouldn't catch.
    expected_pairs: dict[str, str]
    # region_id's ColumnValues class holds codes, not labels (its raw code
    # survives cleaning - see ascwds_labelled_vocab's module docstring), so
    # its membership check is against code_to_label's keys, not its values.
    column_values_holds: str = "labels"

    def as_pytest_param(self):
        return pytest.param(self, id=self.id)


workplace_dict_test_cases = [
    WorkplaceCodeToLabelDictTestCase(
        id="establishment_type",
        code_to_label=ESTABLISHMENT_TYPE_CODE_TO_LABEL,
        column_values_class=EstablishmentType,
        expected_code_count=9,
        expected_pairs={
            "0": EstablishmentType.not_known,
            "1": EstablishmentType.local_authority_adult_services,
            "2": EstablishmentType.local_authority_childrens_services,
            "3": EstablishmentType.local_authority_generic_other,
            "4": EstablishmentType.local_authority_owned,
            "5": EstablishmentType.health,
            "6": EstablishmentType.private_sector,
            "7": EstablishmentType.voluntary_charity,
            "8": EstablishmentType.other,
        },
    ),
    WorkplaceCodeToLabelDictTestCase(
        id="parent_permission",
        code_to_label=PARENT_PERMISSION_CODE_TO_LABEL,
        column_values_class=ParentPermission,
        expected_code_count=2,
        expected_pairs={
            "1": ParentPermission.parent_has_ownership,
            "2": ParentPermission.workplace_has_ownership,
        },
    ),
    WorkplaceCodeToLabelDictTestCase(
        id="is_parent",
        code_to_label=IS_PARENT_CODE_TO_LABEL,
        column_values_class=IsParent,
        expected_code_count=2,
        expected_pairs={
            "0": IsParent.is_not_parent,
            "1": IsParent.is_parent,
        },
    ),
    WorkplaceCodeToLabelDictTestCase(
        id="main_service_id",
        code_to_label=MAIN_SERVICE_ID_CODE_TO_LABEL,
        column_values_class=MainServiceID,
        expected_code_count=70,
        expected_pairs={
            "1": MainServiceID.care_home_services_with_nursing_chn,
            "2": MainServiceID.care_home_services_without_nursing_chs,
            # 4/53, 16/37, and 20/39 each intentionally share one label
            # across two codes - the trickiest pairs to transcribe correctly.
            "4": MainServiceID.sheltered_housing,
            "53": MainServiceID.sheltered_housing,
            "16": MainServiceID.social_work_and_care_management,
            "37": MainServiceID.social_work_and_care_management,
            "20": MainServiceID.information_and_advice_services,
            "39": MainServiceID.information_and_advice_services,
            "42": MainServiceID.nhs_primary_care_trust,
            "46": MainServiceID.any_other_part_of_nhs_hospital_community_health_services,
            "68": MainServiceID.hospital_services_for_people_with_mental_health_needs_learning_disabilities_and_or_problems_with_substance_misuse,
            "75": MainServiceID.any_childrens_young_peoples_service,
        },
    ),
    WorkplaceCodeToLabelDictTestCase(
        id="registration_type",
        code_to_label=REGISTRATION_TYPE_CODE_TO_LABEL,
        column_values_class=RegistrationType,
        expected_code_count=4,
        expected_pairs={
            "-1": RegistrationType.not_recorded,
            "0": RegistrationType.not_regulated,
            "1": RegistrationType.ofsted,
            "2": RegistrationType.cqc_regulated,
        },
    ),
    WorkplaceCodeToLabelDictTestCase(
        id="region_id",
        code_to_label=REGION_ID_CODE_TO_LABEL,
        column_values_class=RegionID,
        expected_code_count=10,
        expected_pairs={
            "1": "I - Eastern",
            "2": "C - East Midlands",
            "3": "G - London",
            "9": "J - Yorkshire Humber",
            "-1": "Not known",
        },
        column_values_holds="codes",
    ),
]


class TestWorkplaceCodeToLabelDicts:
    @pytest.mark.parametrize(
        "case", [c.as_pytest_param() for c in workplace_dict_test_cases]
    )
    def test_every_value_is_a_member_of_the_column_values_vocab(self, case):
        test_object = case.column_values_class("test_column")

        vocab_side = (
            case.code_to_label.keys()
            if case.column_values_holds == "codes"
            else case.code_to_label.values()
        )
        assert set(vocab_side) <= set(test_object.categorical_values)

    @pytest.mark.parametrize(
        "case", [c.as_pytest_param() for c in workplace_dict_test_cases]
    )
    def test_dict_has_one_entry_per_raw_code(self, case):
        assert len(case.code_to_label) == case.expected_code_count

    @pytest.mark.parametrize(
        "case", [c.as_pytest_param() for c in workplace_dict_test_cases]
    )
    def test_specific_codes_map_to_their_expected_label(self, case):
        for code, expected_label in case.expected_pairs.items():
            assert case.code_to_label[code] == expected_label


@dataclass
class WorkerCodeToLabelTestCase:
    id: str
    code_to_label: dict[str, str]
    expected_code_count: int
    # Hand-written pairs, independent of the vocab classes, to catch a
    # transposed or mistyped code or label.
    expected_pairs: dict[str, str]

    def as_pytest_param(self):
        return pytest.param(self, id=self.id)


worker_paired_vocabs = {
    "gender": GENDER,
    "ethnicity": ETHNICITY,
    "disabled": DISABLED,
    "hourly_or_annual_pay": HOURLY_OR_ANNUAL_PAY,
    "social_care_qual_held": SOCIAL_CARE_QUAL_HELD,
    "level_social_care_qualification_held": LEVEL_SOCIAL_CARE_QUALIFICATION_HELD,
    "care_certificate_status": CARE_CERTIFICATE_STATUS,
    "apprentice": APPRENTICE,
    "zero_hours": ZERO_HOURS,
    "source_of_recruitment": SOURCE_OF_RECRUITMENT,
}

worker_code_to_label_test_cases = [
    WorkerCodeToLabelTestCase(
        id="gender",
        code_to_label=GENDER.code_to_label(),
        expected_code_count=5,
        expected_pairs={
            "-1": "Not recorded",
            "1": "Male",
            "2": "Female",
            "3": "Unknown",
            "4": "Other",
        },
    ),
    WorkerCodeToLabelTestCase(
        id="ethnicity",
        code_to_label=ETHNICITY.code_to_label(),
        expected_code_count=20,
        expected_pairs={
            "-1": "Not recorded",
            "31": "English / Welsh / Scottish / Northern Irish / British",
            "38": "Any Other Mixed / multiple ethnic background",
            "46": "Any other Black / African / Caribbean background",
            "98": "Any other ethnic group",
            "99": "Not known",
        },
    ),
    WorkerCodeToLabelTestCase(
        id="disabled",
        code_to_label=DISABLED.code_to_label(),
        expected_code_count=5,
        expected_pairs={
            "-2": "Don't know",
            "-1": "Not recorded",
            "0": "No",
            "1": "Yes",
            "2": "Not disclosed",
        },
    ),
    WorkerCodeToLabelTestCase(
        id="hourly_or_annual_pay",
        code_to_label=HOURLY_OR_ANNUAL_PAY.code_to_label(),
        expected_code_count=4,
        expected_pairs={
            "-2": "Not known",
            "-1": "Not recorded",
            "250": "Annual",
            "252": "Hourly",
        },
    ),
    WorkerCodeToLabelTestCase(
        id="social_care_qual_held",
        code_to_label=SOCIAL_CARE_QUAL_HELD.code_to_label(),
        expected_code_count=4,
        expected_pairs={
            "-1": "Not recorded",
            "1": "Yes",
            "2": "No",
            "3": "Not known",
        },
    ),
    WorkerCodeToLabelTestCase(
        id="level_social_care_qualification_held",
        code_to_label=LEVEL_SOCIAL_CARE_QUALIFICATION_HELD.code_to_label(),
        expected_code_count=12,
        expected_pairs={
            "-2": "N/A if no to the above",
            "-1": "Missing",
            "0": "Entry",
            "1": "Level 1",
            "7": "Level 7",
            "8": "Level 8 or above",
            "10": "Not known",
        },
    ),
    WorkerCodeToLabelTestCase(
        id="care_certificate_status",
        code_to_label=CARE_CERTIFICATE_STATUS.code_to_label(),
        expected_code_count=4,
        expected_pairs={
            "-1": "Not provided",
            "1": "Completed",
            "2": "Not started",
            "3": "In progress or partially completed",
        },
    ),
    WorkerCodeToLabelTestCase(
        id="apprentice",
        code_to_label=APPRENTICE.code_to_label(),
        expected_code_count=4,
        expected_pairs={
            "-1": "Not recorded",
            "1": "Yes",
            "2": "No",
            "3": "Not known",
        },
    ),
    WorkerCodeToLabelTestCase(
        id="zero_hours",
        code_to_label=ZERO_HOURS.code_to_label(),
        expected_code_count=4,
        expected_pairs={
            "-2": "Not known",
            "-1": "Not recorded",
            "0": "No",
            "1": "Yes",
        },
    ),
    WorkerCodeToLabelTestCase(
        id="source_of_recruitment",
        code_to_label=SOURCE_OF_RECRUITMENT.code_to_label(),
        expected_code_count=12,
        expected_pairs={
            "-1": "Not recorded",
            "210": "Adult care sector: local authority",
            "217": "Internal promotion or transfer or career development",
            "225": "Not known",
            "226": "Children’s/young people’s social care",
            "227": "First role after education",
        },
    ),
]


class TestWorkerCodeToLabel:
    @pytest.mark.parametrize(
        "case", [c.as_pytest_param() for c in worker_code_to_label_test_cases]
    )
    def test_code_to_label_has_expected_code_count(self, case):
        assert len(case.code_to_label) == case.expected_code_count

    @pytest.mark.parametrize(
        "case", [c.as_pytest_param() for c in worker_code_to_label_test_cases]
    )
    def test_specific_codes_map_to_expected_label(self, case):
        for code, expected_label in case.expected_pairs.items():
            assert case.code_to_label[code] == expected_label

    @pytest.mark.parametrize(
        "case", [c.as_pytest_param() for c in worker_code_to_label_test_cases]
    )
    def test_labels_have_no_surrounding_whitespace(self, case):
        for label in case.code_to_label.values():
            assert label == label.strip()

    @pytest.mark.parametrize(
        "vocab", worker_paired_vocabs.values(), ids=worker_paired_vocabs.keys()
    )
    def test_unlabelled_codes_is_empty(self, vocab):
        assert vocab.unlabelled_codes() == {}
