from dataclasses import dataclass

import pytest

from utils.column_values.ascwds_labelled_vocab import (
    EMPLOYMENT_STATUS,
    ESTABLISHMENT_TYPE_CODE_TO_LABEL,
    IS_PARENT_CODE_TO_LABEL,
    MAIN_JOB_ROLE,
    MAIN_SERVICE_ID_CODE_TO_LABEL,
    PARENT_PERMISSION_CODE_TO_LABEL,
    REGISTRATION_TYPE_CODE_TO_LABEL,
)
from utils.column_values.categorical_column_values import (
    EstablishmentType,
    IsParent,
    MainServiceID,
    ParentPermission,
    RegistrationType,
)


class TestMainJobRoleVocab:
    def test_code_to_label_returns_every_labelled_code(self):
        assert len(MAIN_JOB_ROLE.code_to_label()) == 37

    def test_code_to_label_maps_a_known_code_to_its_label(self):
        assert MAIN_JOB_ROLE.code_to_label()["1"] == "Senior management"

    def test_unlabelled_codes_returns_technician_and_care_navigator(self):
        assert MAIN_JOB_ROLE.unlabelled_codes() == {
            "technician": "22",
            "care_navigator": "41",
        }


class TestEmploymentStatusVocab:
    def test_code_to_label_returns_every_labelled_code(self):
        assert len(EMPLOYMENT_STATUS.code_to_label()) == 6

    def test_unlabelled_codes_returns_an_empty_dict(self):
        assert EMPLOYMENT_STATUS.unlabelled_codes() == {}


@dataclass
class WorkplaceCodeToLabelDictTestCase:
    id: str
    code_to_label: dict[str, str]
    column_values_class: type
    expected_code_count: int

    def as_pytest_param(self):
        return pytest.param(self, id=self.id)


workplace_dict_test_cases = [
    WorkplaceCodeToLabelDictTestCase(
        id="establishment_type",
        code_to_label=ESTABLISHMENT_TYPE_CODE_TO_LABEL,
        column_values_class=EstablishmentType,
        expected_code_count=9,
    ),
    WorkplaceCodeToLabelDictTestCase(
        id="parent_permission",
        code_to_label=PARENT_PERMISSION_CODE_TO_LABEL,
        column_values_class=ParentPermission,
        expected_code_count=2,
    ),
    WorkplaceCodeToLabelDictTestCase(
        id="is_parent",
        code_to_label=IS_PARENT_CODE_TO_LABEL,
        column_values_class=IsParent,
        expected_code_count=2,
    ),
    WorkplaceCodeToLabelDictTestCase(
        id="main_service_id",
        code_to_label=MAIN_SERVICE_ID_CODE_TO_LABEL,
        column_values_class=MainServiceID,
        expected_code_count=70,
    ),
    WorkplaceCodeToLabelDictTestCase(
        id="registration_type",
        code_to_label=REGISTRATION_TYPE_CODE_TO_LABEL,
        column_values_class=RegistrationType,
        expected_code_count=4,
    ),
]


class TestWorkplaceCodeToLabelDicts:
    @pytest.mark.parametrize(
        "case", [c.as_pytest_param() for c in workplace_dict_test_cases]
    )
    def test_every_value_is_a_member_of_the_column_values_vocab(self, case):
        test_object = case.column_values_class("test_column")

        assert set(case.code_to_label.values()) <= set(test_object.categorical_values)

    @pytest.mark.parametrize(
        "case", [c.as_pytest_param() for c in workplace_dict_test_cases]
    )
    def test_dict_has_one_entry_per_raw_code(self, case):
        assert len(case.code_to_label) == case.expected_code_count
