"""Single source of truth for ASC-WDS code<->label vocabularies.

Retires two duplicate mechanisms:
- data_labels_lookup.csv (was read by both ASC-WDS clean jobs)
- utils/value_labels/ascwds_worker/ascwds_worker_mainjrid.py (hand-copied
  main_job_role code->label dict)

main_job_role and employment_status already have both a code-side and a
label-side ColumnValues class in categorical_column_values.py; PairedVocab
derives their {code: label} mapping by shared field name instead of
maintaining it a third time. The other 5 columns only ever had a label-side
ColumnValues class - their raw ASC-WDS code is discarded during cleaning -
so their {code: label} dicts are declared directly here, transcribed from
data_labels_lookup.csv (source column names in comments for traceability).
"""

from dataclasses import dataclass, fields

from utils.column_values.categorical_column_values import (
    EmploymentStatusID,
    EmploymentStatusLabels,
    EstablishmentType,
    IsParent,
    MainJobRoleID,
    MainJobRoleLabels,
    MainServiceID,
    ParentPermission,
    RegistrationType,
)

# Lives beside main_job_role's owning vocab rather than as a loose module constant.
NOT_KNOWN_JOB_ROLE = "-1"

_BASE_FIELDS = {"column_name", "value_to_remove", "contains_null_values"}


@dataclass(frozen=True)
class PairedVocab:
    """Links a code `ColumnValues` class to its label `ColumnValues` class by field name."""

    id_cls: type
    label_cls: type

    def code_to_label(self) -> dict[str, str]:
        """Build the `{code: label}` mapping for fields both classes declare.

        Fields declared only on `id_cls` (e.g. main_job_role's `technician`/
        `care_navigator`) are excluded, since they have no label.

        Returns:
            dict[str, str]: mapping of raw code to label.
        """
        id_fields = {f.name for f in fields(self.id_cls)} - _BASE_FIELDS
        label_fields = {f.name for f in fields(self.label_cls)} - _BASE_FIELDS
        shared = id_fields & label_fields
        return {
            getattr(self.id_cls, name): getattr(self.label_cls, name) for name in shared
        }

    def unlabelled_codes(self) -> dict[str, str]:
        """Build a mapping of field name to code for `id_cls` codes with no matching label.

        Returns:
            dict[str, str]: mapping of field name to raw code.
        """
        id_fields = {f.name for f in fields(self.id_cls)} - _BASE_FIELDS
        label_fields = {f.name for f in fields(self.label_cls)} - _BASE_FIELDS
        return {name: getattr(self.id_cls, name) for name in id_fields - label_fields}


MAIN_JOB_ROLE = PairedVocab(MainJobRoleID, MainJobRoleLabels)
EMPLOYMENT_STATUS = PairedVocab(EmploymentStatusID, EmploymentStatusLabels)

# Source: data_labels_lookup.csv, column_name "esttype"
ESTABLISHMENT_TYPE_CODE_TO_LABEL: dict[str, str] = {
    "0": EstablishmentType.not_known,
    "1": EstablishmentType.local_authority_adult_services,
    "2": EstablishmentType.local_authority_childrens_services,
    "3": EstablishmentType.local_authority_generic_other,
    "4": EstablishmentType.local_authority_owned,
    "5": EstablishmentType.health,
    "6": EstablishmentType.private_sector,
    "7": EstablishmentType.voluntary_charity,
    "8": EstablishmentType.other,
}

# Source: data_labels_lookup.csv, column_name "parentpermission"
# Code 3 ("Placeholder label") intentionally excluded - legacy/unused code,
# same treatment as MainJobRoleID's technician/care_navigator.
PARENT_PERMISSION_CODE_TO_LABEL: dict[str, str] = {
    "1": ParentPermission.parent_has_ownership,
    "2": ParentPermission.workplace_has_ownership,
}

# Source: data_labels_lookup.csv, column_name "isparent"
IS_PARENT_CODE_TO_LABEL: dict[str, str] = {
    "0": IsParent.is_not_parent,
    "1": IsParent.is_parent,
}

# Source: data_labels_lookup.csv, column_name "mainstid"
# Codes 4/53, 16/37, and 20/39 intentionally share a label each (see the
# matching comments on MainServiceID in categorical_column_values.py).
MAIN_SERVICE_ID_CODE_TO_LABEL: dict[str, str] = {
    "1": MainServiceID.care_home_services_with_nursing_chn,
    "2": MainServiceID.care_home_services_without_nursing_chs,
    "3": MainServiceID.adult_placement_home,
    "4": MainServiceID.sheltered_housing,
    "5": MainServiceID.other_adult_residential_care_service,
    "6": MainServiceID.day_care_and_day_services,
    "7": MainServiceID.other_adult_day_care_services,
    "8": MainServiceID.domiciliary_care_services_adults_dcc,
    "9": MainServiceID.home_nursing_care_for_a_person_aged_18_or_over,
    "10": MainServiceID.domestic_services_and_home_help,
    "11": MainServiceID.meals_on_wheels,
    "12": MainServiceID.other_adult_domiciliary_care_service,
    "13": MainServiceID.carers_support,
    "14": MainServiceID.short_breaks_respite_care,
    "15": MainServiceID.community_support_and_outreach,
    "16": MainServiceID.social_work_and_care_management,
    "17": MainServiceID.shared_lives_shl,
    "18": MainServiceID.disability_adaptations_assistive_technology_services,
    "19": MainServiceID.occupational_employment_related_services,
    "20": MainServiceID.information_and_advice_services,
    "21": MainServiceID.other_adult_community_care_service,
    "22": MainServiceID.care_home_hostel,
    "23": MainServiceID.family_centre_residential,
    "24": MainServiceID.residential_school,
    "25": MainServiceID.other_childrens_residential_care_service,
    "26": MainServiceID.full_day_care_e_g_day_nursery,
    "27": MainServiceID.sessional_day_care_e_g_play_group_preschool,
    "28": MainServiceID.out_of_school_club,
    "29": MainServiceID.holiday_club,
    "30": MainServiceID.creche,
    "31": MainServiceID.childminder,
    "32": MainServiceID.other_childrens_day_care_services,
    "33": MainServiceID.domiciliary_care_services_childrens_dcc,
    "34": MainServiceID.fostering_or_adoption_service_agency,
    "35": MainServiceID.child_protection,
    "36": MainServiceID.family_centre,
    "37": MainServiceID.social_work_and_care_management,
    "38": MainServiceID.family_support,
    "39": MainServiceID.information_and_advice_services,
    "40": MainServiceID.mental_health,
    "41": MainServiceID.other_childrens_community_care_service,
    "42": MainServiceID.nhs_primary_care_trust,
    "43": MainServiceID.social_care_nhs_trust,
    "46": MainServiceID.any_other_part_of_nhs_hospital_community_health_services,
    "48": MainServiceID.independent_acute_or_mental_health_hospital,
    "51": MainServiceID.other_independent_healthcare_setting,
    "52": MainServiceID.any_other_services,
    "53": MainServiceID.sheltered_housing,
    "54": MainServiceID.extra_care_housing_services,
    "55": MainServiceID.supported_living_services,
    "56": MainServiceID.childrens_homes,
    "57": MainServiceID.secure_units,
    "58": MainServiceID.residential_special_schools,
    "59": MainServiceID.boarding_schools,
    "60": MainServiceID.specialist_college_services,
    "61": MainServiceID.community_based_services_for_people_with_a_learning_disability,
    "62": MainServiceID.community_based_services_for_people_with_mental_health_needs,
    "63": MainServiceID.community_based_services_for_people_who_misuse_substances,
    "64": MainServiceID.community_healthcare_services,
    "65": MainServiceID.acute_services,
    "66": MainServiceID.hospice_services,
    "67": MainServiceID.long_term_conditions_services,
    "68": MainServiceID.hospital_services_for_people_with_mental_health_needs_learning_disabilities_and_or_problems_with_substance_misuse,
    "69": MainServiceID.rehabilitation_services,
    "70": MainServiceID.residential_substance_misuse_treatment_rehabilitation_services,
    "71": MainServiceID.other_healthcare_service,
    "72": MainServiceID.head_office_services,
    "73": MainServiceID.live_in_care,
    "74": MainServiceID.nurses_agency,
    "75": MainServiceID.any_childrens_young_peoples_service,
}

# Source: data_labels_lookup.csv, column_name "regtype"
REGISTRATION_TYPE_CODE_TO_LABEL: dict[str, str] = {
    "-1": RegistrationType.not_recorded,
    "0": RegistrationType.not_regulated,
    "1": RegistrationType.ofsted,
    "2": RegistrationType.cqc_regulated,
}
