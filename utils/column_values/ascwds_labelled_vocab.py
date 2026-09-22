"""Single source of truth for ASC-WDS vocab: code/label ColumnValues classes and
code<->label mappings.

Retires several duplicate mechanisms:
- data_labels_lookup.csv (was read by both ASC-WDS clean jobs)
- utils/value_labels/ascwds_worker/ascwds_worker_mainjrid.py (hand-copied
  main_job_role code->label dict)
- utils/value_labels/reconciliation/ascwds_recon_regionid.py (hand-copied
  region_id code->label dict, used only by reconciliation.py)

main_job_role and employment_status have both a code-side and a label-side
ColumnValues class; PairedVocab derives their {code: label} mapping by shared
field name instead of maintaining it a third time. Establishment_type,
parent_permission, is_parent, main_service_id, and registration_type only
ever had a label-side ColumnValues class - their raw ASC-WDS code is
discarded during cleaning - so their {code: label} dicts are declared
directly here. region_id is the exception: its raw code survives cleaning
(only reconciliation.py relabels it, downstream of the general clean job), so
it gets a code-side ColumnValues class like the paired columns above, not a
label-side one.
"""

from dataclasses import dataclass, fields

from utils.column_values.categorical_column_values import ColumnValues

# Lives beside main_job_role's owning vocab rather than as a loose module constant.
NOT_KNOWN_JOB_ROLE = "-1"

_BASE_FIELDS = {"column_name", "value_to_remove", "contains_null_values"}


@dataclass
class MainJobRoleLabels(ColumnValues):
    """The possible values of the main job role column in ASCWDS data"""

    senior_management: str = "Senior management"
    middle_management: str = "Middle management"
    first_line_manager: str = "First line manager"
    registered_manager: str = "Registered manager"
    supervisor: str = "Supervisor"
    social_worker: str = "Social worker"
    senior_care_worker: str = "Senior care worker"
    care_worker: str = "Care worker"
    community_support_and_outreach: str = "Community support and outreach work"
    employment_support: str = "Employment support"
    advocacy: str = "Advice guidance and advocacy"
    occupational_therapist: str = "Occupational therapist"
    registered_nurse: str = "Registered nurse"
    allied_health_professional: str = "Allied health professional"
    other_care_role: str = "Other care-providing job role"
    other_managerial_staff: str = (
        "Managers and staff in care-related but not care-providing roles"
    )
    admin_staff: str = "Administrative or office staff not care-providing"
    ancillary_staff: str = "Ancillary staff not care-providing"
    other_non_care_related_staff: str = "Other non-care-providing job roles"
    activites_worker: str = "Activities worker or co-ordinator"
    safeguarding_officer: str = "Safeguarding and reviewing officer"
    occupational_therapist_assistant: str = "Occupational therapist assistant"
    nursing_associate: str = "Nursing associate"
    nursing_assistant: str = "Nursing assistant"
    assessment_officer: str = "Assessment officer"
    care_coordinator: str = "Care co-ordinator"
    childrens_roles: str = "Any childrens/young peoples job role"
    deputy_manager: str = "Deputy manager"
    learning_and_development_lead: str = "Learning and development lead"
    team_leader: str = "Team leader"
    data_analyst: str = "Data analyst"
    data_governance_manager: str = "Data governance manager"
    it_and_digital_support: str = "IT and digital support"
    it_manager: str = "IT manager"
    it_service_desk_manager: str = "IT service desk manager"
    software_developer: str = "Software developer"
    support_worker: str = "Support worker"


@dataclass
class MainJobRoleID(ColumnValues):
    """The possible values of the main job role column in ASCWDS data"""

    senior_management: str = "1"
    middle_management: str = "2"
    first_line_manager: str = "3"
    registered_manager: str = "4"
    supervisor: str = "5"
    social_worker: str = "6"
    senior_care_worker: str = "7"
    care_worker: str = "8"
    community_support_and_outreach: str = "9"
    employment_support: str = "10"
    advocacy: str = "11"
    occupational_therapist: str = "15"
    registered_nurse: str = "16"
    allied_health_professional: str = "17"
    technician: str = "22"
    other_care_role: str = "23"
    other_managerial_staff: str = "24"
    admin_staff: str = "25"
    ancillary_staff: str = "26"
    other_non_care_related_staff: str = "27"
    activites_worker: str = "34"
    safeguarding_officer: str = "35"
    occupational_therapist_assistant: str = "36"
    nursing_associate: str = "37"
    nursing_assistant: str = "38"
    assessment_officer: str = "39"
    care_coordinator: str = "40"
    care_navigator: str = "41"
    childrens_roles: str = "42"
    deputy_manager: str = "43"
    learning_and_development_lead: str = "44"
    team_leader: str = "45"
    data_analyst: str = "46"
    data_governance_manager: str = "47"
    it_and_digital_support: str = "48"
    it_manager: str = "49"
    it_service_desk_manager: str = "50"
    software_developer: str = "51"
    support_worker: str = "52"


@dataclass
class EmploymentStatusID(ColumnValues):
    """The possible values of the employment status column in ASCWDS data"""

    permanent: str = "190"
    temporary: str = "191"
    bank_or_pool: str = "192"
    agency: str = "193"
    other: str = "196"


@dataclass
class EmploymentStatusLabels(ColumnValues):
    """The possible values of the employment status labelled column in ASCWDS data"""

    permanent: str = "permanent"
    temporary: str = "temporary"
    bank_or_pool: str = "bank_or_pool"
    agency: str = "agency"
    other: str = "other"


@dataclass
class PublishedJobRoleLabels(ColumnValues):
    """The possible values of the published job role label column in the SLV job-role merge/reshape"""

    senior_management: str = MainJobRoleLabels.senior_management
    registered_manager: str = MainJobRoleLabels.registered_manager
    social_worker: str = MainJobRoleLabels.social_worker
    senior_care_worker: str = MainJobRoleLabels.senior_care_worker
    care_worker: str = MainJobRoleLabels.care_worker
    community_support_and_outreach: str = (
        MainJobRoleLabels.community_support_and_outreach
    )
    occupational_therapist: str = MainJobRoleLabels.occupational_therapist
    registered_nurse: str = MainJobRoleLabels.registered_nurse
    allied_health_professional: str = MainJobRoleLabels.allied_health_professional
    deputy_manager: str = MainJobRoleLabels.deputy_manager
    support_worker: str = MainJobRoleLabels.support_worker
    other_managers: str = "Other managers"
    other_regulated_professions: str = "Other regulated professions"
    other_direct_care: str = "Other direct care"
    other: str = "Other"


@dataclass
class EstablishmentType(ColumnValues):
    """The possible values of the establishment type column in ASCWDS data"""

    not_known: str = "Not known"
    local_authority_adult_services: str = "Local authority (adult services)"
    local_authority_childrens_services: str = "Local authority (childrens services)"
    local_authority_generic_other: str = "Local authority (generic/other)"
    local_authority_owned: str = "Local authority owned"
    health: str = "Health"
    private_sector: str = "Private sector"
    voluntary_charity: str = "Voluntary/Charity"
    other: str = "Other"


@dataclass
class ParentPermission(ColumnValues):
    """The possible values of the parent permission column in ASCWDS data"""

    parent_has_ownership: str = "Parent has ownership"
    workplace_has_ownership: str = "Workplace has ownership"


@dataclass
class IsParent(ColumnValues):
    is_parent: str = "Yes"
    is_not_parent: str = "No"


@dataclass
class MainServiceID(ColumnValues):
    """The possible values of the main service id column in ASCWDS data"""

    care_home_services_with_nursing_chn: str = "Care home services with nursing - CHN"
    care_home_services_without_nursing_chs: str = (
        "Care home services without nursing - CHS"
    )
    adult_placement_home: str = "Adult placement home"
    sheltered_housing: str = "Sheltered housing"  # This label has two possible codes
    other_adult_residential_care_service: str = "Other adult residential care service"
    day_care_and_day_services: str = "Day care and day services"
    other_adult_day_care_services: str = "Other adult day care services"
    domiciliary_care_services_adults_dcc: str = (
        "Domiciliary care services (Adults) - DCC"
    )
    home_nursing_care_for_a_person_aged_18_or_over: str = (
        "Home nursing care for a person aged 18 or over"
    )
    domestic_services_and_home_help: str = "Domestic services and home help"
    meals_on_wheels: str = "Meals on wheels"
    other_adult_domiciliary_care_service: str = "Other adult domiciliary care service"
    carers_support: str = "Carers support"
    short_breaks_respite_care: str = "Short breaks / respite care"
    community_support_and_outreach: str = "Community support and outreach"
    social_work_and_care_management: str = (
        "Social work and care management"  # This label has two possible codes
    )
    information_and_advice_services: str = (
        "Information and advice services"  # This label has two possible codes
    )
    shared_lives_shl: str = "Shared lives - SHL"
    disability_adaptations_assistive_technology_services: str = (
        "Disability adaptations / assistive technology services"
    )
    occupational_employment_related_services: str = (
        "Occupational / employment-related services"
    )
    other_adult_community_care_service: str = "Other adult community care service"
    care_home_hostel: str = "Care home / hostel"
    family_centre_residential: str = "Family centre (residential)"
    residential_school: str = "Residential school"
    other_childrens_residential_care_service: str = (
        "Other childrens residential care service"
    )
    full_day_care_e_g_day_nursery: str = "Full day care, e.g. day nursery"
    sessional_day_care_e_g_play_group_preschool: str = (
        "Sessional day care e.g. play group / preschool"
    )
    out_of_school_club: str = "Out of school club"
    holiday_club: str = "Holiday club"
    creche: str = "Crèche"
    childminder: str = "Childminder"
    other_childrens_day_care_services: str = "Other childrens day care services"
    domiciliary_care_services_childrens_dcc: str = (
        "Domiciliary care services (Childrens) - DCC"
    )
    fostering_or_adoption_service_agency: str = "Fostering or adoption service / agency"
    child_protection: str = "Child protection"
    family_centre: str = "Family centre"
    family_support: str = "Family support"
    mental_health: str = "Mental health"
    other_childrens_community_care_service: str = (
        "Other childrens community care service"
    )
    nhs_primary_care_trust: str = "NHS Primary Care Trust"
    social_care_nhs_trust: str = "Social Care NHS Trust"
    independent_acute_or_mental_health_hospital: str = (
        "Independent acute or mental health hospital"
    )
    other_independent_healthcare_setting: str = "Other independent healthcare setting"
    any_other_services: str = "Any other Services"
    extra_care_housing_services: str = "Extra care housing services - EXC"
    supported_living_services: str = "Supported living services - SLS"
    childrens_homes: str = "Childrens homes"
    secure_units: str = "Secure units"
    residential_special_schools: str = "Residential special schools"
    boarding_schools: str = "Boarding schools"
    specialist_college_services: str = "Specialist college services - SPC"
    community_based_services_for_people_with_a_learning_disability: str = (
        "Community based services for people with a learning disability - LDC"
    )
    community_based_services_for_people_with_mental_health_needs: str = (
        "Community based services for people with mental health needs - MHC"
    )
    community_based_services_for_people_who_misuse_substances: str = (
        "Community based services for people who misuse substances - SMC"
    )
    community_healthcare_services: str = "Community healthcare services - CHC"
    acute_services: str = "Acute services - ACS"
    hospice_services: str = "Hospice services - HPS"
    long_term_conditions_services: str = "Long term conditions services - LTC"
    hospital_services_for_people_with_mental_health_needs_learning_disabilities_and_or_problems_with_substance_misuse: (
        str
    ) = "Hospital services for people with mental health needs, learning disabilities and/or problems with substance misuse - MLS"
    rehabilitation_services: str = "Rehabilitation services - RHS"
    residential_substance_misuse_treatment_rehabilitation_services: str = (
        "Residential substance misuse treatment/rehabilitation services - RSM"
    )
    other_healthcare_service: str = "Other healthcare service"
    head_office_services: str = "Head office services"
    live_in_care: str = (
        "Live-in Care (can only be used as Other Service) - CQC Regulated"
    )
    nurses_agency: str = "Nurses Agency"
    any_childrens_young_peoples_service: str = "Any childrens/young peoples service"
    any_other_part_of_nhs_hospital_community_health_services: str = (
        "Any other part of NHS Hospital & Community Health Services"
    )


@dataclass
class RegistrationType(ColumnValues):
    """The possible values of the registration type column in ASCWDS data"""

    not_recorded: str = "Not recorded"
    not_regulated: str = "Not regulated"
    ofsted: str = "Ofsted"
    cqc_regulated: str = "CQC regulated"


@dataclass
class RegionID(ColumnValues):
    """The possible values of the region id column in ASCWDS workplace data.

    Unlike its label-only siblings above, region_id's raw code is what
    survives the general clean job - see the module docstring - so this
    class holds codes, not labels.
    """

    eastern: str = "1"
    east_midlands: str = "2"
    london: str = "3"
    north_east: str = "4"
    north_west: str = "5"
    south_east: str = "6"
    south_west: str = "7"
    west_midlands: str = "8"
    yorkshire_and_humber: str = "9"
    # Confirmed present in production data (ticket 2090) - not in the old
    # reconciliation-only label dict this module's REGION_ID_CODE_TO_LABEL
    # replaces, since that dict only needed to label rows, never validate
    # completeness against every code actually in use.
    not_known: str = "-1"


@dataclass(frozen=True)
class PairedVocab:
    """Links a code `ColumnValues` class to its label `ColumnValues` class by field name."""

    id_cls: type
    label_cls: type

    def __post_init__(self) -> None:
        """Fail fast if the label side declares a field the code side doesn't.

        `id_cls` is allowed extra fields (legacy codes with no label, e.g.
        main_job_role's `technician`/`care_navigator` - see
        `unlabelled_codes()`). `label_cls` is not: an unmatched label field
        would otherwise be silently dropped by `code_to_label()` instead of
        raising, reintroducing the hand-sync drift this class exists to
        eliminate.

        Raises:
            ValueError: if `label_cls` declares a field `id_cls` doesn't.
        """
        orphaned_labels = self._label_fields() - self._id_fields()
        if orphaned_labels:
            raise ValueError(
                f"{self.label_cls.__name__} declares field(s) {sorted(orphaned_labels)} "
                f"with no matching field on {self.id_cls.__name__}"
            )

    def _id_fields(self) -> set[str]:
        return {f.name for f in fields(self.id_cls)} - _BASE_FIELDS

    def _label_fields(self) -> set[str]:
        return {f.name for f in fields(self.label_cls)} - _BASE_FIELDS

    def code_to_label(self) -> dict[str, str]:
        """Build the `{code: label}` mapping for fields both classes declare.

        Fields declared only on `id_cls` (e.g. main_job_role's `technician`/
        `care_navigator`) are excluded, since they have no label.

        Returns:
            dict[str, str]: mapping of raw code to label.
        """
        shared = self._id_fields() & self._label_fields()
        return {
            getattr(self.id_cls, name): getattr(self.label_cls, name) for name in shared
        }

    def unlabelled_codes(self) -> dict[str, str]:
        """Build a mapping of field name to code for `id_cls` codes with no matching label.

        Returns:
            dict[str, str]: mapping of field name to raw code.
        """
        return {
            name: getattr(self.id_cls, name)
            for name in self._id_fields() - self._label_fields()
        }


MAIN_JOB_ROLE = PairedVocab(MainJobRoleID, MainJobRoleLabels)
EMPLOYMENT_STATUS = PairedVocab(EmploymentStatusID, EmploymentStatusLabels)

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

# Code 3 ("Placeholder label") intentionally excluded - legacy/unused code,
# same treatment as MainJobRoleID's technician/care_navigator.
PARENT_PERMISSION_CODE_TO_LABEL: dict[str, str] = {
    "1": ParentPermission.parent_has_ownership,
    "2": ParentPermission.workplace_has_ownership,
}

IS_PARENT_CODE_TO_LABEL: dict[str, str] = {
    "0": IsParent.is_not_parent,
    "1": IsParent.is_parent,
}

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

REGISTRATION_TYPE_CODE_TO_LABEL: dict[str, str] = {
    "-1": RegistrationType.not_recorded,
    "0": RegistrationType.not_regulated,
    "1": RegistrationType.ofsted,
    "2": RegistrationType.cqc_regulated,
}

REGION_ID_CODE_TO_LABEL: dict[str, str] = {
    RegionID.eastern: "I - Eastern",
    RegionID.east_midlands: "C - East Midlands",
    RegionID.london: "G - London",
    RegionID.north_east: "B - North East",
    RegionID.north_west: "F - North West",
    RegionID.south_east: "H - South East",
    RegionID.south_west: "D - South West",
    RegionID.west_midlands: "E - West Midlands",
    RegionID.yorkshire_and_humber: "J - Yorkshire Humber",
    RegionID.not_known: "Not known",
}
