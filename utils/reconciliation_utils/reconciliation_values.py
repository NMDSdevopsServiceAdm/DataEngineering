from dataclasses import dataclass


@dataclass
class ReconciliationColumns:
    parents_or_singles_and_subs: str = "parents_or_singles_and_subs"
    new_potential_subs: str = "new_potential_subs"
    old_potential_subs: str = "old_potential_subs"
    missing_or_incorrect_potential_subs: str = "missing_or_incorrect_potential_subs"
    subject: str = "Subject"
    nmds: str = "NMDS"
    name: str = "Name"
    description: str = "Description"
    requester_name: str = "Requester Name"
    requester_name_2: str = "Requester Name 2"
    sector: str = "Sector"
    status: str = "Status"
    technician: str = "Technician"
    sfc_region: str = "SfC Region"
    manual_call_log: str = "Manual Call Log"
    mode: str = "Mode"
    priority: str = "Priority"
    category: str = "Category"
    sub_category: str = "Subcategory"
    is_requester_named: str = "Is requester named user on account?"
    security_question: str = "Correct answer to security question received"
    website: str = "Website"
    item: str = "Item"
    phone: str = "Phone"
    workplace_id: str = "Workplace Id"


@dataclass
class ReconciliationValues:
    singles_and_subs: str = "singles_and_subs"
    parents: str = "parents"
    single_sub_subject_value: str = "CQC Reconcilliation Work"
    parent_subject_value: str = "CQC Reconcilliation Work - Parent"
    single_sub_deregistered_description: str = "Potential (new): Deregistered ID"
    single_sub_reg_type_description: str = "Potential (new): Regtype"
    is_parent: str = "Yes"
    is_not_parent: str = "No"
