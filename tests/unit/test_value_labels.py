from utils.column_values.categorical_column_values import (
    JobGroupLabels,
    MainJobRoleLabels,
)
from utils.value_labels.ascwds_worker.ascwds_worker_jobgroup_dictionary import (
    AscwdsWorkerValueLabelsJobGroup,
)


class TestFilterJobRoles:
    def test_filter_for_direct_care_roles(self):
        direct_care_roles = AscwdsWorkerValueLabelsJobGroup.filter_roles(
            JobGroupLabels.direct_care
        )
        assert MainJobRoleLabels.care_worker in direct_care_roles
        assert MainJobRoleLabels.nursing_assistant in direct_care_roles
        assert MainJobRoleLabels.registered_manager not in direct_care_roles

    def test_filter_for_managers(self):
        managers = AscwdsWorkerValueLabelsJobGroup.manager_roles()
        assert MainJobRoleLabels.registered_manager in managers
        assert MainJobRoleLabels.supervisor in managers
        assert MainJobRoleLabels.care_worker not in managers
