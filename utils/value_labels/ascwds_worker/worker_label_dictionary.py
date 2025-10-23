from utils.value_labels.ascwds_worker.ascwds_worker_jobgroup_dictionary import (
    AscwdsWorkerValueLabelsJobGroup,
)
from utils.value_labels.ascwds_worker.ascwds_worker_mainjrid import (
    AscwdsWorkerValueLabelsMainjrid,
)

ascwds_worker_labels_dict = {
    AscwdsWorkerValueLabelsMainjrid.column_name: AscwdsWorkerValueLabelsMainjrid.labels_dict,
    AscwdsWorkerValueLabelsJobGroup.column_name: AscwdsWorkerValueLabelsJobGroup.job_role_to_job_group_dict,
}
