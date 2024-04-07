from utils.value_labels.ascwds_workplace.ascwds_workplace_esttype import (
    AscwdsWorkplaceValueLabelsEsttype,
)
from utils.value_labels.ascwds_workplace.ascwds_workplace_parentpermission import (
    AscwdsWorkplaceValueLabelsParentPermission,
)
from utils.value_labels.ascwds_workplace.ascwds_workplace_isparent import (
    AscwdsWorkplaceValueLabelsIsParent,
)
from utils.value_labels.ascwds_workplace.ascwds_workplace_mainstid import (
    AscwdsWorkplaceValueLabelsMainstid,
)
from utils.value_labels.ascwds_workplace.ascwds_workplace_regtype import (
    AscwdsWorkplaceValueLabelsRegtype,
)


ascwds_workplace_labels_dict = {
    AscwdsWorkplaceValueLabelsEsttype.column_name: AscwdsWorkplaceValueLabelsEsttype.labels_dict,
    AscwdsWorkplaceValueLabelsParentPermission.column_name: AscwdsWorkplaceValueLabelsParentPermission.labels_dict,
    AscwdsWorkplaceValueLabelsIsParent.column_name: AscwdsWorkplaceValueLabelsIsParent.labels_dict,
    AscwdsWorkplaceValueLabelsMainstid.column_name: AscwdsWorkplaceValueLabelsMainstid.labels_dict,
    AscwdsWorkplaceValueLabelsRegtype.column_name: AscwdsWorkplaceValueLabelsRegtype.labels_dict,
}
