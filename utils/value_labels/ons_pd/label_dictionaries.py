from utils.value_labels.ons_pd.onspd_cssr import OnspdCssr
from utils.value_labels.ons_pd.onspd_icb import OnspdIcb
from utils.value_labels.ons_pd.onspd_icb_region import OnspdIcbRegion
from utils.value_labels.ons_pd.onspd_lsoa21 import OnspdLsoa21
from utils.value_labels.ons_pd.onspd_msoa21 import OnspdMsoa21
from utils.value_labels.ons_pd.onspd_pcon import OnspdPcon
from utils.value_labels.ons_pd.onspd_region import OnspdRegion
from utils.value_labels.ons_pd.onspd_ru11ind import OnspdRuralUrbanIndicator2011
from utils.value_labels.ons_pd.onspd_sub_icb import OnspdSubIcb

onspd_labels_dict = {
    OnspdCssr.column_name: OnspdCssr.labels_dict,
    OnspdIcb.column_name: OnspdIcb.labels_dict,
    OnspdIcbRegion.column_name: OnspdIcbRegion.labels_dict,
    OnspdLsoa21.column_name: OnspdLsoa21.labels_dict,
    OnspdMsoa21.column_name: OnspdMsoa21.labels_dict,
    OnspdPcon.column_name: OnspdPcon.labels_dict,
    OnspdRegion.column_name: OnspdRegion.labels_dict,
    OnspdRuralUrbanIndicator2011.column_name: OnspdRuralUrbanIndicator2011.labels_dict,
    OnspdSubIcb.column_name: OnspdSubIcb.labels_dict,
}
