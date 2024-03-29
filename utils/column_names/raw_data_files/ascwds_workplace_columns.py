from dataclasses import dataclass


@dataclass
class AscwdsWorkplaceColumns:
    address: str = "address"
    advertising_costs: str = "advertising_costs"
    benchmarks_count_month: str = "benchmarkscount_month"
    benchmarks_count_year: str = "benchmarkscount_year"
    care_cert_accepted: str = "care_cert_accepted"
    care_worker_annual_leave: str = "cwannual_leave"
    care_worker_enhanced_pension: str = "cwenhancedpension"
    care_worker_enhanced_sick_pay: str = "cwenhancedsickpay"
    care_worker_loyalty_bonus_amount: str = "cwloyaltybonusamount"
    cqc_permission: str = "cqcpermission"
    cssr: str = "cssr"
    day: str = "day"
    derived_from_has_bulk_uploaded: str = "derivedfrom_hasbulkuploaded"
    establishment_created_date: str = "estabcreateddate"
    establishment_id: str = "establishmentid"
    establishment_id_from_nmds: str = "tribalid"
    establishment_name: str = "establishmentname"
    establishment_save_date: str = "estabsavedate"
    establishment_type: str = "esttype"
    establishment_type_change_date: str = "esttype_changedate"
    establishment_type_save_date: str = "esttype_savedate"
    establishment_updated_date: str = "estabupdateddate"
    has_care_worker_loyalty_bonus: str = "has_cwloyaltybonus"
    has_mandatory_training: str = "hasmandatorytraining"
    import_date: str = "import_date"
    is_bulk_uploader: str = "isbulkuploader"
    is_parent: str = "isparent"
    job_role_01_agency: str = "jr01agcy"
    job_role_01_employees: str = "jr01emp"
    job_role_01_flag: str = "jr01flag"
    job_role_01_leavers: str = "jr01stop"
    job_role_01_other: str = "jr01oth"
    job_role_01_permanent: str = "jr01perm"
    job_role_01_pool: str = "jr01pool"
    job_role_01_starters: str = "jr01strt"
    job_role_01_temporary: str = "jr01temp"
    job_role_01_vacancies: str = "jr01vacy"
    job_role_01_workers: str = "jr01work"
    job_role_02_agency: str = "jr02agcy"
    job_role_02_employees: str = "jr02emp"
    job_role_02_flag: str = "jr02flag"
    job_role_02_leavers: str = "jr02stop"
    job_role_02_other: str = "jr02oth"
    job_role_02_permanent: str = "jr02perm"
    job_role_02_pool: str = "jr02pool"
    job_role_02_starters: str = "jr02strt"
    job_role_02_temporary: str = "jr02temp"
    job_role_02_vacancies: str = "jr02vacy"
    job_role_02_workers: str = "jr02work"
    job_role_03_agency: str = "jr03agcy"
    job_role_03_employees: str = "jr03emp"
    job_role_03_flag: str = "jr03flag"
    job_role_03_leavers: str = "jr03stop"
    job_role_03_other: str = "jr03oth"
    job_role_03_permanent: str = "jr03perm"
    job_role_03_pool: str = "jr03pool"
    job_role_03_starters: str = "jr03strt"
    job_role_03_temporary: str = "jr03temp"
    job_role_03_vacancies: str = "jr03vacy"
    job_role_03_workers: str = "jr03work"
    job_role_04_agency: str = "jr04agcy"
    job_role_04_employees: str = "jr04emp"
    job_role_04_flag: str = "jr04flag"
    job_role_04_leavers: str = "jr04stop"
    job_role_04_other: str = "jr04oth"
    job_role_04_permanent: str = "jr04perm"
    job_role_04_pool: str = "jr04pool"
    job_role_04_starters: str = "jr04strt"
    job_role_04_temporary: str = "jr04temp"
    job_role_04_vacancies: str = "jr04vacy"
    job_role_04_workers: str = "jr04work"
    job_role_05_agency: str = "jr05agcy"
    job_role_05_employees: str = "jr05emp"
    job_role_05_flag: str = "jr05flag"
    job_role_05_leavers: str = "jr05stop"
    job_role_05_other: str = "jr05oth"
    job_role_05_permanent: str = "jr05perm"
    job_role_05_pool: str = "jr05pool"
    job_role_05_starters: str = "jr05strt"
    job_role_05_temporary: str = "jr05temp"
    job_role_05_vacancies: str = "jr05vacy"
    job_role_05_workers: str = "jr05work"
    job_role_06_agency: str = "jr06agcy"
    job_role_06_employees: str = "jr06emp"
    job_role_06_flag: str = "jr06flag"
    job_role_06_leavers: str = "jr06stop"
    job_role_06_other: str = "jr06oth"
    job_role_06_permanent: str = "jr06perm"
    job_role_06_pool: str = "jr06pool"
    job_role_06_starters: str = "jr06strt"
    job_role_06_temporary: str = "jr06temp"
    job_role_06_vacancies: str = "jr06vacy"
    job_role_06_workers: str = "jr06work"
    job_role_07_agency: str = "jr07agcy"
    job_role_07_employees: str = "jr07emp"
    job_role_07_flag: str = "jr07flag"
    job_role_07_leavers: str = "jr07stop"
    job_role_07_other: str = "jr07oth"
    job_role_07_permanent: str = "jr07perm"
    job_role_07_pool: str = "jr07pool"
    job_role_07_starters: str = "jr07strt"
    job_role_07_temporary: str = "jr07temp"
    job_role_07_vacancies: str = "jr07vacy"
    job_role_07_workers: str = "jr07work"
    job_role_08_agency: str = "jr08agcy"
    job_role_08_employees: str = "jr08emp"
    job_role_08_flag: str = "jr08flag"
    job_role_08_leavers: str = "jr08stop"
    job_role_08_other: str = "jr08oth"
    job_role_08_permanent: str = "jr08perm"
    job_role_08_pool: str = "jr08pool"
    job_role_08_starters: str = "jr08strt"
    job_role_08_temporary: str = "jr08temp"
    job_role_08_vacancies: str = "jr08vacy"
    job_role_08_workers: str = "jr08work"
    job_role_09_agency: str = "jr09agcy"
    job_role_09_employees: str = "jr09emp"
    job_role_09_flag: str = "jr09flag"
    job_role_09_leavers: str = "jr09stop"
    job_role_09_other: str = "jr09oth"
    job_role_09_permanent: str = "jr09perm"
    job_role_09_pool: str = "jr09pool"
    job_role_09_starters: str = "jr09strt"
    job_role_09_temporary: str = "jr09temp"
    job_role_09_vacancies: str = "jr09vacy"
    job_role_09_workers: str = "jr09work"
    job_role_10_agency: str = "jr10agcy"
    job_role_10_employees: str = "jr10emp"
    job_role_10_flag: str = "jr10flag"
    job_role_10_leavers: str = "jr10stop"
    job_role_10_other: str = "jr10oth"
    job_role_10_permanent: str = "jr10perm"
    job_role_10_pool: str = "jr10pool"
    job_role_10_starters: str = "jr10strt"
    job_role_10_temporary: str = "jr10temp"
    job_role_10_vacancies: str = "jr10vacy"
    job_role_10_workers: str = "jr10work"
    job_role_11_agency: str = "jr11agcy"
    job_role_11_employees: str = "jr11emp"
    job_role_11_flag: str = "jr11flag"
    job_role_11_leavers: str = "jr11stop"
    job_role_11_other: str = "jr11oth"
    job_role_11_permanent: str = "jr11perm"
    job_role_11_pool: str = "jr11pool"
    job_role_11_starters: str = "jr11strt"
    job_role_11_temporary: str = "jr11temp"
    job_role_11_vacancies: str = "jr11vacy"
    job_role_11_workers: str = "jr11work"
    job_role_15_agency: str = "jr15agcy"
    job_role_15_employees: str = "jr15emp"
    job_role_15_flag: str = "jr15flag"
    job_role_15_leavers: str = "jr15stop"
    job_role_15_other: str = "jr15oth"
    job_role_15_permanent: str = "jr15perm"
    job_role_15_pool: str = "jr15pool"
    job_role_15_starters: str = "jr15strt"
    job_role_15_temporary: str = "jr15temp"
    job_role_15_vacancies: str = "jr15vacy"
    job_role_15_workers: str = "jr15work"
    job_role_16_agency: str = "jr16agcy"
    job_role_16_employees: str = "jr16emp"
    job_role_16_flag: str = "jr16flag"
    job_role_16_leavers: str = "jr16stop"
    job_role_16_other: str = "jr16oth"
    job_role_16_permanent: str = "jr16perm"
    job_role_16_pool: str = "jr16pool"
    job_role_16_starters: str = "jr16strt"
    job_role_16_temporary: str = "jr16temp"
    job_role_16_vacancies: str = "jr16vacy"
    job_role_16_workers: str = "jr16work"
    job_role_17_agency: str = "jr17agcy"
    job_role_17_employees: str = "jr17emp"
    job_role_17_flag: str = "jr17flag"
    job_role_17_leavers: str = "jr17stop"
    job_role_17_other: str = "jr17oth"
    job_role_17_permanent: str = "jr17perm"
    job_role_17_pool: str = "jr17pool"
    job_role_17_starters: str = "jr17strt"
    job_role_17_temporary: str = "jr17temp"
    job_role_17_vacancies: str = "jr17vacy"
    job_role_17_workers: str = "jr17work"
    job_role_22_agency: str = "jr22agcy"
    job_role_22_employees: str = "jr22emp"
    job_role_22_flag: str = "jr22flag"
    job_role_22_leavers: str = "jr22stop"
    job_role_22_other: str = "jr22oth"
    job_role_22_permanent: str = "jr22perm"
    job_role_22_pool: str = "jr22pool"
    job_role_22_starters: str = "jr22strt"
    job_role_22_temporary: str = "jr22temp"
    job_role_22_vacancies: str = "jr22vacy"
    job_role_22_workers: str = "jr22work"
    job_role_23_agency: str = "jr23agcy"
    job_role_23_employees: str = "jr23emp"
    job_role_23_flag: str = "jr23flag"
    job_role_23_leavers: str = "jr23stop"
    job_role_23_other: str = "jr23oth"
    job_role_23_permanent: str = "jr23perm"
    job_role_23_pool: str = "jr23pool"
    job_role_23_starters: str = "jr23strt"
    job_role_23_temporary: str = "jr23temp"
    job_role_23_vacancies: str = "jr23vacy"
    job_role_23_workers: str = "jr23work"
    job_role_24_agency: str = "jr24agcy"
    job_role_24_employees: str = "jr24emp"
    job_role_24_flag: str = "jr24flag"
    job_role_24_leavers: str = "jr24stop"
    job_role_24_other: str = "jr24oth"
    job_role_24_permanent: str = "jr24perm"
    job_role_24_pool: str = "jr24pool"
    job_role_24_starters: str = "jr24strt"
    job_role_24_temporary: str = "jr24temp"
    job_role_24_vacancies: str = "jr24vacy"
    job_role_24_workers: str = "jr24work"
    job_role_25_agency: str = "jr25agcy"
    job_role_25_employees: str = "jr25emp"
    job_role_25_flag: str = "jr25flag"
    job_role_25_leavers: str = "jr25stop"
    job_role_25_other: str = "jr25oth"
    job_role_25_permanent: str = "jr25perm"
    job_role_25_pool: str = "jr25pool"
    job_role_25_starters: str = "jr25strt"
    job_role_25_temporary: str = "jr25temp"
    job_role_25_vacancies: str = "jr25vacy"
    job_role_25_workers: str = "jr25work"
    job_role_26_agency: str = "jr26agcy"
    job_role_26_employees: str = "jr26emp"
    job_role_26_flag: str = "jr26flag"
    job_role_26_leavers: str = "jr26stop"
    job_role_26_other: str = "jr26oth"
    job_role_26_permanent: str = "jr26perm"
    job_role_26_pool: str = "jr26pool"
    job_role_26_starters: str = "jr26strt"
    job_role_26_temporary: str = "jr26temp"
    job_role_26_vacancies: str = "jr26vacy"
    job_role_26_workers: str = "jr26work"
    job_role_27_agency: str = "jr27agcy"
    job_role_27_employees: str = "jr27emp"
    job_role_27_flag: str = "jr27flag"
    job_role_27_leavers: str = "jr27stop"
    job_role_27_other: str = "jr27oth"
    job_role_27_permanent: str = "jr27perm"
    job_role_27_pool: str = "jr27pool"
    job_role_27_starters: str = "jr27strt"
    job_role_27_temporary: str = "jr27temp"
    job_role_27_vacancies: str = "jr27vacy"
    job_role_27_workers: str = "jr27work"
    job_role_28_agency: str = "jr28agcy"
    job_role_28_employees: str = "jr28emp"
    job_role_28_flag: str = "jr28flag"
    job_role_28_leavers: str = "jr28stop"
    job_role_28_other: str = "jr28oth"
    job_role_28_permanent: str = "jr28perm"
    job_role_28_pool: str = "jr28pool"
    job_role_28_starters: str = "jr28strt"
    job_role_28_temporary: str = "jr28temp"
    job_role_28_vacancies: str = "jr28vacy"
    job_role_28_workers: str = "jr28work"
    job_role_29_agency: str = "jr29agcy"
    job_role_29_employees: str = "jr29emp"
    job_role_29_flag: str = "jr29flag"
    job_role_29_leavers: str = "jr29stop"
    job_role_29_other: str = "jr29oth"
    job_role_29_permanent: str = "jr29perm"
    job_role_29_pool: str = "jr29pool"
    job_role_29_starters: str = "jr29strt"
    job_role_29_temporary: str = "jr29temp"
    job_role_29_vacancies: str = "jr29vacy"
    job_role_29_workers: str = "jr29work"
    job_role_30_agency: str = "jr30agcy"
    job_role_30_employees: str = "jr30emp"
    job_role_30_flag: str = "jr30flag"
    job_role_30_leavers: str = "jr30stop"
    job_role_30_other: str = "jr30oth"
    job_role_30_permanent: str = "jr30perm"
    job_role_30_pool: str = "jr30pool"
    job_role_30_starters: str = "jr30strt"
    job_role_30_temporary: str = "jr30temp"
    job_role_30_vacancies: str = "jr30vacy"
    job_role_30_workers: str = "jr30work"
    job_role_31_agency: str = "jr31agcy"
    job_role_31_employees: str = "jr31emp"
    job_role_31_flag: str = "jr31flag"
    job_role_31_leavers: str = "jr31stop"
    job_role_31_other: str = "jr31oth"
    job_role_31_permanent: str = "jr31perm"
    job_role_31_pool: str = "jr31pool"
    job_role_31_starters: str = "jr31strt"
    job_role_31_temporary: str = "jr31temp"
    job_role_31_vacancies: str = "jr31vacy"
    job_role_31_workers: str = "jr31work"
    job_role_32_agency: str = "jr32agcy"
    job_role_32_employees: str = "jr32emp"
    job_role_32_flag: str = "jr32flag"
    job_role_32_leavers: str = "jr32stop"
    job_role_32_other: str = "jr32oth"
    job_role_32_permanent: str = "jr32perm"
    job_role_32_pool: str = "jr32pool"
    job_role_32_starters: str = "jr32strt"
    job_role_32_temporary: str = "jr32temp"
    job_role_32_vacancies: str = "jr32vacy"
    job_role_32_workers: str = "jr32work"
    job_role_34_agency: str = "jr34agcy"
    job_role_34_employees: str = "jr34emp"
    job_role_34_flag: str = "jr34flag"
    job_role_34_leavers: str = "jr34stop"
    job_role_34_other: str = "jr34oth"
    job_role_34_permanent: str = "jr34perm"
    job_role_34_pool: str = "jr34pool"
    job_role_34_starters: str = "jr34strt"
    job_role_34_temporary: str = "jr34temp"
    job_role_34_vacancies: str = "jr34vacy"
    job_role_34_workers: str = "jr34work"
    job_role_35_agency: str = "jr35agcy"
    job_role_35_employees: str = "jr35emp"
    job_role_35_flag: str = "jr35flag"
    job_role_35_leavers: str = "jr35stop"
    job_role_35_other: str = "jr35oth"
    job_role_35_permanent: str = "jr35perm"
    job_role_35_pool: str = "jr35pool"
    job_role_35_starters: str = "jr35strt"
    job_role_35_temporary: str = "jr35temp"
    job_role_35_vacancies: str = "jr35vacy"
    job_role_35_workers: str = "jr35work"
    job_role_36_agency: str = "jr36agcy"
    job_role_36_employees: str = "jr36emp"
    job_role_36_flag: str = "jr36flag"
    job_role_36_leavers: str = "jr36stop"
    job_role_36_other: str = "jr36oth"
    job_role_36_permanent: str = "jr36perm"
    job_role_36_pool: str = "jr36pool"
    job_role_36_starters: str = "jr36strt"
    job_role_36_temporary: str = "jr36temp"
    job_role_36_vacancies: str = "jr36vacy"
    job_role_36_workers: str = "jr36work"
    job_role_37_agency: str = "jr37agcy"
    job_role_37_employees: str = "jr37emp"
    job_role_37_flag: str = "jr37flag"
    job_role_37_leavers: str = "jr37stop"
    job_role_37_other: str = "jr37oth"
    job_role_37_permanent: str = "jr37perm"
    job_role_37_pool: str = "jr37pool"
    job_role_37_starters: str = "jr37strt"
    job_role_37_temporary: str = "jr37temp"
    job_role_37_vacancies: str = "jr37vacy"
    job_role_37_workers: str = "jr37work"
    job_role_38_agency: str = "jr38agcy"
    job_role_38_employees: str = "jr38emp"
    job_role_38_flag: str = "jr38flag"
    job_role_38_leavers: str = "jr38stop"
    job_role_38_other: str = "jr38oth"
    job_role_38_permanent: str = "jr38perm"
    job_role_38_pool: str = "jr38pool"
    job_role_38_starters: str = "jr38strt"
    job_role_38_temporary: str = "jr38temp"
    job_role_38_vacancies: str = "jr38vacy"
    job_role_38_workers: str = "jr38work"
    job_role_39_agency: str = "jr39agcy"
    job_role_39_employees: str = "jr39emp"
    job_role_39_flag: str = "jr39flag"
    job_role_39_leavers: str = "jr39stop"
    job_role_39_other: str = "jr39oth"
    job_role_39_permanent: str = "jr39perm"
    job_role_39_pool: str = "jr39pool"
    job_role_39_starters: str = "jr39strt"
    job_role_39_temporary: str = "jr39temp"
    job_role_39_vacancies: str = "jr39vacy"
    job_role_39_workers: str = "jr39work"
    job_role_40_agency: str = "jr40agcy"
    job_role_40_employees: str = "jr40emp"
    job_role_40_flag: str = "jr40flag"
    job_role_40_leavers: str = "jr40stop"
    job_role_40_other: str = "jr40oth"
    job_role_40_permanent: str = "jr40perm"
    job_role_40_pool: str = "jr40pool"
    job_role_40_starters: str = "jr40strt"
    job_role_40_temporary: str = "jr40temp"
    job_role_40_vacancies: str = "jr40vacy"
    job_role_40_workers: str = "jr40work"
    job_role_41_agency: str = "jr41agcy"
    job_role_41_employees: str = "jr41emp"
    job_role_41_flag: str = "jr41flag"
    job_role_41_leavers: str = "jr41stop"
    job_role_41_other: str = "jr41oth"
    job_role_41_permanent: str = "jr41perm"
    job_role_41_pool: str = "jr41pool"
    job_role_41_starters: str = "jr41strt"
    job_role_41_temporary: str = "jr41temp"
    job_role_41_vacancies: str = "jr41vacy"
    job_role_41_workers: str = "jr41work"
    job_role_42_agency: str = "jr42agcy"
    job_role_42_employees: str = "jr42emp"
    job_role_42_flag: str = "jr42flag"
    job_role_42_leavers: str = "jr42stop"
    job_role_42_other: str = "jr42oth"
    job_role_42_permanent: str = "jr42perm"
    job_role_42_pool: str = "jr42pool"
    job_role_42_starters: str = "jr42strt"
    job_role_42_temporary: str = "jr42temp"
    job_role_42_vacancies: str = "jr42vacy"
    job_role_42_workers: str = "jr42work"
    job_role_43_agency: str = "jr43agcy"
    job_role_43_employees: str = "jr43emp"
    job_role_43_flag: str = "jr43flag"
    job_role_43_leavers: str = "jr43stop"
    job_role_43_other: str = "jr43oth"
    job_role_43_permanent: str = "jr43perm"
    job_role_43_pool: str = "jr43pool"
    job_role_43_starters: str = "jr43strt"
    job_role_43_temporary: str = "jr43temp"
    job_role_43_vacancies: str = "jr43vacy"
    job_role_43_workers: str = "jr43work"
    job_role_44_agency: str = "jr44agcy"
    job_role_44_employees: str = "jr44emp"
    job_role_44_flag: str = "jr44flag"
    job_role_44_leavers: str = "jr44stop"
    job_role_44_other: str = "jr44oth"
    job_role_44_permanent: str = "jr44perm"
    job_role_44_pool: str = "jr44pool"
    job_role_44_starters: str = "jr44strt"
    job_role_44_temporary: str = "jr44temp"
    job_role_44_vacancies: str = "jr44vacy"
    job_role_44_workers: str = "jr44work"
    job_role_45_agency: str = "jr45agcy"
    job_role_45_employees: str = "jr45emp"
    job_role_45_flag: str = "jr45flag"
    job_role_45_leavers: str = "jr45stop"
    job_role_45_other: str = "jr45oth"
    job_role_45_permanent: str = "jr45perm"
    job_role_45_pool: str = "jr45pool"
    job_role_45_starters: str = "jr45strt"
    job_role_45_temporary: str = "jr45temp"
    job_role_45_vacancies: str = "jr45vacy"
    job_role_45_workers: str = "jr45work"
    la_permission: str = "lapermission"
    last_bulk_upload_date: str = "lastbulkuploaddate"
    last_logged_in: str = "lastloggedin"
    last_viewed_benchmarks: str = "lastviewedbenchmarks"
    local_authority_id: str = "lauthid"
    location_id: str = "locationid"
    login_count_month: str = "logincount_month"
    login_count_year: str = "logincount_year"
    login_date_purge: str = "login_date_purge"
    main_service_id: str = "mainstid"
    main_service_id_change_date: str = "mainstid_changedate"
    main_service_id_save_date: str = "mainstid_savedate"
    master_update_date: str = "mupddate"
    month: str = "month"
    nmds_id: str = "nmdsid"
    number_interviewed: str = "number_interviewed"
    organisation_id: str = "orgid"
    parent_id: str = "parentid"
    parent_permission: str = "parentpermission"
    period: str = "period"
    postcode: str = "postcode"
    previous_login_date: str = "previous_logindate"
    previous_master_update_date: str = "previous_mupddate"
    provider_id: str = "providerid"
    region_id: str = "regionid"
    registration_type: str = "regtype"
    repeat_training_accepted: str = "repeat_training_accepted"
    service_type_01_capacity: str = "st01cap"
    service_type_01_capacity_change_date: str = "st01cap_changedate"
    service_type_01_capacity_save_date: str = "st01cap_savedate"
    service_type_01_flag: str = "st01flag"
    service_type_01_utilisation: str = "st01util"
    service_type_01_utilisation_change_date: str = "st01util_changedate"
    service_type_01_utilisation_save_date: str = "st01util_savedate"
    service_type_02_capacity: str = "st02cap"
    service_type_02_capacity_change_date: str = "st02cap_changedate"
    service_type_02_capacity_save_date: str = "st02cap_savedate"
    service_type_02_flag: str = "st02flag"
    service_type_02_utilisation: str = "st02util"
    service_type_02_utilisation_change_date: str = "st02util_changedate"
    service_type_02_utilisation_save_date: str = "st02util_savedate"
    service_type_05_capacity: str = "st05cap"
    service_type_05_capacity_change_date: str = "st05cap_changedate"
    service_type_05_capacity_save_date: str = "st05cap_savedate"
    service_type_05_flag: str = "st05flag"
    service_type_05_utilisation: str = "st05util"
    service_type_05_utilisation_change_date: str = "st05util_changedate"
    service_type_05_utilisation_save_date: str = "st05util_savedate"
    service_type_06_capacity: str = "st06cap"
    service_type_06_capacity_change_date: str = "st06cap_changedate"
    service_type_06_capacity_save_date: str = "st06cap_savedate"
    service_type_06_flag: str = "st06flag"
    service_type_06_utilisation: str = "st06util"
    service_type_06_utilisation_change_date: str = "st06util_changedate"
    service_type_06_utilisation_save_date: str = "st06util_savedate"
    service_type_07_capacity: str = "st07cap"
    service_type_07_capacity_change_date: str = "st07cap_changedate"
    service_type_07_capacity_save_date: str = "st07cap_savedate"
    service_type_07_flag: str = "st07flag"
    service_type_07_utilisation: str = "st07util"
    service_type_07_utilisation_change_date: str = "st07util_changedate"
    service_type_07_utilisation_save_date: str = "st07util_savedate"
    service_type_08_flag: str = "st08flag"
    service_type_08_utilisation: str = "st08util"
    service_type_08_utilisation_change_date: str = "st08util_changedate"
    service_type_08_utilisation_save_date: str = "st08util_savedate"
    service_type_10_flag: str = "st10flag"
    service_type_10_utilisation: str = "st10util"
    service_type_10_utilisation_change_date: str = "st10util_changedate"
    service_type_10_utilisation_save_date: str = "st10util_savedate"
    service_type_12_flag: str = "st12flag"
    service_type_12_utilisation: str = "st12util"
    service_type_12_utilisation_change_date: str = "st12util_changedate"
    service_type_12_utilisation_save_date: str = "st12util_savedate"
    service_type_13_flag: str = "st13flag"
    service_type_14_flag: str = "st14flag"
    service_type_15_flag: str = "st15flag"
    service_type_16_flag: str = "st16flag"
    service_type_17_capacity: str = "st17cap"
    service_type_17_capacity_change_date: str = "st17cap_changedate"
    service_type_17_capacity_save_date: str = "st17cap_savedate"
    service_type_17_flag: str = "st17flag"
    service_type_17_utilisation: str = "st17util"
    service_type_17_utilisation_change_date: str = "st17util_changedate"
    service_type_17_utilisation_save_date: str = "st17util_savedate"
    service_type_18_flag: str = "st18flag"
    service_type_19_flag: str = "st19flag"
    service_type_20_flag: str = "st20flag"
    service_type_21_flag: str = "st21flag"
    service_type_52_flag: str = "st52flag"
    service_type_53_flag: str = "st53flag"
    service_type_53_utilisation: str = "st53util"
    service_type_53_utilisation_change_date: str = "st53util_changedate"
    service_type_53_utilisation_save_date: str = "st53util_savedate"
    service_type_54_flag: str = "st54flag"
    service_type_54_utilisation: str = "st54util"
    service_type_54_utilisation_change_date: str = "st54util_changedate"
    service_type_54_utilisation_save_date: str = "st54util_savedate"
    service_type_55_flag: str = "st55flag"
    service_type_55_utilisation: str = "st55util"
    service_type_55_utilisation_change_date: str = "st55util_changedate"
    service_type_55_utilisation_save_date: str = "st55util_savedate"
    service_type_60_flag: str = "st60flag"
    service_type_61_flag: str = "st61flag"
    service_type_62_flag: str = "st62flag"
    service_type_63_flag: str = "st63flag"
    service_type_64_flag: str = "st64flag"
    service_type_66_flag: str = "st66flag"
    service_type_67_flag: str = "st67flag"
    service_type_68_flag: str = "st68flag"
    service_type_69_flag: str = "st69flag"
    service_type_70_flag: str = "st70flag"
    service_type_71_flag: str = "st71flag"
    service_type_72_flag: str = "st72flag"
    service_type_73_flag: str = "st73flag"
    service_type_73_utilisation: str = "st73util"
    service_type_73_utilisation_change_date: str = "st73util_changedate"
    service_type_73_utilisation_save_date: str = "st73util_savedate"
    service_type_74_flag: str = "st74flag"
    service_type_74_utilisation: str = "st74util"
    service_type_74_utilisation_change_date: str = "st74util_changedate"
    service_type_74_utilisation_save_date: str = "st74util_savedate"
    service_type_75_flag: str = "st75flag"
    service_type_change_date: str = "st_changedate"
    service_type_save_date: str = "st_savedate"
    service_user_type_change_date: str = "ut_changedate"
    service_user_type_save_date: str = "ut_savedate"
    total_leavers: str = "totalleavers"
    total_leavers_change_date: str = "totalleavers_changedate"
    total_leavers_save_date: str = "totalleavers_savedate"
    total_staff: str = "totalstaff"
    total_staff_change_date: str = "totalstaff_changedate"
    total_staff_save_date: str = "totalstaff_savedate"
    total_starters: str = "totalstarters"
    total_starters_change_date: str = "totalstarters_changedate"
    total_starters_save_date: str = "totalstarters_savedate"
    total_vacancies: str = "totalvacancies"
    total_vacancies_change_date: str = "totalvacancies_changedate"
    total_vacancies_save_date: str = "totalvacancies_savedate"
    update_count_month: str = "updatecount_month"
    update_count_year: str = "updatecount_year"
    user_type_01_flag: str = "ut01flag"
    user_type_02_flag: str = "ut02flag"
    user_type_03_flag: str = "ut03flag"
    user_type_04_flag: str = "ut04flag"
    user_type_05_flag: str = "ut05flag"
    user_type_06_flag: str = "ut06flag"
    user_type_07_flag: str = "ut07flag"
    user_type_08_flag: str = "ut08flag"
    user_type_09_flag: str = "ut09flag"
    user_type_18_flag: str = "ut18flag"
    user_type_19_flag: str = "ut19flag"
    user_type_20_flag: str = "ut20flag"
    user_type_21_flag: str = "ut21flag"
    user_type_22_flag: str = "ut22flag"
    user_type_23_flag: str = "ut23flag"
    user_type_25_flag: str = "ut25flag"
    user_type_26_flag: str = "ut26flag"
    user_type_27_flag: str = "ut27flag"
    user_type_28_flag: str = "ut28flag"
    user_type_29_flag: str = "ut29flag"
    user_type_31_flag: str = "ut31flag"
    user_type_45_flag: str = "ut45flag"
    user_type_46_flag: str = "ut46flag"
    version: str = "version"
    worker_records: str = "wkrrecs"
    worker_records_change_date: str = "wkrrecs_changedate"
    worker_records_wdf_save_date: str = "wkrrecs_wdfsavedate"
    worker_save_date: str = "workersavedate"
    worker_update: str = "workerupdate"
    workplace_status: str = "wkplacestat"
    year: str = "year"


@dataclass
class PartitionKeys:
    year: str = AscwdsWorkplaceColumns.year
    month: str = AscwdsWorkplaceColumns.month
    day: str = AscwdsWorkplaceColumns.day
    import_date: str = AscwdsWorkplaceColumns.import_date
