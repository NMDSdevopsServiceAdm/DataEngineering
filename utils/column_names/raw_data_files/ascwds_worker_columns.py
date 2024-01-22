from dataclasses import dataclass


@dataclass
class AscwdsWorkerColumns:
    age: str = "age"
    age_change_date: str = "age_changedate"
    age_save_date: str = "age_savedate"
    amhp: str = "amhp"
    amhp_change_date: str = "amhp_changedate"
    amhp_save_date: str = "amhp_savedate"
    apprentice: str = "apprentice"
    apprentice_change_date: str = "apprentice_changedate"
    apprentice_save_date: str = "apprentice_savedate"
    average_hours: str = "averagehours"
    average_hours_change_date: str = "averagehours_changedate"
    average_hours_save_date: str = "averagehours_savedate"
    born_in_uk: str = "borninuk"
    born_in_uk_change_date: str = "borninuk_changedate"
    born_in_uk_save_date: str = "borninuk_savedate"
    british_citizen: str = "britishcitizen"
    british_citizen_change_date: str = "britishcitizen_changedate"
    british_citizen_save_date: str = "britishcitizen_savedate"
    care_certificate_status: str = "ccstatus"
    care_certificate_status_change_date: str = "ccstatus_changedate"
    care_certificate_status_save_date: str = "ccstatus_savedate"
    contracted_hours: str = "conthrs"
    contracted_hours_change_date: str = "conthrs_changedate"
    contracted_hours_save_date: str = "conthrs_savedate"
    country_of_birth: str = "countryofbirth"
    cqc_permission: str = "cqcpermission"
    created_date: str = "createddate"
    cssr: str = "cssr"
    day: str = "day"
    days_sick: str = "dayssick"
    days_sick_change_date: str = "dayssick_changedate"
    days_sick_save_date: str = "dayssick_savedate"
    derived_from_has_bulk_uploaded: str = "derivedfrom_hasbulkuploaded"
    disabled: str = "disabled"
    disabled_change_date: str = "disabled_changedate"
    disabled_save_date: str = "disabled_savedate"
    distance_to_work_in_miles: str = "distwrkk"
    employment_status: str = "emplstat"
    employment_status_change_date: str = "emplstat_changedate"
    employment_status_save_date: str = "emplstat_savedate"
    establishment_id: str = "establishmentid"
    establishment_id_from_nmds: str = "tribalid"
    establishment_id_from_nmds_worker: str = "tribalid_worker"
    establishment_type: str = "esttype"
    ethnicity: str = "ethnicity"
    ethnicity_change_date: str = "ethnicity_changedate"
    ethnicity_save_date: str = "ethnicity_savedate"
    flu_jab_2020: str = "flujab2020"
    flu_jab_2020_change_date: str = "flujab2020_changedate"
    flu_jab_2020_save_date: str = "flujab2020_savedate"
    gender: str = "gender"
    gender_change_date: str = "gender_changedate"
    gender_save_date: str = "gender_savedate"
    highest_qual_level: str = "listhiqualev"
    highest_qual_level_change_date: str = "listhiqualev_changedate"
    highest_qual_level_save_date: str = "listhiqualev_savedate"
    home_cssr_id: str = "homecssrid"
    home_local_authority_id: str = "homelauthid"
    home_parliamentary_constituency: str = "homeparliamentaryconstituency"
    home_region_id: str = "homeregionid"
    hourly_or_annual_pay: str = "salaryint"
    hourly_rate: str = "hrlyrate"
    import_date: str = "import_date"
    is_british: str = "isbritish"
    is_british_change_date: str = "isbritish_changedate"
    is_british_save_date: str = "isbritish_savedate"
    jd16_registered: str = "jd16registered"
    jd16_registered_change_date: str = "jd16registered_changedate"
    jd16_registered_save_date: str = "jd16registered_savedate"
    job_role_01_flag: str = "jr01flag"
    job_role_02_flag: str = "jr02flag"
    job_role_03_flag: str = "jr03flag"
    job_role_04_flag: str = "jr04flag"
    job_role_05_flag: str = "jr05flag"
    job_role_06_flag: str = "jr06flag"
    job_role_07_flag: str = "jr07flag"
    job_role_08_flag: str = "jr08flag"
    job_role_09_flag: str = "jr09flag"
    job_role_10_flag: str = "jr10flag"
    job_role_11_flag: str = "jr11flag"
    job_role_15_flag: str = "jr15flag"
    job_role_16_cat_change_date: str = "jr16cat_changedate"
    job_role_16_cat_save_date: str = "jr16cat_savedate"
    job_role_16_cat1: str = "jr16cat1"
    job_role_16_cat2: str = "jr16cat2"
    job_role_16_cat3: str = "jr16cat3"
    job_role_16_cat4: str = "jr16cat4"
    job_role_16_cat5: str = "jr16cat5"
    job_role_16_cat6: str = "jr16cat6"
    job_role_16_cat7: str = "jr16cat7"
    job_role_16_cat8: str = "jr16cat8"
    job_role_16_flag: str = "jr16flag"
    job_role_17_flag: str = "jr17flag"
    job_role_22_flag: str = "jr22flag"
    job_role_23_flag: str = "jr23flag"
    job_role_24_flag: str = "jr24flag"
    job_role_25_flag: str = "jr25flag"
    job_role_26_flag: str = "jr26flag"
    job_role_27_flag: str = "jr27flag"
    job_role_34_flag: str = "jr34flag"
    job_role_35_flag: str = "jr35flag"
    job_role_36_flag: str = "jr36flag"
    job_role_37_flag: str = "jr37flag"
    job_role_38_flag: str = "jr38flag"
    job_role_39_flag: str = "jr39flag"
    job_role_40_flag: str = "jr40flag"
    job_role_41_flag: str = "jr41flag"
    job_role_42_flag: str = "jr42flag"
    job_role_43_flag: str = "jr43flag"
    job_role_44_flag: str = "jr44flag"
    job_role_45_flag: str = "jr45flag"
    la_permission: str = "lapermission"
    level_non_social_care_qualification_held: str = "levelnonscqheld"
    level_non_social_care_qualification_held_change_date: str = "levelnonscqheld_changedate"
    level_non_social_care_qualification_held_save_date: str = "levelnonscqheld_savedate"
    level_social_care_qualification_held: str = "levelscqheld"
    level_social_care_qualification_held_change_date: str = "levelscqheld_changedate"
    level_social_care_qualification_held_save_date: str = "levelscqheld_savedate"
    local_authority_id: str = "lauthid"
    location_id: str = "locationid"
    main_job_role_id: str = "mainjrid"
    main_job_role_id_change_date: str = "mainjrid_changedate"
    main_job_role_id_save_date: str = "mainjrid_savedate"
    main_service_type_id: str = "mainstid"
    month: str = "month"
    nationality: str = "nationality"
    nmds_id: str = "nmdsid"
    non_social_care_qual_held: str = "nonscqheld"
    non_social_care_qual_held_change_date: str = "nonscqheld_changedate"
    non_social_care_qual_held_save_date: str = "nonscqheld_savedate"
    organisation_id: str = "orgid"
    parent_id: str = "parentid"
    parliamentary_constituency: str = "parliamentaryconstituency"
    pay_change_date: str = "pay_changedate"
    pay_save_date: str = "pay_savedate"
    period: str = "period"
    previous_main_job_role_id: str = "previous_mainjrid"
    previous_pay: str = "previous_pay"
    provider_id: str = "providerid"
    qual_01achq2: str = "ql01achq2"
    qual_01year2: str = "ql01year2"
    qual_02achq3: str = "ql02achq3"
    qual_02year3: str = "ql02year3"
    qual_03achq4: str = "ql03achq4"
    qual_03year4: str = "ql03year4"
    qual_04achq2: str = "ql04achq2"
    qual_04year2: str = "ql04year2"
    qual_05achq3: str = "ql05achq3"
    qual_05year3: str = "ql05year3"
    qual_06achq4: str = "ql06achq4"
    qual_06year4: str = "ql06year4"
    qual_08achq: str = "ql08achq"
    qual_08year: str = "ql08year"
    qual_09achq: str = "ql09achq"
    qual_09year: str = "ql09year"
    qual_100achq3: str = "ql100achq3"
    qual_100year3: str = "ql100year3"
    qual_101achq3: str = "ql101achq3"
    qual_101year3: str = "ql101year3"
    qual_102achq5: str = "ql102achq5"
    qual_102year5: str = "ql102year5"
    qual_103achq3: str = "ql103achq3"
    qual_103year3: str = "ql103year3"
    qual_104achq4: str = "ql104achq4"
    qual_104year4: str = "ql104year4"
    qual_105achq4: str = "ql105achq4"
    qual_105year4: str = "ql105year4"
    qual_107achq3: str = "ql107achq3"
    qual_107year3: str = "ql107year3"
    qual_108achq3: str = "ql108achq3"
    qual_108year3: str = "ql108year3"
    qual_109achq4: str = "ql109achq4"
    qual_109year4: str = "ql109year4"
    qual_10achq4: str = "ql10achq4"
    qual_10year4: str = "ql10year4"
    qual_110achq4: str = "ql110achq4"
    qual_110year4: str = "ql110year4"
    qual_111achq: str = "ql111achq"
    qual_111year: str = "ql111year"
    qual_112achq: str = "ql112achq"
    qual_112year: str = "ql112year"
    qual_113achq: str = "ql113achq"
    qual_113year: str = "ql113year"
    qual_114achq: str = "ql114achq"
    qual_114year: str = "ql114year"
    qual_115achq: str = "ql115achq"
    qual_115year: str = "ql115year"
    qual_116achq: str = "ql116achq"
    qual_116year: str = "ql116year"
    qual_117achq: str = "ql117achq"
    qual_117year: str = "ql117year"
    qual_118achq: str = "ql118achq"
    qual_118year: str = "ql118year"
    qual_119achq: str = "ql119achq"
    qual_119year: str = "ql119year"
    qual_120achq: str = "ql120achq"
    qual_120year: str = "ql120year"
    qual_121achq: str = "ql121achq"
    qual_121year: str = "ql121year"
    qual_122achq: str = "ql122achq"
    qual_122year: str = "ql122year"
    qual_123achq: str = "ql123achq"
    qual_123year: str = "ql123year"
    qual_124achq: str = "ql124achq"
    qual_124year: str = "ql124year"
    qual_125achq: str = "ql125achq"
    qual_125year: str = "ql125year"
    qual_126achq: str = "ql126achq"
    qual_126year: str = "ql126year"
    qual_127achq: str = "ql127achq"
    qual_127year: str = "ql127year"
    qual_128achq: str = "ql128achq"
    qual_128year: str = "ql128year"
    qual_129achq: str = "ql129achq"
    qual_129year: str = "ql129year"
    qual_12achq3: str = "ql12achq3"
    qual_12year3: str = "ql12year3"
    qual_130achq: str = "ql130achq"
    qual_130year: str = "ql130year"
    qual_131achq: str = "ql131achq"
    qual_131year: str = "ql131year"
    qual_132achq: str = "ql132achq"
    qual_132year: str = "ql132year"
    qual_133achq: str = "ql133achq"
    qual_133year: str = "ql133year"
    qual_134achq: str = "ql134achq"
    qual_134year: str = "ql134year"
    qual_135achq: str = "ql135achq"
    qual_135year: str = "ql135year"
    qual_136achq: str = "ql136achq"
    qual_136year: str = "ql136year"
    qual_137achq: str = "ql137achq"
    qual_137year: str = "ql137year"
    qual_138achq: str = "ql138achq"
    qual_138year: str = "ql138year"
    qual_139achq: str = "ql139achq"
    qual_139year: str = "ql139year"
    qual_13achq3: str = "ql13achq3"
    qual_13year3: str = "ql13year3"
    qual_140achq: str = "ql140achq"
    qual_140year: str = "ql140year"
    qual_141achq: str = "ql141achq"
    qual_141year: str = "ql141year"
    qual_142achq: str = "ql142achq"
    qual_142year: str = "ql142year"
    qual_143achq: str = "ql143achq"
    qual_143year: str = "ql143year"
    qual_144achq: str = "ql144achq"
    qual_144year: str = "ql144year"
    qual_14achq3: str = "ql14achq3"
    qual_14year3: str = "ql14year3"
    qual_15achq3: str = "ql15achq3"
    qual_15year3: str = "ql15year3"
    qual_16achq4: str = "ql16achq4"
    qual_16year4: str = "ql16year4"
    qual_17achq4: str = "ql17achq4"
    qual_17year4: str = "ql17year4"
    qual_18achq4: str = "ql18achq4"
    qual_18year4: str = "ql18year4"
    qual_19achq4: str = "ql19achq4"
    qual_19year4: str = "ql19year4"
    qual_20achq4: str = "ql20achq4"
    qual_20year4: str = "ql20year4"
    qual_22achq4: str = "ql22achq4"
    qual_22year4: str = "ql22year4"
    qual_25achq4: str = "ql25achq4"
    qual_25year4: str = "ql25year4"
    qual_26achq4: str = "ql26achq4"
    qual_26year4: str = "ql26year4"
    qual_27achq4: str = "ql27achq4"
    qual_27year4: str = "ql27year4"
    qual_28achq4: str = "ql28achq4"
    qual_28year4: str = "ql28year4"
    qual_301app: str = "ql301app"
    qual_301year: str = "ql301year"
    qual_302app: str = "ql302app"
    qual_302year: str = "ql302year"
    qual_303app: str = "ql303app"
    qual_303year: str = "ql303year"
    qual_304app: str = "ql304app"
    qual_304year: str = "ql304year"
    qual_305app: str = "ql305app"
    qual_305year: str = "ql305year"
    qual_306app: str = "ql306app"
    qual_306year: str = "ql306year"
    qual_307app: str = "ql307app"
    qual_307year: str = "ql307year"
    qual_308app: str = "ql308app"
    qual_308year: str = "ql308year"
    qual_309app: str = "ql309app"
    qual_309year: str = "ql309year"
    qual_310app: str = "ql310app"
    qual_310year: str = "ql310year"
    qual_311app: str = "ql311app"
    qual_311year: str = "ql311year"
    qual_312app: str = "ql312app"
    qual_312year: str = "ql312year"
    qual_313app: str = "ql313app"
    qual_313year: str = "ql313year"
    qual_32achq3: str = "ql32achq3"
    qual_32year3: str = "ql32year3"
    qual_33achq4: str = "ql33achq4"
    qual_33year4: str = "ql33year4"
    qual_34achqe: str = "ql34achqe"
    qual_34yeare: str = "ql34yeare"
    qual_35achq1: str = "ql35achq1"
    qual_35year1: str = "ql35year1"
    qual_36achq2: str = "ql36achq2"
    qual_36year2: str = "ql36year2"
    qual_37achq: str = "ql37achq"
    qual_37year: str = "ql37year"
    qual_38achq: str = "ql38achq"
    qual_38year: str = "ql38year"
    qual_39achq: str = "ql39achq"
    qual_39year: str = "ql39year"
    qual_41achq2: str = "ql41achq2"
    qual_41year2: str = "ql41year2"
    qual_42achq3: str = "ql42achq3"
    qual_42year3: str = "ql42year3"
    qual_48achq2: str = "ql48achq2"
    qual_48year2: str = "ql48year2"
    qual_49achq3: str = "ql49achq3"
    qual_49year3: str = "ql49year3"
    qual_50achq2: str = "ql50achq2"
    qual_50year2: str = "ql50year2"
    qual_51achq3: str = "ql51achq3"
    qual_51year3: str = "ql51year3"
    qual_52achq2: str = "ql52achq2"
    qual_52year2: str = "ql52year2"
    qual_53achq2: str = "ql53achq2"
    qual_53year2: str = "ql53year2"
    qual_54achq3: str = "ql54achq3"
    qual_54year3: str = "ql54year3"
    qual_55achq2: str = "ql55achq2"
    qual_55year2: str = "ql55year2"
    qual_56achq3: str = "ql56achq3"
    qual_56year3: str = "ql56year3"
    qual_57achq2: str = "ql57achq2"
    qual_57year2: str = "ql57year2"
    qual_58achq3: str = "ql58achq3"
    qual_58year3: str = "ql58year3"
    qual_62achq5: str = "ql62achq5"
    qual_62year5: str = "ql62year5"
    qual_63achq5: str = "ql63achq5"
    qual_63year5: str = "ql63year5"
    qual_64achq5: str = "ql64achq5"
    qual_64year5: str = "ql64year5"
    qual_67achq2: str = "ql67achq2"
    qual_67year2: str = "ql67year2"
    qual_68achq3: str = "ql68achq3"
    qual_68year3: str = "ql68year3"
    qual_72achq2: str = "ql72achq2"
    qual_72year2: str = "ql72year2"
    qual_73achq2: str = "ql73achq2"
    qual_73year2: str = "ql73year2"
    qual_74achq3: str = "ql74achq3"
    qual_74year3: str = "ql74year3"
    qual_76achq2: str = "ql76achq2"
    qual_76year2: str = "ql76year2"
    qual_77achq3: str = "ql77achq3"
    qual_77year3: str = "ql77year3"
    qual_82achq: str = "ql82achq"
    qual_82year: str = "ql82year"
    qual_83achq: str = "ql83achq"
    qual_83year: str = "ql83year"
    qual_84achq: str = "ql84achq"
    qual_84year: str = "ql84year"
    qual_85achq1: str = "ql85achq1"
    qual_85year1: str = "ql85year1"
    qual_86achq2: str = "ql86achq2"
    qual_86year2: str = "ql86year2"
    qual_87achq3: str = "ql87achq3"
    qual_87year3: str = "ql87year3"
    qual_88achq2: str = "ql88achq2"
    qual_88year2: str = "ql88year2"
    qual_89achq3: str = "ql89achq3"
    qual_89year3: str = "ql89year3"
    qual_90achq2: str = "ql90achq2"
    qual_90year2: str = "ql90year2"
    qual_91achq2: str = "ql91achq2"
    qual_91year2: str = "ql91year2"
    qual_92achq1: str = "ql92achq1"
    qual_92year1: str = "ql92year1"
    qual_93achq1: str = "ql93achq1"
    qual_93year1: str = "ql93year1"
    qual_94achq2: str = "ql94achq2"
    qual_94year2: str = "ql94year2"
    qual_95achq3: str = "ql95achq3"
    qual_95year3: str = "ql95year3"
    qual_96achq2: str = "ql96achq2"
    qual_96year2: str = "ql96year2"
    qual_98achq2: str = "ql98achq2"
    qual_98year2: str = "ql98year2"
    qual_99achq2: str = "ql99achq2"
    qual_99year2: str = "ql99year2"
    qual_ach_change_date: str = "qlach_changedate"
    qual_ach_save_date: str = "qlach_savedate"
    quals_acheived_flag: str = "listqualsachflag"
    quals_acheived_flag_change_date: str = "listqualsachflag_changedate"
    quals_acheived_flag_save_date: str = "listqualsachflag_savedate"
    region_id: str = "regionid"
    registration_type: str = "regtype"
    salary: str = "salary"
    save_date: str = "savedate"
    social_care_qual_held: str = "scqheld"
    social_care_qual_held_change_date: str = "scqheld_changedate"
    social_care_qual_held_save_date: str = "scqheld_savedate"
    source_of_recruitment: str = "scerec"
    source_of_recruitment_change_date: str = "scerec_changedate"
    source_of_recruitment_save_date: str = "scerec_savedate"
    start_age: str = "startage"
    start_date: str = "strtdate"
    start_date_change_date: str = "strtdate_changedate"
    start_date_save_date: str = "strtdate_savedate"
    start_in_sector: str = "startsec"
    start_in_sector_change_date: str = "startsec_changedate"
    start_in_sector_save_date: str = "startsec_savedate"
    training_01_accredited: str = "tr01ac"
    training_01_count: str = "tr01count"
    training_01_flag: str = "tr01flag"
    training_01_latest_date: str = "tr01latestdate"
    training_01_non_accredited: str = "tr01nac"
    training_01_unknown_accreditation: str = "tr01dn"
    training_02_accredited: str = "tr02ac"
    training_02_count: str = "tr02count"
    training_02_flag: str = "tr02flag"
    training_02_latest_date: str = "tr02latestdate"
    training_02_non_accredited: str = "tr02nac"
    training_02_unknown_accreditation: str = "tr02dn"
    training_05_accredited: str = "tr05ac"
    training_05_count: str = "tr05count"
    training_05_flag: str = "tr05flag"
    training_05_latest_date: str = "tr05latestdate"
    training_05_non_accredited: str = "tr05nac"
    training_05_unknown_accreditation: str = "tr05dn"
    training_06_accredited: str = "tr06ac"
    training_06_count: str = "tr06count"
    training_06_flag: str = "tr06flag"
    training_06_latest_date: str = "tr06latestdate"
    training_06_non_accredited: str = "tr06nac"
    training_06_unknown_accreditation: str = "tr06dn"
    training_07_accredited: str = "tr07ac"
    training_07_count: str = "tr07count"
    training_07_flag: str = "tr07flag"
    training_07_latest_date: str = "tr07latestdate"
    training_07_non_accredited: str = "tr07nac"
    training_07_unknown_accreditation: str = "tr07dn"
    training_08_accredited: str = "tr08ac"
    training_08_count: str = "tr08count"
    training_08_flag: str = "tr08flag"
    training_08_latest_date: str = "tr08latestdate"
    training_08_non_accredited: str = "tr08nac"
    training_08_unknown_accreditation: str = "tr08dn"
    training_09_accredited: str = "tr09ac"
    training_09_count: str = "tr09count"
    training_09_flag: str = "tr09flag"
    training_09_latest_date: str = "tr09latestdate"
    training_09_non_accredited: str = "tr09nac"
    training_09_unknown_accreditation: str = "tr09dn"
    training_10_accredited: str = "tr10ac"
    training_10_count: str = "tr10count"
    training_10_flag: str = "tr10flag"
    training_10_latest_date: str = "tr10latestdate"
    training_10_non_accredited: str = "tr10nac"
    training_10_unknown_accreditation: str = "tr10dn"
    training_11_accredited: str = "tr11ac"
    training_11_count: str = "tr11count"
    training_11_flag: str = "tr11flag"
    training_11_latest_date: str = "tr11latestdate"
    training_11_non_accredited: str = "tr11nac"
    training_11_unknown_accreditation: str = "tr11dn"
    training_12_accredited: str = "tr12ac"
    training_12_count: str = "tr12count"
    training_12_flag: str = "tr12flag"
    training_12_latest_date: str = "tr12latestdate"
    training_12_non_accredited: str = "tr12nac"
    training_12_unknown_accreditation: str = "tr12dn"
    training_13_accredited: str = "tr13ac"
    training_13_count: str = "tr13count"
    training_13_flag: str = "tr13flag"
    training_13_latest_date: str = "tr13latestdate"
    training_13_non_accredited: str = "tr13nac"
    training_13_unknown_accreditation: str = "tr13dn"
    training_14_accredited: str = "tr14ac"
    training_14_count: str = "tr14count"
    training_14_flag: str = "tr14flag"
    training_14_latest_date: str = "tr14latestdate"
    training_14_non_accredited: str = "tr14nac"
    training_14_unknown_accreditation: str = "tr14dn"
    training_15_accredited: str = "tr15ac"
    training_15_count: str = "tr15count"
    training_15_flag: str = "tr15flag"
    training_15_latest_date: str = "tr15latestdate"
    training_15_non_accredited: str = "tr15nac"
    training_15_unknown_accreditation: str = "tr15dn"
    training_16_accredited: str = "tr16ac"
    training_16_count: str = "tr16count"
    training_16_flag: str = "tr16flag"
    training_16_latest_date: str = "tr16latestdate"
    training_16_non_accredited: str = "tr16nac"
    training_16_unknown_accreditation: str = "tr16dn"
    training_17_accredited: str = "tr17ac"
    training_17_count: str = "tr17count"
    training_17_flag: str = "tr17flag"
    training_17_latest_date: str = "tr17latestdate"
    training_17_non_accredited: str = "tr17nac"
    training_17_unknown_accreditation: str = "tr17dn"
    training_18_accredited: str = "tr18ac"
    training_18_count: str = "tr18count"
    training_18_flag: str = "tr18flag"
    training_18_latest_date: str = "tr18latestdate"
    training_18_non_accredited: str = "tr18nac"
    training_18_unknown_accreditation: str = "tr18dn"
    training_19_accredited: str = "tr19ac"
    training_19_count: str = "tr19count"
    training_19_flag: str = "tr19flag"
    training_19_latest_date: str = "tr19latestdate"
    training_19_non_accredited: str = "tr19nac"
    training_19_unknown_accreditation: str = "tr19dn"
    training_20_accredited: str = "tr20ac"
    training_20_count: str = "tr20count"
    training_20_flag: str = "tr20flag"
    training_20_latest_date: str = "tr20latestdate"
    training_20_non_accredited: str = "tr20nac"
    training_20_unknown_accreditation: str = "tr20dn"
    training_21_accredited: str = "tr21ac"
    training_21_count: str = "tr21count"
    training_21_flag: str = "tr21flag"
    training_21_latest_date: str = "tr21latestdate"
    training_21_non_accredited: str = "tr21nac"
    training_21_unknown_accreditation: str = "tr21dn"
    training_22_accredited: str = "tr22ac"
    training_22_count: str = "tr22count"
    training_22_flag: str = "tr22flag"
    training_22_latest_date: str = "tr22latestdate"
    training_22_non_accredited: str = "tr22nac"
    training_22_unknown_accreditation: str = "tr22dn"
    training_23_accredited: str = "tr23ac"
    training_23_count: str = "tr23count"
    training_23_flag: str = "tr23flag"
    training_23_latest_date: str = "tr23latestdate"
    training_23_non_accredited: str = "tr23nac"
    training_23_unknown_accreditation: str = "tr23dn"
    training_25_accredited: str = "tr25ac"
    training_25_count: str = "tr25count"
    training_25_flag: str = "tr25flag"
    training_25_latest_date: str = "tr25latestdate"
    training_25_non_accredited: str = "tr25nac"
    training_25_unknown_accreditation: str = "tr25dn"
    training_26_accredited: str = "tr26ac"
    training_26_count: str = "tr26count"
    training_26_flag: str = "tr26flag"
    training_26_latest_date: str = "tr26latestdate"
    training_26_non_accredited: str = "tr26nac"
    training_26_unknown_accreditation: str = "tr26dn"
    training_27_accredited: str = "tr27ac"
    training_27_count: str = "tr27count"
    training_27_flag: str = "tr27flag"
    training_27_latest_date: str = "tr27latestdate"
    training_27_non_accredited: str = "tr27nac"
    training_27_unknown_accreditation: str = "tr27dn"
    training_28_accredited: str = "tr28ac"
    training_28_count: str = "tr28count"
    training_28_flag: str = "tr28flag"
    training_28_latest_date: str = "tr28latestdate"
    training_28_non_accredited: str = "tr28nac"
    training_28_unknown_accreditation: str = "tr28dn"
    training_29_accredited: str = "tr29ac"
    training_29_count: str = "tr29count"
    training_29_flag: str = "tr29flag"
    training_29_latest_date: str = "tr29latestdate"
    training_29_non_accredited: str = "tr29nac"
    training_29_unknown_accreditation: str = "tr29dn"
    training_30_accredited: str = "tr30ac"
    training_30_count: str = "tr30count"
    training_30_flag: str = "tr30flag"
    training_30_latest_date: str = "tr30latestdate"
    training_30_non_accredited: str = "tr30nac"
    training_30_unknown_accreditation: str = "tr30dn"
    training_31_accredited: str = "tr31ac"
    training_31_count: str = "tr31count"
    training_31_flag: str = "tr31flag"
    training_31_latest_date: str = "tr31latestdate"
    training_31_non_accredited: str = "tr31nac"
    training_31_unknown_accreditation: str = "tr31dn"
    training_32_accredited: str = "tr32ac"
    training_32_count: str = "tr32count"
    training_32_flag: str = "tr32flag"
    training_32_latest_date: str = "tr32latestdate"
    training_32_non_accredited: str = "tr32nac"
    training_32_unknown_accreditation: str = "tr32dn"
    training_33_accredited: str = "tr33ac"
    training_33_count: str = "tr33count"
    training_33_flag: str = "tr33flag"
    training_33_latest_date: str = "tr33latestdate"
    training_33_non_accredited: str = "tr33nac"
    training_33_unknown_accreditation: str = "tr33dn"
    training_34_accredited: str = "tr34ac"
    training_34_count: str = "tr34count"
    training_34_flag: str = "tr34flag"
    training_34_latest_date: str = "tr34latestdate"
    training_34_non_accredited: str = "tr34nac"
    training_34_unknown_accreditation: str = "tr34dn"
    training_35_accredited: str = "tr35ac"
    training_35_count: str = "tr35count"
    training_35_flag: str = "tr35flag"
    training_35_latest_date: str = "tr35latestdate"
    training_35_non_accredited: str = "tr35nac"
    training_35_unknown_accreditation: str = "tr35dn"
    training_36_accredited: str = "tr36ac"
    training_36_count: str = "tr36count"
    training_36_flag: str = "tr36flag"
    training_36_latest_date: str = "tr36latestdate"
    training_36_non_accredited: str = "tr36nac"
    training_36_unknown_accreditation: str = "tr36dn"
    training_37_accredited: str = "tr37ac"
    training_37_count: str = "tr37count"
    training_37_flag: str = "tr37flag"
    training_37_latest_date: str = "tr37latestdate"
    training_37_non_accredited: str = "tr37nac"
    training_37_unknown_accreditation: str = "tr37dn"
    training_38_accredited: str = "tr38ac"
    training_38_count: str = "tr38count"
    training_38_flag: str = "tr38flag"
    training_38_latest_date: str = "tr38latestdate"
    training_38_non_accredited: str = "tr38nac"
    training_38_unknown_accreditation: str = "tr38dn"
    training_39_accredited: str = "tr39ac"
    training_39_count: str = "tr39count"
    training_39_flag: str = "tr39flag"
    training_39_latest_date: str = "tr39latestdate"
    training_39_non_accredited: str = "tr39nac"
    training_39_unknown_accreditation: str = "tr39dn"
    training_40_accredited: str = "tr40ac"
    training_40_count: str = "tr40count"
    training_40_flag: str = "tr40flag"
    training_40_latest_date: str = "tr40latestdate"
    training_40_non_accredited: str = "tr40nac"
    training_40_unknown_accreditation: str = "tr40dn"
    training_41_accredited: str = "tr41ac"
    training_41_count: str = "tr41count"
    training_41_flag: str = "tr41flag"
    training_41_latest_date: str = "tr41latestdate"
    training_41_non_accredited: str = "tr41nac"
    training_41_unknown_accreditation: str = "tr41dn"
    training_42_accredited: str = "tr42ac"
    training_42_count: str = "tr42count"
    training_42_flag: str = "tr42flag"
    training_42_latest_date: str = "tr42latestdate"
    training_42_non_accredited: str = "tr42nac"
    training_42_unknown_accreditation: str = "tr42dn"
    training_43_accredited: str = "tr43ac"
    training_43_count: str = "tr43count"
    training_43_flag: str = "tr43flag"
    training_43_latest_date: str = "tr43latestdate"
    training_43_non_accredited: str = "tr43nac"
    training_43_unknown_accreditation: str = "tr43dn"
    training_ain_change_date: str = "train_changedate"
    training_ain_flag: str = "trainflag"
    training_ain_save_date: str = "train_savedate"
    updated_date: str = "updateddate"
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
    worker_global_id: str = "wrkglbid"
    worker_id: str = "workerid"
    workplace_status: str = "wkplacestat"
    year: str = "year"
    year_of_entry: str = "yearofentry"
    year_of_entry_change_date: str = "yearofentry_changedate"
    year_of_entry_save_date: str = "yearofentry_savedate"
    zero_average_hours_change_date: str = "zero_averagehours_changedate"
    zero_average_hours_save_date: str = "zero_averagehours_savedate"
    zero_hours: str = "zerohours"
    zero_hours_change_date: str = "zerohours_changedate"
    zero_hours_save_date: str = "zerohours_savedate"

