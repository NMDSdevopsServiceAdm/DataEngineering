import polars as pl

from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CleanedColNames,
)

POLARS_CLEANED_LOCATIONS_SCHEMA = pl.Schema(
    [
        (CleanedColNames.contemporary_ons_import_date, pl.Date()),
        (CleanedColNames.postcode_cleaned, pl.String()),
        (CleanedColNames.cqc_location_import_date, pl.Date()),
        (CleanedColNames.location_id, pl.String()),
        (CleanedColNames.provider_id, pl.String()),
        (CleanedColNames.name, pl.String()),
        (CleanedColNames.postal_address_line1, pl.String()),
        (CleanedColNames.postal_code, pl.String()),
        (CleanedColNames.registration_status, pl.String()),
        (CleanedColNames.registration_date, pl.Date()),
        (CleanedColNames.deregistration_date, pl.Date()),
        (CleanedColNames.type, pl.String()),
        (
            CleanedColNames.relationships,
            pl.List(
                pl.Struct(
                    {
                        CleanedColNames.related_location_id: pl.String(),
                        CleanedColNames.related_location_name: pl.String(),
                        CleanedColNames.type: pl.String(),
                        CleanedColNames.reason: pl.String(),
                    }
                )
            ),
        ),
        (CleanedColNames.care_home, pl.String()),
        (CleanedColNames.number_of_beds, pl.Int32()),
        (CleanedColNames.dormancy, pl.String()),
        (
            CleanedColNames.gac_service_types,
            pl.List(
                pl.Struct(
                    {
                        CleanedColNames.name: pl.String(),
                        CleanedColNames.description: pl.String(),
                    }
                )
            ),
        ),
        (
            CleanedColNames.regulated_activities,
            pl.List(
                pl.Struct(
                    {
                        CleanedColNames.name: pl.String(),
                        CleanedColNames.code: pl.String(),
                        CleanedColNames.contacts: pl.List(
                            pl.Struct(
                                {
                                    CleanedColNames.person_family_name: pl.String(),
                                    CleanedColNames.person_given_name: pl.String(),
                                    CleanedColNames.person_roles: pl.List(pl.String()),
                                    CleanedColNames.person_title: pl.String(),
                                }
                            )
                        ),
                    }
                )
            ),
        ),
        (
            CleanedColNames.specialisms,
            pl.List(pl.Struct({CleanedColNames.name: pl.String()})),
        ),
        (CleanedColNames.imputed_registration_date, pl.Date()),
        (CleanedColNames.cqc_sector, pl.String()),
        (
            CleanedColNames.imputed_relationships,
            pl.List(
                pl.Struct(
                    {
                        CleanedColNames.related_location_id: pl.String(),
                        CleanedColNames.related_location_name: pl.String(),
                        CleanedColNames.type: pl.String(),
                        CleanedColNames.reason: pl.String(),
                    }
                )
            ),
        ),
        (
            CleanedColNames.imputed_gac_service_types,
            pl.List(
                pl.Struct(
                    {
                        CleanedColNames.name: pl.String(),
                        CleanedColNames.description: pl.String(),
                    }
                )
            ),
        ),
        (
            CleanedColNames.imputed_regulated_activities,
            pl.List(
                pl.Struct(
                    {
                        CleanedColNames.name: pl.String(),
                        CleanedColNames.code: pl.String(),
                        CleanedColNames.contacts: pl.List(
                            pl.Struct(
                                {
                                    CleanedColNames.person_family_name: pl.String(),
                                    CleanedColNames.person_given_name: pl.String(),
                                    CleanedColNames.person_roles: pl.List(pl.String()),
                                    CleanedColNames.person_title: pl.String(),
                                }
                            )
                        ),
                    }
                )
            ),
        ),
        (
            CleanedColNames.imputed_specialisms,
            pl.List(pl.Struct({CleanedColNames.name: pl.String()})),
        ),
        (CleanedColNames.services_offered, pl.List(pl.String())),
        (CleanedColNames.specialisms_offered, pl.List(pl.String())),
        (CleanedColNames.specialist_generalist_other_dementia, pl.String()),
        (CleanedColNames.specialist_generalist_other_lda, pl.String()),
        (CleanedColNames.specialist_generalist_other_mh, pl.String()),
        (CleanedColNames.primary_service_type, pl.String()),
        (CleanedColNames.registered_manager_names, pl.List(pl.String())),
        (CleanedColNames.related_location, pl.String()),
        (CleanedColNames.contemporary_cssr, pl.String()),
        (CleanedColNames.contemporary_region, pl.String()),
        (CleanedColNames.contemporary_sub_icb, pl.String()),
        (CleanedColNames.contemporary_icb, pl.String()),
        (CleanedColNames.contemporary_icb_region, pl.String()),
        (CleanedColNames.contemporary_ccg, pl.String()),
        (CleanedColNames.contemporary_latitude, pl.String()),
        (CleanedColNames.contemporary_longitude, pl.String()),
        (CleanedColNames.contemporary_imd_score, pl.String()),
        (CleanedColNames.contemporary_lsoa11, pl.String()),
        (CleanedColNames.contemporary_msoa11, pl.String()),
        (CleanedColNames.contemporary_rural_urban_ind_11, pl.String()),
        (CleanedColNames.contemporary_lsoa21, pl.String()),
        (CleanedColNames.contemporary_msoa21, pl.String()),
        (CleanedColNames.contemporary_constituency, pl.String()),
        (CleanedColNames.current_ons_import_date, pl.Date()),
        (CleanedColNames.current_cssr, pl.String()),
        (CleanedColNames.current_region, pl.String()),
        (CleanedColNames.current_sub_icb, pl.String()),
        (CleanedColNames.current_icb, pl.String()),
        (CleanedColNames.current_icb_region, pl.String()),
        (CleanedColNames.current_ccg, pl.String()),
        (CleanedColNames.current_latitude, pl.String()),
        (CleanedColNames.current_longitude, pl.String()),
        (CleanedColNames.current_imd_score, pl.String()),
        (CleanedColNames.current_lsoa11, pl.String()),
        (CleanedColNames.current_msoa11, pl.String()),
        (CleanedColNames.current_rural_urban_ind_11, pl.String()),
        (CleanedColNames.current_lsoa21, pl.String()),
        (CleanedColNames.current_msoa21, pl.String()),
        (CleanedColNames.current_constituency, pl.String()),
        (CleanedColNames.postcode_truncated, pl.String()),
    ]
)
