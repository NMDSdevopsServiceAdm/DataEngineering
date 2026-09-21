from dataclasses import dataclass

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.cleaned_data_files.cqc_provider_cleaned import (
    CqcProviderCleanedColumns as CQCPClean,
)
from utils.column_names.coverage_columns import CoverageColumns
from utils.column_names.cqc_ratings_columns import CQCRatingsColumns as CQCRatings
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_names.raw_data_files.ascwds_workplace_columns import (
    AscwdsWorkplaceColumns as AWP,
)
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_names.reconciliation_columns import (
    ReconciliationColumns as ReconColumn,
)


@dataclass
class ReconciliationUtilsSchema:
    parents_or_singles_and_subs_schema = StructType(
        [
            StructField(AWPClean.establishment_id, StringType(), True),
            StructField(AWPClean.is_parent, StringType(), True),
            StructField(AWPClean.parent_permission, StringType(), True),
        ]
    )
    expected_parents_or_singles_and_subs_schema = StructType(
        [
            *parents_or_singles_and_subs_schema,
            StructField(ReconColumn.parents_or_singles_and_subs, StringType(), True),
        ]
    )


@dataclass
class MergeCoverageData:
    clean_cqc_location_for_merge_schema = StructType(
        [
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(CQCLClean.location_id, StringType(), True),
            StructField(CQCLClean.name, StringType(), True),
            StructField(CQCLClean.postal_code, StringType(), True),
            StructField(CQCLClean.cqc_sector, StringType(), True),
            StructField(CQCLClean.care_home, StringType(), True),
            StructField(CQCLClean.number_of_beds, IntegerType(), True),
        ]
    )

    clean_ascwds_workplace_for_merge_schema = StructType(
        [
            StructField(AWPClean.ascwds_workplace_import_date, DateType(), True),
            StructField(AWPClean.location_id, StringType(), True),
            StructField(AWPClean.master_update_date, DateType(), True),
            StructField(AWPClean.establishment_id, StringType(), True),
            StructField(AWPClean.total_staff, IntegerType(), True),
            StructField(AWPClean.workplace_last_active_date, DateType(), True),
            StructField(AWPClean.purge_date, DateType(), True),
        ]
    )

    expected_cqc_and_ascwds_merged_schema = StructType(
        [
            StructField(CQCLClean.location_id, StringType(), True),
            StructField(AWPClean.ascwds_workplace_import_date, DateType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(CQCLClean.name, StringType(), True),
            StructField(CQCLClean.postal_code, StringType(), True),
            StructField(CQCLClean.cqc_sector, StringType(), True),
            StructField(CQCLClean.care_home, StringType(), True),
            StructField(CQCLClean.number_of_beds, IntegerType(), True),
            StructField(AWPClean.master_update_date, DateType(), True),
            StructField(AWPClean.establishment_id, StringType(), True),
            StructField(AWPClean.total_staff, IntegerType(), True),
            StructField(AWPClean.workplace_last_active_date, DateType(), True),
            StructField(AWPClean.purge_date, DateType(), True),
        ]
    )

    sample_in_ascwds_schema = StructType(
        [
            StructField(AWPClean.establishment_id, StringType(), True),
            StructField(AWPClean.removed_by_purge_date_filter, BooleanType(), True),
        ]
    )

    expected_in_ascwds_schema = StructType(
        [
            *sample_in_ascwds_schema,
            StructField(CoverageColumns.in_ascwds, IntegerType(), True),
        ]
    )

    sample_cqc_locations_schema = StructType(
        [StructField(AWPClean.location_id, StringType(), True)]
    )

    sample_cqc_ratings_for_merge_schema = StructType(
        [
            StructField(AWPClean.location_id, StringType(), True),
            StructField(CQCRatings.date, StringType(), True),
            StructField(CQCRatings.overall_rating, StringType(), True),
            StructField(CQCRatings.latest_rating_flag, IntegerType(), True),
            StructField(CQCRatings.current_or_historic, StringType(), True),
        ]
    )

    expected_cqc_locations_and_latest_cqc_rating_schema = StructType(
        [
            *sample_cqc_locations_schema,
            StructField(CQCRatings.date, StringType(), True),
            StructField(CQCRatings.overall_rating, StringType(), True),
        ]
    )
    sample_cqc_providers_for_merge_schema = StructType(
        [
            StructField(CQCPClean.provider_id, StringType(), True),
            StructField(CQCPClean.name, StringType(), True),
            StructField(CQCPClean.cqc_provider_import_date, DateType(), True),
        ]
    )
    sample_merged_coverage_schema = StructType(
        [
            StructField(CQCLClean.location_id, StringType(), True),
            StructField(CQCPClean.provider_id, StringType(), True),
        ]
    )
    expected_merged_covergae_and_provider_name_joined_schema = StructType(
        [
            *sample_merged_coverage_schema,
            StructField(CQCLClean.provider_name, StringType(), True),
        ]
    )


@dataclass
class ValidateMergedCoverageData:
    cqc_locations_schema = StructType(
        [
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(CQCLClean.location_id, StringType(), True),
            StructField(CQCLClean.name, StringType(), True),
            StructField(CQCLClean.postal_code, StringType(), True),
            StructField(CQCLClean.care_home, StringType(), True),
            StructField(CQCLClean.number_of_beds, IntegerType(), True),
        ]
    )
    merged_coverage_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.ascwds_workplace_import_date, DateType(), True),
            StructField(IndCQC.name, StringType(), True),
            StructField(CQCLClean.postal_code, StringType(), True),
            StructField(IndCQC.care_home, StringType(), True),
        ]
    )

    calculate_expected_size_schema = StructType(
        [
            StructField(CQCLClean.location_id, StringType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(CQCLClean.name, StringType(), True),
            StructField(CQCLClean.postal_code, StringType(), True),
            StructField(CQCLClean.care_home, StringType(), True),
        ]
    )


@dataclass
class FlattenCQCRatings:
    test_cqc_locations_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
            StructField(CQCL.type, StringType(), True),
            StructField(Keys.import_date, StringType(), False),
            StructField(Keys.year, StringType(), False),
            StructField(Keys.month, StringType(), False),
            StructField(Keys.day, StringType(), False),
            StructField(
                CQCL.current_ratings,
                StructType(
                    [
                        StructField(
                            CQCL.overall,
                            StructType(
                                [
                                    StructField(
                                        CQCL.organisation_id, StringType(), True
                                    ),
                                    StructField(CQCL.rating, StringType(), True),
                                    StructField(CQCL.report_date, StringType(), True),
                                    StructField(
                                        CQCL.report_link_id, StringType(), True
                                    ),
                                    StructField(
                                        CQCL.use_of_resources,
                                        StructType(
                                            [
                                                StructField(
                                                    CQCL.organisation_id,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    CQCL.summary, StringType(), True
                                                ),
                                                StructField(
                                                    CQCL.use_of_resources_rating,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    CQCL.combined_quality_summary,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    CQCL.combined_quality_rating,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    CQCL.report_date,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    CQCL.report_link_id,
                                                    StringType(),
                                                    True,
                                                ),
                                            ]
                                        ),
                                        True,
                                    ),
                                    StructField(
                                        CQCL.key_question_ratings,
                                        ArrayType(
                                            StructType(
                                                [
                                                    StructField(
                                                        CQCL.name, StringType(), True
                                                    ),
                                                    StructField(
                                                        CQCL.rating,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.report_date,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.organisation_id,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.report_link_id,
                                                        StringType(),
                                                        True,
                                                    ),
                                                ]
                                            ),
                                            True,
                                        ),
                                        True,
                                    ),
                                ]
                            ),
                            True,
                        ),
                        StructField(
                            CQCL.service_ratings,
                            ArrayType(
                                StructType(
                                    [
                                        StructField(CQCL.name, StringType(), True),
                                        StructField(CQCL.rating, StringType(), True),
                                        StructField(
                                            CQCL.report_date, StringType(), True
                                        ),
                                        StructField(
                                            CQCL.organisation_id, StringType(), True
                                        ),
                                        StructField(
                                            CQCL.report_link_id, StringType(), True
                                        ),
                                        StructField(
                                            CQCL.key_question_ratings,
                                            ArrayType(
                                                StructType(
                                                    [
                                                        StructField(
                                                            CQCL.name,
                                                            StringType(),
                                                            True,
                                                        ),
                                                        StructField(
                                                            CQCL.rating,
                                                            StringType(),
                                                            True,
                                                        ),
                                                    ]
                                                ),
                                                True,
                                            ),
                                            True,
                                        ),
                                    ]
                                ),
                                True,
                            ),
                            True,
                        ),
                    ]
                ),
                True,
            ),
            StructField(
                CQCL.historic_ratings,
                ArrayType(
                    StructType(
                        [
                            StructField(CQCL.report_date, StringType(), True),
                            StructField(CQCL.report_link_id, StringType(), True),
                            StructField(CQCL.organisation_id, StringType(), True),
                            StructField(
                                CQCL.service_ratings,
                                ArrayType(
                                    StructType(
                                        [
                                            StructField(CQCL.name, StringType(), True),
                                            StructField(
                                                CQCL.rating, StringType(), True
                                            ),
                                            StructField(
                                                CQCL.key_question_ratings,
                                                ArrayType(
                                                    StructType(
                                                        [
                                                            StructField(
                                                                CQCL.name,
                                                                StringType(),
                                                                True,
                                                            ),
                                                            StructField(
                                                                CQCL.rating,
                                                                StringType(),
                                                                True,
                                                            ),
                                                        ]
                                                    ),
                                                    True,
                                                ),
                                                True,
                                            ),
                                        ]
                                    ),
                                    True,
                                ),
                                True,
                            ),
                            StructField(
                                CQCL.overall,
                                StructType(
                                    [
                                        StructField(CQCL.rating, StringType(), True),
                                        StructField(
                                            CQCL.use_of_resources,
                                            StructType(
                                                [
                                                    StructField(
                                                        CQCL.combined_quality_rating,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.combined_quality_summary,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.use_of_resources_rating,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.use_of_resources_summary,
                                                        StringType(),
                                                        True,
                                                    ),
                                                ]
                                            ),
                                            True,
                                        ),
                                        StructField(
                                            CQCL.key_question_ratings,
                                            ArrayType(
                                                StructType(
                                                    [
                                                        StructField(
                                                            CQCL.name,
                                                            StringType(),
                                                            True,
                                                        ),
                                                        StructField(
                                                            CQCL.rating,
                                                            StringType(),
                                                            True,
                                                        ),
                                                    ]
                                                ),
                                                True,
                                            ),
                                            True,
                                        ),
                                    ]
                                ),
                                True,
                            ),
                        ]
                    ),
                    True,
                ),
                True,
            ),
            StructField(
                CQCL.assessment,
                ArrayType(
                    StructType(
                        [
                            StructField(
                                CQCL.assessment_plan_published_datetime,
                                StringType(),
                                True,
                            ),
                            StructField(
                                CQCL.ratings,
                                StructType(
                                    [
                                        StructField(
                                            CQCL.overall,
                                            ArrayType(
                                                StructType(
                                                    [
                                                        StructField(
                                                            CQCL.rating,
                                                            StringType(),
                                                            True,
                                                        ),
                                                        StructField(
                                                            CQCL.status,
                                                            StringType(),
                                                            True,
                                                        ),
                                                        StructField(
                                                            CQCL.key_question_ratings,
                                                            ArrayType(
                                                                StructType(
                                                                    [
                                                                        StructField(
                                                                            CQCL.name,
                                                                            StringType(),
                                                                            True,
                                                                        ),
                                                                        StructField(
                                                                            CQCL.rating,
                                                                            StringType(),
                                                                            True,
                                                                        ),
                                                                        StructField(
                                                                            CQCL.status,
                                                                            StringType(),
                                                                            True,
                                                                        ),
                                                                    ]
                                                                )
                                                            ),
                                                            True,
                                                        ),
                                                    ]
                                                )
                                            ),
                                            True,
                                        ),
                                        StructField(
                                            CQCL.asg_ratings,
                                            ArrayType(
                                                StructType(
                                                    [
                                                        StructField(
                                                            CQCL.assessment_plan_id,
                                                            StringType(),
                                                            True,
                                                        ),
                                                        StructField(
                                                            CQCL.title,
                                                            StringType(),
                                                            True,
                                                        ),
                                                        StructField(
                                                            CQCL.assessment_date,
                                                            StringType(),
                                                            True,
                                                        ),
                                                        StructField(
                                                            CQCL.assessment_plan_status,
                                                            StringType(),
                                                            True,
                                                        ),
                                                        StructField(
                                                            CQCL.name,
                                                            StringType(),
                                                            True,
                                                        ),
                                                        StructField(
                                                            CQCL.rating,
                                                            StringType(),
                                                            True,
                                                        ),
                                                        StructField(
                                                            CQCL.status,
                                                            StringType(),
                                                            True,
                                                        ),
                                                        StructField(
                                                            CQCL.key_question_ratings,
                                                            ArrayType(
                                                                StructType(
                                                                    [
                                                                        StructField(
                                                                            CQCL.name,
                                                                            StringType(),
                                                                            True,
                                                                        ),
                                                                        StructField(
                                                                            CQCL.rating,
                                                                            StringType(),
                                                                            True,
                                                                        ),
                                                                        StructField(
                                                                            CQCL.status,
                                                                            StringType(),
                                                                            True,
                                                                        ),
                                                                        StructField(
                                                                            CQCL.percentage_score,
                                                                            StringType(),
                                                                            True,
                                                                        ),
                                                                    ]
                                                                )
                                                            ),
                                                            True,
                                                        ),
                                                    ]
                                                )
                                            ),
                                            True,
                                        ),
                                    ]
                                ),
                                True,
                            ),
                        ]
                    ),
                    True,
                ),
                True,
            ),
        ]
    )
    test_ascwds_workplace_schema = StructType(
        [
            StructField(AWP.location_id, StringType(), True),
            StructField(AWP.establishment_id, StringType(), True),
            StructField(Keys.import_date, StringType(), False),
            StructField(Keys.year, StringType(), False),
            StructField(Keys.month, StringType(), False),
            StructField(Keys.day, StringType(), False),
        ]
    )
    filter_to_first_import_of_most_recent_month_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(Keys.import_date, StringType(), False),
            StructField(Keys.year, StringType(), False),
            StructField(Keys.month, StringType(), False),
            StructField(Keys.day, StringType(), False),
        ]
    )
    flatten_current_ratings_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
            StructField(
                CQCL.current_ratings,
                StructType(
                    [
                        StructField(
                            CQCL.overall,
                            StructType(
                                [
                                    StructField(
                                        CQCL.organisation_id, StringType(), True
                                    ),
                                    StructField(CQCL.rating, StringType(), True),
                                    StructField(CQCL.report_date, StringType(), True),
                                    StructField(
                                        CQCL.report_link_id, StringType(), True
                                    ),
                                    StructField(
                                        CQCL.use_of_resources,
                                        StructType(
                                            [
                                                StructField(
                                                    CQCL.organisation_id,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    CQCL.summary, StringType(), True
                                                ),
                                                StructField(
                                                    CQCL.use_of_resources_rating,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    CQCL.combined_quality_summary,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    CQCL.combined_quality_rating,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    CQCL.report_date,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    CQCL.report_link_id,
                                                    StringType(),
                                                    True,
                                                ),
                                            ]
                                        ),
                                        True,
                                    ),
                                    StructField(
                                        CQCL.key_question_ratings,
                                        ArrayType(
                                            StructType(
                                                [
                                                    StructField(
                                                        CQCL.name, StringType(), True
                                                    ),
                                                    StructField(
                                                        CQCL.rating,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.report_date,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.organisation_id,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.report_link_id,
                                                        StringType(),
                                                        True,
                                                    ),
                                                ]
                                            ),
                                            True,
                                        ),
                                        True,
                                    ),
                                ]
                            ),
                            True,
                        ),
                        StructField(
                            CQCL.service_ratings,
                            ArrayType(
                                StructType(
                                    [
                                        StructField(CQCL.name, StringType(), True),
                                        StructField(CQCL.rating, StringType(), True),
                                        StructField(
                                            CQCL.report_date, StringType(), True
                                        ),
                                        StructField(
                                            CQCL.organisation_id, StringType(), True
                                        ),
                                        StructField(
                                            CQCL.report_link_id, StringType(), True
                                        ),
                                        StructField(
                                            CQCL.key_question_ratings,
                                            ArrayType(
                                                StructType(
                                                    [
                                                        StructField(
                                                            CQCL.name,
                                                            StringType(),
                                                            True,
                                                        ),
                                                        StructField(
                                                            CQCL.rating,
                                                            StringType(),
                                                            True,
                                                        ),
                                                    ]
                                                ),
                                                True,
                                            ),
                                            True,
                                        ),
                                    ]
                                ),
                                True,
                            ),
                            True,
                        ),
                    ]
                ),
                True,
            ),
        ]
    )

    flatten_historic_ratings_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
            StructField(
                CQCL.historic_ratings,
                ArrayType(
                    StructType(
                        [
                            StructField(CQCL.report_date, StringType(), True),
                            StructField(CQCL.report_link_id, StringType(), True),
                            StructField(CQCL.organisation_id, StringType(), True),
                            StructField(
                                CQCL.service_ratings,
                                ArrayType(
                                    StructType(
                                        [
                                            StructField(CQCL.name, StringType(), True),
                                            StructField(
                                                CQCL.rating, StringType(), True
                                            ),
                                            StructField(
                                                CQCL.key_question_ratings,
                                                ArrayType(
                                                    StructType(
                                                        [
                                                            StructField(
                                                                CQCL.name,
                                                                StringType(),
                                                                True,
                                                            ),
                                                            StructField(
                                                                CQCL.rating,
                                                                StringType(),
                                                                True,
                                                            ),
                                                        ]
                                                    ),
                                                    True,
                                                ),
                                                True,
                                            ),
                                        ]
                                    ),
                                    True,
                                ),
                                True,
                            ),
                            StructField(
                                CQCL.overall,
                                StructType(
                                    [
                                        StructField(CQCL.rating, StringType(), True),
                                        StructField(
                                            CQCL.use_of_resources,
                                            StructType(
                                                [
                                                    StructField(
                                                        CQCL.combined_quality_rating,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.combined_quality_summary,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.use_of_resources_rating,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.use_of_resources_summary,
                                                        StringType(),
                                                        True,
                                                    ),
                                                ]
                                            ),
                                            True,
                                        ),
                                        StructField(
                                            CQCL.key_question_ratings,
                                            ArrayType(
                                                StructType(
                                                    [
                                                        StructField(
                                                            CQCL.name,
                                                            StringType(),
                                                            True,
                                                        ),
                                                        StructField(
                                                            CQCL.rating,
                                                            StringType(),
                                                            True,
                                                        ),
                                                    ]
                                                ),
                                                True,
                                            ),
                                            True,
                                        ),
                                    ]
                                ),
                                True,
                            ),
                        ]
                    ),
                    True,
                ),
                True,
            ),
        ]
    )
    expected_flatten_ratings_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
            StructField(CQCRatings.date, StringType(), True),
            StructField(CQCRatings.overall_rating, StringType(), True),
            StructField(CQCRatings.safe_rating, StringType(), True),
            StructField(CQCRatings.well_led_rating, StringType(), True),
            StructField(CQCRatings.caring_rating, StringType(), True),
            StructField(CQCRatings.responsive_rating, StringType(), True),
            StructField(CQCRatings.effective_rating, StringType(), True),
        ]
    )

    prepare_assessment_ratings_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
            StructField(
                CQCL.assessment,
                ArrayType(
                    StructType(
                        [
                            StructField(
                                CQCL.assessment_plan_published_datetime,
                                StringType(),
                                True,
                            ),
                            StructField(
                                CQCL.ratings,
                                StructType(
                                    [
                                        StructField(
                                            CQCL.overall,
                                            ArrayType(
                                                StructType(
                                                    [
                                                        StructField(
                                                            CQCL.rating,
                                                            StringType(),
                                                            True,
                                                        ),
                                                        StructField(
                                                            CQCL.status,
                                                            StringType(),
                                                            True,
                                                        ),
                                                        StructField(
                                                            CQCL.key_question_ratings,
                                                            ArrayType(
                                                                StructType(
                                                                    [
                                                                        StructField(
                                                                            CQCL.name,
                                                                            StringType(),
                                                                            True,
                                                                        ),
                                                                        StructField(
                                                                            CQCL.rating,
                                                                            StringType(),
                                                                            True,
                                                                        ),
                                                                        StructField(
                                                                            CQCL.status,
                                                                            StringType(),
                                                                            True,
                                                                        ),
                                                                    ]
                                                                )
                                                            ),
                                                            True,
                                                        ),
                                                    ]
                                                )
                                            ),
                                            True,
                                        ),
                                        StructField(
                                            CQCL.asg_ratings,
                                            ArrayType(
                                                StructType(
                                                    [
                                                        StructField(
                                                            CQCL.assessment_plan_id,
                                                            StringType(),
                                                            True,
                                                        ),
                                                        StructField(
                                                            CQCL.title,
                                                            StringType(),
                                                            True,
                                                        ),
                                                        StructField(
                                                            CQCL.assessment_date,
                                                            StringType(),
                                                            True,
                                                        ),
                                                        StructField(
                                                            CQCL.assessment_plan_status,
                                                            StringType(),
                                                            True,
                                                        ),
                                                        StructField(
                                                            CQCL.name,
                                                            StringType(),
                                                            True,
                                                        ),
                                                        StructField(
                                                            CQCL.rating,
                                                            StringType(),
                                                            True,
                                                        ),
                                                        StructField(
                                                            CQCL.status,
                                                            StringType(),
                                                            True,
                                                        ),
                                                        StructField(
                                                            CQCL.key_question_ratings,
                                                            ArrayType(
                                                                StructType(
                                                                    [
                                                                        StructField(
                                                                            CQCL.name,
                                                                            StringType(),
                                                                            True,
                                                                        ),
                                                                        StructField(
                                                                            CQCL.rating,
                                                                            StringType(),
                                                                            True,
                                                                        ),
                                                                        StructField(
                                                                            CQCL.status,
                                                                            StringType(),
                                                                            True,
                                                                        ),
                                                                        StructField(
                                                                            CQCL.percentage_score,
                                                                            StringType(),
                                                                            True,
                                                                        ),
                                                                    ]
                                                                )
                                                            ),
                                                            True,
                                                        ),
                                                    ]
                                                )
                                            ),
                                            True,
                                        ),
                                    ]
                                ),
                                True,
                            ),
                        ]
                    ),
                    True,
                ),
                True,
            ),
        ]
    )
    expected_prepare_assessment_ratings_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
            StructField(CQCL.assessment_plan_published_datetime, StringType(), True),
            StructField(CQCL.assessment_plan_id, StringType(), True),
            StructField(CQCL.title, StringType(), True),
            StructField(CQCL.assessment_date, StringType(), True),
            StructField(CQCL.assessment_plan_status, StringType(), True),
            StructField(CQCL.dataset, StringType(), True),
            StructField(CQCL.name, StringType(), True),
            StructField(CQCL.status, StringType(), True),
            StructField(CQCL.rating, StringType(), True),
            StructField(CQCL.source_path, StringType(), True),
            StructField(CQCL.safe, StringType(), True),
            StructField(CQCL.effective, StringType(), True),
            StructField(CQCL.caring, StringType(), True),
            StructField(CQCL.responsive, StringType(), True),
            StructField(CQCL.well_led, StringType(), True),
        ]
    )

    raise_error_when_assessment_df_contains_overall_data_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.source_path, StringType(), True),
            StructField(CQCL.rating, StringType(), True),
        ]
    )

    assessment_ratings_for_merging_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
            StructField(CQCL.assessment_plan_published_datetime, StringType(), True),
            StructField(CQCL.assessment_plan_id, StringType(), True),
            StructField(CQCL.title, StringType(), True),
            StructField(CQCL.assessment_date, StringType(), True),
            StructField(CQCL.assessment_plan_status, StringType(), True),
            StructField(CQCL.name, StringType(), True),
            StructField(CQCL.source_path, StringType(), True),
            StructField(CQCL.dataset, StringType(), True),
            StructField(CQCL.status, StringType(), True),
            StructField(CQCL.rating, StringType(), True),
            StructField(CQCL.safe, StringType(), True),
            StructField(CQCL.effective, StringType(), True),
            StructField(CQCL.caring, StringType(), True),
            StructField(CQCL.responsive, StringType(), True),
            StructField(CQCL.well_led, StringType(), True),
        ]
    )
    standard_ratings_for_merging_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
            StructField(CQCRatings.date, StringType(), True),
            StructField(CQCRatings.current_or_historic, StringType(), True),
            StructField(CQCRatings.overall_rating, StringType(), True),
            StructField(CQCRatings.safe_rating, StringType(), True),
            StructField(CQCRatings.well_led_rating, StringType(), True),
            StructField(CQCRatings.caring_rating, StringType(), True),
            StructField(CQCRatings.responsive_rating, StringType(), True),
            StructField(CQCRatings.effective_rating, StringType(), True),
        ]
    )
    expected_merge_cqc_ratings_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
            StructField(CQCRatings.date, StringType(), True),
            StructField(CQCL.assessment_plan_id, StringType(), True),
            StructField(CQCL.title, StringType(), True),
            StructField(CQCL.assessment_date, StringType(), True),
            StructField(CQCL.assessment_plan_status, StringType(), True),
            StructField(CQCL.name, StringType(), True),
            StructField(CQCL.source_path, StringType(), True),
            StructField(CQCL.dataset, StringType(), True),
            StructField(CQCRatings.current_or_historic, StringType(), True),
            StructField(CQCRatings.overall_rating, StringType(), True),
            StructField(CQCRatings.safe_rating, StringType(), True),
            StructField(CQCRatings.well_led_rating, StringType(), True),
            StructField(CQCRatings.caring_rating, StringType(), True),
            StructField(CQCRatings.responsive_rating, StringType(), True),
            StructField(CQCRatings.effective_rating, StringType(), True),
        ]
    )

    add_current_or_historic_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
        ]
    )
    expected_add_current_or_historic_schema = StructType(
        [
            *add_current_or_historic_schema,
            StructField(CQCRatings.current_or_historic, StringType(), True),
        ]
    )

    remove_blank_rows_schema = expected_flatten_ratings_schema

    add_rating_sequence_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCRatings.date, StringType(), True),
            StructField(CQCL.assessment_date, StringType(), True),
        ]
    )
    expected_add_rating_sequence_schema = StructType(
        [
            *add_rating_sequence_schema,
            StructField(CQCRatings.rating_sequence, IntegerType(), True),
        ]
    )
    expected_reversed_add_rating_sequence_schema = StructType(
        [
            *add_rating_sequence_schema,
            StructField(CQCRatings.reversed_rating_sequence, IntegerType(), True),
        ]
    )

    add_latest_rating_flag_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCRatings.reversed_rating_sequence, IntegerType(), True),
        ]
    )
    expected_add_latest_rating_flag_schema = StructType(
        [
            *add_latest_rating_flag_schema,
            StructField(CQCRatings.latest_rating_flag, IntegerType(), True),
        ]
    )

    create_standard_ratings_dataset_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
            StructField(CQCRatings.date, StringType(), True),
            StructField(CQCL.assessment_plan_id, StringType(), True),
            StructField(CQCL.title, StringType(), True),
            StructField(CQCL.assessment_date, StringType(), True),
            StructField(CQCL.assessment_plan_status, StringType(), True),
            StructField(CQCL.name, StringType(), True),
            StructField(CQCL.source_path, StringType(), True),
            StructField(CQCL.dataset, StringType(), True),
            StructField(CQCRatings.overall_rating, StringType(), True),
            StructField(CQCRatings.safe_rating, StringType(), True),
            StructField(CQCRatings.well_led_rating, StringType(), True),
            StructField(CQCRatings.caring_rating, StringType(), True),
            StructField(CQCRatings.responsive_rating, StringType(), True),
            StructField(CQCRatings.effective_rating, StringType(), True),
            StructField(CQCRatings.current_or_historic, StringType(), True),
            StructField(CQCRatings.rating_sequence, IntegerType(), True),
            StructField(CQCRatings.reversed_rating_sequence, IntegerType(), True),
            StructField(CQCRatings.latest_rating_flag, IntegerType(), True),
            StructField(CQCRatings.safe_rating_value, IntegerType(), True),
            StructField(CQCRatings.well_led_rating_value, IntegerType(), True),
            StructField(CQCRatings.caring_rating_value, IntegerType(), True),
            StructField(CQCRatings.responsive_rating_value, IntegerType(), True),
            StructField(CQCRatings.effective_rating_value, IntegerType(), True),
            StructField(CQCRatings.total_rating_value, IntegerType(), True),
        ]
    )
    expected_create_standard_ratings_dataset_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
            StructField(CQCRatings.date, StringType(), True),
            StructField(CQCL.assessment_plan_id, StringType(), True),
            StructField(CQCL.title, StringType(), True),
            StructField(CQCL.assessment_date, StringType(), True),
            StructField(CQCL.assessment_plan_status, StringType(), True),
            StructField(CQCL.name, StringType(), True),
            StructField(CQCL.source_path, StringType(), True),
            StructField(CQCL.dataset, StringType(), True),
            StructField(CQCRatings.latest_rating_flag, IntegerType(), True),
            StructField(CQCRatings.current_or_historic, StringType(), True),
            StructField(CQCRatings.overall_rating, StringType(), True),
            StructField(CQCRatings.safe_rating, StringType(), True),
            StructField(CQCRatings.well_led_rating, StringType(), True),
            StructField(CQCRatings.caring_rating, StringType(), True),
            StructField(CQCRatings.responsive_rating, StringType(), True),
            StructField(CQCRatings.effective_rating, StringType(), True),
            StructField(CQCRatings.safe_rating_value, IntegerType(), True),
            StructField(CQCRatings.well_led_rating_value, IntegerType(), True),
            StructField(CQCRatings.caring_rating_value, IntegerType(), True),
            StructField(CQCRatings.responsive_rating_value, IntegerType(), True),
            StructField(CQCRatings.effective_rating_value, IntegerType(), True),
            StructField(CQCRatings.total_rating_value, IntegerType(), True),
        ]
    )

    select_ratings_for_benchmarks_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
            StructField(CQCL.name, StringType(), True),
            StructField(CQCRatings.current_or_historic, StringType(), True),
        ]
    )

    add_good_and_outstanding_flag_column_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.name, StringType(), True),
            StructField(CQCRatings.overall_rating_value, IntegerType(), True),
        ]
    )
    expected_add_good_and_outstanding_flag_column_schema = StructType(
        [
            *add_good_and_outstanding_flag_column_schema,
            StructField(CQCRatings.good_or_outstanding_flag, IntegerType(), True),
        ]
    )

    ratings_join_establishment_ids_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField("other_ratings_column", StringType(), True),
        ]
    )
    ascwds_join_establishment_ids_schema = StructType(
        [
            StructField(AWP.location_id, StringType(), True),
            StructField(AWP.establishment_id, StringType(), True),
            StructField(AWP.import_date, StringType(), True),
        ]
    )
    expected_join_establishment_ids_schema = StructType(
        [
            *ratings_join_establishment_ids_schema,
            StructField(AWP.establishment_id, StringType(), True),
        ]
    )

    create_benchmark_ratings_dataset_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(AWP.establishment_id, StringType(), True),
            StructField(CQCL.name, StringType(), True),
            StructField(CQCL.dataset, StringType(), True),
            StructField(CQCRatings.good_or_outstanding_flag, IntegerType(), True),
            StructField(CQCRatings.overall_rating, StringType(), True),
            StructField(CQCRatings.date, StringType(), True),
            StructField("other column", StringType(), True),
        ]
    )
    expected_create_benchmark_ratings_dataset_schema = StructType(
        [
            StructField(CQCRatings.benchmarks_location_id, StringType(), True),
            StructField(CQCRatings.benchmarks_establishment_id, StringType(), True),
            StructField(CQCL.name, StringType(), True),
            StructField(CQCL.dataset, StringType(), True),
            StructField(CQCRatings.good_or_outstanding_flag, IntegerType(), True),
            StructField(CQCRatings.benchmarks_overall_rating, StringType(), True),
            StructField(CQCRatings.inspection_date, StringType(), True),
        ]
    )

    add_numerical_ratings_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCRatings.overall_rating, StringType(), True),
            StructField(CQCRatings.safe_rating, StringType(), True),
            StructField(CQCRatings.well_led_rating, StringType(), True),
            StructField(CQCRatings.caring_rating, StringType(), True),
            StructField(CQCRatings.responsive_rating, StringType(), True),
            StructField(CQCRatings.effective_rating, StringType(), True),
        ]
    )
    expected_add_numerical_ratings_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCRatings.overall_rating, StringType(), True),
            StructField(CQCRatings.safe_rating, StringType(), True),
            StructField(CQCRatings.well_led_rating, StringType(), True),
            StructField(CQCRatings.caring_rating, StringType(), True),
            StructField(CQCRatings.responsive_rating, StringType(), True),
            StructField(CQCRatings.effective_rating, StringType(), True),
            StructField(CQCRatings.overall_rating_value, IntegerType(), True),
            StructField(CQCRatings.safe_rating_value, IntegerType(), True),
            StructField(CQCRatings.well_led_rating_value, IntegerType(), True),
            StructField(CQCRatings.caring_rating_value, IntegerType(), True),
            StructField(CQCRatings.responsive_rating_value, IntegerType(), True),
            StructField(CQCRatings.effective_rating_value, IntegerType(), True),
            StructField(CQCRatings.total_rating_value, IntegerType(), True),
        ]
    )

    location_id_hash_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
        ]
    )

    expected_location_id_hash_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCRatings.location_id_hash, StringType(), True),
        ]
    )


@dataclass
class LmEngagementUtilsSchemas:
    add_columns_for_locality_manager_dashboard_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.current_cssr, StringType(), True),
            StructField(CoverageColumns.in_ascwds, IntegerType(), True),
            StructField("_year", IntegerType(), True),
        ]
    )
    expected_add_columns_for_locality_manager_dashboard_schema = StructType(
        [
            *add_columns_for_locality_manager_dashboard_schema,
            StructField(CoverageColumns.la_monthly_coverage, FloatType(), True),
            StructField(CoverageColumns.coverage_monthly_change, FloatType(), True),
            StructField(CoverageColumns.locations_monthly_change, IntegerType(), True),
            StructField(CoverageColumns.new_registrations_monthly, IntegerType(), True),
            StructField(CoverageColumns.new_registrations_ytd, IntegerType(), True),
        ]
    )

    expected_calculate_la_coverage_monthly_schema = StructType(
        [
            *add_columns_for_locality_manager_dashboard_schema,
            StructField(CoverageColumns.la_monthly_coverage, FloatType(), True),
        ]
    )
    calculate_coverage_monthly_change_schema = (
        expected_calculate_la_coverage_monthly_schema
    )

    expected_calculate_coverage_monthly_change_schema = StructType(
        [
            *expected_calculate_la_coverage_monthly_schema,
            StructField(CoverageColumns.coverage_monthly_change, FloatType(), True),
        ]
    )

    calculate_locations_monthly_change_schema = (
        expected_calculate_coverage_monthly_change_schema
    )
    expected_calculate_locations_monthly_change_schema = StructType(
        [
            *expected_calculate_coverage_monthly_change_schema,
            StructField(CoverageColumns.in_ascwds_last_month, IntegerType(), True),
            StructField(CoverageColumns.locations_monthly_change, IntegerType(), True),
        ]
    )

    calculate_new_registrations_schema = (
        expected_calculate_locations_monthly_change_schema
    )
    expected_calculate_new_registrations_schema = StructType(
        [
            *expected_calculate_coverage_monthly_change_schema,
            StructField(CoverageColumns.locations_monthly_change, IntegerType(), True),
            StructField(CoverageColumns.new_registrations_monthly, IntegerType(), True),
            StructField(CoverageColumns.new_registrations_ytd, IntegerType(), True),
        ]
    )
