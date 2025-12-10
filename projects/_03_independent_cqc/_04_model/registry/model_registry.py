from projects._03_independent_cqc._04_model.utils.value_labels import (
    ModelTypes,
    RegionLabels,
    RelatedLocationLabels,
    RuralUrbanLabels,
    ServicesLabels,
    SpecialismsLabels,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import ModelRegistryKeys as MRKeys

model_registry = {
    "care_home": {
        MRKeys.version: "7.0.0",
        MRKeys.auto_retrain: True,
        MRKeys.model_type: ModelTypes.lasso,
        MRKeys.model_params: {"alpha": 0.001},
        MRKeys.dependent: IndCQC.imputed_filled_posts_per_bed_ratio_model,
        MRKeys.features: sorted(
            [
                IndCQC.activity_count_capped,
                IndCQC.cqc_location_import_date_indexed,
                IndCQC.number_of_beds,
                IndCQC.banded_bed_ratio_rolling_average_model,
                IndCQC.service_count_capped,
                *RegionLabels.labels_dict.keys(),
                *RuralUrbanLabels.care_home_labels_dict.keys(),
                *ServicesLabels.care_home_labels_dict.keys(),
                *SpecialismsLabels.labels_dict.keys(),
            ]
        ),
    },
    "non_residential_without_dormancy": {
        MRKeys.version: "5.0.0",
        MRKeys.auto_retrain: True,
        MRKeys.model_type: ModelTypes.lasso,
        MRKeys.model_params: {"alpha": 0.001},
        MRKeys.dependent: IndCQC.imputed_filled_post_model,
        MRKeys.features: sorted(
            [
                IndCQC.activity_count_capped,
                IndCQC.cqc_location_import_date_indexed,
                IndCQC.posts_rolling_average_model,
                IndCQC.service_count_capped,
                IndCQC.time_registered_capped_at_four_years,
                *RelatedLocationLabels.labels_dict.keys(),
                *RegionLabels.labels_dict.keys(),
                *RuralUrbanLabels.non_res_labels_dict.keys(),
                *ServicesLabels.non_res_labels_dict.keys(),
                *SpecialismsLabels.labels_dict.keys(),
            ]
        ),
    },
    "non_residential_with_dormancy": {
        MRKeys.version: "6.0.0",
        MRKeys.auto_retrain: True,
        MRKeys.model_type: ModelTypes.lasso,
        MRKeys.model_params: {"alpha": 0.001},
        MRKeys.dependent: IndCQC.imputed_filled_post_model,
        MRKeys.features: sorted(
            [
                IndCQC.activity_count_capped,
                IndCQC.cqc_location_import_date_indexed,
                IndCQC.cqc_location_import_date_indexed_squared,
                IndCQC.posts_rolling_average_model,
                IndCQC.service_count_capped,
                IndCQC.time_registered,
                IndCQC.time_since_dormant,
                *RelatedLocationLabels.labels_dict.keys(),
                *RegionLabels.labels_dict.keys(),
                *RuralUrbanLabels.non_res_labels_dict.keys(),
                *ServicesLabels.non_res_labels_dict.keys(),
                *SpecialismsLabels.labels_dict.keys(),
            ]
        ),
    },
    "non_res_pir": {
        MRKeys.version: "3.0.0",
        MRKeys.auto_retrain: True,
        MRKeys.model_type: ModelTypes.linear_regression,
        MRKeys.model_params: {"fit_intercept": False},
        MRKeys.dependent: IndCQC.ascwds_filled_posts_dedup_clean,
        MRKeys.features: [IndCQC.pir_people_directly_employed_dedup],
    },
}
