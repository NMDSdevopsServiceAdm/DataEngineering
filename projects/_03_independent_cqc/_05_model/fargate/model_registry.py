from projects._03_independent_cqc._05_model.utils.model import ModelType
import os

ENVIRONMENT = os.environ.get("ENVIRONMENT")

model_definitions = {
    "non_res_pir": {
        "model_type": ModelType.SIMPLE_LINEAR,
        "model_identifier": "non_res_pir",
        "model_params": dict(),
        "version_parameter_location": f"/models/{ENVIRONMENT}/non_res_pir",
        "data_source_prefix": "domain=ind_cqc_filled_posts/dataset=ind_cqc_estimated_missing_ascwds_filled_posts/",
        "target_columns": ["ascwds_filled_posts_deduplicated_clean"],
        "feature_columns": ["pir_people_directly_employed_deduplicated"],
    }
}
