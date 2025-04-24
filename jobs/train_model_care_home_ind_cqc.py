from utils import utils


def main(
    care_home_features_source: str,
    care_home_model_source: str,
) -> None:
    print("Training care home linear regression model...")

    locations_df = utils.read_from_parquet(care_home_features_source)
