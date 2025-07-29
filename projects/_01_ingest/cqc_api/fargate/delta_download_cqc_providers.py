

def main(destination):
    # get secret
    # for provider in providers_generator:
    #     create provider df
    #     union to existing
    # drop duplicates
    # write to parquet

    # print("Collecting all providers from API")
    # spark = utils.get_spark()
    # df = None
    # cqc_api_primary_key_value = json.loads(
    #     ars.get_secret(secret_name="cqc_api_primary_key", region_name="eu-west-2")
    # )["Ocp-Apim-Subscription-Key"]
    # for paginated_providers in cqc.get_all_objects(
    #     object_type="providers",
    #     object_identifier=ColNames.provider_id,
    #     cqc_api_primary_key=cqc_api_primary_key_value,
    # ):
    #     providers_df = spark.createDataFrame(paginated_providers, PROVIDER_SCHEMA)
    #     if df:
    #         df = df.union(providers_df)
    #     else:
    #         df = providers_df
    #
    # df = df.dropDuplicates([ColNames.provider_id])
    # utils.write_to_parquet(df, destination, "append")
    #
    # print(f"Finished! Files can be found in {destination}")


if __name__ == "__main__":
    # destination_prefix, *_ = utils.collect_arguments(
    #     (
    #         "--destination_prefix",
    #         "Source s3 directory for parquet CQC providers dataset",
    #         False,
    #     ),
    # )
    # todays_date = date.today()
    # destination = utils.generate_s3_datasets_dir_date_path(
    #     destination_prefix=destination_prefix,
    #     domain="CQC",
    #     dataset="providers_api",
    #     date=todays_date,
    #     version="2.0.0",
    # )
    #
    # main(destination)
