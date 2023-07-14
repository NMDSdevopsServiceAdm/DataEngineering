import boto3


def generate_ASCWDS_directories():
    directories = []
    for dataset in ["worker", "workplace"]:
        for year in range(2010, 2030):
            for month in range(1, 13):
                for day in range(1, 32):
                    folder_name = f"domain=ASCWDS/dataset={dataset}/version=0.0.1/year={year}/month={f'{month:02d}'}/day={f'{day:02d}'}/import_date={year}{f'{month:02d}'}{f'{day:02d}'}"
                    directories.append(folder_name)

    return directories


def generate_historical_CQC_directories():
    directories = []
    for year in range(2013, 2022):
        for month in range(1, 13):
            folder_name = f"domain=CQC/dataset=care-directory/version=0.0.1/year={year}/month={f'{month:02d}'}/day=01/import_date={year}{f'{month:02d}'}01"
            directories.append(folder_name)

    return directories


def main():
    s3 = boto3.client("s3")
    bucket_name = "sfc-data-engineering-raw"

    directories = generate_historical_CQC_directories()
    for directory in directories:
        s3.put_object(Bucket=bucket_name, Key=(directory + "/"))


if __name__ == "__main__":
    main()
