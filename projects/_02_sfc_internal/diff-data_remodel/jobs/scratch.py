from utils import list_bucket_objects

fps = list_bucket_objects(
    "sfc-main-datasets",
    "domain=CQC/dataset=providers_api/version=2.0.0/year=2024/month=05",
)

print(fps)

print(set(fp.rsplit("/", 1)[-1] for fp in fps))
