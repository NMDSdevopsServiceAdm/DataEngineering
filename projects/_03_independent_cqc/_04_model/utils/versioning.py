import json
from datetime import datetime

import boto3


def get_run_number(s3_root: str) -> int:
    """
    Determines the most recent run number stored under an S3 model root.

    The function scans all objects under the given S3 prefix and looks for keys
    that end with `metadata.json`.

    Each run is assumed to be structured like:
        s3_root/<run_number>/metadata.json

    It extracts the run numbers and returns the maximum one found.
    If no runs exist, it returns `0`.

    Args:
        s3_root (str): S3 directory prefix for a model's run outputs (e.g. "s3://pipeline-resources/models/model_A/")

    Returns:
        int: The highest existing run number, or `0` if none exist.
    """
    s3 = boto3.client("s3")
    bucket = s3_root.replace("s3://", "").split("/")[0]
    prefix = "/".join(s3_root.replace("s3://", "").split("/")[1:])
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    runs = []
    for obj in response.get("Contents", []):
        if obj["Key"].endswith("metadata.json"):
            try:
                run = int(obj["Key"].split("/")[-2])
                runs.append(run)
            except:
                pass

    return max(runs) if runs else 0


def save_metadata(s3_root: str, run_number: int, metadata: dict) -> None:
    """
    Saves a JSON metadata file for a specific model run in S3.

    This function writes a `metadata.json` file under:
        s3_root/<run_number>/metadata.json

    A timestamp field is automatically added to the metadata before saving.

    Args:
        s3_root (str): S3 directory prefix for a model's run outputs (e.g. "s3://pipeline-resources/models/model_A/")
        run_number (int): The run/version number to save metadata under.
        metadata (dict): Metadata describing the model run.

    Return:
        None
    """
    s3 = boto3.client("s3")
    bucket = s3_root.replace("s3://", "").split("/")[0]
    prefix = "/".join(s3_root.replace("s3://", "").split("/")[1:])

    key = f"{prefix}{run_number}/metadata.json"

    metadata["timestamp"] = datetime.now().isoformat()
    body = json.dumps(metadata, indent=2)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )
