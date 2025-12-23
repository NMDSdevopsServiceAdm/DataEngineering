import io
import json
from datetime import datetime, timezone

import boto3
import joblib

from projects._03_independent_cqc._05_model.utils.model import Model


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


def save_model_and_metadata(
    s3_root: str, run_number: int, model: Model, metadata: dict
) -> None:
    """
    Saves a model and a JSON metadata file for a specific model run in S3.

    This function writes a `metadata.json` file under:
        s3_root/<run_number>/metadata.json

    A timestamp field is automatically added to the metadata before saving.

    Structure:
        s3_root/<run_number>/
            ├── model.pkl
            └── metadata.json

    Args:
        s3_root (str): S3 directory prefix for a model's run outputs (e.g. "s3://pipeline-resources/models/model_A/")
        run_number (int): The run/version number to save metadata under.
        model (Model): The trained model object to be saved.
        metadata (dict): Metadata describing the model run.

    Return:
        None
    """
    s3 = boto3.client("s3")
    bucket = s3_root.replace("s3://", "").split("/")[0]
    prefix = "/".join(s3_root.replace("s3://", "").split("/")[1:])
    run_prefix = f"{prefix}{run_number}/"

    # Save the model
    model_buffer = io.BytesIO()
    joblib.dump(model, model_buffer)
    model_buffer.seek(0)

    s3.upload_fileobj(
        model_buffer,
        bucket,
        f"{run_prefix}model.pkl",
    )

    # Save the metadata
    metadata_to_save = {
        **metadata,
        "run_number": run_number,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    s3.put_object(
        Bucket=bucket,
        Key=f"{run_prefix}metadata.json",
        Body=json.dumps(metadata_to_save, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
