"""Throwaway instrumentation for the ticket 1814 validate_00_prepare OOM isolation experiments.

Not part of the permanent pipeline - delete this module, the diag_0N_*.py scripts
that import it, their Dockerfile COPY lines, and the temporary
Ind-CQC-SLV-1814-oom-diag.json state machine once the OOM root cause has been
isolated.
"""

import json
import resource

import boto3


def peak_rss_kb() -> int:
    """Returns this process's peak RSS in KB.

    Uses `resource.getrusage` rather than `psutil` because `ru_maxrss` is
    already reported in KB on Linux (the Fargate runtime), giving a true
    monotonic peak with no need to diff before/after readings.
    """
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss


def write_checkpoint(
    bucket_name: str, reports_path: str, label: str, **extra: object
) -> None:
    """Writes a diagnostic checkpoint straight to S3 and flushes stdout.

    The original OOM-killed run produced zero log output because the ECS log
    driver's stdout buffer never flushed before the container was killed.
    Writing each checkpoint directly to S3 as it happens means results survive
    even if this process is OOM-killed immediately afterwards.

    Args:
        bucket_name (str): the bucket to write the checkpoint to.
        reports_path (str): the folder (relative to the bucket) to write under.
        label (str): a short identifier for this checkpoint, used as both the
            S3 key and a log marker.
        **extra (object): any additional JSON-serialisable fields to record
            alongside the peak RSS reading (e.g. row_count).
    """
    payload = {"label": label, "peak_rss_kb": peak_rss_kb(), **extra}
    print(f"CHECKPOINT: {payload}", flush=True)
    s3_client = boto3.client("s3")
    s3_client.put_object(
        Body=json.dumps(payload).encode("utf-8"),
        Bucket=bucket_name,
        Key=f"{reports_path.strip('/')}/{label}.json",
    )
