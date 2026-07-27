"""TEMPORARY - ticket 1820 memory instrumentation for validate_00_prepare.

Added to measure how much memory the job actually uses either side of the row
reduction, rather than assuming the saving. Not part of the permanent pipeline.

To remove once the measurement has been taken: delete this module, the
`write_checkpoint` calls and the `dHelpers` import in `validate_00_prepare.py`,
and the `mock_write_checkpoint` patches in `tests/fargate/test_validate_00_prepare.py`.
No Dockerfile change is needed either way - it copies `fargate/utils` wholesale.
"""

import json

import boto3


def peak_rss_kb() -> int:
    """Returns this process's peak RSS in KB.

    Uses `resource.getrusage` rather than `psutil` because `ru_maxrss` is already
    reported in KB on Linux (the Fargate runtime), giving a true monotonic peak
    with no need to diff before/after readings.

    Returns:
        int: peak resident set size in KB.
    """
    try:
        import resource
    except ModuleNotFoundError:
        # `resource` is Unix-only, so this branch is local Windows test runs only,
        # where the reading is incidental - the measurement that matters is taken
        # on Fargate. `psutil` is a dev dependency and deliberately not imported at
        # module level, since it is absent from the container's requirements.txt.
        import psutil

        return psutil.Process().memory_info().peak_wset // 1024

    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss


def write_checkpoint(
    bucket_name: str, reports_path: str, label: str, **extra: object
) -> None:
    """Writes a diagnostic checkpoint straight to S3 and flushes stdout.

    Ticket 1814's OOM-killed run produced zero log output because the ECS log
    driver's stdout buffer never flushed before the container was killed. Writing
    each checkpoint directly to S3 as it happens means results survive even if this
    process is OOM-killed immediately afterwards.

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
