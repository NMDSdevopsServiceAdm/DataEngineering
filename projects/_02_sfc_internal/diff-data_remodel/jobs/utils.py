import boto3

def download_from_s3(src_bucket, src_filename, dest_filename):
    s3 = boto3.client('s3')
    s3.download_file(
        src_bucket,
        src_filename,
        dest_filename,
    )

def list_bucket_objects(bucket, prefix):
    s3 = boto3.client('s3')
    objects = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return [o["Key"] for o in objects["Contents"]]
