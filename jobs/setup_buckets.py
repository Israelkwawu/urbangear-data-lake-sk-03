"""Create UrbanGear's data-lake bucket. Safe to re-run (idempotent)."""
import os

import boto3
from botocore.exceptions import ClientError

s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["MINIO_ENDPOINT"],          # http://minio:9000 inside Docker
    aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
    aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
)

# One bucket. Zones (raw/, processed/, rejected/, metrics/) are key prefixes inside it.
BUCKETS = ["urbangear-data-lake"]

for bucket in BUCKETS:
    try:
        s3.head_bucket(Bucket=bucket)          # cheap "does it exist?" check
        print(f"exists : {bucket}")
    except ClientError:
        s3.create_bucket(Bucket=bucket)
        print(f"created: {bucket}")
