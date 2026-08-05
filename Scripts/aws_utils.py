"""Optional S3 persistence for projection data.

Dormant: the only caller (``Scripts/scrape_FP.py``) has its S3 write commented
out, so nothing in the pipeline currently uses this.

The boto3 resource is created on demand rather than at import time. It used to
be constructed at module scope, which meant that merely importing this module
required AWS credentials -- and ``scrape_FP.py`` did ``from aws_utils import *``
unconditionally, so the FantasyPros scraper could not run on a machine without
them, for a code path that was disabled anyway.
"""

import functools
from typing import Optional

BUCKET_NAME = "espn-ffl-data"

# Backwards-compatible alias for the previous module-level name.
bucket_name = BUCKET_NAME


@functools.cache
def get_s3_resource():
    """Create (once) and return the boto3 S3 resource.

    Returns:
        boto3.resources.factory.ServiceResource: S3 resource.

    Raises:
        ImportError: If boto3 is not installed.
    """
    import boto3

    return boto3.resource("s3")


def get_bucket(bkt: Optional[str] = None):
    """Return the S3 Bucket object.

    Args:
        bkt: Bucket name. Defaults to :data:`BUCKET_NAME`.

    Returns:
        The boto3 Bucket.
    """
    return get_s3_resource().Bucket(bkt or BUCKET_NAME)


def s3_write_parquet(df, s3_obj: str, bkt: str = BUCKET_NAME) -> str:
    """Write a dataframe to S3 as parquet.

    Args:
        df: A pandas DataFrame (must expose ``to_parquet``).
        s3_obj: Object key, without the ``.parquet`` suffix.
        bkt: Bucket name.

    Returns:
        str: The ``s3://`` URI written.
    """
    path = f"s3://{bkt}/{s3_obj}.parquet"
    df.to_parquet(path)
    return path
