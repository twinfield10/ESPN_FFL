import io
import os
import uuid
import pandas as pd
import polars as pl

import boto3
from boto3.s3.transfer import S3UploadFailedError
from botocore.exceptions import ClientError

s3_resource = boto3.resource("s3")
bucket_name = "espn-ffl-data"
bucket = s3_resource.Bucket(bucket_name)

def s3_write_parquet(df, s3_obj, bkt=bucket_name):
    path = f"s3://{bkt}/{s3_obj}.parquet"
    df.to_parquet(path)

