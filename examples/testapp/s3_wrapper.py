"""Thin wrapper around a boto3 S3 client and a fixed bucket."""

import boto3


class S3Wrapper:
    """Wraps a boto3 S3 client scoped to a single bucket."""

    def __init__(self, bucket: str, region: str = "us-east-1"):
        self._client = boto3.client("s3", region_name=region)
        self._bucket = bucket

    def read(self, key: str) -> str | None:
        """Read an object as UTF-8 text. Returns None if the key doesn't exist."""
        try:
            resp = self._client.get_object(Bucket=self._bucket, Key=key)
            return resp["Body"].read().decode("utf-8")
        except self._client.exceptions.NoSuchKey:
            return None

    def write(self, key: str, body: str) -> None:
        """Write a UTF-8 string to an object."""
        self._client.put_object(Bucket=self._bucket, Key=key, Body=body.encode("utf-8"))
