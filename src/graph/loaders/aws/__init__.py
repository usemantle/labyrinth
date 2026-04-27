"""AWS account loader and resource discovery plugins."""

import boto3

from src.graph.loaders.aws.loader import AwsAccountLoader


def session_from_credentials(credentials: dict, region_name: str | None = None) -> boto3.Session:
    """Build a boto3 Session from the credentials dict produced by the worker.

    Handles two shapes:
      - ``{"profile": "name"}`` — SSO / named profile
      - ``{"aws_access_key_id": ..., "aws_secret_access_key": ..., "aws_session_token": ...}``
        — temporary credentials from an assumed role
    """
    if "aws_access_key_id" in credentials:
        return boto3.Session(
            aws_access_key_id=credentials["aws_access_key_id"],
            aws_secret_access_key=credentials["aws_secret_access_key"],
            aws_session_token=credentials.get("aws_session_token"),
            region_name=region_name,
        )
    return boto3.Session(
        profile_name=credentials.get("profile", "default"),
        region_name=region_name,
    )


__all__ = ["AwsAccountLoader", "session_from_credentials"]
