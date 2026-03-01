from __future__ import annotations

from typing import Literal

from pydantic import BaseModel


class CredentialBase(BaseModel):
    """Base for all credential types."""

    type: str  # pyrefly: ignore  # narrowed to Literal in subclasses


class NoCredential(CredentialBase):
    """No credentials required (e.g. local filesystem)."""

    type: Literal["none"] = "none"


class UsernamePasswordCredential(CredentialBase):
    """Username/password credentials (e.g. on-prem Postgres)."""

    type: Literal["username_password"] = "username_password"
    username: str
    password: str


class AWSProfileCredential(CredentialBase):
    """AWS profile credential — uses boto3 default credential chain."""

    type: Literal["aws_profile"] = "aws_profile"
    profile: str = "default"


class GithubTokenCredential(CredentialBase):
    """GitHub personal access token."""

    type: Literal["github_token"] = "github_token"
    token: str
