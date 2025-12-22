from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

import boto3
from botocore.exceptions import ClientError

from google.cloud import storage as gcs_storage
from google.oauth2 import service_account


@dataclass(frozen=True)
class S3Target:
    bucket: str
    prefix: str = ""  # e.g. "alphapd"


@dataclass(frozen=True)
class GCSTarget:
    bucket: str
    prefix: str = ""  # e.g. "alphapd"


def _join(prefix: str, key: str) -> str:
    p = (prefix or "").strip("/")
    k = key.lstrip("/")
    return f"{p}/{k}" if p else k


# -------------------------
# AWS S3
# -------------------------

def s3_client():
    # Uses env vars AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY/AWS_REGION on GitHub Actions
    return boto3.client("s3", region_name=os.environ.get("AWS_REGION"))


def s3_upload(local_path: Path, target: S3Target, key: str, content_type: Optional[str] = None) -> str:
    local_path = Path(local_path)
    s3 = s3_client()
    obj_key = _join(target.prefix, key)

    extra = {}
    if content_type:
        extra["ContentType"] = content_type

    s3.upload_file(str(local_path), target.bucket, obj_key, ExtraArgs=extra or None)
    return f"s3://{target.bucket}/{obj_key}"


def s3_download_if_exists(target: S3Target, key: str, dest_path: Path) -> Tuple[bool, Path]:
    dest_path = Path(dest_path)
    s3 = s3_client()
    obj_key = _join(target.prefix, key)

    try:
        s3.download_file(target.bucket, obj_key, str(dest_path))
        return True, dest_path
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False, dest_path
        raise


# -------------------------
# Google Cloud Storage
# -------------------------

def gcs_client_from_env():
    """
    Uses service account JSON passed in env var GCP_SA_JSON.
    """
    sa_json = os.environ["GCP_SA_JSON"]
    info = json.loads(sa_json)
    creds = service_account.Credentials.from_service_account_info(info)
    return gcs_storage.Client(credentials=creds, project=info.get("project_id"))


def gcs_upload(local_path: Path, target: GCSTarget, key: str, content_type: Optional[str] = None) -> str:
    local_path = Path(local_path)
    client = gcs_client_from_env()
    bucket = client.bucket(target.bucket)

    obj_key = _join(target.prefix, key)
    blob = bucket.blob(obj_key)
    blob.upload_from_filename(str(local_path), content_type=content_type)

    return f"gs://{target.bucket}/{obj_key}"


def gcs_download_if_exists(target: GCSTarget, key: str, dest_path: Path) -> Tuple[bool, Path]:
    dest_path = Path(dest_path)
    client = gcs_client_from_env()
    bucket = client.bucket(target.bucket)

    obj_key = _join(target.prefix, key)
    blob = bucket.blob(obj_key)

    if not blob.exists(client=client):
        return False, dest_path

    blob.download_to_filename(str(dest_path))
    return True, dest_path
