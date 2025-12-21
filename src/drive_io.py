from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Optional, Tuple

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

# Full Drive access (needed for create/update/upload)
SCOPES = ["https://www.googleapis.com/auth/drive"]


def build_drive_service(service_account_json_str: str):
    """
    Build a Google Drive API v3 service using a Service Account JSON string.
    Note: Service Accounts cannot upload to 'My Drive' due to quota limitations.
    Use a Shared Drive (recommended) and add the Service Account as a member.
    """
    info = json.loads(service_account_json_str)
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("drive", "v3", credentials=creds, cache_discovery=False)


# -------------------------
# Helpers that work with Shared Drives
# -------------------------

def _list_files(service, q: str, fields: str = "files(id,name)"):
    """
    List files with Shared Drive support.
    """
    return service.files().list(
        q=q,
        fields=fields,
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
        corpora="allDrives",
        pageSize=1000,
    ).execute()


def _create_file(service, metadata: dict, media=None, fields: str = "id"):
    """
    Create file/folder with Shared Drive support.
    """
    kwargs = dict(
        body=metadata,
        fields=fields,
        supportsAllDrives=True,
    )
    if media is not None:
        kwargs["media_body"] = media
    return service.files().create(**kwargs).execute()


def _update_file(service, file_id: str, media=None, fields: str = "id"):
    """
    Update file with Shared Drive support.
    """
    kwargs = dict(
        fileId=file_id,
        fields=fields,
        supportsAllDrives=True,
    )
    if media is not None:
        kwargs["media_body"] = media
    return service.files().update(**kwargs).execute()


# -------------------------
# Folder / file primitives
# -------------------------

def find_folder(service, parent_id: str, name: str) -> Optional[str]:
    """
    Find a folder by name under a parent folder ID.
    Returns folder_id or None.
    """
    q = (
        f"'{parent_id}' in parents and "
        f"name = '{name}' and "
        "mimeType = 'application/vnd.google-apps.folder' and "
        "trashed = false"
    )
    resp = _list_files(service, q=q, fields="files(id,name)")
    files = resp.get("files", [])
    return files[0]["id"] if files else None


def get_or_create_folder(service, parent_id: str, name: str) -> str:
    """
    Get or create a folder named `name` under `parent_id`.
    Works in Shared Drives and My Drive (but My Drive uploads will fail for service accounts).
    """
    folder_id = find_folder(service, parent_id, name)
    if folder_id:
        return folder_id

    metadata = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id],
    }
    created = _create_file(service, metadata=metadata, fields="id")
    return created["id"]


def find_file_in_folder(service, folder_id: str, filename: str) -> Optional[str]:
    """
    Find a file by exact name in a folder. Returns file_id or None.
    """
    q = f"'{folder_id}' in parents and name = '{filename}' and trashed = false"
    resp = _list_files(service, q=q, fields="files(id,name)")
    files = resp.get("files", [])
    return files[0]["id"] if files else None


def upload_or_update_file(
    service,
    folder_id: str,
    local_path: Path,
    filename: str,
    mime_type: str,
) -> str:
    """
    Uploads local_path to Drive folder as filename.
    If file exists, updates it; else creates it.
    Shared Drive-safe.

    Raises HttpError with helpful message context if upload fails.
    """
    local_path = Path(local_path)
    media = MediaFileUpload(str(local_path), mimetype=mime_type, res
