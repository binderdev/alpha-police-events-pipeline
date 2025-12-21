from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Optional, Tuple

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload


SCOPES = ["https://www.googleapis.com/auth/drive"]


def build_drive_service(service_account_json_str: str):
    info = json.loads(service_account_json_str)
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _find_child_folder(service, parent_id: str, name: str) -> Optional[str]:
    q = (
        f"'{parent_id}' in parents and "
        f"name = '{name}' and "
        "mimeType = 'application/vnd.google-apps.folder' and "
        "trashed = false"
    )
    resp = service.files().list(q=q, fields="files(id,name)").execute()
    files = resp.get("files", [])
    return files[0]["id"] if files else None


def get_or_create_folder(service, parent_id: str, name: str) -> str:
    folder_id = _find_child_folder(service, parent_id, name)
    if folder_id:
        return folder_id

    metadata = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id],
    }
    created = service.files().create(body=metadata, fields="id").execute()
    return created["id"]


def find_file_in_folder(service, folder_id: str, filename: str) -> Optional[str]:
    q = f"'{folder_id}' in parents and name = '{filename}' and trashed = false"
    resp = service.files().list(q=q, fields="files(id,name)").execute()
    files = resp.get("files", [])
    return files[0]["id"] if files else None


def upload_or_update_file(
    service,
    folder_id: str,
    local_path: Path,
    filename: str,
    mime_type: str,
) -> str:
    local_path = Path(local_path)
    media = MediaFileUpload(str(local_path), mimetype=mime_type, resumable=True)

    file_id = find_file_in_folder(service, folder_id, filename)
    if file_id:
        updated = service.files().update(fileId=file_id, media_body=media).execute()
        return updated["id"]

    metadata = {"name": filename, "parents": [folder_id]}
    created = service.files().create(body=metadata, media_body=media, fields="id").execute()
    return created["id"]


def download_file_if_exists(
    service,
    folder_id: str,
    filename: str,
    dest_path: Path,
) -> Tuple[bool, Path]:
    dest_path = Path(dest_path)
    file_id = find_file_in_folder(service, folder_id, filename)
    if not file_id:
        return False, dest_path

    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(dest_path, "wb")
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        _, done = downloader.next_chunk()

    return True, dest_path
