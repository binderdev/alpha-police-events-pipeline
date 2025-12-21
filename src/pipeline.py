from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

from drive_io import (
    build_drive_service,
    download_file_if_exists,
    get_or_create_folder,
    upload_or_update_file,
)

# Your ArcGIS layer (base URL)
LAYER_URL = "https://alphagis.alpharetta.ga.us/arcgis/rest/services/OpenData/OpenData_PS_Full/FeatureServer/1"


def fetch_all_features_geojson(where: str = "1=1", batch_size: int = 2000) -> dict:
    """
    Fetch all features with pagination. Attempts GeoJSON first.
    """
    features = []
    offset = 0

    while True:
        params = {
            "where": where,
            "outFields": "*",
            "outSR": 4326,
            "resultOffset": offset,
            "resultRecordCount": batch_size,
            "f": "geojson",
        }
        r = requests.get(f"{LAYER_URL}/query", params=params, timeout=60)
        r.raise_for_status()
        data = r.json()

        batch = data.get("features", [])
        features.extend(batch)

        if len(batch) < batch_size:
            break
        offset += batch_size

    return {"type": "FeatureCollection", "features": features}


def flatten_featurecollection(fc: dict) -> pd.DataFrame:
    rows = []
    for feat in fc.get("features", []):
        props = feat.get("properties") or {}
        geom = feat.get("geometry") or {}
        props["_geometry_json"] = json.dumps(geom, sort_keys=True)
        rows.append(props)
    return pd.DataFrame(rows)


def choose_dedupe_key(df: pd.DataFrame) -> pd.Series:
    """
    Priority:
      1) GlobalID (or variations) if present
      2) Otherwise, stable hash across all columns except obvious volatile/system ones
    """
    # Common ArcGIS GlobalID naming variants
    for col in ["GlobalID", "globalid", "GLOBALID"]:
        if col in df.columns:
            return df[col].astype(str)

    # If you later find a better ID (case number, incident id), add it here.

    # Fallback: hash row content excluding likely-volatile fields
    exclude = set([
        "OBJECTID", "ObjectId", "objectid",
        "_dedupe_key",
    ])

    cols = [c for c in df.columns if c not in exclude]
    cols_sorted = sorted(cols)

    def row_hash(row) -> str:
        payload = {c: row.get(c) for c in cols_sorted}
        s = json.dumps(payload, sort_keys=True, default=str)
        return hashlib.sha256(s.encode("utf-8")).hexdigest()

    return df.apply(row_hash, axis=1)


def main():
    # --- Drive auth from environment (GitHub Secrets) ---
    import os

    sa_json = os.environ["GDRIVE_SA_JSON"]
    parent_folder_id = os.environ["GDRIVE_PARENT_FOLDER_ID"]

    service = build_drive_service(sa_json)

    # Ensure folders exist
    snapshots_folder_id = get_or_create_folder(service, parent_folder_id, "snapshots")
    master_folder_id = get_or_create_folder(service, parent_folder_id, "master")

    # Local temp workspace inside repo (GitHub runner)
    root = Path.cwd()
    tmp = root / "data"
    tmp.mkdir(parents=True, exist_ok=True)

    # --- 1) Fetch rolling 28-day data ---
    fc = fetch_all_features_geojson(where="1=1")
    df_new = flatten_featurecollection(fc)

    # Snapshot filename (YYYYMMDD)
    suffix = datetime.now().strftime("%Y%m%d")
    snap_name = f"AlphaPoliceEvent_{suffix}.geojson"
    snap_path = tmp / snap_name
    snap_path.write_text(json.dumps(fc), encoding="utf-8")

    # Upload snapshot
    upload_or_update_file(
        service=service,
        folder_id=snapshots_folder_id,
        local_path=snap_path,
        filename=snap_name,
        mime_type="application/geo+json",
    )

    # --- 2) Load master parquet (if exists) ---
    master_parquet_name = "AlphaPoliceEvent_master.parquet"
    master_csv_name = "AlphaPoliceEvent_master.csv"
    local_master_parquet = tmp / master_parquet_name

    exists, _ = download_file_if_exists(
        service=service,
        folder_id=master_folder_id,
        filename=master_parquet_name,
        dest_path=local_master_parquet,
    )

    if exists:
        df_master = pd.read_parquet(local_master_parquet)
    else:
        df_master = pd.DataFrame()

    # --- 3) Dedup + append only new ---
    df_new = df_new.copy()
    df_new["_dedupe_key"] = choose_dedupe_key(df_new).astype(str)

    if not df_master.empty and "_dedupe_key" in df_master.columns:
        existing = set(df_master["_dedupe_key"].astype(str))
        df_append = df_new[~df_new["_dedupe_key"].isin(existing)].copy()
        df_out = pd.concat([df_master, df_append], ignore_index=True)
    else:
        # First run: build master from current pull
        df_out = df_new
        df_append = df_new

    # --- 4) Write master parquet + master csv locally ---
    df_out.to_parquet(local_master_parquet, index=False)

    local_master_csv = tmp / master_csv_name
    df_out.to_csv(local_master_csv, index=False)

    # --- 5) Upload both master files ---
    upload_or_update_file(
        service=service,
        folder_id=master_folder_id,
        local_path=local_master_parquet,
        filename=master_parquet_name,
        mime_type="application/octet-stream",
    )

    upload_or_update_file(
        service=service,
        folder_id=master_folder_id,
        local_path=local_master_csv,
        filename=master_csv_name,
        mime_type="text/csv",
    )

    print(f"Snapshot uploaded: {snap_name}")
    print(f"New rows appended: {len(df_append)}")
    print(f"Master rows total: {len(df_out)}")


if __name__ == "__main__":
    main()
