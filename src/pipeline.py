from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

from object_store_io import S3Target, GCSTarget, s3_upload, gcs_upload
from master_sync import sync_master_s3, sync_master_gcs


# ArcGIS Feature Layer base URL
LAYER_URL = "https://alphagis.alpharetta.ga.us/arcgis/rest/services/OpenData/OpenData_PS_Full/FeatureServer/0"
def fetch_all_geojson(where: str = "1=1", batch_size: int = 2000) -> dict:
    features = []
    offset = 0

    while True:
        params = {
            "where": where,
            "outFields": "*",
            "outSR": 4326,
            "f": "geojson",                 # <-- force GeoJSON
            "resultOffset": offset,
            "resultRecordCount": batch_size,
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


def flatten_geojson(fc: dict) -> pd.DataFrame:
    rows = []
    for feat in fc.get("features", []):
        props = feat.get("properties") or {}
        geom = feat.get("geometry") or {}
        props["_geometry_json"] = json.dumps(geom, sort_keys=True)
        rows.append(props)
    return pd.DataFrame(rows)


def add_dedupe_key(df: pd.DataFrame) -> pd.DataFrame:
    """
    Priority:
      - Use GlobalID if present (best)
      - Else hash a stable representation of the row (including geometry)
    """
    df = df.copy()

    for col in ["GlobalID", "globalid", "GLOBALID"]:
        if col in df.columns:
            df["_dedupe_key"] = df[col].astype(str)
            return df

    # fallback hash across all columns
    cols = sorted(df.columns.tolist())

    def row_hash(row) -> str:
        payload = {c: row.get(c) for c in cols}
        s = json.dumps(payload, sort_keys=True, default=str)
        return hashlib.sha256(s.encode("utf-8")).hexdigest()

    df["_dedupe_key"] = df.apply(row_hash, axis=1)
    return df


def main():
    import os

    # Targets
    s3_target = S3Target(bucket=os.environ["S3_BUCKET"], prefix=os.environ.get("S3_PREFIX", "alphapd"))
    gcs_target = GCSTarget(bucket=os.environ["GCS_BUCKET"], prefix=os.environ.get("GCS_PREFIX", "alphapd"))

    # Local workspace
    work_root = Path("work")
    work_root.mkdir(parents=True, exist_ok=True)

    # 1) Fetch current rolling dataset
    fc = fetch_all_geojson(where="1=1")
    df_new = add_dedupe_key(flatten(fc))

    # 2) Write snapshot locally
    suffix = datetime.now().strftime("%Y%m%d")
    snap_name = f"AlphaPoliceEvent_{suffix}.geojson"
    snap_path = work_root / snap_name
    snap_path.write_text(json.dumps(fc), encoding="utf-8")

    # 3) Upload snapshot to BOTH stores
    snap_key = f"snapshots/{snap_name}"
    print(s3_upload(snap_path, s3_target, snap_key, content_type="application/geo+json"))
    print(gcs_upload(snap_path, gcs_target, snap_key, content_type="application/geo+json"))

    # 4) Independently update master on S3
    s3_result = sync_master_s3(df_new, workdir=work_root / "s3_master", target=s3_target)
    print(s3_result)

    # 5) Independently update master on GCS
    gcs_result = sync_master_gcs(df_new, workdir=work_root / "gcs_master", target=gcs_target)
    print(gcs_result)


if __name__ == "__main__":
    main()
