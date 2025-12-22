from __future__ import annotations

from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

from object_store_io import (
    S3Target,
    GCSTarget,
    s3_download_if_exists,
    s3_upload,
    gcs_download_if_exists,
    gcs_upload,
)

MASTER_PARQUET = "master/AlphaPoliceEvent_master.parquet"
MASTER_CSV = "master/AlphaPoliceEvent_master.csv"


def _ensure_dedupe(df_new: pd.DataFrame) -> pd.DataFrame:
    if "_dedupe_key" not in df_new.columns:
        raise ValueError("df_new must include '_dedupe_key' before syncing.")
    df = df_new.copy()
    df["_dedupe_key"] = df["_dedupe_key"].astype(str)
    return df


def _merge(df_new: pd.DataFrame, df_master: Optional[pd.DataFrame]) -> Tuple[pd.DataFrame, int]:
    df_new = _ensure_dedupe(df_new)

    if df_master is None or df_master.empty:
        return df_new, len(df_new)

    if "_dedupe_key" not in df_master.columns:
        # If an old master without dedupe exists, rebuild from new
        return df_new, len(df_new)

    df_master = df_master.copy()
    df_master["_dedupe_key"] = df_master["_dedupe_key"].astype(str)

    existing = set(df_master["_dedupe_key"])
    df_append = df_new[~df_new["_dedupe_key"].isin(existing)].copy()
    df_out = pd.concat([df_master, df_append], ignore_index=True)

    return df_out, len(df_append)


def sync_master_s3(df_new: pd.DataFrame, workdir: Path, target: S3Target) -> dict:
    """
    Check master on S3, update S3 master only (independent).
    """
    workdir = Path(workdir)
    workdir.mkdir(parents=True, exist_ok=True)

    local_parquet = workdir / "AlphaPoliceEvent_master.parquet"
    local_csv = workdir / "AlphaPoliceEvent_master.csv"

    existed, _ = s3_download_if_exists(target, MASTER_PARQUET, local_parquet)
    df_master = pd.read_parquet(local_parquet) if existed else None

    df_out, appended = _merge(df_new, df_master)

    df_out.to_parquet(local_parquet, index=False)
    df_out.to_csv(local_csv, index=False)

    parquet_uri = s3_upload(local_parquet, target, MASTER_PARQUET, content_type="application/octet-stream")
    csv_uri = s3_upload(local_csv, target, MASTER_CSV, content_type="text/csv")

    return {
        "store": "s3",
        "master_existed": existed,
        "appended": appended,
        "master_total": len(df_out),
        "parquet_uri": parquet_uri,
        "csv_uri": csv_uri,
    }


def sync_master_gcs(df_new: pd.DataFrame, workdir: Path, target: GCSTarget) -> dict:
    """
    Check master on GCS, update GCS master only (independent).
    """
    workdir = Path(workdir)
    workdir.mkdir(parents=True, exist_ok=True)

    local_parquet = workdir / "AlphaPoliceEvent_master.parquet"
    local_csv = workdir / "AlphaPoliceEvent_master.csv"

    existed, _ = gcs_download_if_exists(target, MASTER_PARQUET, local_parquet)
    df_master = pd.read_parquet(local_parquet) if existed else None

    df_out, appended = _merge(df_new, df_master)

    df_out.to_parquet(local_parquet, index=False)
    df_out.to_csv(local_csv, index=False)

    parquet_uri = gcs_upload(local_parquet, target, MASTER_PARQUET, content_type="application/octet-stream")
    csv_uri = gcs_upload(local_csv, target, MASTER_CSV, content_type="text/csv")

    return {
        "store": "gcs",
        "master_existed": existed,
        "appended": appended,
        "master_total": len(df_out),
        "parquet_uri": parquet_uri,
        "csv_uri": csv_uri,
    }
