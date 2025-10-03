#!/usr/bin/env python3
"""
strava_upload_from_csv.py

Create activities on Strava from a CSV file (or upload GPX/TCX/FIT files).

Usage (examples):
  # Basic: read CSV and create manual activities
  GOOGLE_SHEETS_JSON='...' SHEET_URL='...' TARGET_ATHLETE_ID='...' \
  STRAVA_CLIENT_ID='...' STRAVA_CLIENT_SECRET='...' \
  STRAVA_ACCESS_TOKEN='YOUR_MANUAL_ACCESS_TOKEN' \
  python3 strava_upload_from_csv.py --csv /path/to/athlete_12345_20251003.csv

  # Or provide refresh token in CSV row or use env STRAVA_REFRESH_TOKEN to exchange
  python3 strava_upload_from_csv.py --csv ./strava_output/single_athlete_activities.csv

CSV format (expected columns; case-insensitive):
  - name (activity title)
  - type (Run, Ride, Walk, Hike, etc.)
  - start_date_local or start_date_utc (ISO 8601 string)
  - elapsed_time_s or elapsed_time (seconds; integer required)
  - distance_km or distance_m (distance; km or meters supported)
  - description (optional)
  - access_token (optional per-row access token — if present it will override env token)
  - refresh_token (optional per-row refresh token to exchange for access token)

Notes:
  - The script will try to exchange a refresh token for an access token if available
    (requires STRAVA_CLIENT_ID and STRAVA_CLIENT_SECRET env vars).
  - Creating manual activities requires the token to have scope: activity:write. See Strava docs.
    (Create Activity endpoint: POST /api/v3/activities). :contentReference[oaicite:3]{index=3}
  - If you want to upload GPX/TCX/FIT files (file-based), use --upload-dir and a "file" column
    in CSV or provide filenames; the Uploads endpoint is asynchronous. :contentReference[oaicite:4]{index=4}
"""
import os
import sys
import time
import json
import argparse
from typing import Optional

import requests
import pandas as pd
from datetime import datetime

# ---------------------------
# Config / env
# ---------------------------
STRAVA_CLIENT_ID = os.environ.get("STRAVA_CLIENT_ID")
STRAVA_CLIENT_SECRET = os.environ.get("STRAVA_CLIENT_SECRET")
# You can provide a default access token via env; per-row access_token in CSV overrides it.
DEFAULT_ACCESS_TOKEN = os.environ.get("STRAVA_ACCESS_TOKEN")
DEFAULT_REFRESH_TOKEN = os.environ.get("STRAVA_REFRESH_TOKEN")

STRAVA_API_BASE = "https://www.strava.com/api/v3"
CREATE_ACTIVITY_URL = STRAVA_API_BASE + "/activities"
UPLOADS_URL = STRAVA_API_BASE + "/uploads"
EXCHANGE_URL = "https://www.strava.com/oauth/token"

# ---------------------------
# Helpers
# ---------------------------
def exchange_refresh_for_access(refresh_token: str) -> Optional[str]:
    """Exchange a refresh token for an access token (returns access_token or None)."""
    if not (STRAVA_CLIENT_ID and STRAVA_CLIENT_SECRET and refresh_token):
        return None
    payload = {
        "client_id": STRAVA_CLIENT_ID,
        "client_secret": STRAVA_CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }
    try:
        r = requests.post(EXCHANGE_URL, data=payload, timeout=30)
        r.raise_for_status()
        data = r.json()
        return data.get("access_token")
    except requests.RequestException as e:
        print("Token exchange failed:", e, getattr(e, "response", None) and e.response.text)
        return None

def create_manual_activity(access_token: str, name: str, activity_type: str,
                           start_date_local: str, elapsed_time_s: int,
                           distance_m: Optional[float] = None,
                           description: Optional[str] = None,
                           commute: bool = False,
                           trainer: bool = False,
                           external_id: Optional[str] = None) -> Optional[dict]:
    """
    Create a manual activity via POST /activities.
    Returns the created activity JSON on success (HTTP 201) or None on failure.
    """
    headers = {"Authorization": f"Bearer {access_token}"}
    payload = {
        "name": name,
        "type": activity_type,
        "start_date_local": start_date_local,
        "elapsed_time": int(elapsed_time_s),
        "description": description or "",
        "trainer": 1 if trainer else 0,
        "commute": 1 if commute else 0,
    }
    if distance_m is not None:
        payload["distance"] = float(distance_m)
    if external_id:
        payload["external_id"] = external_id

    try:
        r = requests.post(CREATE_ACTIVITY_URL, headers=headers, data=payload, timeout=30)
    except requests.RequestException as e:
        print("Request error creating activity:", e)
        return None

    if r.status_code in (200, 201):
        print(f"Created activity: {name} -> id {r.json().get('id')}")
        return r.json()
    else:
        print(f"Failed to create activity '{name}': {r.status_code} {r.text}")
        return None

def upload_activity_file(access_token: str, file_path: str, data_type: str = "gpx",
                         name: Optional[str] = None, description: Optional[str] = None) -> Optional[dict]:
    """
    Upload a GPX/TCX/FIT file to Strava (POST /uploads).
    Returns the initial upload response which contains 'id' for polling.
    Note: processing is asynchronous — you'll need to poll GET /uploads/{upload_id} to check status.
    """
    headers = {"Authorization": f"Bearer {access_token}"}
    files = {"file": open(file_path, "rb")}
    data = {"data_type": data_type}
    if name:
        data["name"] = name
    if description:
        data["description"] = description

    try:
        r = requests.post(UPLOADS_URL, headers=headers, files=files, data=data, timeout=120)
    except requests.RequestException as e:
        print("Upload request error:", e)
        return None
    finally:
        files["file"].close()

    if r.status_code in (200, 201):
        print(f"Upload queued for {file_path}: {r.json()}")
        return r.json()
    else:
        print(f"Failed to upload {file_path}: {r.status_code} {r.text}")
        return None

def poll_upload_status(access_token: str, upload_id: int, poll_interval: int = 3, timeout: int = 120):
    """Poll GET /uploads/{upload_id} to wait until processing finishes (or times out)."""
    url = f"{UPLOADS_URL}/{upload_id}"
    headers = {"Authorization": f"Bearer {access_token}"}
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = requests.get(url, headers=headers, timeout=15)
            if r.status_code == 200:
                js = r.json()
                status = js.get("status")
                if status and status.lower() in ("your activity is ready", "ready"):
                    print("Upload processed successfully:", js)
                    return js
                elif status and status.lower().startswith("error"):
                    print("Upload processing error:", js)
                    return js
                else:
                    # still processing
                    print(f"Upload {upload_id} status: {status}; waiting...")
            else:
                print(f"Polling upload {upload_id}: HTTP {r.status_code} {r.text}")
        except requests.RequestException as e:
            print("Poll error:", e)
        time.sleep(poll_interval)
    print("Timed out waiting for upload processing")
    return None

# ---------------------------
# CSV helpers
# ---------------------------
def normalize_col(df: pd.DataFrame, col_names):
    """Return the first matching column name present in df (case-insensitive), or None."""
    lower_map = {c.lower(): c for c in df.columns}
    for name in col_names:
        if name.lower() in lower_map:
            return lower_map[name.lower()]
    return None

def value_from_row(row, df_col):
    return None if df_col is None else row.get(df_col)

# ---------------------------
# Main CLI
# ---------------------------
def main():
    p = argparse.ArgumentParser(description="Create Strava activities from CSV (or upload files).")
    p.add_argument("--csv", required=True, help="Path to CSV file containing activities to create/upload")
    p.add_argument("--upload-dir", help="If present, treat 'file' column values as relative to this dir and upload files")
    p.add_argument("--poll-uploads", action="store_true", help="If set, poll upload status after posting file uploads")
    args = p.parse_args()

    csv_path = args.csv
    if not os.path.exists(csv_path):
        print("CSV not found:", csv_path)
        sys.exit(2)

    df = pd.read_csv(csv_path)
    if df.empty:
        print("CSV is empty:", csv_path)
        sys.exit(0)

    # Identify columns (flexible names)
    col_name = normalize_col(df, ["name", "activity_name", "title"])
    col_type = normalize_col(df, ["type", "activity_type"])
    col_start_local = normalize_col(df, ["start_date_local", "start_date_utc", "start_date"])
    col_elapsed = normalize_col(df, ["elapsed_time_s", "elapsed_time", "elapsed_time_seconds"])
    col_distance_km = normalize_col(df, ["distance_km", "distance_km_rounded", "distance"])
    col_distance_m = normalize_col(df, ["distance_m", "distance_meters"])
    col_description = normalize_col(df, ["description", "notes"])
    col_access = normalize_col(df, ["access_token", "access token", "access"])
    col_refresh = normalize_col(df, ["refresh_token", "refresh token"])
    col_file = normalize_col(df, ["file", "gpx_file", "fit_file", "tcx_file"])

    for idx, row in df.iterrows():
        # Determine access token for this row
        row_access = value_from_row(row, col_access) or DEFAULT_ACCESS_TOKEN
        row_refresh = value_from_row(row, col_refresh) or DEFAULT_REFRESH_TOKEN

        if not row_access and row_refresh and STRAVA_CLIENT_ID and STRAVA_CLIENT_SECRET:
            print("Exchanging refresh token for access token (row)", idx)
            row_access = exchange_refresh_for_access(row_refresh)

        if not row_access:
            print("Skipping row", idx, "- no access token available (provide access_token or refresh_token).")
            continue

        name = value_from_row(row, col_name) or f"Activity {idx}"
        act_type = value_from_row(row, col_type) or "Run"
        start_date = value_from_row(row, col_start_local) or datetime.utcnow().isoformat()
        elapsed = value_from_row(row, col_elapsed)
        if elapsed is None or (str(elapsed).strip() == ""):
            print(f"Skipping row {idx} ('{name}') because elapsed time is missing.")
            continue

        # distance handling (km or m)
        distance_m = None
        if value_from_row(row, col_distance_m) is not None:
            try:
                distance_m = float(value_from_row(row, col_distance_m))
            except Exception:
                distance_m = None
        elif value_from_row(row, col_distance_km) is not None:
            try:
                distance_m = float(value_from_row(row, col_distance_km)) * 1000.0
            except Exception:
                distance_m = None

        description = value_from_row(row, col_description) or ""

        # If file column exists and --upload-dir provided, upload file
        if col_file and args.upload_dir:
            fname = str(value_from_row(row, col_file)).strip()
            if not fname:
                print(f"Row {idx}: file column empty, skipping upload.")
                continue
            file_path = os.path.join(args.upload_dir, fname)
            if not os.path.exists(file_path):
                print(f"Row {idx}: file not found: {file_path}; skipping.")
                continue
            # infer data_type from extension
            ext = os.path.splitext(file_path)[1].lower()
            data_type = "gpx"
            if ext.endswith("fit"):
                data_type = "fit"
            elif ext.endswith("tcx"):
                data_type = "tcx"
            elif ext.endswith("gpx"):
                data_type = "gpx"
            print(f"Uploading file for row {idx}: {file_path} data_type={data_type}")
            up_resp = upload_activity_file(row_access, file_path, data_type=data_type,
                                           name=name, description=description)
            if up_resp and args.poll_uploads:
                upload_id = up_resp.get("id") or up_resp.get("upload_id") or up_resp.get("external_id")
                if upload_id:
                    poll_upload_status(row_access, upload_id)
            continue

        # Otherwise create manual activity
        print(f"Creating manual activity for row {idx}: {name} ({act_type})")
        created = create_manual_activity(
            row_access,
            name=name,
            activity_type=act_type,
            start_date_local=start_date,
            elapsed_time_s=int(elapsed),
            distance_m=distance_m,
            description=description
        )
        # Sleep briefly to avoid hitting rate limits
        time.sleep(0.5)

if __name__ == "__main__":
    main()
