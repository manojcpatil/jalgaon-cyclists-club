#!/usr/bin/env python3
"""
fetch_strava_activities.py

Download last 30 activities for ALL athletes listed in a Google Sheet,
with 2s delay between athletes, auto-save rotated refresh_tokens and
fill missing Athlete ID / name by calling Strava /athlete endpoint.

Env required:
  - GOOGLE_SHEETS_JSON (service-account JSON text)
  - SHEET_URL
  - STRAVA_CLIENT_ID
  - STRAVA_CLIENT_SECRET

Optional:
  - OUTPUT_DIR (default ./strava_output)
"""

from __future__ import annotations
import os
import sys
import json
import time
import random
import sqlite3
from typing import Optional, List, Dict
from datetime import datetime

try:
    import requests
    import pandas as pd
except Exception as e:
    print("Missing runtime dependency:", e)
    print("Please pip install requests pandas")
    raise

# ---------------------
# Config / env
# ---------------------
GOOGLE_SHEETS_JSON = os.environ.get("GOOGLE_SHEETS_JSON")
SHEET_URL = os.environ.get("SHEET_URL")
STRAVA_CLIENT_ID = os.environ.get("STRAVA_CLIENT_ID")
STRAVA_CLIENT_SECRET = os.environ.get("STRAVA_CLIENT_SECRET")

if not (GOOGLE_SHEETS_JSON and SHEET_URL and STRAVA_CLIENT_ID and STRAVA_CLIENT_SECRET):
    print("ERROR: set GOOGLE_SHEETS_JSON, SHEET_URL, STRAVA_CLIENT_ID and STRAVA_CLIENT_SECRET")
    sys.exit(2)

OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "strava_output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

OUT_CSV = os.path.join(OUTPUT_DIR, "all_athletes_activities.csv")
OUT_JSON = os.path.join(OUTPUT_DIR, "all_athletes_activities.json")
OUT_DB = os.path.join(OUTPUT_DIR, "all_athletes_activities.db")
OUT_SQL = os.path.join(OUTPUT_DIR, "all_athletes_activities.sql")

API_ACTIVITIES = "https://www.strava.com/api/v3/athlete/activities"
API_ATHLETE = "https://www.strava.com/api/v3/athlete"
PER_PAGE = 5
PAGE = 1

# Delay fixed at 2 seconds
DELAY_MIN = 1.0
DELAY_MAX = 1.5

# ---------------------
# Google Sheets helpers
# ---------------------
def init_sheet_client():
    """
    Initialize gspread client using service account JSON stored in GOOGLE_SHEETS_JSON env var.
    """
    try:
        creds = json.loads(GOOGLE_SHEETS_JSON)
    except Exception as e:
        print("ERROR: Invalid GOOGLE_SHEETS_JSON:", e)
        raise

    try:
        import gspread
        from oauth2client.service_account import ServiceAccountCredentials
    except Exception:
        print("Missing gspread/oauth2client; please pip install gspread oauth2client")
        raise

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds, scope)
    client = gspread.authorize(credentials)
    sheet = client.open_by_url(SHEET_URL).sheet1
    return sheet

def read_sheet_rows_and_headers(sheet):
    headers = sheet.row_values(1)
    rows = sheet.get_all_records()
    return rows, headers

def find_col_index(headers, name_variants):
    for i, h in enumerate(headers, start=1):
        for v in name_variants:
            if h is None:
                continue
            try:
                if h.strip().lower() == v.strip().lower():
                    return i
            except Exception:
                continue
    return None

def update_sheet_cell(sheet, sheet_row_num, col_idx, value):
    if col_idx is None:
        return
    try:
        sheet.update_cell(sheet_row_num, col_idx, value)
        print(f"  ↳ Updated sheet row {sheet_row_num} col {col_idx} -> {value}")
    except Exception as e:
        print(f"  ⚠ Failed to update sheet row {sheet_row_num} col {col_idx}: {e}")

# ---------------------
# Strava helpers
# ---------------------
def exchange_refresh_for_access(refresh_token: str) -> Optional[dict]:
    url = "https://www.strava.com/oauth/token"
    payload = {
        "client_id": STRAVA_CLIENT_ID,
        "client_secret": STRAVA_CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }
    try:
        r = requests.post(url, data=payload, timeout=30)
        if r.status_code == 200:
            return r.json()
        else:
            print(f"Token exchange failed: {r.status_code} {r.text}")
            return None
    except requests.RequestException as e:
        print("Token exchange error:", e)
        return None

def fetch_activities(access_token: str) -> List[Dict]:
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"per_page": PER_PAGE, "page": PAGE}
    try:
        r = requests.get(API_ACTIVITIES, headers=headers, params=params, timeout=30)
    except requests.RequestException as e:
        print("Request error fetching activities:", e)
        return []
    if r.status_code == 200:
        try:
            return r.json()
        except Exception as e:
            print("Failed to parse activities JSON:", e)
            return []
    else:
        print(f"Fetch activities failed: {r.status_code} {r.text}")
        return []

def fetch_athlete_profile(access_token: str) -> Optional[Dict]:
    headers = {"Authorization": f"Bearer {access_token}"}
    try:
        r = requests.get(API_ATHLETE, headers=headers, timeout=20)
    except requests.RequestException as e:
        print("Request error fetching athlete profile:", e)
        return None
    if r.status_code == 200:
        try:
            return r.json()
        except Exception as e:
            print("Failed to parse athlete profile JSON:", e)
            return None
    else:
        print(f"Fetch athlete profile failed: {r.status_code} {r.text}")
        return None

# ---------------------
# DB / storage
# ---------------------
def ensure_db():
    conn = sqlite3.connect(OUT_DB)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS activities (
       athlete_id TEXT,
       athlete_name TEXT,
       activity_id INTEGER PRIMARY KEY,
       name TEXT,
       type TEXT,
       start_date_local TEXT,
       start_date_utc TEXT,
       distance_m REAL,
       distance_km REAL,
       moving_time_s INTEGER,
       elapsed_time_s INTEGER,
       total_elevation_gain_m REAL,
       average_speed_mps REAL,
       calories REAL,
       fetched_at_utc TEXT
    );
    """)
    conn.commit()
    conn.close()

def append_to_db(rows: List[dict]):
    if not rows:
        return
    conn = sqlite3.connect(OUT_DB)
    cur = conn.cursor()
    insert_sql = """INSERT OR REPLACE INTO activities (
       athlete_id, athlete_name, activity_id, name, type,
       start_date_local, start_date_utc, distance_m, distance_km,
       moving_time_s, elapsed_time_s, total_elevation_gain_m,
       average_speed_mps, calories, fetched_at_utc
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"""
    to_insert = []
    for r in rows:
        to_insert.append((
            r.get("athlete_id"),
            r.get("athlete_name"),
            r.get("activity_id"),
            r.get("name"),
            r.get("type"),
            r.get("start_date_local"),
            r.get("start_date_utc"),
            r.get("distance_m"),
            r.get("distance_km"),
            r.get("moving_time_s"),
            r.get("elapsed_time_s"),
            r.get("total_elevation_gain_m"),
            r.get("average_speed_mps"),
            r.get("calories"),
            r.get("fetched_at_utc")
        ))
    cur.executemany(insert_sql, to_insert)
    conn.commit()
    conn.close()

def persist_csv_json():
    conn = sqlite3.connect(OUT_DB)
    try:
        df = pd.read_sql_query("SELECT * FROM activities", conn)
        if not df.empty:
            df.drop_duplicates(subset=["activity_id"], inplace=True)
            for c in ("start_date_local", "start_date_utc", "fetched_at_utc"):
                if c in df.columns:
                    df[c] = pd.to_datetime(df[c], errors="coerce")
            df.to_csv(OUT_CSV, index=False)
            df.to_json(OUT_JSON, orient="records", date_format="iso")
            print(f"Persisted CSV/JSON with {len(df)} unique activities.")
        else:
            print("DB empty; nothing to write for CSV/JSON yet.")
    finally:
        conn.close()

def write_sql_dump():
    conn = sqlite3.connect(OUT_DB)
    try:
        df = pd.read_sql_query("SELECT * FROM activities", conn)
        with open(OUT_SQL, "w", encoding="utf-8") as fh:
            fh.write("-- SQL dump generated by script\n")
            fh.write(
                "CREATE TABLE IF NOT EXISTS activities (\n"
                "   athlete_id TEXT, athlete_name TEXT, activity_id INTEGER PRIMARY KEY, name TEXT, type TEXT,\n"
                "   start_date_local TEXT, start_date_utc TEXT, distance_m REAL, distance_km REAL,\n"
                "   moving_time_s INTEGER, elapsed_time_s INTEGER, total_elevation_gain_m REAL,\n"
                "   average_speed_mps REAL, calories REAL, fetched_at_utc TEXT\n"
                ");\n"
            )
            def fmt(v):
                if pd.isna(v):
                    return "NULL"
                if v is None:
                    return "NULL"
                if isinstance(v, (int, float)):
                    return str(v)
                escaped = str(v).replace("'", "''")
                return "'" + escaped + "'"
            for _, row in df.iterrows():
                vals = [row.get(c) for c in df.columns]
                fh.write("INSERT OR REPLACE INTO activities VALUES (" + ", ".join(fmt(x) for x in vals) + ");\n")
    finally:
        conn.close()
    print(f"Wrote SQL dump: {OUT_SQL}")

# ---------------------
# Utilities
# ---------------------
def _get_field(row: dict, *variants, default=None):
    for v in variants:
        if v in row and row[v] not in (None, ""):
            return row[v]
    return default

def flatten_activity(act: dict, athlete_id: str, athlete_name: str) -> dict:
    return {
        "athlete_id": athlete_id,
        "athlete_name": athlete_name,
        "activity_id": act.get("id"),
        "name": act.get("name"),
        "type": act.get("type"),
        "start_date_local": act.get("start_date_local"),
        "start_date_utc": act.get("start_date"),
        "distance_m": act.get("distance"),
        "distance_km": (act.get("distance") or 0) / 1000.0,
        "moving_time_s": act.get("moving_time"),
        "elapsed_time_s": act.get("elapsed_time"),
        "total_elevation_gain_m": act.get("total_elevation_gain"),
        "average_speed_mps": act.get("average_speed"),
        "calories": act.get("calories"),
        "fetched_at_utc": datetime.utcnow().isoformat()
    }

# ---------------------
# Main loop
# ---------------------
def main():
    sheet = None
    try:
        sheet = init_sheet_client()
    except Exception as e:
        print("ERROR initializing Google Sheets client:", e)
        sys.exit(2)

    rows, headers = read_sheet_rows_and_headers(sheet)
    total = len(rows)
    print(f"Sheet loaded: {total} rows. Headers: {headers}")

    # map header columns for quick writeback
    col_idx_refresh = find_col_index(headers, ["Refresh Token", "RefreshToken", "refresh token", "refresh_token"])
    col_idx_access = find_col_index(headers, ["Access Token", "AccessToken", "access token", "access_token"])
    col_idx_aid = find_col_index(headers, ["Athlete ID", "AthleteID", "Athlete Id", "athlete id", "Athlete_Id"])
    col_idx_fname = find_col_index(headers, ["Firstname", "First Name", "First", "firstname"])
    col_idx_lname = find_col_index(headers, ["Lastname", "Last Name", "Last", "lastname"])
    col_idx_username = find_col_index(headers, ["Username", "username", "user"])

    ensure_db()
    all_fetched = 0

    for idx, r in enumerate(rows):
        sheet_row_num = idx + 2

        athlete_id = str(_get_field(r, "Athlete ID", "AthleteID", "Athlete Id", default="") or "").strip()
        username = str(_get_field(r, "Username", "username", default="") or "").strip()
        firstname = _get_field(r, "Firstname", "First Name", default="") or ""
        lastname = _get_field(r, "Lastname", "Last Name", default="") or ""
        athlete_name = f"{firstname} {lastname}".strip() or username or athlete_id or f"row-{idx}"

        access_token = _get_field(r, "Access Token", "AccessToken", "access token", default=None)
        refresh_token = _get_field(r, "Refresh Token", "RefreshToken", "refresh token", default=None)

        print(f"\n[{idx+1}/{total}] Processing athlete row {sheet_row_num}: {athlete_name} (id={athlete_id})")

        token_json = None
        if refresh_token:
            token_json = exchange_refresh_for_access(refresh_token)
            if token_json and token_json.get("access_token"):
                access_token = token_json.get("access_token")
                # persist rotated refresh token back to sheet if it changed
                if token_json.get("refresh_token") and token_json.get("refresh_token") != refresh_token:
                    try:
                        update_sheet_cell(sheet, sheet_row_num, col_idx_refresh, token_json.get("refresh_token"))
                        refresh_token = token_json.get("refresh_token")
                    except Exception as e:
                        print(f"  ⚠ Failed to persist rotated refresh token: {e}")

        if not access_token:
            print(" ⚠ No access token available for this athlete. Skipping.")
            continue

        if not athlete_id:
            profile = fetch_athlete_profile(access_token)
            if profile:
                try:
                    new_aid = str(profile.get("id"))
                except Exception:
                    new_aid = ""
                new_fname = profile.get("firstname")
                new_lname = profile.get("lastname")
                new_uname = profile.get("username") or profile.get("profile") or ""
                if new_aid:
                    update_sheet_cell(sheet, sheet_row_num, col_idx_aid, new_aid)
                    athlete_id = new_aid
                if new_fname and col_idx_fname:
                    update_sheet_cell(sheet, sheet_row_num, col_idx_fname, new_fname)
                    firstname = new_fname
                if new_lname and col_idx_lname:
                    update_sheet_cell(sheet, sheet_row_num, col_idx_lname, new_lname)
                    lastname = new_lname
                if new_uname and col_idx_username:
                    update_sheet_cell(sheet, sheet_row_num, col_idx_username, new_uname)
                    username = new_uname
                athlete_name = f"{firstname} {lastname}".strip() or username or athlete_id

        acts = fetch_activities(access_token)
        if not isinstance(acts, list):
            print(" ⚠ Unexpected activities response; skipping athlete.")
            continue

        flat = [flatten_activity(a, athlete_id or username or f"row-{idx}", athlete_name) for a in acts]
        append_to_db(flat)
        fetched_count = len(flat)
        all_fetched += fetched_count
        print(f" ✅ Fetched {fetched_count} activities for {athlete_name} (total fetched so far: {all_fetched})")

        if token_json and token_json.get("access_token") and col_idx_access:
            try:
                update_sheet_cell(sheet, sheet_row_num, col_idx_access, token_json.get("access_token"))
            except Exception:
                pass

        # persist after every athlete
        try:
            persist_csv_json()
            write_sql_dump()
        except Exception as e:
            print("⚠ Error while persisting files:", e)

        # delay before next athlete
        if idx < total - 1:
            delay = random.uniform(DELAY_MIN, DELAY_MAX)
            print(f"⏳ Sleeping {delay:.1f}s before next athlete...")
            time.sleep(delay)

    print(f"\nDone. Processed {len(rows)} athletes, fetched {all_fetched} activities total.")

if __name__ == "__main__":
    main()
