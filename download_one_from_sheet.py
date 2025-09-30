#!/usr/bin/env python3
"""
Download last 30 activities for a single athlete looked up from a Google Sheet,
and merge them into the shared all_athletes dataset.

Environment variables required:
  - GOOGLE_SHEETS_JSON  (service-account JSON text)
  - SHEET_URL           (your sheet URL)
  - TARGET_ATHLETE_ID   (the Athlete ID string OR username to fetch; required)
  - STRAVA_CLIENT_ID    (for refresh->access exchange)
  - STRAVA_CLIENT_SECRET

Optional:
  - OUTPUT_DIR (default: ./strava_output)
"""

import os
import sys
import json
import sqlite3
from typing import Optional
from datetime import datetime

import requests
import pandas as pd

# ---------------------------
# Config / env
# ---------------------------
GOOGLE_SHEETS_JSON = os.environ.get("GOOGLE_SHEETS_JSON")
SHEET_URL = os.environ.get("SHEET_URL")
TARGET_ATHLETE_ID = os.environ.get("TARGET_ATHLETE_ID")
STRAVA_CLIENT_ID = os.environ.get("STRAVA_CLIENT_ID")
STRAVA_CLIENT_SECRET = os.environ.get("STRAVA_CLIENT_SECRET")

if not (GOOGLE_SHEETS_JSON and SHEET_URL and TARGET_ATHLETE_ID):
    print("ERROR: set GOOGLE_SHEETS_JSON, SHEET_URL and TARGET_ATHLETE_ID")
    sys.exit(2)

OUT_DIR = os.environ.get("OUTPUT_DIR", "strava_output")
os.makedirs(OUT_DIR, exist_ok=True)

# use the *shared* outputs instead of per-athlete
OUT_DB = os.path.join(OUT_DIR, "all_athletes_activities.db")
OUT_CSV = os.path.join(OUT_DIR, "all_athletes_activities.csv")
OUT_JSON = os.path.join(OUT_DIR, "all_athletes_activities.json")
OUT_SQL = os.path.join(OUT_DIR, "all_athletes_activities.sql")

API_URL = "https://www.strava.com/api/v3/athlete/activities"
PER_PAGE = 30
PAGE = 1

# ---------------------------
# Google Sheets read
# ---------------------------
def read_sheet_rows():
    creds = json.loads(GOOGLE_SHEETS_JSON)
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds, scope)
    client = gspread.authorize(credentials)
    sheet = client.open_by_url(SHEET_URL).sheet1
    return sheet.get_all_records()

# ---------------------------
# Strava token exchange
# ---------------------------
def exchange_refresh_for_access(refresh_token: str) -> Optional[str]:
    if not (STRAVA_CLIENT_ID and STRAVA_CLIENT_SECRET and refresh_token):
        return None
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
            data = r.json()
            return data.get("access_token")
        else:
            print(f"Token exchange failed: {r.status_code} {r.text}")
            return None
    except requests.RequestException as e:
        print("Token exchange error:", e)
        return None

# ---------------------------
# Fetch activities
# ---------------------------
def fetch_activities(access_token: str):
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"per_page": PER_PAGE, "page": PAGE}
    try:
        r = requests.get(API_URL, headers=headers, params=params, timeout=30)
    except requests.RequestException as e:
        print("Request error fetching activities:", e)
        return []
    if r.status_code == 200:
        return r.json()
    else:
        print("Failed to fetch activities:", r.status_code, r.text)
        return []

# ---------------------------
# Helpers
# ---------------------------
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
        "fetched_at_utc": datetime.utcnow().isoformat(),
    }

# ---------------------------
# Storage helpers (shared DB)
# ---------------------------
def ensure_db():
    conn = sqlite3.connect(OUT_DB)
    cur = conn.cursor()
    cur.execute(
        """
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
        """
    )
    conn.commit()
    conn.close()

def append_to_db(rows):
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
    to_insert = [(
        r.get("athlete_id"), r.get("athlete_name"), r.get("activity_id"),
        r.get("name"), r.get("type"), r.get("start_date_local"),
        r.get("start_date_utc"), r.get("distance_m"), r.get("distance_km"),
        r.get("moving_time_s"), r.get("elapsed_time_s"),
        r.get("total_elevation_gain_m"), r.get("average_speed_mps"),
        r.get("calories"), r.get("fetched_at_utc")
    ) for r in rows]
    cur.executemany(insert_sql, to_insert)
    conn.commit()
    conn.close()

def persist_csv_json_sql():
    conn = sqlite3.connect(OUT_DB)
    try:
        df = pd.read_sql_query("SELECT * FROM activities", conn)
        if df.empty:
            print("DB empty; nothing to export.")
            return
        df.drop_duplicates(subset=["activity_id"], inplace=True)
        for c in ("start_date_local", "start_date_utc", "fetched_at_utc"):
            if c in df.columns:
                df[c] = pd.to_datetime(df[c], errors="coerce")
        df.to_csv(OUT_CSV, index=False)
        df.to_json(OUT_JSON, orient="records", date_format="iso")


        print(f"Persisted CSV/JSON with {len(df)} unique activities.")

        with open(OUT_SQL, "w", encoding="utf-8") as fh:
            fh.write("-- SQL dump generated by script\n")
            fh.write("CREATE TABLE IF NOT EXISTS activities (\n"
                     "   athlete_id TEXT, athlete_name TEXT, activity_id INTEGER PRIMARY KEY, name TEXT, type TEXT,\n"
                     "   start_date_local TEXT, start_date_utc TEXT, distance_m REAL, distance_km REAL,\n"
                     "   moving_time_s INTEGER, elapsed_time_s INTEGER, total_elevation_gain_m REAL,\n"
                     "   average_speed_mps REAL, calories REAL, fetched_at_utc TEXT\n);")
            for _, row in df.iterrows():
                vals = [row.get(c) for c in df.columns]
                def fmt(v):
                    if v is None:
                        return "NULL"
                    if isinstance(v, (int, float)):
                        return str(v)
                    return "'" + str(v).replace("'", "''") + "'"
                fh.write("\nINSERT OR REPLACE INTO activities VALUES (" + ", ".join(fmt(x) for x in vals) + ");")
        print(f"Wrote SQL dump: {OUT_SQL}")
    finally:
        conn.close()

# ---------------------------
# Main
# ---------------------------
def main():
    rows = read_sheet_rows()

    target = TARGET_ATHLETE_ID.strip()
    found = None
    for r in rows:
        aid = str(_get_field(r, "Athlete ID", "AthleteID", "Athlete Id", "athlete id", "Athlete_Id", default="") or "").strip()
        uname = str(_get_field(r, "Username", "username", "user", default="") or "").strip()
        firstname = _get_field(r, "Firstname", "First Name", "First", "firstname", default="") or ""
        lastname = _get_field(r, "Lastname", "Last Name", "Last", "lastname", default="") or ""
        name = f"{firstname} {lastname}".strip()
        if aid == target or uname == target:
            found = {"row": r, "name": name or uname or aid, "athlete_id": aid or uname}
            break

    if not found:
        print("ERROR: target athlete not found in sheet")
        sys.exit(3)

    row = found["row"]
    athlete_id = found["athlete_id"]
    athlete_name = found["name"]

    access_token = _get_field(row, "Access Token", "AccessToken", "access token", "access_token")
    refresh_token = _get_field(row, "Refresh Token", "RefreshToken", "refresh token", "refresh_token")

    if refresh_token and STRAVA_CLIENT_ID and STRAVA_CLIENT_SECRET:
        new_access = exchange_refresh_for_access(refresh_token)
        if new_access:
            access_token = new_access

    if not access_token:
        print("ERROR: no access token available for athlete")
        sys.exit(4)

    acts = fetch_activities(access_token)
    flat = [flatten_activity(a, athlete_id, athlete_name) for a in acts]
    print(f"Fetched {len(flat)} activities for {athlete_name} ({athlete_id})")

    ensure_db()
    append_to_db(flat)
    persist_csv_json_sql()

if __name__ == "__main__":
    main()
