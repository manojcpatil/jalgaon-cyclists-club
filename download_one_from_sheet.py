#!/usr/bin/env python3
"""
Download last 30 activities for a single athlete looked up from a Google Sheet.

Environment variables required:
  - GOOGLE_SHEETS_JSON  (service-account JSON text)
  - SHEET_URL           (your sheet URL)
  - TARGET_ATHLETE_ID   (the Athlete ID string OR username to fetch; required)
  - STRAVA_CLIENT_ID    (for refresh->access exchange)
  - STRAVA_CLIENT_SECRET
Optional:
  - OUTPUT_DIR (default: ./strava_output)
"""

import os, sys, json, sqlite3, time, requests
import pandas as pd
from typing import Optional
from datetime import datetime

# Config
GOOGLE_SHEETS_JSON = os.environ.get("GOOGLE_SHEETS_JSON")
SHEET_URL = os.environ.get("SHEET_URL")
TARGET_ATHLETE_ID = os.environ.get("TARGET_ATHLETE_ID")
STRAVA_CLIENT_ID = os.environ.get("STRAVA_CLIENT_ID")
STRAVA_CLIENT_SECRET = os.environ.get("STRAVA_CLIENT_SECRET")

if not (GOOGLE_SHEETS_JSON and SHEET_URL and TARGET_ATHLETE_ID):
    print("ERROR: set GOOGLE_SHEETS_JSON, SHEET_URL and TARGET_ATHLETE_ID"); sys.exit(2)

OUT_DIR = os.environ.get("OUTPUT_DIR", "strava_output")
os.makedirs(OUT_DIR, exist_ok=True)
OUT_CSV = os.path.join(OUT_DIR, f"athlete_{TARGET_ATHLETE_ID}_activities.csv")
OUT_JSON = os.path.join(OUT_DIR, f"athlete_{TARGET_ATHLETE_ID}_activities.json")
OUT_DB  = os.path.join(OUT_DIR, f"athlete_{TARGET_ATHLETE_ID}_activities.db")
OUT_SQL = os.path.join(OUT_DIR, f"athlete_{TARGET_ATHLETE_ID}_activities.sql")

API_URL = "https://www.strava.com/api/v3/athlete/activities"
PER_PAGE = 30

# Google Sheets read
def read_sheet_rows():
    creds = json.loads(GOOGLE_SHEETS_JSON)
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds, scope)
    client = gspread.authorize(credentials)
    sheet = client.open_by_url(SHEET_URL).sheet1
    # get as list of dicts using headers
    rows = sheet.get_all_records()
    return rows

# Token exchange
def exchange_refresh_for_access(refresh_token: str) -> Optional[str]:
    if not (STRAVA_CLIENT_ID and STRAVA_CLIENT_SECRET and refresh_token):
        return None
    url = "https://www.strava.com/oauth/token"
    payload = {
        "client_id": STRAVA_CLIENT_ID,
        "client_secret": STRAVA_CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token
    }
    try:
        r = requests.post(url, data=payload, timeout=30)
        if r.status_code == 200:
            data = r.json()
            # NOTE: Strava may return a rotated refresh token in data["refresh_token"]
            if data.get("refresh_token"):
                print("🔁 Strava returned a new refresh token — consider saving it in your sheet/secrets.")
            return data.get("access_token")
        else:
            print("Token exchange failed:", r.status_code, r.text)
            return None
    except requests.RequestException as e:
        print("Token exchange error:", e)
        return None

# Fetch last PER_PAGE activities
def fetch_activities(access_token: str):
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"per_page": PER_PAGE, "page": 1}
    r = requests.get(API_URL, headers=headers, params=params, timeout=30)
    if r.status_code == 200:
        return r.json()
    else:
        print("Failed to fetch activities:", r.status_code, r.text)
        return []

# Flatten activity for storage
def flatten_activity(act, athlete_id, athlete_name):
    return {
        "athlete_id": athlete_id,
        "athlete_name": athlete_name,
        "activity_id": act.get("id"),
        "name": act.get("name"),
        "type": act.get("type"),
        "start_date_local": act.get("start_date_local"),
        "start_date_utc": act.get("start_date"),
        "distance_m": act.get("distance"),
        "distance_km": (act.get("distance") or 0)/1000.0,
        "moving_time_s": act.get("moving_time"),
        "elapsed_time_s": act.get("elapsed_time"),
        "total_elevation_gain_m": act.get("total_elevation_gain"),
        "average_speed_mps": act.get("average_speed"),
        "calories": act.get("calories"),
    }

def save_outputs(rows):
    df = pd.DataFrame(rows)
    for c in ("start_date_local","start_date_utc"):
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    df.to_csv(OUT_CSV, index=False)
    df.to_json(OUT_JSON, orient="records", date_format="iso")
    # sqlite
    conn = sqlite3.connect(OUT_DB)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS activities (
       athlete_id TEXT, athlete_name TEXT, activity_id INTEGER PRIMARY KEY, name TEXT, type TEXT,
       start_date_local TEXT, start_date_utc TEXT, distance_m REAL, distance_km REAL,
       moving_time_s INTEGER, elapsed_time_s INTEGER, total_elevation_gain_m REAL,
       average_speed_mps REAL, calories REAL
    );
    """)
    insert_sql = """INSERT OR REPLACE INTO activities (
       athlete_id, athlete_name, activity_id, name, type, start_date_local, start_date_utc,
       distance_m, distance_km, moving_time_s, elapsed_time_s, total_elevation_gain_m,
       average_speed_mps, calories
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
    to_insert = []
    for r in rows:
        to_insert.append((
            r.get("athlete_id"), r.get("athlete_name"), r.get("activity_id"),
            r.get("name"), r.get("type"), str(r.get("start_date_local")), str(r.get("start_date_utc")),
            r.get("distance_m"), r.get("distance_km"), r.get("moving_time_s"), r.get("elapsed_time_s"),
            r.get("total_elevation_gain_m"), r.get("average_speed_mps"), r.get("calories")
        ))
    cur.executemany(insert_sql, to_insert)
    conn.commit()
    conn.close()

    # SQL dump
    with open(OUT_SQL, "w", encoding="utf-8") as fh:
        fh.write("-- SQL dump\n")
        fh.write("CREATE TABLE IF NOT EXISTS activities (...);\n")  # short header
        for r in rows:
            vals = [
                r.get("athlete_id"), r.get("athlete_name"), r.get("activity_id"), r.get("name"),
                r.get("type"), r.get("start_date_local"), r.get("start_date_utc"), r.get("distance_m"),
                r.get("distance_km"), r.get("moving_time_s"), r.get("elapsed_time_s"),
                r.get("total_elevation_gain_m"), r.get("average_speed_mps"), r.get("calories")
            ]
            def fmt(v):
                if v is None:
                    return "NULL"
                if isinstance(v, (int,float)):
                    return str(v)
                return f"'{str(v).replace(\"'\",\"''\")}'"
            fh.write("INSERT OR REPLACE INTO activities VALUES (" + ", ".join(fmt(x) for x in vals) + ");\n")

    print(f"Saved: {OUT_CSV}, {OUT_JSON}, {OUT_DB}, {OUT_SQL}")

def main():
    rows = read_sheet_rows()
    # try to match by Athlete ID (allow numeric or string), fallback to Username
    target = TARGET_ATHLETE_ID.strip()
    found = None
    for r in rows:
        # possible header names in your sheet: "Athlete ID", "Username", "Firstname", "Lastname", "Access Token", "Refresh Token"
        aid = str(r.get("Athlete ID") or r.get("AthleteID") or "").strip()
        uname = str(r.get("Username") or r.get("username") or "").strip()
        name = f"{r.get('Firstname') or ''} {r.get('Lastname') or ''}".strip()
        if aid == target or uname == target:
            found = {"row": r, "name": name, "athlete_id": aid or uname}
            break
    if not found:
        print("ERROR: target athlete not found in sheet"); sys.exit(3)

    row = found["row"]
    athlete_id = found["athlete_id"]
    athlete_name = found["name"] or athlete_id

    access_token = row.get("Access Token") or row.get("access token")
    refresh_token = row.get("Refresh Token") or row.get("refresh token")

    # Prefer exchanging refresh token for a fresh access token if available
    if refresh_token and STRAVA_CLIENT_ID and STRAVA_CLIENT_SECRET:
        new_access = exchange_refresh_for_access(refresh_token)
        if new_access:
            access_token = new_access

    if not access_token:
        print("ERROR: no access token available for athlete"); sys.exit(4)

    acts = fetch_activities(access_token)
    flat = [flatten_activity(a, athlete_id, athlete_name) for a in acts]
    print(f"Fetched {len(flat)} activities for {athlete_name} ({athlete_id})")
    save_outputs(flat)

if __name__ == "__main__":
    main()
