#!/usr/bin/env python3
"""
Strava batch extractor with rate-limit-safe requests, batching, resume/checkpoint logic,
AND Excel/JSON append + dedupe by Activity_ID.

Set environment variables:
 - GOOGLE_SHEETS_JSON, SHEET_URL
 - STRAVA_CLIENT_ID, STRAVA_CLIENT_SECRET
 - DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME (optional)
 - BATCH_SIZE, STRAVA_PER_PAGE, CHECKPOINT_FILE, OUTPUT_FILE, SAVE_TO_DB
"""
import os
import json
import time
import requests
import gspread
import pandas as pd
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials
import mysql.connector
from mysql.connector import Error
from typing import List, Optional

# -----------------------
# Configuration (env vars)
# -----------------------
CHECKPOINT_FILE = os.environ.get("CHECKPOINT_FILE", "strava_checkpoint.json")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "50"))
PER_PAGE = int(os.environ.get("STRAVA_PER_PAGE", "100"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "5"))
INITIAL_RETRY_SLEEP = int(os.environ.get("INITIAL_RETRY_SLEEP", "5"))
RATE_LIMIT_SAFETY_BUFFER = int(os.environ.get("RATE_LIMIT_SAFETY_BUFFER", "10"))
OUTPUT_FILE = os.environ.get("OUTPUT_FILE", "athlete_data.xlsx")
JSON_FILE = os.environ.get("JSON_FILE", "athlete_data.json")

# MySQL config (optional)
DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "port": int(os.environ.get("DB_PORT", "3306")),
    "user": os.environ.get("DB_USER", ""),
    "password": os.environ.get("DB_PASSWORD", ""),
    "database": os.environ.get("DB_NAME", "jalga2bc_strava")
}

STRAVA_CLIENT_ID = os.environ.get("STRAVA_CLIENT_ID")
STRAVA_CLIENT_SECRET = os.environ.get("STRAVA_CLIENT_SECRET")

# -----------------------
# Checkpoint utilities
# -----------------------
def load_checkpoint() -> dict:
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, "r") as fh:
                return json.load(fh)
        except Exception:
            return {"last_batch_index": 0, "athletes": {}}
    return {"last_batch_index": 0, "athletes": {}}

def save_checkpoint(cp: dict):
    with open(CHECKPOINT_FILE, "w") as fh:
        json.dump(cp, fh, indent=2, default=str)

# -----------------------
# Google Sheets auth & athletes read
# -----------------------
def authenticate_google_sheets():
    google_creds = os.environ.get("GOOGLE_SHEETS_JSON")
    if not google_creds:
        raise ValueError("Missing GOOGLE_SHEETS_JSON in env.")
    creds_dict = json.loads(google_creds)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(credentials)

    SHEET_URL = os.environ.get("SHEET_URL")
    if not SHEET_URL:
        raise ValueError("Missing SHEET_URL in env.")
    sheet = client.open_by_url(SHEET_URL).sheet1
    rows = sheet.get_all_values()
    header, data = rows[0], rows[1:]

    athletes = []
    for r_index, row in enumerate(data, start=2):
        # Adapt indexes as needed: example uses row[3]=first name, row[4]=last name, row[7]=refresh token
        name = f"{row[3]} {row[4]}".strip() if len(row) > 4 else f"row-{r_index}"
        refresh_token = row[7] if len(row) > 7 else None
        athletes.append({"row_index": r_index, "name": name, "refresh_token": refresh_token})
    return athletes

# -----------------------
# Strava token exchange
# -----------------------
def exchange_refresh_for_access(refresh_token: str) -> Optional[dict]:
    if not STRAVA_CLIENT_ID or not STRAVA_CLIENT_SECRET:
        raise ValueError("Missing STRAVA_CLIENT_ID or STRAVA_CLIENT_SECRET in env.")
    url = "https://www.strava.com/oauth/token"
    payload = {
        "client_id": STRAVA_CLIENT_ID,
        "client_secret": STRAVA_CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token
    }
    try:
        r = requests.post(url, data=payload, timeout=30)
    except requests.RequestException as e:
        print("❌ Token exchange request error:", e)
        return None

    if r.status_code == 200:
        return r.json()
    else:
        print("❌ Token exchange failed:", r.status_code, r.text)
        return None

# -----------------------
# Rate-limit safe requests & backoff
# -----------------------
def parse_rate_headers(headers: dict) -> dict:
    limits = headers.get("X-RateLimit-Limit", "")
    usage = headers.get("X-RateLimit-Usage", "")
    parsed = {"limit_overall": None, "limit_read": None, "usage_overall": None, "usage_read": None}
    try:
        if limits:
            o, r = [int(x) for x in limits.split(",")]
            parsed["limit_overall"], parsed["limit_read"] = o, r
        if usage:
            ou, ru = [int(x) for x in usage.split(",")]
            parsed["usage_overall"], parsed["usage_read"] = ou, ru
    except Exception:
        pass
    return parsed

def should_sleep_for_rate(parsed_headers: dict, safety_buffer: int = RATE_LIMIT_SAFETY_BUFFER) -> bool:
    try:
        if parsed_headers.get("limit_read") and parsed_headers.get("usage_read") is not None:
            remaining_read = parsed_headers["limit_read"] - parsed_headers["usage_read"]
            return remaining_read <= safety_buffer
    except Exception:
        pass
    return False

def safe_get(session: requests.Session, url: str, headers=None, params=None, retries=MAX_RETRIES):
    attempt = 0
    sleep = INITIAL_RETRY_SLEEP
    while attempt <= retries:
        try:
            resp = session.get(url, headers=headers, params=params, timeout=60)
        except requests.RequestException as e:
            attempt += 1
            print(f"⚠ Request exception (attempt {attempt}/{retries}): {e} -- sleeping {sleep}s")
            time.sleep(sleep)
            sleep *= 2
            continue

        if resp.status_code == 429:
            print("⚠ 429 Rate limit reached. Sleeping 15 minutes...")
            time.sleep(15 * 60)
            attempt += 1
            continue

        if resp.status_code >= 500:
            attempt += 1
            print(f"⚠ Server error {resp.status_code}. Sleeping {sleep}s and retrying.")
            time.sleep(sleep)
            sleep *= 2
            continue

        return resp

    raise RuntimeError(f"Failed GET {url} after {retries} retries")

# -----------------------
# Fetch activities (incremental)
# -----------------------
def fetch_activities_for_athlete(session: requests.Session, access_token: str, after_ts: Optional[int], start_date: datetime, end_date: datetime) -> List[dict]:
    url = "https://www.strava.com/api/v3/athlete/activities"
    headers = {"Authorization": f"Bearer {access_token}"}
    page = 1
    activities = []
    end_epoch = int((end_date + timedelta(days=1)).timestamp())

    while True:
        params = {
            "before": end_epoch,
            "after": int(after_ts) if after_ts else int(start_date.timestamp()),
            "page": page,
            "per_page": PER_PAGE
        }
        resp = safe_get(session, url, headers=headers, params=params)
        if resp.status_code != 200:
            print("⚠ Error fetching activities:", resp.status_code, resp.text)
            break
        batch = resp.json()
        if not batch:
            break
        activities.extend(batch)
        if len(batch) < PER_PAGE:
            break
        page += 1

        parsed = parse_rate_headers(resp.headers)
        if should_sleep_for_rate(parsed):
            print("⏳ Approaching rate limit. Sleeping 15 minutes.")
            time.sleep(15 * 60)

    return activities

# -----------------------
# SQL save helper
# -----------------------
def ensure_table_exists(connection):
    create_sql = """
    CREATE TABLE IF NOT EXISTS activities (
        id BIGINT PRIMARY KEY,
        athlete_id VARCHAR(64),
        created_at DATETIME,
        updated_at DATETIME,
        name VARCHAR(255),
        type VARCHAR(50),
        distance DOUBLE,
        moving_time INT,
        elapsed_time INT,
        start_date DATETIME,
        start_date_local DATETIME,
        timezone VARCHAR(100),
        map_polyline TEXT
    );
    """
    cursor = connection.cursor()
    cursor.execute(create_sql)
    cursor.close()

def save_activities_to_db(activities_data: List[dict], db_config: dict):
    if not db_config.get("user"):
        print("ℹ️ DB not configured (DB_USER missing). Skipping DB save.")
        return
    try:
        connection = mysql.connector.connect(**db_config)
        ensure_table_exists(connection)
        cursor = connection.cursor()
        insert_query = """
        INSERT INTO activities (
            id, athlete_id, created_at, updated_at, name, type, distance,
            moving_time, elapsed_time, start_date, start_date_local, timezone, map_polyline
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            name=VALUES(name), type=VALUES(type), distance=VALUES(distance),
            moving_time=VALUES(moving_time), elapsed_time=VALUES(elapsed_time),
            updated_at=VALUES(updated_at)
        """
        now = datetime.now()
        for act in activities_data:
            tup = (
                act.get("Activity_ID"),
                str(act.get("Athlete_ID")),
                now, now,
                act.get("Name"),
                act.get("Type"),
                act.get("Distance_m"),
                act.get("Moving_Time_s"),
                act.get("Elapsed_Time_s"),
                act.get("Start_Date_UTC"),
                act.get("Start_Date"),
                act.get("Timezone"),
                act.get("map_polyline")
            )
            try:
                cursor.execute(insert_query, tup)
            except Error as e:
                print("⚠ Error inserting:", e)
                continue
        connection.commit()
        cursor.close()
        connection.close()
        print("✅ Saved activities to DB.")
    except Error as e:
        print("❌ DB connection/insert error:", e)

# -----------------------
# Main extraction logic
# -----------------------
def extract_athlete_data(start_date_str: str, end_date_str: str, output_file: str = OUTPUT_FILE, save_to_db: bool = False):
    start_dt = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date_str, "%Y-%m-%d")
    if end_dt > datetime.today():
        end_dt = datetime.today()

    cp = load_checkpoint()
    athletes = authenticate_google_sheets()
    total_athletes = len(athletes)
    print(f"ℹ️ Total athletes in sheet: {total_athletes}")

    batch_index = cp.get("last_batch_index", 0)
    start_i = batch_index * BATCH_SIZE
    end_i = start_i + BATCH_SIZE
    batch = athletes[start_i:end_i]
    print(f"ℹ️ Processing batch {batch_index} -> athletes {start_i}..{end_i-1} (count {len(batch)})")

    session = requests.Session()
    all_dfs = []
    sql_activities = []

    for i, athlete in enumerate(batch):
        athlete_key = f"{athlete['row_index']}_{athlete['name']}"
        print(f"\n➡ Processing athlete {start_i + i + 1}/{total_athletes}: {athlete['name']} (sheet row {athlete['row_index']})")

        stored = cp.get("athletes", {}).get(athlete_key, {})
        refresh_token = stored.get("refresh_token") or athlete.get("refresh_token")
        last_ts_str = stored.get("last_activity_ts")
        last_ts = None
        if last_ts_str:
            try:
                last_dt = datetime.fromisoformat(last_ts_str)
                last_ts = int(last_dt.timestamp())
            except Exception:
                last_ts = None

        if not refresh_token:
            print(f"⚠ No refresh token for athlete {athlete['name']}. Skipping.")
            continue

        token_resp = exchange_refresh_for_access(refresh_token)
        if not token_resp:
            print(f"⚠ Token exchange failed for {athlete['name']}. Skipping.")
            continue

        access_token = token_resp.get("access_token")
        new_refresh = token_resp.get("refresh_token")
        if new_refresh:
            cp.setdefault("athletes", {}).setdefault(athlete_key, {})["refresh_token"] = new_refresh
            print("🔁 Received new refresh_token from Strava; checkpoint updated.")

        try:
            activities = fetch_activities_for_athlete(session, access_token, after_ts=last_ts, start_date=start_dt, end_date=end_dt)
        except RuntimeError as e:
            print("⚠ Fetch failed:", e)
            activities = []

        if not activities:
            print(f"ℹ️ No new activities for {athlete['name']}.")
            cp.setdefault("athletes", {}).setdefault(athlete_key, {})["last_activity_ts"] = datetime.utcnow().isoformat()
            save_checkpoint(cp)
            continue

        activity_data = []
        newest_ts = last_ts or 0
        for act in activities:
            start_date_utc = act.get("start_date")
            try:
                dt = datetime.fromisoformat(start_date_utc.replace("Z", "+00:00"))
                ts = int(dt.timestamp())
                if ts > newest_ts:
                    newest_ts = ts
            except Exception:
                ts = None

            row = {
                "Activity_ID": act.get("id"),
                "Name": act.get("name"),
                "Type": act.get("type"),
                "Start_Date": act.get("start_date_local"),
                "Distance_m": act.get("distance"),
                "Distance_km": act.get("distance", 0) / 1000.0 if act.get("distance") else 0.0,
                "Moving_Time_s": act.get("moving_time"),
                "Elapsed_Time_s": act.get("elapsed_time"),
                "Total_Elevation_Gain_m": act.get("total_elevation_gain"),
                "Average_Speed_mps": act.get("average_speed"),
                "Max_Speed_mps": act.get("max_speed"),
                "Average_Cadence": act.get("average_cadence"),
                "Average_Watts": act.get("average_watts"),
                "Max_Watts": act.get("max_watts"),
                "Calories": act.get("calories"),
                "Start_Date_UTC": act.get("start_date"),
                "Timezone": act.get("timezone"),
                "Athlete_ID": act.get("athlete", {}).get("id", None),
                "Athlete_Name": athlete["name"],
                "map_polyline": act.get("map", {}).get("polyline", None)
            }
            activity_data.append(row)
            sql_activities.append(row)

        df = pd.DataFrame(activity_data)
        if not df.empty:
            for col in ["Start_Date", "Start_Date_UTC"]:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce").dt.tz_localize(None)
            df["Distance_km"] = df["Distance_km"].round(2)
            all_dfs.append(df)

        if newest_ts:
            cp.setdefault("athletes", {}).setdefault(athlete_key, {})["last_activity_ts"] = datetime.utcfromtimestamp(newest_ts).isoformat()

        save_checkpoint(cp)
        time.sleep(1.0)  # small sleep between athletes to reduce bursts

    next_batch_index = batch_index + 1
    if next_batch_index * BATCH_SIZE >= total_athletes:
        next_batch_index = 0
    cp["last_batch_index"] = next_batch_index
    save_checkpoint(cp)
    print(f"\nℹ️ Batch {batch_index} completed. Next run will process batch {next_batch_index}.")

    # Combine this run's data
    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
    else:
        final_df = pd.DataFrame([{"Athlete_ID": None, "Athlete_Name": None, "Message": "No activities found in this batch"}])

    # Tidy datetimes
    for col in ["Start_Date", "Start_Date_UTC"]:
        if col in final_df.columns:
            final_df[col] = pd.to_datetime(final_df[col], errors="coerce").dt.tz_localize(None)

    if "Start_Date" in final_df.columns:
        final_df["Month"] = final_df["Start_Date"].dt.month
        final_df["Day"] = final_df["Start_Date"].dt.day

    # -----------------------
    # Append to existing Excel/JSON and dedupe by Activity_ID
    # -----------------------
    try:
        # Merge with existing JSON if present
        if os.path.exists(JSON_FILE):
            try:
                prev_json = pd.read_json(JSON_FILE)
                combined = pd.concat([prev_json, final_df], ignore_index=True, sort=False)
                if "Activity_ID" in combined.columns:
                    combined.drop_duplicates(subset=["Activity_ID"], inplace=True)
                final_df = combined
                print(f"ℹ️ Merged with existing JSON ({JSON_FILE}), deduped.")
            except Exception as e:
                print("⚠ Could not read/merge existing JSON, continuing with fresh batch:", e)
    except Exception:
        pass

    try:
        # Merge with existing Excel Raw_Data if present
        if os.path.exists(output_file):
            try:
                prev_df = pd.read_excel(output_file, sheet_name="Raw_Data")
                combined = pd.concat([prev_df, final_df], ignore_index=True, sort=False)
                if "Activity_ID" in combined.columns:
                    combined.drop_duplicates(subset=["Activity_ID"], inplace=True)
                final_df = combined
                print(f"ℹ️ Merged with existing Excel ({output_file}), deduped.")
            except Exception as e:
                print("⚠ Could not read/merge existing Excel, continuing with fresh batch:", e)
    except Exception:
        pass

    # Recompute pivot after merging
    try:
        pivot_df = pd.pivot_table(
            final_df,
            values="Distance_km",
            index=["Athlete_Name", "Type"] if "Type" in final_df.columns else ["Athlete_Name"],
            columns=["Month", "Day"] if "Month" in final_df.columns else None,
            aggfunc="max",
            fill_value=0
        )
    except Exception:
        pivot_df = None

    # Save Excel and JSON
    try:
        with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
            # Ensure Raw_Data saved
            try:
                final_df.to_excel(writer, sheet_name="Raw_Data", index=False)
            except Exception as e:
                print("⚠ Could not write Raw_Data to Excel:", e)
            # Save JSON on disk
            try:
                final_df.to_json(JSON_FILE, orient="records", date_format="iso")
            except Exception as e:
                print("⚠ Could not write JSON file:", e)
            # Save pivot sheet if available
            try:
                if pivot_df is not None:
                    pivot_df.to_excel(writer, sheet_name="Pivot_Table")
            except Exception as e:
                print("⚠ Could not write Pivot_Table to Excel:", e)
        print(f"✅ Athlete data saved to {output_file} and {JSON_FILE}")
    except Exception as e:
        print("❌ Error saving Excel/JSON:", e)

    # Save to DB if requested
    if save_to_db and sql_activities:
        save_activities_to_db(sql_activities, DB_CONFIG)

# -----------------------
# Entrypoint
# -----------------------
if __name__ == "__main__":
    START_DATE = os.environ.get("START_DATE", (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%d"))
    END_DATE = os.environ.get("END_DATE", datetime.utcnow().strftime("%Y-%m-%d"))
    SAVE_TO_DB = os.environ.get("SAVE_TO_DB", "true").lower() in ("true", "1", "yes")

    extract_athlete_data(START_DATE, END_DATE, output_file=OUTPUT_FILE, save_to_db=SAVE_TO_DB)
