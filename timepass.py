#!/usr/bin/env python3
"""
Strava batch extractor with small safe defaults for GitHub Actions.

Put this file in repo as timepass.py and call it from your workflow.

Environment variables used (same as before) plus:
 - OUTPUT_DIR   (optional) directory to write OUTPUT_CSV / OUTPUT_JSON / CHECKPOINT_FILE into.
"""

import os
import json
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Optional

# -----------------------
# Configuration (env vars)
# -----------------------
# Allow caller to set an output directory (e.g. the repo root)
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", ".")
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, os.environ.get("CHECKPOINT_FILE", "strava_checkpoint.json"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "50"))
PER_PAGE = int(os.environ.get("STRAVA_PER_PAGE", "100"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "5"))
INITIAL_RETRY_SLEEP = int(os.environ.get("INITIAL_RETRY_SLEEP", "5"))
RATE_LIMIT_SAFETY_BUFFER = int(os.environ.get("RATE_LIMIT_SAFETY_BUFFER", "10"))

OUTPUT_CSV = os.path.join(OUTPUT_DIR, os.environ.get("OUTPUT_CSV", "athlete_data.csv"))
OUTPUT_JSON = os.path.join(OUTPUT_DIR, os.environ.get("OUTPUT_JSON", "athlete_data.json"))

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
    tmp = CHECKPOINT_FILE + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(cp, fh, indent=2, default=str)
    os.replace(tmp, CHECKPOINT_FILE)
    print(f"✅ Checkpoint saved: {CHECKPOINT_FILE}")

# -----------------------
# (unchanged) Google Sheets auth & athletes read
# -----------------------
def authenticate_google_sheets():
    # Deliberately kept minimal here: original code expects GOOGLE_SHEETS_JSON & SHEET_URL in env.
    google_creds = os.environ.get("GOOGLE_SHEETS_JSON")
    if not google_creds:
        raise ValueError("Missing GOOGLE_SHEETS_JSON in env.")
    creds_dict = json.loads(google_creds)
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
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
        name = f"{row[3]} {row[4]}".strip() if len(row) > 4 else f"row-{r_index}"
        refresh_token = row[7] if len(row) > 7 else None
        athletes.append({"row_index": r_index, "name": name, "refresh_token": refresh_token})
    return athletes

# -----------------------
# Strava token exchange (unchanged)
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
# Rate-limit safe requests & backoff (unchanged)
# -----------------------
def parse_rate_headers(headers: dict) -> dict:
    limits = headers.get("X-RateLimit-Limit", "") or headers.get("X-Ratelimit-Limit", "")
    usage = headers.get("X-RateLimit-Usage", "") or headers.get("X-Ratelimit-Usage", "")
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
# Fetch activities (unchanged)
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
# Main extraction logic (mostly unchanged)
# -----------------------
def extract_athlete_data(start_date_str: str, end_date_str: str, output_csv: str = OUTPUT_CSV, output_json: str = OUTPUT_JSON):
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
        time.sleep(1.0)

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

    # -----------------------
    # Append to existing CSV/JSON and dedupe by Activity_ID
    # -----------------------
    try:
        if os.path.exists(output_json):
            prev_json = pd.read_json(output_json)
            combined = pd.concat([prev_json, final_df], ignore_index=True, sort=False)
            if "Activity_ID" in combined.columns:
                combined.drop_duplicates(subset=["Activity_ID"], inplace=True)
            final_df = combined
            print(f"ℹ️ Merged with existing JSON ({output_json}), deduped.")
    except Exception as e:
        print("⚠ Could not read/merge existing JSON, continuing with fresh batch:", e)

    try:
        if os.path.exists(output_csv):
            prev_csv = pd.read_csv(output_csv)
            combined = pd.concat([prev_csv, final_df], ignore_index=True, sort=False)
            if "Activity_ID" in combined.columns:
                combined.drop_duplicates(subset=["Activity_ID"], inplace=True)
            final_df = combined
            print(f"ℹ️ Merged with existing CSV ({output_csv}), deduped.")
    except Exception as e:
        print("⚠ Could not read/merge existing CSV, continuing with fresh batch:", e)

    # Save CSV and JSON atomically
    try:
        csv_tmp = output_csv + ".tmp"
        json_tmp = output_json + ".tmp"
        final_df.to_csv(csv_tmp, index=False)
        final_df.to_json(json_tmp, orient="records", date_format="iso")
        os.replace(csv_tmp, output_csv)
        os.replace(json_tmp, output_json)
        print(f"✅ Athlete data saved to {output_csv} and {output_json}")
    except Exception as e:
        print("❌ Error saving CSV/JSON:", e)

# -----------------------
# Entrypoint
# -----------------------
# -----------------------
# Entrypoint (robust to empty env vars)
# -----------------------
if __name__ == "__main__":
    # handle env vars that may exist but be empty strings
    raw_start = os.environ.get("START_DATE")
    raw_end = os.environ.get("END_DATE")

    if raw_start and raw_start.strip():
        START_DATE = raw_start.strip()
    else:
        START_DATE = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%d")

    if raw_end and raw_end.strip():
        END_DATE = raw_end.strip()
    else:
        END_DATE = datetime.utcnow().strftime("%Y-%m-%d")

    print(f"ℹ️ Using START_DATE={START_DATE}, END_DATE={END_DATE}")
    extract_athlete_data(START_DATE, END_DATE, output_csv=OUTPUT_CSV, output_json=OUTPUT_JSON)

