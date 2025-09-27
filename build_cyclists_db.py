#!/usr/bin/env python3
"""
build_cyclists_db.py
Fetch Strava athlete profiles and store/upsert into cyclists.db (SQLite).
Respects client-side rate limits: 100 requests / 15 minutes and 300 requests / hour.
Uses Google Sheets service account JSON in env var GOOGLE_SHEETS_JSON and SHEET_URL.

Environment:
 - GOOGLE_SHEETS_JSON : JSON string for service account (same as original script)
 - SHEET_URL          : URL of the sheet containing athletes (refresh token expected in column H / index 7)
 - STRAVA_CLIENT_ID
 - STRAVA_CLIENT_SECRET
Optional:
 - OUTPUT_DB (default cyclists.db)
 - CHECKPOINT_FILE (default strava_profiles_checkpoint.json)
 - BATCH_SIZE (default 50)
 - MAX_RETRIES, INITIAL_RETRY_SLEEP
"""

import os
import json
import time
import sqlite3
import requests
from collections import deque
from datetime import datetime, timedelta
from typing import Optional, Dict, List

# -----------------------
# Configuration (env)
# -----------------------
OUTPUT_DB = os.environ.get("OUTPUT_DB", "cyclists.db")
CHECKPOINT_FILE = os.environ.get("CHECKPOINT_FILE", "strava_profiles_checkpoint.json")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "50"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "5"))
INITIAL_RETRY_SLEEP = float(os.environ.get("INITIAL_RETRY_SLEEP", "5"))
RATE_LIMIT_BUFFER_SEC = float(os.environ.get("RATE_LIMIT_BUFFER_SEC", "2"))

STRAVA_CLIENT_ID = os.environ.get("STRAVA_CLIENT_ID")
STRAVA_CLIENT_SECRET = os.environ.get("STRAVA_CLIENT_SECRET")

# Rate limit targets:
REQ_LIMIT_15MIN = 100
REQ_WINDOW_15MIN = 15 * 60  # seconds
REQ_LIMIT_1H = 300
REQ_WINDOW_1H = 60 * 60  # seconds

# -----------------------
# Utilities: checkpoint
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
# DB: create and upsert
# -----------------------
def init_db(path: str):
    conn = sqlite3.connect(path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("""
    CREATE TABLE IF NOT EXISTS athletes (
        athlete_id INTEGER PRIMARY KEY,
        name TEXT,
        firstname TEXT,
        lastname TEXT,
        city TEXT,
        state TEXT,
        country TEXT,
        sex TEXT,
        profile TEXT,
        profile_medium TEXT,
        created_at TEXT,
        updated_at TEXT,
        raw_json TEXT,
        last_seen TEXT
    )
    """)
    conn.commit()
    return conn

def upsert_athlete(conn: sqlite3.Connection, profile: dict):
    # extract fields safely
    athlete_id = profile.get("id")
    name = profile.get("username") or f"{profile.get('firstname','')} {profile.get('lastname','')}".strip()
    firstname = profile.get("firstname")
    lastname = profile.get("lastname")
    city = profile.get("city")
    state = profile.get("state")
    country = profile.get("country")
    sex = profile.get("sex")
    profile_url = profile.get("profile")
    profile_medium = profile.get("profile_medium")
    created_at = profile.get("created_at")
    updated_at = profile.get("updated_at")
    raw = json.dumps(profile, default=str)

    now = datetime.utcnow().isoformat()
    cur = conn.cursor()
    cur.execute("""
    INSERT INTO athletes (athlete_id,name,firstname,lastname,city,state,country,sex,profile,profile_medium,created_at,updated_at,raw_json,last_seen)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(athlete_id) DO UPDATE SET
       name=excluded.name,
       firstname=excluded.firstname,
       lastname=excluded.lastname,
       city=excluded.city,
       state=excluded.state,
       country=excluded.country,
       sex=excluded.sex,
       profile=excluded.profile,
       profile_medium=excluded.profile_medium,
       created_at=excluded.created_at,
       updated_at=excluded.updated_at,
       raw_json=excluded.raw_json,
       last_seen=excluded.last_seen
    """, (athlete_id, name, firstname, lastname, city, state, country, sex, profile_url, profile_medium, created_at, updated_at, raw, now))
    conn.commit()

# -----------------------
# Google Sheets: read athletes list
# -----------------------
def authenticate_google_sheets():
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
        if r.status_code == 200:
            return r.json()
        else:
            print("❌ Token exchange failed:", r.status_code, r.text)
            return None
    except requests.RequestException as e:
        print("❌ Token exchange request error:", e)
        return None

# -----------------------
# Rate limiter (client-side)
# -----------------------
class RateLimiter:
    def __init__(self):
        # store timestamps (float seconds) of recent requests
        self.req_deque_15 = deque()
        self.req_deque_1h = deque()

    def note_request(self):
        now = time.time()
        self.req_deque_15.append(now)
        self.req_deque_1h.append(now)
        self._prune()

    def _prune(self):
        now = time.time()
        while self.req_deque_15 and self.req_deque_15[0] < now - REQ_WINDOW_15MIN:
            self.req_deque_15.popleft()
        while self.req_deque_1h and self.req_deque_1h[0] < now - REQ_WINDOW_1H:
            self.req_deque_1h.popleft()

    def wait_if_needed(self):
        self._prune()
        now = time.time()

        wait_until = None

        # Check 15-min window
        if len(self.req_deque_15) >= REQ_LIMIT_15MIN:
            earliest = self.req_deque_15[0]
            candidate = earliest + REQ_WINDOW_15MIN + RATE_LIMIT_BUFFER_SEC
            wait_until = candidate if wait_until is None else max(wait_until, candidate)

        # Check 1-hour window
        if len(self.req_deque_1h) >= REQ_LIMIT_1H:
            earliest = self.req_deque_1h[0]
            candidate = earliest + REQ_WINDOW_1H + RATE_LIMIT_BUFFER_SEC
            wait_until = candidate if wait_until is None else max(wait_until, candidate)

        if wait_until and wait_until > now:
            to_sleep = wait_until - now
            mins = to_sleep / 60.0
            print(f"⏳ Rate limiter sleeping {to_sleep:.1f}s (~{mins:.2f} min) to respect limits.")
            time.sleep(to_sleep)

# -----------------------
# Safe get with retries — also uses rate limiter
# -----------------------
def safe_get(session: requests.Session, url: str, headers=None, params=None, retries=MAX_RETRIES, rate_limiter: Optional[RateLimiter]=None):
    attempt = 0
    sleep = INITIAL_RETRY_SLEEP
    while attempt <= retries:
        if rate_limiter:
            rate_limiter.wait_if_needed()
        try:
            resp = session.get(url, headers=headers, params=params, timeout=30)
        except requests.RequestException as e:
            attempt += 1
            print(f"⚠ Request exception (attempt {attempt}/{retries}): {e} -- sleeping {sleep}s")
            time.sleep(sleep)
            sleep *= 2
            continue

        # note request for client-side accounting
        if rate_limiter:
            rate_limiter.note_request()

        if resp.status_code == 429:
            # if Strava sent Retry-After, respect it; else sleep a safe 60s or 15min if headers show heavy usage
            retry_after = int(resp.headers.get("Retry-After", "0") or "0")
            if retry_after > 0:
                print(f"⚠ 429 received. Respecting Retry-After: {retry_after}s")
                time.sleep(retry_after + RATE_LIMIT_BUFFER_SEC)
            else:
                print("⚠ 429 Rate limit reached. Sleeping 60s.")
                time.sleep(60)
            attempt += 1
            continue

        if 500 <= resp.status_code < 600:
            attempt += 1
            print(f"⚠ Server error {resp.status_code}. Sleeping {sleep}s and retrying.")
            time.sleep(sleep)
            sleep *= 2
            continue

        return resp

    raise RuntimeError(f"Failed GET {url} after {retries} retries")

# -----------------------
# Fetch athlete profile
# -----------------------
def fetch_athlete_profile(session: requests.Session, access_token: str, rate_limiter: RateLimiter) -> Optional[dict]:
    url = "https://www.strava.com/api/v3/athlete"
    headers = {"Authorization": f"Bearer {access_token}"}
    resp = safe_get(session, url, headers=headers, rate_limiter=rate_limiter)
    if resp.status_code == 200:
        return resp.json()
    else:
        print("⚠ Failed fetching athlete profile:", resp.status_code, resp.text)
        return None

# -----------------------
# Main pipeline: build cyclists.db
# -----------------------
def build_profiles_db():
    cp = load_checkpoint()
    athletes = authenticate_google_sheets()
    total = len(athletes)
    print(f"ℹ️ Athletes in sheet: {total}")

    batch_index = cp.get("last_batch_index", 0)
    start_i = batch_index * BATCH_SIZE
    end_i = min(start_i + BATCH_SIZE, total)
    batch = athletes[start_i:end_i]
    print(f"ℹ️ Processing batch {batch_index} -> rows {start_i}..{end_i-1} (count {len(batch)})")

    conn = init_db(OUTPUT_DB)
    session = requests.Session()
    rate_limiter = RateLimiter()

    for i, athlete in enumerate(batch):
        sheet_row = athlete["row_index"]
        athlete_key = f"{sheet_row}_{athlete['name']}"
        print(f"\n➡ Processing {start_i + i + 1}/{total}: {athlete['name']} (sheet row {sheet_row})")

        stored = cp.get("athletes", {}).get(athlete_key, {})
        refresh_token = stored.get("refresh_token") or athlete.get("refresh_token")
        if not refresh_token:
            print(f"⚠ No refresh token for {athlete['name']} (row {sheet_row}). Skipping.")
            cp.setdefault("athletes", {}).setdefault(athlete_key, {})["last_seen"] = datetime.utcnow().isoformat()
            save_checkpoint(cp)
            continue

        token_resp = exchange_refresh_for_access(refresh_token)
        if not token_resp:
            print(f"⚠ Token exchange failed for {athlete['name']}. Skipping.")
            # record attempt time to avoid tight retry loops next run
            cp.setdefault("athletes", {}).setdefault(athlete_key, {})["last_seen"] = datetime.utcnow().isoformat()
            save_checkpoint(cp)
            continue

        access_token = token_resp.get("access_token")
        new_refresh = token_resp.get("refresh_token")
        if new_refresh:
            cp.setdefault("athletes", {}).setdefault(athlete_key, {})["refresh_token"] = new_refresh
            print("🔁 Received new refresh_token from Strava; checkpoint updated.")

        # fetch profile
        try:
            profile = fetch_athlete_profile(session, access_token, rate_limiter)
        except RuntimeError as e:
            print("⚠ Fetch failed:", e)
            profile = None

        if not profile:
            print(f"ℹ️ No profile for {athlete['name']}.")
            cp.setdefault("athletes", {}).setdefault(athlete_key, {})["last_seen"] = datetime.utcnow().isoformat()
            save_checkpoint(cp)
            continue

        # upsert into DB
        try:
            upsert_athlete(conn, profile)
            print(f"✅ Saved athlete {profile.get('id')} / {profile.get('username') or profile.get('firstname')}")
        except Exception as e:
            print("❌ DB upsert error:", e)

        # update checkpoint last seen and refresh token (if any)
        cp.setdefault("athletes", {}).setdefault(athlete_key, {})["last_seen"] = datetime.utcnow().isoformat()
        if new_refresh:
            cp["athletes"][athlete_key]["refresh_token"] = new_refresh

        save_checkpoint(cp)

        # small polite pause (helps vertical pacing)
        time.sleep(0.5)

    # advance batch index
    next_batch_idx = batch_index + 1
    if next_batch_idx * BATCH_SIZE >= total:
        next_batch_idx = 0
    cp["last_batch_index"] = next_batch_idx
    save_checkpoint(cp)
    print(f"\nℹ️ Batch {batch_index} completed. Next run will process batch {next_batch_idx}.")
    conn.close()

# -----------------------
# Entrypoint
# -----------------------
if __name__ == "__main__":
    print("ℹ️ Starting cyclists DB builder")
    try:
        build_profiles_db()
    except Exception as e:
        print("❌ Fatal error:", e)
        raise
