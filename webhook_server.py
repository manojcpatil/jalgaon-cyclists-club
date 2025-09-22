#!/usr/bin/env python3
"""
Strava Webhook Receiver (complete)

Features:
- GET /strava-webhook : verification (hub.challenge)
- POST /strava-webhook: receives events, handles activity.create
- Exchanges refresh_token -> access_token (uses STRAVA_CLIENT_ID/SECRET)
- Fetches activity details from Strava and appends/updates athlete_data.json and athlete_data.csv
- Atomic file writes and permission tightening
- Uses webhook_strava_checkpoint.json to store athlete refresh tokens:
    { "athletes": { "<athlete_id>": { "refresh_token": "...", "name": "...", "seeded_at": "..." } } }

Environment variables:
- STRAVA_CLIENT_ID
- STRAVA_CLIENT_SECRET
- VERIFY_TOKEN         (for Strava webhook verification)
- OUTPUT_JSON (optional, default: ./athlete_data.json)
- OUTPUT_CSV  (optional, default: ./athlete_data.csv)
- CHECKPOINT_FILE (optional, default: ./webhook_strava_checkpoint.json)
- PORT (optional, default: 5000)
"""
import os
import json
import requests
import pandas as pd
from flask import Flask, request, jsonify
from datetime import datetime
from math import isfinite

# -----------------------
# Config
# -----------------------
OUTPUT_JSON = os.environ.get("OUTPUT_JSON", "athlete_data.json")
OUTPUT_CSV = os.environ.get("OUTPUT_CSV", "athlete_data.csv")
CHECKPOINT_FILE = os.environ.get("CHECKPOINT_FILE", "webhook_strava_checkpoint.json")

STRAVA_CLIENT_ID = os.environ.get("STRAVA_CLIENT_ID")
STRAVA_CLIENT_SECRET = os.environ.get("STRAVA_CLIENT_SECRET")
VERIFY_TOKEN = os.environ.get("VERIFY_TOKEN", "test-verify-token")

# Helpful defaults
if not os.path.exists(OUTPUT_JSON):
    with open(OUTPUT_JSON, "w", encoding="utf-8") as fh:
        json.dump([], fh, ensure_ascii=False, indent=2)

# -----------------------
# Utilities: atomic write & file helpers
# -----------------------
def _atomic_write(path: str, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2, default=str)
    os.replace(tmp, path)
    try:
        os.chmod(path, 0o600)
    except Exception:
        pass

def _ensure_csv(path: str, header: list):
    if not os.path.exists(path):
        df = pd.DataFrame(columns=header)
        df.to_csv(path, index=False)
        try:
            os.chmod(path, 0o600)
        except Exception:
            pass

def load_checkpoint() -> dict:
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except Exception:
            return {"athletes": {}}
    return {"athletes": {}}

def save_checkpoint(cp: dict):
    tmp = CHECKPOINT_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(cp, fh, indent=2, ensure_ascii=False)
    os.replace(tmp, CHECKPOINT_FILE)
    try:
        os.chmod(CHECKPOINT_FILE, 0o600)
    except Exception:
        pass

# -----------------------
# Strava token exchange
# -----------------------
def exchange_refresh_for_access(refresh_token: str) -> dict | None:
    if not STRAVA_CLIENT_ID or not STRAVA_CLIENT_SECRET:
        print("❌ STRAVA_CLIENT_ID/SECRET not set")
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
    except requests.RequestException as e:
        print("❌ Token exchange error:", e)
        return None
    if r.status_code == 200:
        return r.json()
    print("❌ Token exchange failed:", r.status_code, r.text)
    return None

# -----------------------
# Mapping Strava activity -> your JSON schema
# -----------------------
def _safe_get(d, k, default=None):
    v = d.get(k, default)
    try:
        if isinstance(v, float) and not isfinite(v):
            return None
    except Exception:
        pass
    return v

def _activity_to_record(activity: dict) -> dict:
    start_date_local = activity.get("start_date_local") or activity.get("start_date")
    start_date_utc = activity.get("start_date") or activity.get("start_date_local")
    month = day = None
    try:
        if start_date_local:
            dt = datetime.fromisoformat(start_date_local.replace("Z", ""))
            month = float(dt.month)
            day = float(dt.day)
    except Exception:
        month = None
        day = None

    distance_m = _safe_get(activity, "distance", None)
    distance_km = None
    try:
        if distance_m is not None:
            distance_km = round(float(distance_m) / 1000.0, 2)
    except Exception:
        distance_km = None

    rec = {
        "Activity_ID": activity.get("id"),
        "Name": activity.get("name"),
        "Type": activity.get("type"),
        "Start_Date": start_date_local,
        "Distance_m": distance_m,
        "Distance_km": distance_km,
        "Moving_Time_s": _safe_get(activity, "moving_time"),
        "Elapsed_Time_s": _safe_get(activity, "elapsed_time"),
        "Total_Elevation_Gain_m": _safe_get(activity, "total_elevation_gain"),
        "Average_Speed_mps": _safe_get(activity, "average_speed"),
        "Max_Speed_mps": _safe_get(activity, "max_speed"),
        "Average_Cadence": _safe_get(activity, "average_cadence"),
        "Average_Watts": _safe_get(activity, "average_watts"),
        "Max_Watts": _safe_get(activity, "max_watts"),
        "Calories": _safe_get(activity, "calories"),
        "Start_Date_UTC": start_date_utc,
        "Timezone": activity.get("timezone") or "(GMT+05:30) Asia/Kolkata",
        "Athlete_ID": activity.get("athlete", {}).get("id") if isinstance(activity.get("athlete"), dict) else activity.get("owner_id") or activity.get("athlete_id") or None,
        "Athlete_Name": None,
        "Month": month,
        "Day": day,
        "map_polyline": activity.get("map", {}).get("polyline") if activity.get("map") else None
    }

    try:
        if isinstance(activity.get("athlete"), dict):
            fname = activity["athlete"].get("firstname", "")
            lname = activity["athlete"].get("lastname", "")
            rec["Athlete_Name"] = (fname + " " + lname).strip() or None
    except Exception:
        rec["Athlete_Name"] = None

    return rec

# -----------------------
# Append/update JSON + CSV
# -----------------------
def append_activity_to_json(activity: dict, json_path: str = OUTPUT_JSON):
    try:
        with open(json_path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
            if not isinstance(data, list):
                data = []
    except Exception:
        data = []

    rec = _activity_to_record(activity)

    existing_idx = None
    for i, r in enumerate(data):
        if r.get("Activity_ID") == rec.get("Activity_ID"):
            existing_idx = i
            break

    if existing_idx is not None:
        data[existing_idx] = rec
        action = "updated"
    else:
        data.append(rec)
        action = "added"

    _atomic_write(json_path, data)
    print(f"✅ {action} activity {rec.get('Activity_ID')} -> {json_path}")
    return rec

def append_activity_to_csv(activity: dict, csv_path: str = OUTPUT_CSV):
    # Define CSV columns to match JSON keys (some chosen subset)
    cols = ["Activity_ID","Name","Type","Start_Date","Start_Date_UTC","Distance_m","Distance_km",
            "Moving_Time_s","Elapsed_Time_s","Total_Elevation_Gain_m","Average_Speed_mps",
            "Max_Speed_mps","Average_Cadence","Average_Watts","Calories","Athlete_ID","Athlete_Name","Month","Day","map_polyline"]
    _ensure_csv(csv_path, cols)
    rec = _activity_to_record(activity)
    # Ensure same columns ordering
    row = {c: rec.get(c) for c in cols}
    try:
        df_existing = pd.read_csv(csv_path, dtype=object)
    except Exception:
        df_existing = pd.DataFrame(columns=cols)
    df_new = pd.DataFrame([row], columns=cols)
    # Combine and dedupe by Activity_ID (keep last occurrence -> new)
    df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    df_combined = df_combined.drop_duplicates(subset=["Activity_ID"], keep="last")
    # Write atomically
    tmp = csv_path + ".tmp"
    df_combined.to_csv(tmp, index=False)
    os.replace(tmp, csv_path)
    try:
        os.chmod(csv_path, 0o600)
    except Exception:
        pass
    print(f"✅ CSV synced activity {rec.get('Activity_ID')} -> {csv_path}")

# -----------------------
# Handle incoming event
# -----------------------
def handle_event(event: dict):
    """
    Expected Strava webhook structure:
    {
      "object_type":"activity",
      "object_id":12345,
      "owner_id": 67890,
      "aspect_type":"create",
      ...
    }
    """
    if event.get("object_type") != "activity":
        print("ℹ️ Ignoring non-activity event")
        return

    if event.get("aspect_type") not in ("create", "update"):
        print("ℹ️ Ignoring aspect_type:", event.get("aspect_type"))
        return

    athlete_id = str(event.get("owner_id") or event.get("owner") or event.get("owner_id"))
    activity_id = event.get("object_id") or event.get("object_id")

    if not athlete_id or not activity_id:
        print("⚠ Event missing owner_id or object_id:", event)
        return

    cp = load_checkpoint()
    athlete_entry = cp.get("athletes", {}).get(athlete_id)
    if not athlete_entry or not athlete_entry.get("refresh_token"):
        print(f"⚠ No refresh token for athlete {athlete_id}. Event ignored.")
        return

    refresh_token = athlete_entry["refresh_token"]
    token_resp = exchange_refresh_for_access(refresh_token)
    if not token_resp:
        print(f"⚠ Token exchange failed for athlete {athlete_id}")
        return

    access_token = token_resp.get("access_token")
    new_refresh = token_resp.get("refresh_token")
    # update refresh token if Strava returned a new one
    if new_refresh:
        cp.setdefault("athletes", {}).setdefault(athlete_id, {})["refresh_token"] = new_refresh
        cp["athletes"][athlete_id].setdefault("refreshed_at", datetime.utcnow().isoformat() + "Z")
        save_checkpoint(cp)

    # fetch activity details
    url = f"https://www.strava.com/api/v3/activities/{activity_id}"
    headers = {"Authorization": f"Bearer {access_token}"}
    try:
        r = requests.get(url, headers=headers, timeout=30)
    except requests.RequestException as e:
        print("⚠ Error fetching activity:", e)
        return

    if r.status_code != 200:
        print("⚠ Error fetching activity (status):", r.status_code, r.text)
        return

    activity = r.json()
    # Append to JSON and CSV (dedupe handled)
    append_activity_to_json(activity, OUTPUT_JSON)
    append_activity_to_csv(activity, OUTPUT_CSV)

# -----------------------
# Flask app endpoints
# -----------------------
app = Flask(__name__)

@app.route("/strava-webhook", methods=["GET", "POST"])
def strava_webhook():
    if request.method == "GET":
        # Verification from Strava subscription
        token = request.args.get("hub.verify_token")
        challenge = request.args.get("hub.challenge")
        if token and token == VERIFY_TOKEN:
            return jsonify({"hub.challenge": challenge})
        return "Invalid verify token", 403

    # POST events from Strava
    if request.method == "POST":
        payload = request.get_json(silent=True)
        print("ℹ️ Received payload:", payload)
        if not payload:
            return "Bad request", 400
        # Strava sometimes sends an array of events; handle both cases
        if isinstance(payload, dict) and payload.get("aspect_type"):
            handle_event(payload)
        elif isinstance(payload, dict) and payload.get("object_type") is None and "events" in payload:
            # Some webhook payload wrappers
            for ev in payload.get("events", []):
                handle_event(ev)
        elif isinstance(payload, list):
            for ev in payload:
                handle_event(ev)
        else:
            # possibly wrapped differently; try to find event-like dicts
            if isinstance(payload, dict):
                for v in payload.values():
                    if isinstance(v, dict) and v.get("object_type") == "activity":
                        handle_event(v)
        return "", 200

# Basic health
@app.route("/healthz", methods=["GET"])
def healthz():
    return jsonify({"ok": True, "time": datetime.utcnow().isoformat() + "Z"})

# -----------------------
# Entrypoint
# -----------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"🚀 Starting Strava webhook server on port {port}")
    app.run(host="0.0.0.0", port=port)
