import os
import json
import requests
import gspread
import pandas as pd
from datetime import datetime, timedelta
import time  # Added for rate limiting
from oauth2client.service_account import ServiceAccountCredentials

# ==============================
# 1. Google Sheets Authentication (unchanged)
# ==============================
google_creds = os.environ.get("GOOGLE_SHEETS_JSON")
if not google_creds:
    raise ValueError("❌ Missing GOOGLE_SHEETS_JSON secret in GitHub.")
creds_dict = json.loads(google_creds)
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(credentials)
SHEET_URL = os.environ.get("SHEET_URL")
if not SHEET_URL:
    raise ValueError("❌ Missing SHEET_URL secret in GitHub.")
sheet = client.open_by_url(SHEET_URL).sheet1
rows = sheet.get_all_values()
header, data = rows[0], rows[1:]
athletes = [
    {"name": f"{row[3]} {row[4]}".strip(), "refresh_token": row[7]}
    for row in data if len(row) >= 8
]

# ==============================
# 2. Helper Functions
# ==============================
def blank_zero(v):
    try:
        if float(v) == 0:
            return "--"
        return f"{v:.1f}"
    except:
        return v

# ==============================
# 3. Strava Token Exchange with Rate Limiting
# ==============================
STRAVA_CLIENT_ID = os.environ.get("STRAVA_CLIENT_ID")
STRAVA_CLIENT_SECRET = os.environ.get("STRAVA_CLIENT_SECRET")
if not STRAVA_CLIENT_ID or not STRAVA_CLIENT_SECRET:
    raise ValueError("❌ Missing STRAVA_CLIENT_ID or STRAVA_CLIENT_SECRET in GitHub Secrets.")

# Rate limit: 300 requests per minute = 1 request every 0.2 seconds
REQUEST_INTERVAL = 0.2  # seconds

def get_access_token(refresh_token):
    time.sleep(REQUEST_INTERVAL)  # Add delay before each request
    url = "https://www.strava.com/oauth/token"
    payload = {
        "client_id": STRAVA_CLIENT_ID,
        "client_secret": STRAVA_CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token
    }
    r = requests.post(url, data=payload)
    if r.status_code == 200:
        return r.json()["access_token"]
    else:
        print(f"❌ Token exchange failed for refresh_token: {r.text}")
        return None

# ==============================
# 4. Fetch Activities with Rate Limiting
# ==============================
def fetch_activities(access_token, start_date, end_date):
    url = "https://www.strava.com/api/v3/athlete/activities"
    headers = {"Authorization": f"Bearer {access_token}"}
    page, per_page = 1, 100
    activities = []
    end_date += timedelta(days=1)

    while True:
        time.sleep(REQUEST_INTERVAL)  # Add delay before each request
        params = {
            "before": int(end_date.timestamp()),
            "after": int(start_date.timestamp()),
            "page": page,
            "per_page": per_page,
        }
        r = requests.get(url, headers=headers, params=params)
        if r.status_code != 200:
            print(f"❌ Error fetching activities: {r.text}")
            break
        data = r.json()
        if not data:
            break
        activities.extend(data)
        page += 1

    return activities

# ==============================
# 5. Build Leaderboard (unchanged except for date parsing)
# ==============================
THRESHOLDS = {"Ride": 15, "Run": 5, "Walk": 5}
SUMMARY_COLS = ["Total", "Active_Days"]

def build_leaderboard(start_date: str, end_date: str):
    start_dt = datetime.strptime(start_date + "-00-00-00", "%Y-%m-%d-%H-%M-%S")
    end_dt = datetime.strptime(end_date + "-00-00-00", "%Y-%m-%d-%H-%M-%S")
    today = datetime.today()
    if end_dt > today:
        end_dt = today

    valid_types = {"Ride", "Run", "Walk"}
    exclude_types = {"VirtualRide", "EBikeRide"}
    all_dates = [start_dt + timedelta(days=i) for i in range((end_dt - start_dt).days + 1)]
    day_labels = [(d.strftime("%b-%Y"), d.strftime("%d")) for d in all_dates]
    daily_cols = pd.MultiIndex.from_tuples(day_labels, names=["Month", "Day"])
    summary_cols = pd.MultiIndex.from_tuples([("Summary", "Total"), ("Summary", "Active_Days")], names=["Month", "Day"])
    all_columns = daily_cols.append(summary_cols)
    index = pd.MultiIndex.from_product([[a["name"] for a in athletes], sorted(valid_types)], names=["Athlete", "Type"])
    leaderboard = pd.DataFrame(0.0, index=index, columns=all_columns)

    for athlete in athletes:
        print(f"➡ Fetching {athlete['name']}")
        access_token = get_access_token(athlete["refresh_token"])
        if not access_token:
            print(f"⚠ Skipping {athlete['name']} (no access token)")
            continue
        activities = fetch_activities(access_token, start_dt, end_dt)
        for act in activities:
            act_type = act.get("type")
            if act_type in valid_types and act_type not in exclude_types:
                act_date = datetime.strptime(act["start_date_local"], "%Y-%m-%dT%H:%M:%SZ")
                if start_dt <= act_date <= end_dt:
                    col = (act_date.strftime("%b-%Y"), act_date.strftime("%d"))
                    if col in leaderboard.columns:
                        distance_km = act["distance"] / 1000.0
                        leaderboard.loc[(athlete["name"], act_type), col] += distance_km

    leaderboard[("Summary", "Total")] = leaderboard[daily_cols].sum(axis=1)
    active_days = []
    for (athlete, act_type), row in leaderboard.iterrows():
        threshold = THRESHOLDS.get(act_type, 0)
        days_count = sum(1 for col in daily_cols if row[col] >= threshold)
        active_days.append(days_count)
    leaderboard[("Summary", "Active_Days")] = active_days
    leaderboard = leaderboard.round(1)
    return leaderboard

# ==============================
# 6. Cell Coloring Function (unchanged)
# ==============================
def color_cells_by_threshold(row):
    act_type = row.name[1]
    threshold = THRESHOLDS.get(act_type, 0)
    styles = []
    for col in row.index:
        if col in SUMMARY_COLS:
            styles.append("")
        else:
            if row[col] == "--":
                styles.append("background-color: white")
            elif row[col] >= threshold:
                styles.append("background-color: lightgreen")
            else:
                styles.append("background-color: lightyellow")
    return styles

# ==============================
# 7. Save Leaderboard (unchanged)
# ==============================
if __name__ == "__main__":
    leaderboard = build_leaderboard("2025-09-15", "2025-10-31")
    leaderboard.to_csv("leaderboard.csv")
    with open("leaderboard.md", "w", encoding="utf-8") as f:
        f.write("# 🚴 Jalgaon Cyclist Club – Daily Leaderboard\n\n")
        f.write(leaderboard.to_markdown())
    daily_cols = [c for c in leaderboard.columns if c not in SUMMARY_COLS] + ["Total"]
    formatters = {col: blank_zero for col in daily_cols}
    formatters["Active_Days"] = lambda v: "" if pd.isna(v) else f"{int(v)}"
    styled = leaderboard.style.apply(color_cells_by_threshold, axis=1).format(formatters)
    with open("leaderboard.html", "w", encoding="utf-8") as f:
        f.write("""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="utf-8">
            <title>Jalgaon Cyclist Club Leaderboard</title>
            <style>
                body { font-family: Arial, sans-serif; background: #f5f5f5; text-align: center; padding: 20px; }
                h1 { color: #fc4c02; }
                table { margin: 20px auto; border-collapse: collapse; width: 80%; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 6px rgba(0,0,0,0.15); }
                th, td { padding: 10px 12px; border-bottom: 1px solid #ddd; text-align: center; }
                th { background: #fc4c02; color: white; position: sticky; top: 0; }
                tr:nth-child(even) { background: #fafafa; }
                tr:hover { background: #ffe9e0; }
            </style>
        </head>
        <body>
            <h1>🚴 Jalgaon Cyclist Club – Daily Leaderboard</h1>
        """)
        f.write(styled.to_html(escape=False))
        f.write("""
        <a href="https://www.strava.com" target="_blank">
        <img src="api_logo_pwrdBy_strava_horiz_orange.png" 
         alt="Powered by Strava" 
         height="40">
        </a>
        </body>
        </html>
        """)
    print("✅ Leaderboard built and saved (CSV, MD, HTML)")
