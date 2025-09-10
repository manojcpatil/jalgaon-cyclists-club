import os
import json
import requests
import gspread
import pandas as pd
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials


# ==============================
# 1. Google Sheets Authentication
# ==============================
google_creds = os.environ.get("GOOGLE_SHEETS_JSON")

if not google_creds:
    raise ValueError("❌ Missing GOOGLE_SHEETS_JSON secret in GitHub.")

creds_dict = json.loads(google_creds)

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(credentials)

# Load sheet from secret
SHEET_URL = os.environ.get("SHEET_URL")
if not SHEET_URL:
    raise ValueError("❌ Missing SHEET_URL secret in GitHub.")

sheet = client.open_by_url(SHEET_URL).sheet1

# Expected format: [Timestamp, AthleteName, RefreshToken]
rows = sheet.get_all_values()
header, data = rows[0], rows[1:]

athletes = [
    {
        "name": f"{row[3]} {row[4]}".strip(),
        "refresh_token": row[7]
    }
    for row in data if len(row) >= 8
]


# ==============================
# 2. Strava Token Exchange
# ==============================
STRAVA_CLIENT_ID = os.environ.get("STRAVA_CLIENT_ID")
STRAVA_CLIENT_SECRET = os.environ.get("STRAVA_CLIENT_SECRET")

if not STRAVA_CLIENT_ID or not STRAVA_CLIENT_SECRET:
    raise ValueError("❌ Missing STRAVA_CLIENT_ID or STRAVA_CLIENT_SECRET in GitHub Secrets.")

def get_access_token(refresh_token):
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
        print("❌ Token exchange failed:", r.text)
        return None


# ==============================
# 3. Fetch Activities
# ==============================
def fetch_activities(access_token, start_date, end_date):
    url = "https://www.strava.com/api/v3/athlete/activities"
    headers = {"Authorization": f"Bearer {access_token}"}
    page, per_page = 1, 100
    activities = []
    end_date += timedelta(days=1)

    while True:
        params = {
            "before": int(end_date.timestamp()),
            "after": int(start_date.timestamp()),
            "page": page,
            "per_page": per_page,
        }
        r = requests.get(url, headers=headers, params=params)
        if r.status_code != 200:
            print("❌ Error fetching activities:", r.text)
            break

        data = r.json()
        if not data:
            break
        activities.extend(data)
        page += 1

    return activities

# ==============================
# 4. Build Leaderboard (multi-type + totals + active days)
# ==============================
THRESHOLDS = {
    "Ride": 15,
    "Run": 5,
    "Walk": 5
}

SUMMARY_COLS = ["Total", "Active_Days"]

def build_leaderboard(start_date: str, end_date: str):
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt   = datetime.strptime(end_date, "%Y-%m-%d")

    today = datetime.today()
    if end_dt > today:
        end_dt = today

    # Format date labels
    date_fmt = "%d/%m/%y" if start_dt.year != end_dt.year else "%d/%m"
    days = [
        (start_dt + timedelta(days=i)).strftime(date_fmt)
        for i in range((end_dt - start_dt).days + 1)
    ]

    valid_types   = {"Ride", "Run", "Walk"}
    exclude_types = {"VirtualRide", "EBikeRide"}

    # MultiIndex = Athlete + Activity type
    index = pd.MultiIndex.from_product(
        [[a["name"] for a in athletes], sorted(valid_types)],
        names=["Athlete", "Type"]
    )
    leaderboard = pd.DataFrame(0.0, index=index, columns=days)

    # Fill distances
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
                    col = act_date.strftime(date_fmt)
                    distance_km = act["distance"] / 1000.0
                    leaderboard.loc[(athlete["name"], act_type), col] += distance_km

    # Add totals
    leaderboard["Total"] = leaderboard.sum(axis=1)

    # Add "Active_Days" (count of days above threshold)
    active_days = []
    for (athlete, act_type), row in leaderboard.iterrows():
        threshold = THRESHOLDS.get(act_type, 0)
        days_count = sum(1 for col in days if row[col] >= threshold)
        active_days.append(days_count)

    leaderboard["Active_Days"] = active_days

    return leaderboard.round(1)


# ==============================
# 4b. Cell Coloring Function
# ==============================
def color_cells_by_threshold(row):
    act_type = row.name[1]   # (Athlete, Type)
    threshold = THRESHOLDS.get(act_type, 0)

    styles = []
    for col in row.index:
        if col in SUMMARY_COLS:
            styles.append("")  # no style for summary columns
        else:
            val = row[col]
            if val >= threshold:
                styles.append("background-color: lightgreen")   # met threshold
            elif val > 0:
                styles.append("background-color: lightyellow")  # some activity
            else:
                styles.append("background-color: white")   # no activity
    return styles


# ==============================
# 5. Save Leaderboard
# ==============================
if __name__ == "__main__":
    leaderboard = build_leaderboard("2025-08-15", "2025-09-30")

    leaderboard.to_csv("leaderboard.csv")

    with open("leaderboard.md", "w", encoding="utf-8") as f:
        f.write("# 🚴 Jalgaon Cyclist Club – Daily Leaderboard\n\n")
        f.write(leaderboard.to_markdown())

    with open("leaderboard.html", "w", encoding="utf-8") as f:
        f.write("""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="utf-8">
            <title>Jalgaon Cyclist Club Leaderboard</title>
            <style>
                body {
                    font-family: Arial, sans-serif;
                    background: #f5f5f5;
                    text-align: center;
                    padding: 20px;
                }
                h1 {
                    color: #fc4c02;
                }
                table {
                    margin: 20px auto;
                    border-collapse: collapse;
                    width: 80%;
                    background: white;
                    border-radius: 8px;
                    overflow: hidden;
                    box-shadow: 0 2px 6px rgba(0,0,0,0.15);
                }
                th, td {
                    padding: 10px 12px;
                    border-bottom: 1px solid #ddd;
                    text-align: center;
                }
                th {
                    background: #fc4c02;
                    color: white;
                    position: sticky;
                    top: 0;
                }
                tr:nth-child(even) {
                    background: #fafafa;
                }
                tr:hover {
                    background: #ffe9e0;
                }
            </style>
        </head>
        <body>
            <h1>🚴 Jalgaon Cyclist Club – Daily Leaderboard</h1>
        """)    
        f.write(leaderboard.style.apply(color_cells_by_threshold, axis=1).format("{:.1f}").to_html(escape=False))
        f.write("""
        <a href="https://www.strava.com" target="_blank">
        <img src="api_logo_pwrdBy_strava_horiz_orange.png" 
         alt="Powered by Strava" 
         height="40">
        </a>
        <h1>🚴 Powered by Strava</h1>
        </body>
        </html>
        """)

    print("✅ Leaderboard built and saved (CSV, MD, HTML)")
