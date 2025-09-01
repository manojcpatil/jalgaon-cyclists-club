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
creds_dict = json.loads(google_creds)

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(credentials)

# Load sheet
SHEET_URL = os.environ.get("SHEET_URL")  # stored in GitHub Secrets
sheet = client.open_by_url(SHEET_URL).sheet1

# Expected format: [Timestamp, AthleteName, RefreshToken]
rows = sheet.get_all_values()
header, data = rows[0], rows[1:]

# Convert to dict list
athletes = [{"name": row[1], "refresh_token": row[2]} for row in data if len(row) >= 3]


# ==============================
# 2. Strava Token Exchange
# ==============================
STRAVA_CLIENT_ID = os.environ.get("STRAVA_CLIENT_ID")
STRAVA_CLIENT_SECRET = os.environ.get("STRAVA_CLIENT_SECRET")

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
# 4. Build Leaderboard
# ==============================
def build_leaderboard(month: str = "2025-08"):
    # Setup date range
    start_date = datetime.strptime(month, "%Y-%m")
    days_in_month = (start_date.replace(month=start_date.month % 12 + 1, day=1) - timedelta(days=1)).day
    end_date = start_date.replace(day=days_in_month, hour=23, minute=59, second=59)

    # Prepare leaderboard DataFrame
    days = [f"{day:02d}/{start_date.strftime('%b')}" for day in range(1, days_in_month + 1)]
    leaderboard = pd.DataFrame(0, index=[a["name"] for a in athletes], columns=days)

    for athlete in athletes:
        print(f"➡ Fetching {athlete['name']}")

        access_token = get_access_token(athlete["refresh_token"])
        if not access_token:
            continue

        activities = fetch_activities(access_token, start_date, end_date)
        for act in activities:
            if act.get("type") == "Ride":
                act_date = datetime.strptime(act["start_date_local"], "%Y-%m-%dT%H:%M:%SZ")
                col = f"{act_date.day:02d}/{act_date.strftime('%b')}"
                distance_km = act["distance"] / 1000.0
                leaderboard.loc[athlete["name"], col] += round(distance_km, 1)

    return leaderboard


# ==============================
# 5. Save Leaderboard
# ==============================
if __name__ == "__main__":
    leaderboard = build_leaderboard("2025-08")

    # Save as CSV (for analysis)
    leaderboard.to_csv("leaderboard.csv")

    # Save as Markdown (for GitHub Pages content)
    with open("leaderboard.md", "w") as f:
        f.write("# 🚴 Jalgaon Cyclist Club – August Leaderboard\n\n")
        f.write(leaderboard.to_markdown())

    # 🔥 Save as HTML (for direct viewing on GitHub Pages)
    with open("leaderboard.html", "w", encoding="utf-8") as f:
        f.write("<html><head><meta charset='utf-8'><title>Leaderboard</title></head><body>")
        f.write("<h1>🚴 Jalgaon Cyclist Club – August Leaderboard</h1>")
        f.write(leaderboard.to_html(escape=False))  # HTML table
        f.write("</body></html>")

    print("✅ Leaderboard built and saved (CSV, MD, HTML)")
