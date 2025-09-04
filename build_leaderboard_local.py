import os
import json
import requests
import gspread
import pandas as pd
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials

# Instead of environment variables, hardcode here for local testing
STRAVA_CLIENT_ID = "173295"   # <-- replace with your real client ID
STRAVA_CLIENT_SECRET = "f2e31dce78855292af40bd0936c7f19258be5558"  # <-- replace with your secret

# ==============================
# 1. Google Sheets Authentication
# ==============================
google_creds = os.environ.get("GOOGLE_SHEETS_JSON")

if google_creds:
    # Load from environment (string)
    creds_dict = json.loads(google_creds)
else:
    # Fallback: load from local file
    if not os.path.exists("service_account.json"):
        raise FileNotFoundError("❌ No GOOGLE_SHEETS_JSON env var and no service_account.json file found.")
    with open("service_account.json", "r") as f:
        creds_dict = json.load(f)

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(credentials)

# Load sheet
SHEET_URL = os.environ.get("SHEET_URL")  # stored in GitHub Secrets
if not SHEET_URL:
    SHEET_URL = "https://docs.google.com/spreadsheets/d/1XSYLi5k6xjoLFo9CW8XkgciPgMN1m7sofWlE1BhbVmU/edit"

sheet = client.open_by_url(SHEET_URL).sheet1

# Expected format: [Timestamp, AthleteName, RefreshToken]
rows = sheet.get_all_values()
header, data = rows[0], rows[1:]

# Convert to dict list (Firstname + Lastname, and refresh token from col 7)
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
# STRAVA_CLIENT_ID = os.environ.get("STRAVA_CLIENT_ID")
# STRAVA_CLIENT_SECRET = os.environ.get("STRAVA_CLIENT_SECRET")

def get_access_token(refresh_token):
    url = "https://www.strava.com/oauth/token"
    payload = {
        "client_id": STRAVA_CLIENT_ID,
        "client_secret": STRAVA_CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token
    }
    print("DEBUG - Using Client ID:", STRAVA_CLIENT_ID)
    print("DEBUG - Using Client Secret starts with:", STRAVA_CLIENT_SECRET[:6] if STRAVA_CLIENT_SECRET else None)
    print("DEBUG - Refresh token starts with:", refresh_token[:6] if refresh_token else None)

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

from datetime import datetime, timedelta
import pandas as pd

def build_leaderboard(start_date: str, end_date: str):
    """
    Build leaderboard for a custom date range (up to today).
    Args:
        start_date (str): "YYYY-MM-DD"
        end_date   (str): "YYYY-MM-DD"
    """
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt   = datetime.strptime(end_date, "%Y-%m-%d")

    # Don't go beyond today's date
    today = datetime.today()
    if end_dt > today:
        end_dt = today

    # Build list of day labels (DD/MM, but add year if range spans > 1 year)
    if start_dt.year != end_dt.year:
        date_fmt = "%d/%m/%y"
    else:
        date_fmt = "%d/%m"

    days = [
        (start_dt + timedelta(days=i)).strftime(date_fmt)
        for i in range((end_dt - start_dt).days + 1)
    ]

    leaderboard = pd.DataFrame(0.0, index=[a["name"] for a in athletes], columns=days)

    for athlete in athletes:
        print(f"➡ Fetching {athlete['name']}")
        access_token = get_access_token(athlete["refresh_token"])
        if not access_token:
            print(f"⚠ Skipping {athlete['name']} (no access token)")
            continue

        activities = fetch_activities(access_token, start_dt, end_dt)
        for act in activities:
            if act.get("type") == "Ride":
                act_date = datetime.strptime(act["start_date_local"], "%Y-%m-%dT%H:%M:%SZ")
                if start_dt <= act_date <= end_dt:
                    col = act_date.strftime(date_fmt)
                    distance_km = act["distance"] / 1000.0
                    leaderboard.loc[athlete["name"], col] += distance_km

    # Add total column at the end
    leaderboard["Total"] = leaderboard.sum(axis=1)
	# Sort by Total descending
	#leaderboard = leaderboard.sort_values("Total", ascending=False)

    # Round only at the very end
    return leaderboard.round(1)


def color_rows_by_total(row):
    total = row["Total"]
    if total >= 200:   # high mileage
        return ["background-color: lightgreen"] * len(row)
    elif total >= 100: # medium mileage
        return ["background-color: lightyellow"] * len(row)
    else:              # low mileage
        return ["background-color: lightcoral"] * len(row)


# ==============================
# 5. Save Leaderboard
# ==============================
if __name__ == "__main__":
    leaderboard = build_leaderboard("2025-08-15", "2025-09-30")

    # Save as CSV (for analysis)
    leaderboard.to_csv("leaderboard.csv")

    # Save as Markdown (for GitHub Pages content)
    with open("leaderboard.md", "w", encoding="utf-8") as f:
        f.write("# 🚴 Jalgaon Cyclist Club – Daily Leaderboard\n\n")
        f.write(leaderboard.to_markdown())

# 🔥 Save as HTML (for direct viewing on GitHub Pages)
with open("leaderboard.html", "w", encoding="utf-8") as f:
    f.write("""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="utf-8">
        <title>Jalgaon Cyclist Club Leaderboard</title>
        <title>powered by Strava</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                background: #f5f5f5;
                text-align: center;
                padding: 20px;
            }
            h1 {
                color: #fc4c02; /* Strava orange */
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
    f.write(leaderboard.round(1).style.apply(color_rows_by_total, axis=1).format("{:.1f}").to_html(escape=False))
    f.write("""
	<h1>🚴 Powered by Strava</h1>
    </body>
    </html>
    """)

    print("✅ Leaderboard built and saved (CSV, MD, HTML)")
