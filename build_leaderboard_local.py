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


def blank_zero(v):
    try:
        if float(v) == 0:
            return "--"   # render blank
        return f"{v:.1f}"   # render with 1 decimal place
    except:
        return v

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

SUMMARY_COLS = ["Total","Active_Days"]

def build_leaderboard(start_date: str, end_date: str):
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    today = datetime.today()
    if end_dt > today:
        end_dt = today

    valid_types = {"Ride", "Run", "Walk"}
    exclude_types = {"VirtualRide", "EBikeRide"}

    # Generate all dates in the range
    all_dates = [start_dt + timedelta(days=i) for i in range((end_dt - start_dt).days + 1)]
    # Create MultiIndex tuples for columns: (Month, Day)
    day_labels = [(d.strftime("%b-%Y"), d.strftime("%d")) for d in all_dates]

    # MultiIndex for daily columns
    daily_cols = pd.MultiIndex.from_tuples(day_labels, names=["Month", "Day"])
    # MultiIndex for summary columns
    summary_cols = pd.MultiIndex.from_tuples([("Summary", "Total"), ("Summary", "Active_Days")],
                                             names=["Month", "Day"])
    # Combine daily + summary columns
    all_columns = daily_cols.append(summary_cols)

    # MultiIndex for rows: Athlete x Activity Type
    index = pd.MultiIndex.from_product(
        [[a["name"] for a in athletes], sorted(valid_types)],
        names=["Athlete", "Type"]
    )

    # Initialize leaderboard
    leaderboard = pd.DataFrame(0.0, index=index, columns=all_columns)

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
                    col = (act_date.strftime("%b-%Y"), act_date.strftime("%d"))
                    if col in leaderboard.columns:
                        distance_km = act["distance"] / 1000.0
                        leaderboard.loc[(athlete["name"], act_type), col] += distance_km

    # Add summary columns
    leaderboard[("Summary", "Total")] = leaderboard[daily_cols].sum(axis=1)

    # Active_Days: count of days above threshold per activity type
    active_days = []
    for (athlete, act_type), row in leaderboard.iterrows():
        threshold = THRESHOLDS.get(act_type, 0)
        days_count = sum(1 for col in daily_cols if row[col] >= threshold)
        active_days.append(days_count)

    leaderboard[("Summary", "Active_Days")] = active_days

    # Round distances
    leaderboard = leaderboard.round(1)

    return leaderboard

# ==============================
# 4b. Cell Coloring Function
# ==============================
def color_cells_by_threshold(row):
    act_type = row.name[1]
    threshold = THRESHOLDS.get(act_type, 0)

    styles = []
    for col in row.index:
        if col in SUMMARY_COLS:
            styles.append("")  # skip styling
        else:
            if row[col] == "--":
                styles.append("background-color: white")
            elif row[col] >= threshold:
                styles.append("background-color: lightgreen")
            else:
                styles.append("background-color: lightyellow")
    return styles


# ==============================
# 5. Save Leaderboard
# ==============================
if __name__ == "__main__":
    leaderboard = build_leaderboard("2025-08-14", "2025-10-31")

    leaderboard.to_csv("leaderboard.csv")

    with open("leaderboard.md", "w", encoding="utf-8") as f:
        f.write("# 🚴 Jalgaon Cyclist Club – Daily Leaderboard\n\n")
        f.write(leaderboard.to_markdown())

    # Prepare formatters: daily columns -> blank_zero, totals -> one decimal, Active_Days -> integer
    daily_cols = [c for c in leaderboard.columns if c not in SUMMARY_COLS]+["Total"]

    formatters = {col: blank_zero for col in daily_cols}
    # keep Total numeric with 1 decimal
    # formatters["Total"] = lambda v: "" if pd.isna(v) else f"{v:.1f}"
    # Active_Days as integer (no decimals)
    formatters["Active_Days"] = lambda v: "" if pd.isna(v) else f"{int(v)}"

    # Apply styling and formatting (no mutation of leaderboard values)
    styled = (
        leaderboard.style
        .apply(color_cells_by_threshold, axis=1)
        .format(formatters)
    )
    
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
        # f.write(leaderboard.style.apply(color_cells_by_threshold, axis=1).format(blank_zero).to_html(escape=False))
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
