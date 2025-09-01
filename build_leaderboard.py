import os
import json
import requests
import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

# =========================
# CONFIG
# =========================
STRAVA_CLIENT_ID = os.environ["STRAVA_CLIENT_ID"]
STRAVA_CLIENT_SECRET = os.environ["STRAVA_CLIENT_SECRET"]

# Date range (example: whole August 2025)
START_DATE = datetime.date(2025, 8, 1)
END_DATE   = datetime.date(2025, 8, 31)

# Google Sheet details
SHEET_NAME = "Strava Auth Codes"
WORKSHEET  = "Sheet1"

# =========================
# AUTH TO GOOGLE SHEETS
# =========================
def get_sheet():
    scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds_dict = json.loads(os.environ["GOOGLE_SHEETS_JSON"])  # stored in GitHub secret
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    return client.open(SHEET_NAME).worksheet(WORKSHEET)

# =========================
# STRAVA TOKEN REFRESH
# =========================
def refresh_access_token(refresh_token):
    url = "https://www.strava.com/oauth/token"
    payload = {
        "client_id": STRAVA_CLIENT_ID,
        "client_secret": STRAVA_CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token
    }
    resp = requests.post(url, data=payload)
    if resp.status_code != 200:
        raise Exception(f"Token refresh failed: {resp.text}")
    data = resp.json()
    return data["access_token"], data["refresh_token"], data["athlete"]["id"]

# =========================
# GET ACTIVITIES
# =========================
def fetch_rides(access_token, after, before):
    url = "https://www.strava.com/api/v3/athlete/activities"
    headers = {"Authorization": f"Bearer {access_token}"}
    rides = []
    page = 1
    while True:
        params = {"after": after, "before": before, "page": page, "per_page": 100}
        r = requests.get(url, headers=headers, params=params)
        if r.status_code != 200:
            print("Error fetching activities:", r.text)
            break
        data = r.json()
        if not data:
            break
        for act in data:
            if act["type"] == "Ride":
                rides.append(act)
        page += 1
    return rides

# =========================
# MAIN
# =========================
def main():
    ws = get_sheet()
    rows = ws.get_all_records()  # expects columns: AthleteID, Name, RefreshToken

    # Prepare leaderboard structure
    all_days = pd.date_range(START_DATE, END_DATE)
    leaderboard = { row["Name"]: { d.strftime("%d/%b"): 0.0 for d in all_days } for row in rows }

    for row in rows:
        athlete = row["Name"]
        refresh_token = row["RefreshToken"]

        try:
            access_token, new_refresh, athlete_id = refresh_access_token(refresh_token)

            # (Optional) update refresh token in Google Sheet if changed
            if new_refresh != refresh_token:
                cell = ws.find(refresh_token)
                ws.update_cell(cell.row, cell.col, new_refresh)

            # Fetch rides
            after = int(datetime.datetime.combine(START_DATE, datetime.time.min).timestamp())
            before = int(datetime.datetime.combine(END_DATE + datetime.timedelta(days=1), datetime.time.min).timestamp())
            rides = fetch_rides(access_token, after, before)

            # Aggregate km per day
            for ride in rides:
                ride_date = datetime.datetime.strptime(ride["start_date_local"], "%Y-%m-%dT%H:%M:%SZ").date()
                key = ride_date.strftime("%d/%b")
                distance_km = ride["distance"] / 1000.0
                if key in leaderboard[athlete]:
                    leaderboard[athlete][key] += round(distance_km, 1)

            print(f"✅ Processed {athlete}, {len(rides)} rides")

        except Exception as e:
            print(f"❌ Error processing {athlete}: {e}")

    # Save leaderboard to JSON
    with open("data.json", "w") as f:
        json.dump(leaderboard, f, indent=2)

    print("🏆 Leaderboard built and saved to data.json")

if __name__ == "__main__":
    main()
