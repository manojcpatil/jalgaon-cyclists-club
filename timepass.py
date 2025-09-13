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
def authenticate_google_sheets():
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
        {
            "name": f"{row[3]} {row[4]}".strip(),
            "refresh_token": row[7]
        }
        for row in data if len(row) >= 8
    ]
    return athletes


# ==============================
# 2. Strava Token Exchange
# ==============================
def get_access_token(refresh_token):
    STRAVA_CLIENT_ID = os.environ.get("STRAVA_CLIENT_ID")
    STRAVA_CLIENT_SECRET = os.environ.get("STRAVA_CLIENT_SECRET")

    if not STRAVA_CLIENT_ID or not STRAVA_CLIENT_SECRET:
        raise ValueError("❌ Missing STRAVA_CLIENT_ID or STRAVA_CLIENT_SECRET in GitHub Secrets.")

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
# 4. Extract All Athlete Data to Excel
# ==============================
def extract_athlete_data_to_excel(start_date: str, end_date: str, output_file: str = "athlete_data.xlsx"):
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    today = datetime.today()
    if end_dt > today:
        end_dt = today

    athletes = authenticate_google_sheets()

    all_data = []

    for athlete in athletes:
        print(f"➡ Fetching data for {athlete['name']}")
        access_token = get_access_token(athlete["refresh_token"])
        if not access_token:
            print(f"⚠ Skipping {athlete['name']} (no access token)")
            continue

        activities = fetch_activities(access_token, start_dt, end_dt)
        if not activities:
            print(f"⚠ No activities found for {athlete['name']}")
            continue

        activity_data = []
        for act in activities:
            activity_dict = {
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
                "Athlete_Name": athlete["name"]
            }
            activity_data.append(activity_dict)

        df = pd.DataFrame(activity_data)

        if not df.empty:
            # Clean datetimes
            for col in ["Start_Date", "Start_Date_UTC"]:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce")

            # Round numeric
            df["Distance_km"] = df["Distance_km"].round(2)

            all_data.append(df)

    # Combine all athlete data
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
    else:
        final_df = pd.DataFrame([{"Athlete_ID": None, "Athlete_Name": None, "Message": "No activities found"}])

    # Add Month and Day columns for pivoting
    if "Start_Date" in final_df.columns:
        final_df["Month"] = final_df["Start_Date"].dt.month
        final_df["Day"] = final_df["Start_Date"].dt.day

    # Create pivot table
    pivot_df = pd.pivot_table(
        final_df,
        values="Distance_km",
        index=["Athlete_Name", "Type"],
        columns=["Month", "Day"],
        aggfunc="sum",
        fill_value=0
    )

    # Save both sheets
    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        final_df.to_excel(writer, sheet_name="Raw_Data", index=False)
        pivot_df.to_excel(writer, sheet_name="Pivot_Table")

    print(f"✅ Athlete data saved with pivot table to {output_file}")


# ==============================
# 5. Main Execution
# ==============================
if __name__ == "__main__":
    # Extract all athlete data to Excel for the specified date range
    extract_athlete_data_to_excel("2025-08-01", "2025-10-31", "athlete_data.xlsx")
