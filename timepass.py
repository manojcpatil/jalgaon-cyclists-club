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

    # Authenticate and get athlete list
    athletes = authenticate_google_sheets()

    # Create Excel writer
    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        for athlete in athletes:
            print(f"➡ Fetching data for {athlete['name']}")
            access_token = get_access_token(athlete["refresh_token"])
            if not access_token:
                print(f"⚠ Skipping {athlete['name']} (no access token)")
                continue

            # Fetch all activities for the athlete
            activities = fetch_activities(access_token, start_dt, end_dt)

            # Prepare data for the athlete
            if not activities:
                print(f"⚠ No activities found for {athlete['name']}")
                continue

            # Extract all fields from activities
            activity_data = []
            for act in activities:
                # Convert activity to a flat dictionary with all available fields
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
                    "Start_Latitude": act.get("start_latitude"),
                    "Start_Longitude": act.get("start_longitude"),
                    "End_Latitude": act.get("end_latitude"),
                    "End_Longitude": act.get("end_longitude"),
                    "Achievement_Count": act.get("achievement_count"),
                    "Kudos_Count": act.get("kudos_count"),
                    "Comment_Count": act.get("comment_count"),
                    "Athlete_Count": act.get("athlete_count"),
                    "Map_ID": act.get("map", {}).get("id"),
                    "Gear_ID": act.get("gear_id"),
                    "Has_Heart_Rate": act.get("has_heartrate"),
                    "Average_Heart_Rate": act.get("average_heartrate"),
                    "Max_Heart_Rate": act.get("max_heartrate"),
                    "Device_Name": act.get("device_name"),
                    "Workout_Type": act.get("workout_type"),
                    "External_ID": act.get("external_id"),
                    "Manual": act.get("manual"),
                    "Private": act.get("private"),
                    "Visibility": act.get("visibility"),
                    "Description": act.get("description"),
                    "Trainer": act.get("trainer"),
                    "Commute": act.get("commute"),
                    "Flagged": act.get("flagged"),
                    "Start_Date_UTC": act.get("start_date"),
                    "Timezone": act.get("timezone"),
                    "Total_Photo_Count": act.get("total_photo_count"),
                    "PR_Count": act.get("pr_count"),
                }
                activity_data.append(activity_dict)

            # Convert to DataFrame
            df = pd.DataFrame(activity_data)

            # Sort by Start_Date for readability
            if not df.empty:
                df["Start_Date"] = pd.to_datetime(df["Start_Date"], utc=True).dt.tz_localize(None)
                df = df.sort_values(by="Start_Date")
                # Format numeric columns
                df["Distance_km"] = df["Distance_km"].round(2)

                for col in ["Moving_Time_s", "Elapsed_Time_s", "Total_Elevation_Gain_m",
                            "Average_Speed_mps", "Max_Speed_mps", "Average_Cadence",
                            "Average_Watts", "Max_Watts", "Calories", "Average_Heart_Rate",
                            "Max_Heart_Rate"]:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors="coerce")  # convert or NaN
                        df[col] = df[col].round(2).fillna("")

            # Write to Excel sheet named after the athlete
            sheet_name = athlete["name"][:31]  # Excel sheet names have a 31-char limit
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"✅ Athlete data saved to {output_file}")


# ==============================
# 5. Main Execution
# ==============================
if __name__ == "__main__":
    # Extract all athlete data to Excel for the specified date range
    extract_athlete_data_to_excel("2025-08-01", "2025-10-31", "athlete_data.xlsx")
