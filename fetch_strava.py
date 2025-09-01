import os, requests, time, json, calendar, datetime

CLIENT_ID = os.getenv("STRAVA_CLIENT_ID")
CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET")
REFRESH_TOKENS = os.getenv("REFRESH_TOKENS").split(",")  # comma-separated

def refresh_access_token(refresh_token):
    url = "https://www.strava.com/oauth/token"
    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token
    }
    res = requests.post(url, data=payload)
    res.raise_for_status()
    return res.json()["access_token"]

def fetch_activities(access_token, after_timestamp):
    url = "https://www.strava.com/api/v3/athlete/activities"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"after": after_timestamp, "per_page": 200}
    res = requests.get(url, headers=headers, params=params)
    res.raise_for_status()
    return res.json()

if __name__ == "__main__":
    today = datetime.date.today()
    year, month = today.year, today.month

    # start of this month
    start_date = datetime.date(year, month, 1)
    _, last_day = calendar.monthrange(year, month)
    end_date = datetime.date(year, month, last_day)

    after = int(time.mktime(start_date.timetuple()))
    before = int(time.mktime((end_date + datetime.timedelta(days=1)).timetuple()))

    leaderboard = {
        "year": year,
        "month": month,
        "days": [f"{d:02d}/{calendar.month_abbr[month]}" for d in range(1, last_day+1)],
        "riders": []
    }

    for rt in REFRESH_TOKENS:
        try:
            at = refresh_access_token(rt)
            rides = fetch_activities(at, after)

            # daily distances map
            daily = {d: 0.0 for d in range(1, last_day+1)}
            name, athlete_id = None, None

            for r in rides:
                if r["type"] != "Ride":
                    continue
                dt = datetime.datetime.strptime(r["start_date_local"], "%Y-%m-%dT%H:%M:%SZ")
                if dt.month == month and dt.year == year:
                    day = dt.day
                    km = r["distance"] / 1000
                    daily[day] += km
                    if not name:
                        athlete_id = r["athlete"]["id"]
                        # try to extract athlete name if present
                        name = r["athlete"].get("firstname","") + " " + r["athlete"].get("lastname","")

            leaderboard["riders"].append({
                "athlete_id": athlete_id,
                "name": name.strip() or f"Athlete {athlete_id}",
                "daily": [round(daily[d],1) for d in range(1,last_day+1)],
                "total": round(sum(daily.values()),1)
            })

        except Exception as e:
            print("Error:", e)

    with open("data.json","w") as f:
        json.dump(leaderboard,f,indent=2)
