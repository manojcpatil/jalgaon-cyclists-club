# Insert near the point where you have `athletes = read_sheet()` and before processing them
import math, time

PER_PAGE = 30  # will fetch last 30 activities in one GET
READ_LIMIT_15MIN = 300
OVERALL_LIMIT_15MIN = 600
SAFETY_BUFFER_READ = 20    # leave some headroom
SAFETY_BUFFER_OVERALL = 50

# compute safe batch size:
usable_read = READ_LIMIT_15MIN - SAFETY_BUFFER_READ
# Ensure we also respect overall limit if each athlete needs 1 token-exchange + 1 read worst-case:
usable_overall_batch = (OVERALL_LIMIT_15MIN - SAFETY_BUFFER_OVERALL) // 2
batch_size = min(usable_read, usable_overall_batch)
# cap for extra safety
batch_size = min(batch_size, 250)

print(f"Using batch_size={batch_size} (per_page={PER_PAGE})")

def chunk_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

# athletes is the list of athlete rows from your sheet
batches = list(chunk_list(athletes, batch_size))
for batch_index, batch in enumerate(batches, start=1):
    print(f"Processing batch {batch_index}/{len(batches)}: {len(batch)} athletes")
    for a in batch:
        # attempt: use Access Token first to avoid unnecessary token exchange
        access_token = a.get("Access Token")  # whatever your sheet header is
        refresh_token = a.get("Refresh Token")

        # Try GET with access_token
        headers = {"Authorization": f"Bearer {access_token}"} if access_token else None
        params = {"per_page": PER_PAGE, "page": 1}
        resp = requests.get("https://www.strava.com/api/v3/athlete/activities",
                            headers=headers, params=params, timeout=30)

        if resp.status_code == 200:
            acts = resp.json()
        elif resp.status_code == 401 and refresh_token:
            # exchange refresh -> access, then retry
            token = exchange_refresh_for_access(refresh_token)  # your existing function
            if token:
                headers = {"Authorization": f"Bearer {token}"}
                resp = requests.get("https://www.strava.com/api/v3/athlete/activities",
                                    headers=headers, params=params, timeout=30)
                if resp.status_code == 200:
                    acts = resp.json()
                else:
                    print(f"Failed to fetch after refresh for {a.get('Firstname')}: {resp.status_code}")
                    acts = []
            else:
                print(f"Refresh exchange failed for {a.get('Firstname')}")
                acts = []
        else:
            print(f"Failed fetch for {a.get('Firstname')}, status: {resp.status_code}")
            acts = []

        # flatten & append acts to global list (as your script already does)
        # ...

    # after finishing the batch, if more batches remain, sleep to respect 15-min window
    if batch_index < len(batches):
        sleep_seconds = 15 * 60
        print(f"Batch {batch_index} done. Sleeping {sleep_seconds}s to respect Strava rate limits...")
        time.sleep(sleep_seconds)
