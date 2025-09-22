def get_refresh_token_from_sheet_by_athlete_id(target_athlete_id: str):
    """
    Try to find refresh token and a human name for a given athlete id in the Google Sheet.
    Returns dict: {"refresh_token": "...", "row_index": <int>, "name": "..."} or None if not found.

    Looks for common column names: Athlete_ID, athlete_id, strava_id, id
    Looks for refresh token columns: refresh_token, RefreshToken, refreshToken
    """
    google_creds = os.environ.get("GOOGLE_SHEETS_JSON")
    SHEET_URL = os.environ.get("SHEET_URL")

    if not google_creds or not SHEET_URL:
        print("⚠ GOOGLE_SHEETS_JSON or SHEET_URL not configured; cannot read sheet for athlete tokens.")
        return None

    try:
        creds_dict = json.loads(google_creds)
    except Exception as e:
        print("⚠ GOOGLE_SHEETS_JSON is not valid JSON:", e)
        return None

    try:
        import gspread
        from oauth2client.service_account import ServiceAccountCredentials
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(credentials)
        sheet = client.open_by_url(SHEET_URL).sheet1
        records = sheet.get_all_records()
    except Exception as e:
        print("⚠ Error opening Google Sheet:", e)
        return None

    # Normalize target id to string
    target_str = str(target_athlete_id).strip()

    id_candidates = ["Athlete_ID", "athlete_id", "strava_id", "id", "owner_id"]
    token_candidates = ["refresh_token", "RefreshToken", "refreshToken", "refresh"]

    for idx, row in enumerate(records, start=2):
        # row is a dict keyed by header
        for id_col in id_candidates:
            if id_col in row and row[id_col] not in (None, "") and str(row[id_col]).strip() == target_str:
                # found row
                # find a refresh token
                for tok_col in token_candidates:
                    if tok_col in row and row[tok_col] not in (None, ""):
                        name = None
                        # try to construct a friendly name from columns if present
                        for ncol in ("Name", "name", "Athlete_Name", "Firstname", "firstname"):
                            if ncol in row and row[ncol]:
                                name = str(row[ncol]).strip()
                                break
                        # fallback to reading columns 3 & 4 if sheet is positional
                        if not name:
                            try:
                                values = sheet.row_values(idx)
                                if len(values) > 4:
                                    name = f"{values[3]} {values[4]}".strip()
                            except Exception:
                                name = None
                        return {"refresh_token": str(row[tok_col]).strip(), "row_index": idx, "name": name}
    return None


def fetch_and_sync_single_athlete(athlete_id: str,
                                  start_date: Optional[str] = None,
                                  end_date: Optional[str] = None,
                                  output_csv: str = OUTPUT_CSV,
                                  output_json: str = OUTPUT_JSON) -> dict:
    """
    Fetch activities for a single athlete (by athlete id) and upsert into CSV/JSON.

    Returns a dict with status and summary, e.g. {"status":"ok","fetched": N, "merged": M}
    Raises/returns error dicts in case of problems.
    """
    # determine date window
    if start_date and start_date.strip():
        s_date = start_date
    else:
        s_date = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%d")
    if end_date and end_date.strip():
        e_date = end_date
    else:
        e_date = datetime.utcnow().strftime("%Y-%m-%d")

    print(f"ℹ️ fetch_and_sync_single_athlete: athlete_id={athlete_id} start={s_date} end={e_date}")

    # 1) try to find refresh token in Google Sheet first
    sheet_lookup = get_refresh_token_from_sheet_by_athlete_id(athlete_id)
    refresh_token = None
    athlete_name = None
    sheet_row_index = None
    if sheet_lookup:
        refresh_token = sheet_lookup.get("refresh_token")
        athlete_name = sheet_lookup.get("name")
        sheet_row_index = sheet_lookup.get("row_index")
        print(f"ℹ️ Found refresh token in Google Sheet for athlete {athlete_id} (row {sheet_row_index})")
    else:
        # 2) try checkpoint
        cp = load_checkpoint()
        athletes = cp.get("athletes", {})
        # search checkpoint for matching athlete_id
        for k, v in athletes.items():
            if str(v.get("athlete_id", "") ) == str(athlete_id) or str(v.get("Athlete_ID", "")) == str(athlete_id):
                refresh_token = v.get("refresh_token")
                athlete_name = v.get("athlete_name") or v.get("Athlete_Name")
                print(f"ℹ️ Found refresh token in checkpoint for key {k}")
                break

    if not refresh_token:
        # 3) try CSV lookup (maybe earlier runs stored refresh_token there)
        try:
            if os.path.exists(output_csv):
                df = pd.read_csv(output_csv, dtype=str)
                if "Athlete_ID" in df.columns:
                    match = df[df["Athlete_ID"].astype(str) == str(athlete_id)]
                    if not match.empty and "refresh_token" in df.columns:
                        refresh_token = match.iloc[0].get("refresh_token")
                        athlete_name = match.iloc[0].get("Athlete_Name") or athlete_name
                        print(f"ℹ️ Found refresh_token in CSV for athlete {athlete_id}")
        except Exception as e:
            print("⚠ CSV lookup error while searching refresh token:", e)

    if not refresh_token:
        msg = f"No refresh token found for athlete {athlete_id}. Cannot fetch activities."
        print("⚠ " + msg)
        return {"status": "error", "reason": "no_refresh_token", "message": msg}

    # 2) exchange refresh token for access token
    token_resp = exchange_refresh_for_access(refresh_token)
    if not token_resp or "access_token" not in token_resp:
        msg = f"Token exchange failed for athlete {athlete_id}"
        print("⚠ " + msg)
        return {"status": "error", "reason": "token_exchange_failed", "message": msg, "raw": token_resp}

    access_token = token_resp.get("access_token")
    new_refresh = token_resp.get("refresh_token")
    athlete_id_from_token = token_resp.get("athlete", {}).get("id") if token_resp.get("athlete") else None
    if athlete_id_from_token:
        # prefer token-provided athlete id
        print(f"ℹ️ Token response athlete id: {athlete_id_from_token}")

    # update checkpoint with refreshed token (rotation)
    try:
        cp = load_checkpoint()
        athletes = cp.setdefault("athletes", {})
        key = f"{sheet_row_index or athlete_id}_{athlete_name or ''}"
        athletes.setdefault(key, {})
        if new_refresh:
            athletes[key]["refresh_token"] = new_refresh
        if athlete_id_from_token:
            athletes[key]["athlete_id"] = str(athlete_id_from_token)
        if athlete_name:
            athletes[key]["athlete_name"] = athlete_name
        save_checkpoint(cp)
        print(f"✅ Updated checkpoint for key {key}")
    except Exception as e:
        print("⚠ Could not update checkpoint:", e)

    # 3) fetch activities using existing fetch_activities_for_athlete helper
    session = requests.Session()
    last_ts = None
    # if there is a stored last_activity_ts for this athlete in checkpoint, use it to avoid duplicates
    try:
        cp = load_checkpoint()
        for k, v in cp.get("athletes", {}).items():
            if str(v.get("athlete_id", "")) == str(athlete_id) or k.startswith(str(athlete_id)):
                last_ts_str = v.get("last_activity_ts")
                if last_ts_str:
                    try:
                        last_ts = int(datetime.fromisoformat(last_ts_str).timestamp())
                    except Exception:
                        last_ts = None
                break
    except Exception:
        last_ts = None

    try:
        activities = fetch_activities_for_athlete(session, access_token, after_ts=last_ts, start_date=datetime.strptime(s_date, "%Y-%m-%d"), end_date=datetime.strptime(e_date, "%Y-%m-%d"))
    except Exception as e:
        print("⚠ Error fetching activities:", e)
        return {"status": "error", "reason": "fetch_failed", "message": str(e)}

    if not activities:
        print(f"ℹ️ No new activities found for athlete {athlete_id} in window {s_date}..{e_date}")
        # still update last_activity_ts to now so we don't keep scanning old window
        try:
            cp = load_checkpoint()
            athletes = cp.setdefault("athletes", {})
            key = f"{sheet_row_index or athlete_id}_{athlete_name or ''}"
            athletes.setdefault(key, {})["last_activity_ts"] = datetime.utcnow().isoformat()
            save_checkpoint(cp)
        except Exception:
            pass
        return {"status": "ok", "fetched": 0, "merged": 0, "message": "no_new_activities"}

    # convert fetched activities into DataFrame
    rows = []
    newest_ts = last_ts or 0
    for act in activities:
        start_date_utc = act.get("start_date")
        try:
            dt = datetime.fromisoformat(start_date_utc.replace("Z", "+00:00"))
            ts = int(dt.timestamp())
            if ts > newest_ts:
                newest_ts = ts
        except Exception:
            ts = None

        row = {
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
            "Athlete_Name": athlete_name or (act.get("athlete", {}).get("firstname") if act.get("athlete") else None),
            "map_polyline": act.get("map", {}).get("polyline", None)
        }
        rows.append(row)

    df_new = pd.DataFrame(rows)
    if "Start_Date" in df_new.columns:
        df_new["Start_Date"] = pd.to_datetime(df_new["Start_Date"], errors="coerce").dt.tz_localize(None)
    if "Start_Date_UTC" in df_new.columns:
        df_new["Start_Date_UTC"] = pd.to_datetime(df_new["Start_Date_UTC"], errors="coerce").dt.tz_localize(None)
    if "Distance_km" in df_new.columns:
        df_new["Distance_km"] = df_new["Distance_km"].round(2)

    # Merge with existing output files (similar to main flow)
    final_df = df_new
    try:
        if os.path.exists(output_json):
            prev_json = pd.read_json(output_json)
            combined = pd.concat([prev_json, final_df], ignore_index=True, sort=False)
            if "Activity_ID" in combined.columns:
                combined.drop_duplicates(subset=["Activity_ID"], keep="last", inplace=True)
            final_df = combined
    except Exception as e:
        print("⚠ Could not read/merge existing JSON:", e)

    try:
        if os.path.exists(output_csv):
            prev_csv = pd.read_csv(output_csv, dtype=str)
            # convert numeric columns if required
            combined = pd.concat([prev_csv, final_df], ignore_index=True, sort=False)
            if "Activity_ID" in combined.columns:
                combined.drop_duplicates(subset=["Activity_ID"], keep="last", inplace=True)
            final_df = combined
    except Exception as e:
        print("⚠ Could not read/merge existing CSV:", e)

    # Write outputs atomically
    try:
        csv_tmp = output_csv + ".tmp"
        json_tmp = output_json + ".tmp"
        # Save CSV: ensure dataframe columns normalized
        pd.DataFrame(final_df).to_csv(csv_tmp, index=False)
        final_df.to_json(json_tmp, orient="records", date_format="iso")
        os.replace(csv_tmp, output_csv)
        os.replace(json_tmp, output_json)
        print(f"✅ Athlete activities merged & saved to {output_csv} and {output_json}")
    except Exception as e:
        print("❌ Error saving merged outputs:", e)
        return {"status": "error", "reason": "save_failed", "message": str(e)}

    # update checkpoint last_activity_ts to newest fetched
    try:
        cp = load_checkpoint()
        athletes = cp.setdefault("athletes", {})
        key = f"{sheet_row_index or athlete_id}_{athlete_name or ''}"
        athletes.setdefault(key, {})["last_activity_ts"] = datetime.utcfromtimestamp(newest_ts).isoformat() if newest_ts else datetime.utcnow().isoformat()
        # store rotated refresh token if present
        if new_refresh:
            athletes[key]["refresh_token"] = new_refresh
        save_checkpoint(cp)
        print(f"✅ Checkpoint updated for {key}")
    except Exception as e:
        print("⚠ Could not update checkpoint after merge:", e)

    return {"status": "ok", "fetched": len(rows), "merged_total_rows": len(final_df)}


def process_missing_athletes_file(missing_file: str = "missing athletes.txt",
                                  processed_file: str = "missing_athletes_processed.log",
                                  backup_file: str = "missing_athletes.backup",
                                  max_per_run: int = 50,
                                  start_date: Optional[str] = None,
                                  end_date: Optional[str] = None) -> dict:
    """
    Read athlete IDs from `missing_file` (one ID per line), call fetch_and_sync_single_athlete()
    for each, remove successfully processed IDs from the file, and log results.

    Returns a summary dict:
      {"processed": N, "errors": M, "details": [{"athlete_id": id, "status": "...", "raw": ...}, ...]}

    Behaviour:
      - Lines starting with '#' or empty lines are ignored.
      - If fetch_and_sync_single_athlete is not available, returns error.
      - Successfully processed lines are removed from missing_file (atomic write).
      - Failed lines remain in missing_file for retry (and get one-line log in processed_file).
    """
    summary = {"processed": 0, "errors": 0, "details": []}

    # Ensure the missing_file exists
    if not os.path.exists(missing_file):
        print(f"ℹ️ No missing file found at {missing_file}; nothing to do.")
        return summary

    # Read lines
    with open(missing_file, "r", encoding="utf-8") as fh:
        raw_lines = fh.readlines()

    # Normalize and filter
    candidates = []
    for ln in raw_lines:
        s = ln.strip()
        if not s or s.startswith("#"):
            continue
        # possibly lines with comma-separated values -> take first token
        s_clean = s.split()[0].strip().strip(",")
        if s_clean:
            candidates.append(s_clean)

    if not candidates:
        print(f"ℹ️ No valid athlete ids found in {missing_file}.")
        return summary

    # Limit how many processed in a single run (avoid rate limits)
    to_process = candidates[:max_per_run]
    remaining = candidates[max_per_run:]

    print(f"ℹ️ Found {len(candidates)} athlete ids; processing {len(to_process)} (max_per_run={max_per_run}).")

    details = []
    processed_ids = []
    failed_ids = []

    for athlete_id in to_process:
        try:
            print(f"\n➡ Processing missing athlete: {athlete_id}")
            # call the helper you already added earlier
            res = fetch_and_sync_single_athlete(str(athlete_id), start_date=start_date, end_date=end_date)
            # Accept success status loosely: {"status":"ok", ...}
            if isinstance(res, dict) and res.get("status") == "ok":
                print(f"✅ Success: {athlete_id} -> fetched {res.get('fetched', 0)} activities")
                summary["processed"] += 1
                processed_ids.append(athlete_id)
                details.append({"athlete_id": athlete_id, "status": "ok", "result": res})
            else:
                # treat anything else as failure, but capture raw res
                print(f"⚠ Failure for {athlete_id}: {res}")
                summary["errors"] += 1
                failed_ids.append(athlete_id)
                details.append({"athlete_id": athlete_id, "status": "error", "result": res})
        except Exception as e:
            print(f"❌ Exception processing {athlete_id}: {e}")
            summary["errors"] += 1
            failed_ids.append(athlete_id)
            details.append({"athlete_id": athlete_id, "status": "exception", "error": str(e)})

        # small sleep to avoid burst rate limits
        time.sleep(1.0)

    # Build new contents for missing_file: remaining (unprocessed) + failed_ids (to retry next time)
    new_remaining = []
    # keep any original header/comments from raw_lines (lines starting with '#')
    header_comments = [ln for ln in raw_lines if ln.strip().startswith("#")]
    new_remaining.extend(header_comments)

    # keep IDs that were not touched (beyond max_per_run)
    for r in remaining:
        new_remaining.append(r + "\n")
    # keep failed ids to try again next run
    for f in failed_ids:
        new_remaining.append(f + "\n")

    # Backup the original file (atomic)
    try:
        if os.path.exists(backup_file):
            # rotate older backup
            os.replace(backup_file, backup_file + ".old")
        os.replace(missing_file, backup_file)
    except Exception:
        # If rename fails, fall back to copy
        try:
            import shutil
            shutil.copyfile(missing_file, backup_file)
        except Exception:
            pass

    # Write new missing_file atomically
    try:
        tmp = missing_file + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            for ln in new_remaining:
                fh.write(ln if ln.endswith("\n") else (ln + "\n"))
        os.replace(tmp, missing_file)
        print(f"ℹ️ Updated {missing_file}: retained {len(new_remaining)} lines (header/comments + remaining + failed).")
    except Exception as e:
        print(f"❌ Failed to write updated missing file: {e}")

    # Append details to processed log
    try:
        with open(processed_file, "a", encoding="utf-8") as pf:
            ts = datetime.utcnow().isoformat()
            for d in details:
                pf.write(json.dumps({"ts": ts, **d}, default=str) + "\n")
    except Exception as e:
        print(f"⚠ Could not append to processed log {processed_file}: {e}")

    summary["details"] = details
    return summary


# --- Simple CLI wrapper so you can call this script directly ---
def run_process_missing_cli():
    import argparse
    parser = argparse.ArgumentParser(description="Process missing athletes file and fetch their activities.")
    parser.add_argument("--file", "-f", default="missing athletes.txt", help="Path to missing athletes file")
    parser.add_argument("--max", "-m", type=int, default=50, help="Max athletes to process per run")
    parser.add_argument("--start", default=None, help="Optional START_DATE override (YYYY-MM-DD)")
    parser.add_argument("--end", default=None, help="Optional END_DATE override (YYYY-MM-DD)")
    args = parser.parse_args()

    summary = process_missing_athletes_file(missing_file=args.file, max_per_run=args.max,
                                           start_date=args.start, end_date=args.end)
    print("\n=== Summary ===")
    print(json.dumps(summary, indent=2, default=str))


# If you saved this as a separate script, you can run it
if __name__ == "__main__" and False:   # set to True if you want this file's CLI to run standalone
    run_process_missing_cli()
