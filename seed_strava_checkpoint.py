#!/usr/bin/env python3
"""
Seed or update strava_checkpoint.json with athlete refresh tokens.

Usage examples:
  python seed_strava_checkpoint.py --csv athletes.csv
  python seed_strava_checkpoint.py --interactive
  python seed_strava_checkpoint.py --csv athletes.csv --output ./strava_checkpoint.json
"""
import argparse
import csv
import json
import os
import sys
from datetime import datetime

DEFAULT_OUTPUT = "strava_checkpoint.json"

def load_checkpoint(path: str) -> dict:
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except Exception as e:
            print(f"⚠️ Warning: failed to load existing checkpoint ({e}). Starting fresh.")
    return {"athletes": {}}

def save_checkpoint_atomic(path: str, data: dict):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, sort_keys=True, ensure_ascii=False)
    os.replace(tmp, path)
    try:
        os.chmod(path, 0o600)
    except Exception:
        pass

def seed_from_csv(path: str, checkpoint: dict):
    added = 0
    with open(path, "r", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        for i, row in enumerate(reader, start=1):
            if not row or all(not c.strip() for c in row):
                continue
            if i == 1 and (row[0].strip().lower().startswith("athlete") or row[0].strip().lower().startswith("id")):
                continue
            if len(row) < 2:
                print(f"⚠ Skipping line {i}: expected at least athlete_id,refresh_token (got: {row})")
                continue
            athlete_id = str(row[0]).strip()
            refresh_token = row[1].strip()
            name = row[2].strip() if len(row) >= 3 else None
            if not athlete_id or not refresh_token:
                print(f"⚠ Skipping line {i}: empty athlete_id or refresh_token.")
                continue
            entry = checkpoint.setdefault("athletes", {}).setdefault(athlete_id, {})
            entry["refresh_token"] = refresh_token
            if name:
                entry["name"] = name
            entry.setdefault("seeded_at", datetime.utcnow().isoformat() + "Z")
            added += 1
    return added

def seed_interactive(checkpoint: dict):
    print("Interactive mode — enter athlete_id and refresh_token separated by comma.")
    print("Type ENTER on empty line to finish.")
    added = 0
    while True:
        try:
            line = input("athlete_id,refresh_token[,name]> ").strip()
        except EOFError:
            print()
            break
        if not line:
            break
        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 2:
            print("❌ Need at least athlete_id and refresh_token. Try again.")
            continue
        athlete_id = parts[0]
        refresh_token = parts[1]
        name = parts[2] if len(parts) >= 3 else None
        entry = checkpoint.setdefault("athletes", {}).setdefault(str(athlete_id), {})
        entry["refresh_token"] = refresh_token
        if name:
            entry["name"] = name
        entry.setdefault("seeded_at", datetime.utcnow().isoformat() + "Z")
        added += 1
    return added

def main():
    parser = argparse.ArgumentParser(description="Seed strava_checkpoint.json with athlete refresh tokens.")
    parser.add_argument("--csv", "-c", help="CSV file path (athlete_id,refresh_token[,name])")
    parser.add_argument("--interactive", "-i", help="Interactive input mode", action="store_true")
    parser.add_argument("--output", "-o", help="Output checkpoint file", default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    if not args.csv and not args.interactive:
        parser.print_help()
        print("\nError: must pass --csv or --interactive")
        sys.exit(1)

    output_path = args.output
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    checkpoint = load_checkpoint(output_path)
    before_count = len(checkpoint.get("athletes", {}))

    added = 0
    if args.csv:
        if not os.path.exists(args.csv):
            print(f"Error: CSV file not found: {args.csv}")
            sys.exit(2)
        added = seed_from_csv(args.csv, checkpoint)
    if args.interactive:
        added += seed_interactive(checkpoint)

    save_checkpoint_atomic(output_path, checkpoint)
    after_count = len(checkpoint.get("athletes", {}))
    print(f"\n✅ Done. Athletes before: {before_count}, after: {after_count}. New/updated entries: {added}")
    print(f"Saved checkpoint to: {output_path}")
    print("Keep this file secure. Example structure:")
    print(json.dumps({"athletes": {"12345": {"refresh_token": "xxx", "name": "Anil", "seeded_at": "2025-09-22T...Z"}}}, indent=2))

if __name__ == "__main__":
    main()
