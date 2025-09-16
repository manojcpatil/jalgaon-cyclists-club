#!/usr/bin/env python3
"""
build_dashboard.py
Reads athlete_data.json and writes dashboard.html (standalone, interactive).
Requirements:
    pip install pandas plotly jinja2
Run:
    python build_dashboard.py --input athlete_data.json --output dashboard.html
"""

import argparse
import json
import os
from datetime import datetime

import pandas as pd
import plotly.express as px
import plotly.io as pio
from jinja2 import Template

# ---------------------------
# Helpers
# ---------------------------
def safe_read_json(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data

def seconds_to_hms(s):
    try:
        s = int(s)
        h = s // 3600
        m = (s % 3600) // 60
        sec = s % 60
        return f"{h:02d}:{m:02d}:{sec:02d}"
    except Exception:
        return ""

# ---------------------------
# Build figures
# ---------------------------
def make_figures(df):
    figs = {}

    # Ensure numeric columns exist
    numeric_cols = ["Distance_km", "Moving_Time_s", "Average_Speed_mps", "Total_Elevation_Gain_m", "Average_Watts"]
    for c in numeric_cols:
        if c not in df.columns:
            df[c] = pd.NA
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce").fillna(0)

    # Parse date
    if "Start_Date" in df.columns:
        df["Start_Date"] = pd.to_datetime(df["Start_Date"], errors="coerce")
    else:
        df["Start_Date"] = pd.NaT

    # 1) Histogram of Distance_km
    figs["hist_distance"] = px.histogram(
        df,
        x="Distance_km",
        nbins=30,
        title="Distribution of activity distances (km)",
        labels={"Distance_km": "Distance (km)"},
        marginal="box",
        hover_data=["Athlete_Name", "Type", "Start_Date"]
    )

    # 2) Histogram of Average Speed
    figs["hist_speed"] = px.histogram(
        df[df["Average_Speed_mps"] > 0],
        x="Average_Speed_mps",
        nbins=30,
        title="Distribution of average speed (m/s)",
        labels={"Average_Speed_mps": "Average speed (m/s)"},
        marginal="box",
        hover_data=["Athlete_Name", "Type", "Start_Date"]
    )

    # 3) Pie chart: activity type share by count
    type_counts = df["Type"].fillna("Unknown").value_counts().reset_index()
    type_counts.columns = ["Type", "Count"]
    figs["pie_type"] = px.pie(
        type_counts,
        names="Type",
        values="Count",
        title="Activity type distribution (by count)",
        hole=0.3
    )

    # 4) Time series: total distance per day (stacked by athlete)
    daily = (
        df.dropna(subset=["Start_Date"])
        .assign(Date=lambda d: d["Start_Date"].dt.date)
        .groupby(["Date", "Athlete_Name"], as_index=False)["Distance_km"]
        .sum()
    )
    if len(daily) > 0:
        figs["line_daily"] = px.line(
            daily,
            x="Date",
            y="Distance_km",
            color="Athlete_Name",
            title="Daily total distance (km) by athlete",
            labels={"Distance_km": "Total distance (km)"}
        )
    else:
        figs["line_daily"] = None

    # 5) Scatter: distance vs avg speed
    figs["scatter_dist_speed"] = px.scatter(
        df,
        x="Distance_km",
        y="Average_Speed_mps",
        color="Type",
        hover_data=["Athlete_Name", "Start_Date"],
        title="Distance vs average speed"
    )

    # 6) Pivot table: Athlete x Type sum distance
    pivot = pd.pivot_table(
        df,
        index="Athlete_Name",
        columns="Type",
        values="Distance_km",
        aggfunc="sum",
        fill_value=0
    )
    pivot = pivot.reset_index().rename_axis(None, axis=1)

    return figs, pivot

# ---------------------------
# HTML template
# ---------------------------
HTML_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Athlete Dashboard</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <!-- Plotly -->
  <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
  <!-- jQuery + DataTables -->
  <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
  <link rel="stylesheet" href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css" />
  <script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
  <style>
    body { font-family: system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial; margin: 20px; }
    .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 18px; }
    .full { grid-column: 1 / -1; }
    .card { background: #fff; border-radius: 8px; padding: 12px; box-shadow: 0 1px 6px rgba(0,0,0,0.06); }
    table.dataTable { width: 100% !important; }
    h2 { margin: 6px 0 12px; font-size: 1.1rem; }
    .small { font-size: 0.85rem; color: #555; }
    .meta { margin-bottom: 12px; }
  </style>
</head>
<body>
  <h1>Activity dashboard</h1>
  <div class="meta small">Generated: {{ generated_at }}</div>

  <div class="grid">
    <div class="card">
      <h2>Activity type distribution</h2>
      <div id="pie_type"></div>
    </div>

    <div class="card">
      <h2>Distance distribution (km)</h2>
      <div id="hist_distance"></div>
    </div>

    <div class="card">
      <h2>Average speed distribution (m/s)</h2>
      <div id="hist_speed"></div>
    </div>

    <div class="card">
      <h2>Distance vs Average Speed</h2>
      <div id="scatter_dist_speed"></div>
    </div>

    <div class="card full">
      <h2>Daily total distance by athlete</h2>
      <div id="line_daily"></div>
    </div>

    <div class="card full">
      <h2>Raw data (interactive)</h2>
      {{ raw_table_html | safe }}
    </div>

    <div class="card full">
      <h2>Pivot table: total distance (km) by athlete and type</h2>
      {{ pivot_table_html | safe }}
    </div>
  </div>

  <script>
    // Plotly figures embedded as JSON -> Plotly.react targets
    const figs = {};
    {% for k,v in plot_json.items() %}
    figs["{{k}}"] = {{ v | safe }};
    {% endfor %}

    // Render plotly figures to divs (if figure is null skip)
    if (figs['pie_type']) Plotly.newPlot('pie_type', figs['pie_type'].data, figs['pie_type'].layout || {});
    if (figs['hist_distance']) Plotly.newPlot('hist_distance', figs['hist_distance'].data, figs['hist_distance'].layout || {});
    if (figs['hist_speed']) Plotly.newPlot('hist_speed', figs['hist_speed'].data, figs['hist_speed'].layout || {});
    if (figs['scatter_dist_speed']) Plotly.newPlot('scatter_dist_speed', figs['scatter_dist_speed'].data, figs['scatter_dist_speed'].layout || {});
    if (figs['line_daily']) Plotly.newPlot('line_daily', figs['line_daily'].data, figs['line_daily'].layout || {});

    // DataTables init
    $(document).ready(function() {
      $('#raw_table').DataTable({
          pageLength: 15,
          order: [[0, 'desc']]
      });
      $('#pivot_table').DataTable({
          paging: false,
          searching: false,
          info: false,
          ordering: true
      });
    });
  </script>
</body>
</html>
"""

# ---------------------------
# Main
# ---------------------------
def build_dashboard(input_path, output_path):
    data = safe_read_json(input_path)
    df = pd.DataFrame(data)

    # small cleaning & ordering
    if "Start_Date" in df.columns:
        df["Start_Date"] = pd.to_datetime(df["Start_Date"], errors="coerce")
    # ensure columns we want exist
    default_cols = [
        "Activity_ID", "Athlete_ID", "Athlete_Name", "Type", "Name", "Start_Date",
        "Distance_km", "Moving_Time_s", "Elapsed_Time_s", "Total_Elevation_Gain_m",
        "Average_Speed_mps", "Max_Speed_mps", "Average_Cadence", "Average_Watts"
    ]
    for c in default_cols:
        if c not in df.columns:
            df[c] = pd.NA

    # Create human-friendly columns
    df["Distance_km"] = pd.to_numeric(df["Distance_km"], errors="coerce").fillna(0)
    df["Moving_Time_readable"] = df["Moving_Time_s"].apply(seconds_to_hms)
    df["Start_Date_str"] = df["Start_Date"].dt.strftime("%Y-%m-%d %H:%M:%S")

    # figures and pivot
    figs, pivot_df = make_figures(df)

    # Prepare plot JSON for template
    plot_json = {}
    for k, fig in figs.items():
        if fig is None:
            plot_json[k] = "null"
        else:
            # Use plotly.io.to_json to capture data+layout
            plot_json[k] = pio.to_json(fig, validate=False)

    # Raw table HTML
    # display_cols = ["Activity_ID", "Athlete_Name", "Type", "Name", "Start_Date_str", "Distance_km", "Moving_Time_readable", "Total_Elevation_Gain_m", "Average_Speed_mps"]
    # raw_table_html = df[display_cols].sort_values("Start_Date_str", ascending=False).to_html(classes="display nowrap", table_id="raw_table", index=False, escape=False)
    # Raw table HTML
    display_cols = ["Athlete_Name", "Type", "Distance_km", "Total_Elevation_Gain_m"]
    raw_table_html = df[display_cols].to_html(
        classes="display nowrap",
        table_id="raw_table",
        index=False,
        escape=False
    )

    # Pivot HTML
    pivot_table_html = pivot_df.to_html(classes="display", table_id="pivot_table", index=False, float_format="%.2f", escape=False)

    # Render template
    tpl = Template(HTML_TEMPLATE)
    out_html = tpl.render(
        generated_at=datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        raw_table_html=raw_table_html,
        pivot_table_html=pivot_table_html,
        plot_json={k: plot_json[k] for k in plot_json}
    )

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(out_html)

    print(f"Dashboard written to {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build athlete dashboard from JSON.")
    parser.add_argument("--input", "-i", default="athlete_data.json", help="Path to athlete_data.json")
    parser.add_argument("--output", "-o", default="dashboard.html", help="Output HTML file")
    args = parser.parse_args()

    if not os.path.exists(args.input):
        raise FileNotFoundError(f"Input file not found: {args.input}")

    build_dashboard(args.input, args.output)
