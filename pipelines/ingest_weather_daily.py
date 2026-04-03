from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import date, datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RAW_BASE = os.path.join(ROOT, "data", "raw", "weather_city_daily")

CITIES: list[tuple[str, str, float, float]] = [
    ("moscow", "Moscow", 55.7558, 37.6173),
    ("berlin", "Berlin", 52.52, 13.405),
    ("london", "London", 51.5074, -0.1278),
    ("nyc", "New York", 40.7128, -74.0060),
    ("tokyo", "Tokyo", 35.6762, 139.6503),
]

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"


def fetch_city_day(
    city_slug: str,
    city_name: str,
    lat: float,
    lon: float,
    day: date,
    timeout_sec: float = 30.0,
) -> dict:
    params = urllib.parse.urlencode(
        {
            "latitude": lat,
            "longitude": lon,
            "start_date": day.isoformat(),
            "end_date": day.isoformat(),
            "daily": "temperature_2m_max,temperature_2m_min",
            "timezone": "UTC",
        }
    )
    url = f"{ARCHIVE_URL}?{params}"
    req = urllib.request.Request(url, headers={"User-Agent": "openmeteo-dbt-pipeline/1.0"})
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        body = resp.read().decode("utf-8")
    data = json.loads(body)
    daily = data.get("daily") or {}
    times = daily.get("time") or []
    tmax = daily.get("temperature_2m_max") or []
    tmin = daily.get("temperature_2m_min") or []
    if not times or not tmax or not tmin:
        raise ValueError(f"Unexpected payload for {city_slug}: {data!r:.500}")
    return {
        "city_slug": city_slug,
        "city_name": city_name,
        "latitude": lat,
        "longitude": lon,
        "observation_date": times[0],
        "temp_max_c": float(tmax[0]) if tmax[0] is not None else None,
        "temp_min_c": float(tmin[0]) if tmin[0] is not None else None,
    }


def default_target_day() -> date:
    return (datetime.now(timezone.utc) - timedelta(days=1)).date()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default=None)
    args = parser.parse_args()
    day = date.fromisoformat(args.date) if args.date else default_target_day()
    run_id = str(uuid.uuid4())
    ingested_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    rows: list[dict] = []
    for slug, name, lat, lon in CITIES:
        try:
            r = fetch_city_day(slug, name, lat, lon, day)
            r["ingested_at_utc"] = ingested_at
            r["pipeline_run_id"] = run_id
            rows.append(r)
            print("OK", slug, day, r.get("temp_max_c"), r.get("temp_min_c"))
        except (urllib.error.URLError, urllib.error.HTTPError, ValueError, json.JSONDecodeError) as e:
            print("ERR", slug, e, file=sys.stderr)
        time.sleep(0.35)

    if not rows:
        print("No rows ingested; aborting.", file=sys.stderr)
        return 1

    out_dir = os.path.join(RAW_BASE, f"dt={day.isoformat()}")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "cities.parquet")

    table = pa.Table.from_pylist(rows)
    pq.write_table(table, out_path, compression="zstd")
    print("Wrote", out_path, "rows=", len(rows))
    return 0


if __name__ == "__main__":
    sys.exit(main())
