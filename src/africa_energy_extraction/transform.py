"""Raw portal JSON -> clean long-format table (one row per country/indicator/year).

Unlike the original wide-format version, missing values stay missing (null).
Turning "no data" into 0 corrupts averages and trends.
"""
from __future__ import annotations

import json
import math
import re
from pathlib import Path

import pandas as pd
import pycountry

from . import config

KEY = ["iso3", "group", "indicator", "year"]
COLUMNS = ["iso3", "country", "region", "group", "topic", "indicator", "metric",
           "unit", "source", "source_url", "year", "value"]


def to_iso3(iso2: str | None) -> str:
    """Portal ids are ISO2. ISO3 is the key because ISO2 'NA' (Namibia) is read
    as a missing value by many CSV tools."""
    match = pycountry.countries.get(alpha_2=iso2) if iso2 else None
    if match is None:
        raise ValueError(f"Cannot map portal country id {iso2!r} to an ISO3 code")
    return match.alpha_3


def clean_metric(indicator: str) -> str:
    """'Population access to electricity-National (% of population)' -> without unit."""
    metric = re.sub(r"\s*\(.*?\)\s*$", "", indicator)
    metric = re.sub(r"^(Energy|Electricity):\s*", "", metric)
    return metric.strip()


def _to_float(value) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(number) else number


def flatten(response: list[dict]) -> list[dict]:
    records = []
    for block in response:
        for row in block.get("data", []):
            indicator = (row.get("indicator_name") or block.get("_id") or "").strip()
            url = row.get("url")
            records.append({
                "iso3": to_iso3(row.get("id")),
                "country": row.get("name"),
                "region": row.get("region_name"),
                "group": row.get("indicator_group"),
                "topic": row.get("indicator_topic"),
                "indicator": indicator,
                "metric": clean_metric(indicator),
                "unit": row.get("unit"),
                "source": row.get("indicator_source"),
                "source_url": f"{config.BASE_URL}{url}" if url else config.DATABASE_PAGE,
                "year": int(row["year"]),
                "value": _to_float(row.get("score")),
            })
    return records


def build_dataframe(raw_dir: Path = config.RAW_DIR) -> pd.DataFrame:
    records: list[dict] = []
    for path in sorted(Path(raw_dir).rglob("*.json")):
        records.extend(flatten(json.loads(path.read_text(encoding="utf-8"))))
    if not records:
        return pd.DataFrame(columns=COLUMNS)
    df = pd.DataFrame(records, columns=COLUMNS)
    df = df.drop_duplicates(subset=KEY, keep="last")
    return df.sort_values(KEY).reset_index(drop=True)


def validate(df: pd.DataFrame) -> list[str]:
    """Return human-readable findings. Never raises, never mutates the data."""
    if df.empty:
        return ["ERROR: no rows produced"]
    issues = []

    pct = df[df["unit"].fillna("").str.contains("%") & df["value"].notna()]
    out_of_range = pct[(pct["value"] < 0) | (pct["value"] > 100)]
    if len(out_of_range):
        issues.append(f"WARN: {len(out_of_range)} percentage values outside 0-100")

    units = df.groupby("indicator")["unit"].nunique()
    mixed = units[units > 1]
    if len(mixed):
        issues.append(f"WARN: {len(mixed)} indicators have more than one unit: {list(mixed.index)[:3]}")

    nulls = int(df["value"].isna().sum())
    issues.append(f"INFO: {nulls} of {len(df)} rows have no value (kept as null)")

    series = df.dropna(subset=["value"]).groupby(["iso3", "indicator"])["value"]
    all_zero = int((series.max() == 0).sum())
    if all_zero:
        issues.append(
            f"INFO: {all_zero} country/indicator series are zero in every year; "
            "the portal may use 0 for 'no data', so treat these with care")

    issues.append(
        f"INFO: {df['iso3'].nunique()} countries, {df['indicator'].nunique()} indicators, "
        f"years {df['year'].min()}-{df['year'].max()}")
    return issues


def run(raw_dir: Path = config.RAW_DIR, out_csv: Path = config.PROCESSED_CSV) -> pd.DataFrame:
    df = build_dataframe(raw_dir)
    for line in validate(df):
        print(line)
    out_csv = Path(out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv, index=False)
    print(f"wrote {len(df)} rows to {out_csv}")
    return df
