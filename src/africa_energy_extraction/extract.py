"""Resumable extraction: one raw JSON file per (group, topic, year).

The endpoint accepts every year in one request, so each topic costs a single
call (7 total). The response is split by year on disk, which keeps files small
and makes re-runs skip whatever is already there.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

from . import config
from .catalog import iter_topics
from .client import CloudflareBlocked, PortalError


def slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def raw_path(raw_dir: Path, group: str, topic: str, year: int) -> Path:
    return Path(raw_dir) / slug(group) / slug(topic) / f"{year}.json"


def countries_in(response: list[dict]) -> set[str]:
    return {row["name"] for block in response for row in block.get("data", [])}


def split_by_year(response: list[dict], years: list[int]) -> dict[int, list[dict]]:
    """Re-slice a multi-year response into one response-shaped list per year."""
    out: dict[int, list[dict]] = {y: [] for y in years}
    for block in response:
        rows_by_year: dict[int, list[dict]] = {}
        for row in block.get("data", []):
            rows_by_year.setdefault(int(row["year"]), []).append(row)
        for year, rows in rows_by_year.items():
            if year in out:
                out[year].append({**block, "data": rows})
    return out


def _write(path: Path, data: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data), encoding="utf-8")
    tmp.replace(path)


def extract(client, countries: list[str], years: list[int] | None = None,
            raw_dir: Path = config.RAW_DIR, force: bool = False, log=print) -> dict:
    """Fetch every (group, topic) that has missing years. Written atomically."""
    years = years or config.YEARS
    stats = {"fetched": 0, "skipped": 0, "failed": [], "missing_countries": []}
    seen: set[str] = set()

    for group, topic, indicators in iter_topics():
        needed = [y for y in years
                  if force or not raw_path(raw_dir, group, topic, y).exists()]
        stats["skipped"] += len(years) - len(needed)
        if not needed:
            continue
        try:
            data = client.post_database(group, topic, indicators, needed, countries)
        except CloudflareBlocked:
            raise
        except PortalError as exc:
            log(f"FAILED {group}/{topic} ({needed[0]}-{needed[-1]}): {exc}")
            stats["failed"].append(f"{group}/{topic}")
            continue
        for year, year_data in split_by_year(data, needed).items():
            _write(raw_path(raw_dir, group, topic, year), year_data)
        seen |= countries_in(data)
        stats["fetched"] += len(needed)
        log(f"ok {group}/{topic}: {len(needed)} years, {len(data)} indicators")

    if seen:
        stats["missing_countries"] = sorted(set(countries) - seen)
    return stats
