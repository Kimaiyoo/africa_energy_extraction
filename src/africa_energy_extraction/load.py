"""Idempotent MongoDB loader: re-running never duplicates documents."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from pymongo import MongoClient, UpdateOne

from . import config

KEY_FIELDS = ("iso3", "group", "indicator", "year")


def build_ops(records: list[dict]) -> list[UpdateOne]:
    now = datetime.now(timezone.utc)
    return [
        UpdateOne({k: rec[k] for k in KEY_FIELDS}, {"$set": {**rec, "updated_at": now}}, upsert=True)
        for rec in records
    ]


def ensure_indexes(collection) -> None:
    collection.create_index([(k, 1) for k in KEY_FIELDS], unique=True, name="uniq_observation")
    collection.create_index([("indicator", 1), ("year", 1)], name="by_indicator_year")
    collection.create_index([("iso3", 1), ("year", 1)], name="by_country_year")


def load_records(records: list[dict], uri: str = config.MONGO_URI, db: str = config.MONGO_DB,
                 collection: str = config.MONGO_COLLECTION, batch_size: int = 1000) -> dict:
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    try:
        col = client[db][collection]
        ensure_indexes(col)
        upserted = modified = 0
        for i in range(0, len(records), batch_size):
            result = col.bulk_write(build_ops(records[i:i + batch_size]), ordered=False)
            upserted += result.upserted_count
            modified += result.modified_count
        return {"upserted": upserted, "modified": modified, "total": len(records)}
    finally:
        client.close()


def records_from_csv(path: Path = config.PROCESSED_CSV) -> list[dict]:
    # Read everything as text and convert explicitly, so no value is ever
    # reinterpreted by pandas' NA rules. Empty cells become null.
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    records = []
    for rec in df.to_dict(orient="records"):
        rec = {k: (None if v == "" else v) for k, v in rec.items()}
        rec["year"] = int(rec["year"])
        rec["value"] = float(rec["value"]) if rec["value"] is not None else None
        records.append(rec)
    return records


def run(path: Path = config.PROCESSED_CSV) -> dict:
    stats = load_records(records_from_csv(path))
    print(f"loaded: {stats}")
    return stats
