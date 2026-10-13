"""Read-only REST API over the MongoDB collection.

Run: uvicorn africa_energy_extraction.api:app --reload   (docs at /docs)
"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from pymongo import MongoClient

from . import config

_state: dict = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=3000)
    _state["client"] = client
    _state["col"] = client[config.MONGO_DB][config.MONGO_COLLECTION]
    yield
    client.close()


app = FastAPI(title="Africa Energy API", version="1.0.0", lifespan=lifespan)


def col():
    return _state["col"]


@app.get("/health")
def health():
    _state["client"].admin.command("ping")
    return {"status": "ok", "documents": col().estimated_document_count()}


@app.get("/indicators")
def indicators(group: str | None = None):
    """All indicators with their group, topic and unit."""
    pipeline = []
    if group:
        pipeline.append({"$match": {"group": group}})
    pipeline += [
        {"$group": {"_id": {"group": "$group", "topic": "$topic",
                            "indicator": "$indicator", "unit": "$unit"}}},
        {"$replaceRoot": {"newRoot": "$_id"}},
        {"$sort": {"group": 1, "topic": 1, "indicator": 1}},
    ]
    return list(col().aggregate(pipeline))


@app.get("/countries")
def countries():
    pipeline = [
        {"$group": {"_id": "$iso3", "country": {"$first": "$country"},
                    "region": {"$first": "$region"}}},
        {"$project": {"_id": 0, "iso3": "$_id", "country": 1, "region": 1}},
        {"$sort": {"country": 1}},
    ]
    return list(col().aggregate(pipeline))


@app.get("/timeseries")
def timeseries(
    indicator: str = Query(..., min_length=1, description="Exact indicator name"),
    countries: str | None = Query(None, description="Comma-separated ISO3 codes, e.g. KEN,UGA"),
    start: int | None = None,
    end: int | None = None,
):
    query: dict = {"indicator": indicator}
    if countries:
        query["iso3"] = {"$in": [c.strip().upper() for c in countries.split(",") if c.strip()]}
    if start is not None or end is not None:
        query["year"] = {}
        if start is not None:
            query["year"]["$gte"] = start
        if end is not None:
            query["year"]["$lte"] = end
    rows = list(col().find(query, {"_id": 0, "iso3": 1, "country": 1, "year": 1,
                                   "value": 1, "unit": 1}).sort([("iso3", 1), ("year", 1)]))
    if not rows:
        raise HTTPException(404, "No data for that indicator/filter")
    return rows


@app.get("/latest")
def latest(
    indicator: str = Query(..., min_length=1),
    year: int | None = Query(None, description="Defaults to the latest year with data"),
    limit: int = Query(100, ge=1, le=100),
):
    """Country snapshot for one indicator and year, highest value first."""
    if year is None:
        newest = col().find_one({"indicator": indicator, "value": {"$ne": None}},
                                sort=[("year", -1)])
        if not newest:
            raise HTTPException(404, "Unknown indicator or no data")
        year = newest["year"]
    rows = list(col().find(
        {"indicator": indicator, "year": year, "value": {"$ne": None}},
        {"_id": 0, "iso3": 1, "country": 1, "region": 1, "value": 1, "unit": 1},
    ).sort("value", -1).limit(limit))
    if not rows:
        raise HTTPException(404, f"No data for {indicator} in {year}")
    return {"indicator": indicator, "year": year, "unit": rows[0]["unit"], "data": rows}
