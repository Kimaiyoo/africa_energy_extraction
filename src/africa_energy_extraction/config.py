"""Central configuration. Everything overridable through environment / .env."""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://africa-energy-portal.org"
DATABASE_PAGE = f"{BASE_URL}/database"
DATABASE_ENDPOINT = f"{BASE_URL}/get-database-data"

# From 2000-2023.
YEARS = list(range(2000, 2024))

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
PROCESSED_CSV = PROCESSED_DIR / "energy_long.csv"

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB = os.getenv("MONGO_DB", "energy_data")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "observations")

# cookie only necessary if blocked
# Cloudflare sits in front of the portal. cf_clearance is tied to the browser
# user agent that earned it, so the two values below must come from the same
# browser session (DevTools -> Network -> get-database-data -> Request Headers).
AEP_COOKIE = os.getenv("AEP_COOKIE", "")
AEP_USER_AGENT = os.getenv(
    "AEP_USER_AGENT",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/154.0.0.0 Safari/537.36",
)

# Optional text file, one country name per line, copied exactly as the portal
# spells them (the name[] values in the request payload).
COUNTRIES_FILE = os.getenv("COUNTRIES_FILE", "")
