"""Static snapshot of the portal's indicator catalog (from get-homemap-filter).

Indicator strings are sent back to the API verbatim, so they are kept exactly
as the portal spells them .
"""
from __future__ import annotations

from pathlib import Path
from typing import Iterator

# (group, topic) -> indicators
CATALOG: dict[tuple[str, str], list[str]] = {
    ("Electricity", "Access"): [
        "Population access to electricity-National (% of population)",
        "Population access to electricity-Rural (% of population)",
        "Population access to electricity-Urban (% of population)",
        "Population with access to electricity-National (millions of people)",
        "Population with access to electricity-Rural (millions of people)",
        "Population with access to electricity-Urban (millions of people)",
        "Population without access to electricity-National (millions of people)",
        "Population without access to electricity-Rural (millions of people)",
        "Population without access to electricity-Urban (millions of people)",
    ],
    ("Electricity", "Supply"): [
        "Electricity import (GWh)",
        "Electricity export (GWh)",
        "Electricity: Net imports ( GWh )",
        "Electricity generation, Total (GWh)",
        "Electricity generated from nuclear power (GWh)",
        "Electricity generated from fossil fuels (GWh)",
        "Electricity generated from hydropower (GWh)",
        "Electricity generated from biofuels and waste (GWh)",
        "Electricity generated from solar, wind, tide, wave and other sources (GWh)",
        "Electricity generated from renewable sources (GWh)",
        "Electricity generated from geothermal energy (GWh)",
        "Electricity final consumption (GWh)",
        "Electricity final consumption per capita (KWh)",
        "Electricity generation per capita (KWh)",
    ],
    ("Electricity", "Technical"): [
        "Electricity installed capacity in Fossil fuels (MW)",
        "Electricity installed capacity in Geothermal (MW)",
        "Electricity installed capacity in Non-renewable energy (MW)",
        "Electricity installed capacity in Wind (MW)",
        "Electricity installed capacity in Hydropower (MW)",
        "Electricity installed capacity in Nuclear (MW)",
        "Electricity installed capacity in Solar (MW)",
        "Electricity installed capacity, Total (MW)",
        "Electricity installed capacity in Total renewable energy (MW)",
        "Electricity installed capacity in other Non-renewable energy (MW)",
        "Electricity installed capacity in Bioenergy (MW)",
    ],
    ("Energy", "Access"): [
        "Energy: Population without access to clean cooking fuels (millions of people)",
        "Energy: Population with access to clean cooking fuels (% of population)",
    ],
    ("Energy", "Efficiency"): [
        "Energy intensity level of primary energy (MJ/2021 PPP GDP)",
    ],
    ("Social and Economic", "National Account"): [
        "GDP (current US$)",
    ],
    ("Social and Economic", "Population"): [
        "Population; Total (millions of people)",
        "Rural population (millions of people)",
        "Urban population (millions of people)",
    ],
}

# Country names exactly as the portal spells them, grouped by region.
# Source: the portal's get-homemapregion-filter response (54 countries).
REGIONS: dict[str, list[str]] = {
    "East Africa": [
        "Sudan", "Comoros", "Rwanda", "Uganda", "Ethiopia", "South Sudan",
        "Tanzania", "Seychelles", "Madagascar", "Mauritius", "Burundi",
        "Eritrea", "Kenya", "Somalia", "Djibouti",
    ],
    "North Africa": ["Algeria", "Morocco", "Libya", "Mauritania", "Egypt", "Tunisia"],
    "West Africa": [
        "Cote d'Ivoire", "Senegal", "Burkina Faso", "Nigeria", "Guinea Bissau",
        "Cape Verde", "Guinea", "Ghana", "Niger", "Sierra Leone", "Benin",
        "Togo", "Liberia", "Mali", "Gambia",
    ],
    "Central Africa": [
        "Congo Democratic Republic", "Cameroon", "Sao Tome and Principe",
        "Congo Republic", "Chad", "Equatorial Guinea",
        "Central African Republic", "Gabon",
    ],
    "Southern Africa": [
        "Lesotho", "Angola", "Zimbabwe", "Malawi", "Mozambique", "Botswana",
        "Eswatini", "Zambia", "Namibia", "South Africa",
    ],
}

DEFAULT_COUNTRIES: list[str] = sorted(c for names in REGIONS.values() for c in names)


def iter_topics() -> Iterator[tuple[str, str, list[str]]]:
    for (group, topic), indicators in CATALOG.items():
        yield group, topic, indicators


def load_countries(path: str | Path | None = None) -> list[str]:
    if path:
        lines = Path(path).read_text(encoding="utf-8").splitlines()
        names = [ln.strip() for ln in lines if ln.strip()]
        if not names:
            raise ValueError(f"{path} contains no country names")
        return names
    return list(DEFAULT_COUNTRIES)
