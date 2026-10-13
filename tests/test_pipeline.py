import json

import pandas as pd
import pytest

from africa_energy_extraction import extract as ex
from africa_energy_extraction import transform as tf
from africa_energy_extraction.client import build_payload
from africa_energy_extraction.load import KEY_FIELDS, build_ops

PCT = "Population access to electricity-National (% of population)"
EXPORT = "Electricity export (GWh)"


def row(iso2, name, score, indicator, unit, topic, group="Electricity", year=2023, source=None):
    return {"id": iso2, "name": name, "score": score, "unit": unit,
            "region_name": "Test Region", "indicator_topic": topic,
            "indicator_source": source, "indicator_name": indicator,
            "indicator_group": group, "year": year, "url": f"/aep/country/{name.lower()}"}


SAMPLE = [
    {"_id": PCT, "data": [
        row("DZ", "Algeria", 99.7, PCT, "% of population", "Access"),
        row("AO", "Angola", None, PCT, "% of population", "Access"),
    ]},
    {"_id": EXPORT, "data": [
        row("AO", "Angola", 0, EXPORT, "GWh", "Supply", source="AFREC Database"),
    ]},
]


def test_flatten_maps_fields():
    rec = tf.flatten(SAMPLE)[0]
    assert rec["iso3"] == "DZA"
    assert rec["metric"] == "Population access to electricity-National"
    assert rec["unit"] == "% of population"
    assert rec["year"] == 2023
    assert rec["source_url"].endswith("/aep/country/algeria")


def test_null_and_zero_are_different():
    by_key = {(r["iso3"], r["indicator"]): r["value"] for r in tf.flatten(SAMPLE)}
    assert by_key[("AGO", PCT)] is None
    assert by_key[("AGO", EXPORT)] == 0.0


def test_clean_metric_strips_prefix_and_unit():
    assert tf.clean_metric("Energy: Population with access to clean cooking fuels (% of population)") \
        == "Population with access to clean cooking fuels"


def test_build_dataframe_dedupes(tmp_path):
    for name in ("a.json", "b.json"):
        (tmp_path / name).write_text(json.dumps(SAMPLE))
    df = tf.build_dataframe(tmp_path)
    assert len(df) == 3
    assert not df.duplicated(subset=tf.KEY).any()


def test_validate_flags_bad_percentage_and_all_zero():
    bad = [{"_id": PCT, "data": [row("DZ", "Algeria", 140.0, PCT, "% of population", "Access")]}]
    df = pd.DataFrame(tf.flatten(bad + SAMPLE))
    findings = " ".join(tf.validate(df))
    assert "outside 0-100" in findings
    assert "zero in every year" in findings


def test_validate_empty():
    assert tf.validate(pd.DataFrame(columns=tf.COLUMNS))[0].startswith("ERROR")


def test_build_payload_uses_repeated_keys():
    p = build_payload("Electricity", "Access", [PCT], [2022, 2023], ["Algeria", "Angola"])
    assert p["mainGroup"] == "Electricity"
    assert p["year[]"] == ["2022", "2023"]
    assert p["name[]"] == ["Algeria", "Angola"]


class FakeClient:
    def __init__(self):
        self.calls = 0

    def post_database(self, group, topic, indicators, years, countries):
        self.calls += 1
        return [{"_id": PCT, "data": [
            row("DZ", "Algeria", 50.0, PCT, "% of population", "Access", year=y)
            for y in years]}]


def test_split_by_year():
    resp = FakeClient().post_database("g", "t", [], [2022, 2023], [])
    parts = ex.split_by_year(resp, [2022, 2023])
    assert [r["year"] for r in parts[2022][0]["data"]] == [2022]
    assert [r["year"] for r in parts[2023][0]["data"]] == [2023]


def test_extract_one_call_per_topic_and_resumable(tmp_path):
    client = FakeClient()
    first = ex.extract(client, ["Algeria", "Nowhere"], [2022, 2023],
                       raw_dir=tmp_path, log=lambda *_: None)
    assert client.calls == 7                      # one request per topic, not per year
    assert first["fetched"] == 7 * 2
    assert first["missing_countries"] == ["Nowhere"]
    assert ex.raw_path(tmp_path, "Electricity", "Access", 2022).exists()

    second = ex.extract(client, ["Algeria"], [2022, 2023], raw_dir=tmp_path, log=lambda *_: None)
    assert client.calls == 7                      # nothing re-downloaded
    assert second["fetched"] == 0 and second["skipped"] == 14


def test_default_countries_match_portal():
    from africa_energy_extraction.catalog import DEFAULT_COUNTRIES
    assert len(DEFAULT_COUNTRIES) == 54 and "Guinea Bissau" in DEFAULT_COUNTRIES


def test_build_ops_filter_is_the_unique_key():
    records = tf.flatten(SAMPLE)
    ops = build_ops(records)
    assert len(ops) == len(records)
    assert set(ops[0]._filter) == set(KEY_FIELDS)
    assert ops[0]._upsert is True


@pytest.mark.parametrize("value", [None, "abc", float("nan")])
def test_to_float_handles_junk(value):
    assert tf._to_float(value) is None


def test_namibia_maps_to_nam_and_survives_csv_roundtrip(tmp_path):
    from africa_energy_extraction.load import records_from_csv
    rec = tf.flatten([{"_id": PCT, "data": [
        row("NA", "Namibia", None, PCT, "% of population", "Access", source=None)]}])
    assert rec[0]["iso3"] == "NAM"
    path = tmp_path / "x.csv"
    pd.DataFrame(rec, columns=tf.COLUMNS).to_csv(path, index=False)
    out = records_from_csv(path)[0]
    assert out["iso3"] == "NAM" and out["country"] == "Namibia"
    assert out["value"] is None and out["source"] is None and out["year"] == 2023


def test_csv_loader_keeps_literal_NA_text_and_zero(tmp_path):
    from africa_energy_extraction.load import records_from_csv
    path = tmp_path / "y.csv"
    path.write_text(",".join(tf.COLUMNS) + "\n"
                    "NAM,Namibia,R,G,T,I,M,GWh,NA,u,2020,0.0\n")
    out = records_from_csv(path)[0]
    assert out["source"] == "NA" and out["value"] == 0.0


def test_unknown_country_id_fails_loudly():
    with pytest.raises(ValueError):
        tf.to_iso3("ZZ")
