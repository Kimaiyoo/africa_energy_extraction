"""Streamlit dashboard that reads from the API (not from Mongo directly).

Run: uv run streamlit run src/africa_energy_extraction/dashboard.py
"""
from __future__ import annotations

import os

import httpx
import pandas as pd
import plotly.express as px
import streamlit as st

API_URL = os.getenv("API_URL", "http://localhost:8000")

st.set_page_config(page_title="Africa Energy Dashboard", layout="wide")


@st.cache_data(ttl=300)
def api(path: str, **params):
    params = {k: v for k, v in params.items() if v is not None}
    r = httpx.get(f"{API_URL}{path}", params=params, timeout=30)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()


try:
    catalog = pd.DataFrame(api("/indicators"))
    country_df = pd.DataFrame(api("/countries"))
except Exception as exc:  # API down or Mongo empty
    st.error(f"Cannot reach the API at {API_URL}: {exc}")
    st.stop()

st.title("Africa Energy Dashboard")
st.caption("Source: Africa Energy Portal, 2000-2023")

group = st.sidebar.selectbox("Group", sorted(catalog["group"].unique()))
options = catalog[catalog["group"] == group]
indicator = st.sidebar.selectbox("Indicator", options["indicator"].tolist(), index=0)
unit = options.loc[options["indicator"] == indicator, "unit"].iloc[0]

map_tab, trend_tab, rank_tab = st.tabs(["Map", "Trend", "Ranking"])

with map_tab:
    year = st.slider("Year", 2000, 2023, 2023)
    snap = api("/latest", indicator=indicator, year=year)
    if not snap:
        st.info("No data for this indicator and year.")
    else:
        df = pd.DataFrame(snap["data"])
        fig = px.choropleth(df, locations="iso3", color="value",
                            hover_name="country", scope="africa",
                            color_continuous_scale="YlGn", labels={"value": unit})
        fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))
        st.plotly_chart(fig, use_container_width=True)

with trend_tab:
    names = dict(zip(country_df["iso3"], country_df["country"]))
    defaults = [c for c in ("KEN", "UGA", "TZA") if c in names]
    picked = st.multiselect("Countries", list(names), default=defaults,
                            format_func=lambda c: names[c])
    if picked:
        rows = api("/timeseries", indicator=indicator, countries=",".join(picked))
        if rows:
            fig = px.line(pd.DataFrame(rows), x="year", y="value", color="country",
                          markers=True, labels={"value": unit})
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data for this selection.")

with rank_tab:
    snap = api("/latest", indicator=indicator)
    if snap:
        st.subheader(f"Top 15 in {snap['year']}")
        df = pd.DataFrame(snap["data"]).head(15)
        fig = px.bar(df, x="value", y="country", orientation="h", labels={"value": unit})
        fig.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data for this indicator.")
