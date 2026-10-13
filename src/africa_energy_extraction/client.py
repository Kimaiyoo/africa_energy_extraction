"""HTTP client for the portal's get-database-data endpoint."""
from __future__ import annotations

import random
import time

import httpx

from . import config


class PortalError(RuntimeError):
    """Any non-recoverable problem talking to the portal."""


class CloudflareBlocked(PortalError):
    """Cloudflare challenged or rejected the request (cookie missing/expired)."""


CLOUDFLARE_HELP = (
    "Cloudflare rejected the request. Open the portal in your browser, copy the "
    "Cookie and User-Agent request headers from DevTools (Network -> "
    "get-database-data -> Headers) into AEP_COOKIE and AEP_USER_AGENT in .env, "
    "and run again. cf_clearance expires and is tied to that browser's IP and UA."
)


def build_payload(group: str, topic: str, indicators: list[str],
                  years: list[int], countries: list[str]) -> dict:
    """Form fields exactly as the site sends them. Lists become repeated keys."""
    return {
        "mainGroup": group,
        "mainIndicator[]": [topic],
        "mainIndicatorValue[]": list(indicators),
        "year[]": [str(y) for y in years],
        "name[]": list(countries),
    }


def _is_challenge(resp: httpx.Response) -> bool:
    return "just a moment" in resp.text[:2000].lower() or \
        resp.headers.get("cf-mitigated") == "challenge"


class AEPClient:
    def __init__(self, cookie: str = "", user_agent: str = "", timeout: float = 90.0,
                 max_retries: int = 5, pause: float = 0.5):
        headers = {
            "User-Agent": user_agent or config.AEP_USER_AGENT,
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "en-US,en;q=0.9",
            "Origin": config.BASE_URL,
            "Referer": config.DATABASE_PAGE,
            "X-Requested-With": "XMLHttpRequest",
        }
        if cookie:
            headers["Cookie"] = cookie
        self._http = httpx.Client(headers=headers, timeout=timeout, follow_redirects=True)
        self.max_retries = max_retries
        self.pause = pause

    def __enter__(self) -> "AEPClient":
        return self

    def __exit__(self, *exc) -> None:
        self._http.close()

    def warm_up(self) -> None:
        """Load the database page once so the session picks up any cookies."""
        r = self._http.get(config.DATABASE_PAGE)
        if r.status_code in (403, 503) and (r.status_code == 403 or _is_challenge(r)):
            raise CloudflareBlocked(CLOUDFLARE_HELP)

    def post_database(self, group: str, topic: str, indicators: list[str],
                      years: list[int], countries: list[str]) -> list[dict]:
        payload = build_payload(group, topic, indicators, years, countries)
        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                r = self._http.post(config.DATABASE_ENDPOINT, data=payload)
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_error = exc
            else:
                if r.status_code == 403 or (r.status_code == 503 and _is_challenge(r)):
                    raise CloudflareBlocked(CLOUDFLARE_HELP)
                if r.status_code == 429 or r.status_code >= 500:
                    last_error = PortalError(f"HTTP {r.status_code}")
                elif r.status_code >= 400:
                    raise PortalError(f"HTTP {r.status_code}: {r.text[:200]}")
                else:
                    try:
                        data = r.json()
                    except ValueError:
                        if _is_challenge(r):
                            raise CloudflareBlocked(CLOUDFLARE_HELP)
                        raise PortalError(f"Non-JSON response: {r.text[:200]}")
                    time.sleep(self.pause)
                    return data
            time.sleep(min(60, 2 ** attempt) + random.random())
        raise PortalError(f"Gave up after {self.max_retries} attempts: {last_error}")


def make_client() -> AEPClient:
    return AEPClient(cookie=config.AEP_COOKIE, user_agent=config.AEP_USER_AGENT)
