"""
detector.py — Async ATS probing logic.

Each probe_<ats>() function:
  - Accepts a list of slug candidates and an httpx.AsyncClient.
  - Tries each slug in order, returning on first confirmed hit.
  - Returns a dict on success:  {ats, slug, jobs_found, careers_url, probe_time_ms}
  - Returns None on miss.
  - Never raises — all exceptions are caught and logged at DEBUG level.

Priority order (defined in probe_all()):
  Greenhouse → Lever → Ashby → Workable → SmartRecruiters →
  Rippling → BambooHR → Recruitee → Personio → Teamtailor → Freshteam
"""

from __future__ import annotations

import asyncio
import logging
import time
import xml.etree.ElementTree as ET
from typing import Optional

import httpx

from normalizer import careers_url

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

TIMEOUT = httpx.Timeout(10.0, connect=5.0)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; JobRadar/1.0; "
        "+https://github.com/kayden-vs/jobradar)"
    ),
    "Accept": "application/json, text/html, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

# Global semaphore — injected from main.py via set_semaphore()
_semaphore: asyncio.Semaphore = asyncio.Semaphore(10)

# Workable gets its own mutex (1 at a time + 3s sleep)
_workable_lock: asyncio.Lock = asyncio.Lock()

# 100 ms inter-request delay per domain
_domain_last_request: dict[str, float] = {}
_domain_lock: asyncio.Lock = asyncio.Lock()


def set_semaphore(sem: asyncio.Semaphore) -> None:
    global _semaphore
    _semaphore = sem


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _domain_delay(domain: str, delay_ms: int = 100) -> None:
    """Enforce a minimum inter-request gap to the same domain."""
    async with _domain_lock:
        last = _domain_last_request.get(domain, 0)
        now = time.monotonic()
        wait = delay_ms / 1000 - (now - last)
        if wait > 0:
            await asyncio.sleep(wait)
        _domain_last_request[domain] = time.monotonic()


async def _get_json(
    client: httpx.AsyncClient,
    url: str,
    *,
    domain: str,
    follow_redirects: bool = True,
    max_retries: int = 3,
    **kwargs,
) -> Optional[dict | list]:
    """
    GET a URL and return parsed JSON.
    Implements exponential backoff on 429.
    Returns None on 4xx (except 429), 3xx when follow_redirects=False, or error.
    """
    await _domain_delay(domain)
    for attempt in range(max_retries):
        try:
            async with _semaphore:
                resp = await client.get(
                    url,
                    headers=HEADERS,
                    timeout=TIMEOUT,
                    follow_redirects=follow_redirects,
                    **kwargs,
                )
            if resp.status_code == 429:
                wait = 2 ** attempt
                logger.debug("429 on %s, backing off %ss", url, wait)
                await asyncio.sleep(wait)
                continue
            if not follow_redirects and resp.is_redirect:
                return None
            if resp.status_code >= 400:
                logger.debug("HTTP %s for %s", resp.status_code, url)
                return None
            return resp.json()
        except (httpx.HTTPError, Exception) as exc:
            logger.debug("GET %s failed (attempt %d): %s", url, attempt + 1, exc)
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)
    return None


async def _post_json(
    client: httpx.AsyncClient,
    url: str,
    *,
    domain: str,
    payload: dict,
    max_retries: int = 3,
) -> Optional[dict | list]:
    """POST JSON and return parsed response JSON."""
    await _domain_delay(domain)
    for attempt in range(max_retries):
        try:
            async with _semaphore:
                resp = await client.post(
                    url,
                    json=payload,
                    headers={**HEADERS, "Content-Type": "application/json"},
                    timeout=TIMEOUT,
                )
            if resp.status_code == 429:
                wait = 2 ** attempt
                logger.debug("429 on %s, backing off %ss", url, wait)
                await asyncio.sleep(wait)
                continue
            if resp.status_code >= 400:
                logger.debug("HTTP %s for %s", resp.status_code, url)
                return None
            return resp.json()
        except (httpx.HTTPError, Exception) as exc:
            logger.debug("POST %s failed (attempt %d): %s", url, attempt + 1, exc)
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)
    return None


async def _get_html(
    client: httpx.AsyncClient,
    url: str,
    *,
    domain: str,
    follow_redirects: bool = True,
    max_retries: int = 3,
) -> Optional[str]:
    """GET a URL and return HTML text."""
    await _domain_delay(domain)
    for attempt in range(max_retries):
        try:
            async with _semaphore:
                resp = await client.get(
                    url,
                    headers=HEADERS,
                    timeout=TIMEOUT,
                    follow_redirects=follow_redirects,
                )
            if resp.status_code == 429:
                await asyncio.sleep(2 ** attempt)
                continue
            if not follow_redirects and resp.is_redirect:
                return None
            if resp.status_code >= 400:
                return None
            return resp.text
        except (httpx.HTTPError, Exception) as exc:
            logger.debug("GET HTML %s failed (attempt %d): %s", url, attempt + 1, exc)
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)
    return None


def _result(ats: str, slug: str, jobs_found: int, elapsed_ms: float) -> dict:
    return {
        "ats": ats,
        "slug": slug,
        "jobs_found": jobs_found,
        "careers_url": careers_url(ats, slug),
        "probe_time_ms": round(elapsed_ms),
        "status": "found",
    }


# ---------------------------------------------------------------------------
# 1. Greenhouse
# ---------------------------------------------------------------------------

async def probe_greenhouse(slugs: list[str], client: httpx.AsyncClient) -> Optional[dict]:
    domain = "boards-api.greenhouse.io"
    for slug in slugs:
        t0 = time.monotonic()
        url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
        data = await _get_json(client, url, domain=domain)
        if isinstance(data, dict) and data.get("jobs"):
            elapsed = (time.monotonic() - t0) * 1000
            jobs = len(data["jobs"])
            logger.info("Greenhouse hit: slug=%s, jobs=%d", slug, jobs)
            return _result("greenhouse", slug, jobs, elapsed)
    return None


# ---------------------------------------------------------------------------
# 2. Lever
# ---------------------------------------------------------------------------

async def probe_lever(slugs: list[str], client: httpx.AsyncClient) -> Optional[dict]:
    domain = "api.lever.co"
    for slug in slugs:
        t0 = time.monotonic()
        url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
        data = await _get_json(client, url, domain=domain)
        if isinstance(data, list) and len(data) > 0:
            elapsed = (time.monotonic() - t0) * 1000
            logger.info("Lever hit: slug=%s, jobs=%d", slug, len(data))
            return _result("lever", slug, len(data), elapsed)
    return None


# ---------------------------------------------------------------------------
# 3. Ashby
# ---------------------------------------------------------------------------

async def probe_ashby(slugs: list[str], client: httpx.AsyncClient) -> Optional[dict]:
    domain = "api.ashbyhq.com"
    for slug in slugs:
        t0 = time.monotonic()
        url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}"
        data = await _get_json(client, url, domain=domain)
        if isinstance(data, dict) and data.get("jobs"):
            elapsed = (time.monotonic() - t0) * 1000
            jobs = len(data["jobs"])
            logger.info("Ashby hit: slug=%s, jobs=%d", slug, jobs)
            return _result("ashby", slug, jobs, elapsed)
    return None


# ---------------------------------------------------------------------------
# 4. Workable  (serialized, 3s sleep after each attempt)
# ---------------------------------------------------------------------------

async def probe_workable(slugs: list[str], client: httpx.AsyncClient) -> Optional[dict]:
    domain = "apply.workable.com"
    payload = {"query": "", "location": [], "workplace": [], "department": []}

    async with _workable_lock:
        for slug in slugs:
            t0 = time.monotonic()
            url = f"https://apply.workable.com/api/v3/accounts/{slug}/jobs"
            data = await _post_json(client, url, domain=domain, payload=payload)
            await asyncio.sleep(3)  # always sleep after Workable probe
            if isinstance(data, dict) and data.get("results"):
                elapsed = (time.monotonic() - t0) * 1000
                jobs = len(data["results"])
                logger.info("Workable hit: slug=%s, jobs=%d", slug, jobs)
                return _result("workable", slug, jobs, elapsed)
    return None


# ---------------------------------------------------------------------------
# 5. SmartRecruiters
# ---------------------------------------------------------------------------

async def probe_smartrecruiters(slugs: list[str], client: httpx.AsyncClient) -> Optional[dict]:
    domain = "api.smartrecruiters.com"
    for slug in slugs:
        # SmartRecruiters slug is case-sensitive; try original, title, and lowercase
        variants = list(dict.fromkeys([slug, slug.title(), slug.capitalize(), slug.lower()]))
        for variant in variants:
            t0 = time.monotonic()
            url = f"https://api.smartrecruiters.com/v1/companies/{variant}/postings"
            data = await _get_json(client, url, domain=domain)
            if isinstance(data, dict) and data.get("totalFound", 0) > 0:
                elapsed = (time.monotonic() - t0) * 1000
                jobs = data["totalFound"]
                logger.info("SmartRecruiters hit: slug=%s, jobs=%d", variant, jobs)
                return _result("smartrecruiters", variant, jobs, elapsed)
    return None


# ---------------------------------------------------------------------------
# 6. Rippling
# ---------------------------------------------------------------------------

async def probe_rippling(slugs: list[str], client: httpx.AsyncClient) -> Optional[dict]:
    domain = "ats.rippling.com"
    for slug in slugs:
        t0 = time.monotonic()
        url = f"https://ats.rippling.com/api/v2/board/{slug}/jobs?page=0&pageSize=100"
        data = await _get_json(client, url, domain=domain)
        if isinstance(data, dict) and data.get("totalItems", 0) > 0:
            elapsed = (time.monotonic() - t0) * 1000
            jobs = data["totalItems"]
            logger.info("Rippling hit: slug=%s, jobs=%d", slug, jobs)
            return _result("rippling", slug, jobs, elapsed)
    return None


# ---------------------------------------------------------------------------
# 7. BambooHR  (redirect = miss)
# ---------------------------------------------------------------------------

async def probe_bamboohr(slugs: list[str], client: httpx.AsyncClient) -> Optional[dict]:
    for slug in slugs:
        t0 = time.monotonic()
        domain = f"{slug}.bamboohr.com"
        url = f"https://{slug}.bamboohr.com/careers/list"
        # Do NOT follow redirects — a redirect means the company isn't on BambooHR
        data = await _get_json(client, url, domain=domain, follow_redirects=False)
        if isinstance(data, dict) and data.get("result"):
            elapsed = (time.monotonic() - t0) * 1000
            jobs = len(data["result"])
            logger.info("BambooHR hit: slug=%s, jobs=%d", slug, jobs)
            return _result("bamboohr", slug, jobs, elapsed)
    return None


# ---------------------------------------------------------------------------
# 8. Recruitee
# ---------------------------------------------------------------------------

async def probe_recruitee(slugs: list[str], client: httpx.AsyncClient) -> Optional[dict]:
    for slug in slugs:
        t0 = time.monotonic()
        domain = f"{slug}.recruitee.com"
        url = f"https://{slug}.recruitee.com/api/offers/"
        data = await _get_json(client, url, domain=domain)
        if isinstance(data, dict) and data.get("offers"):
            elapsed = (time.monotonic() - t0) * 1000
            jobs = len(data["offers"])
            logger.info("Recruitee hit: slug=%s, jobs=%d", slug, jobs)
            return _result("recruitee", slug, jobs, elapsed)
    return None


# ---------------------------------------------------------------------------
# 9. Personio  (XML; tries .de then .com)
# ---------------------------------------------------------------------------

async def probe_personio(slugs: list[str], client: httpx.AsyncClient) -> Optional[dict]:
    for slug in slugs:
        for tld in ("de", "com"):
            t0 = time.monotonic()
            domain = f"{slug}.jobs.personio.{tld}"
            url = f"https://{slug}.jobs.personio.{tld}/xml?language=en"
            await _domain_delay(domain)
            try:
                async with _semaphore:
                    resp = await client.get(
                        url,
                        headers=HEADERS,
                        timeout=TIMEOUT,
                        follow_redirects=True,
                    )
                if resp.status_code == 404:
                    continue
                if resp.status_code != 200:
                    continue
                try:
                    root = ET.fromstring(resp.text)
                except ET.ParseError:
                    continue
                positions = root.findall(".//position")
                if positions:
                    elapsed = (time.monotonic() - t0) * 1000
                    logger.info("Personio hit: slug=%s, tld=%s, jobs=%d", slug, tld, len(positions))
                    return _result("personio", slug, len(positions), elapsed)
            except (httpx.HTTPError, Exception) as exc:
                logger.debug("Personio %s.%s failed: %s", slug, tld, exc)
    return None


# ---------------------------------------------------------------------------
# 10. Teamtailor  (HTML detection only — no API key available)
# ---------------------------------------------------------------------------

async def probe_teamtailor(slugs: list[str], client: httpx.AsyncClient) -> Optional[dict]:
    for slug in slugs:
        t0 = time.monotonic()
        domain = f"{slug}.teamtailor.com"
        url = f"https://{slug}.teamtailor.com/"
        html = await _get_html(client, url, domain=domain)
        if html and "teamtailor-cdn" in html.lower():
            elapsed = (time.monotonic() - t0) * 1000
            logger.info("Teamtailor hit (HTML): slug=%s", slug)
            # Teamtailor requires a per-company API key — flag accordingly
            return {
                "ats": "teamtailor",
                "slug": slug,
                "jobs_found": -1,  # unknown without API key
                "careers_url": careers_url("teamtailor", slug),
                "probe_time_ms": round(elapsed),
                "status": "requires_key",
                "requires_key": True,
            }
    return None


# ---------------------------------------------------------------------------
# 11. Freshteam  (HTML detection only — no public JSON API)
# ---------------------------------------------------------------------------

async def probe_freshteam(slugs: list[str], client: httpx.AsyncClient) -> Optional[dict]:
    for slug in slugs:
        t0 = time.monotonic()
        domain = f"{slug}.freshteam.com"
        url = f"https://{slug}.freshteam.com/jobs"
        html = await _get_html(client, url, domain=domain)
        if html and ("freshteam.com/jobs" in html or "freshteam" in html.lower()):
            elapsed = (time.monotonic() - t0) * 1000
            logger.info("Freshteam hit (HTML): slug=%s", slug)
            return {
                "ats": "freshteam",
                "slug": slug,
                "jobs_found": -1,
                "careers_url": careers_url("freshteam", slug),
                "probe_time_ms": round(elapsed),
                "status": "found",
                "scrape_only": True,
            }
    return None


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

# Registry of all probers in priority order
_ALL_PROBERS = {
    "greenhouse":       probe_greenhouse,
    "lever":            probe_lever,
    "ashby":            probe_ashby,
    "workable":         probe_workable,
    "smartrecruiters":  probe_smartrecruiters,
    "rippling":         probe_rippling,
    "bamboohr":         probe_bamboohr,
    "recruitee":        probe_recruitee,
    "personio":         probe_personio,
    "teamtailor":       probe_teamtailor,
    "freshteam":        probe_freshteam,
}


async def probe_all(
    company: str,
    slugs: list[str],
    client: httpx.AsyncClient,
    skip_ats: set[str] | None = None,
) -> dict:
    """
    Run all enabled ATS probers concurrently for a single company.

    Returns a result dict with status='found' | 'not_found'.
    Never raises.
    """
    skip_ats = skip_ats or set()
    active_probers = {
        name: fn
        for name, fn in _ALL_PROBERS.items()
        if name not in skip_ats
    }

    tasks = {
        name: asyncio.create_task(fn(slugs, client))
        for name, fn in active_probers.items()
    }

    done_order: list[dict] = []

    # Gather all, catching individual failures
    results = await asyncio.gather(*tasks.values(), return_exceptions=True)

    for name, result in zip(tasks.keys(), results):
        if isinstance(result, Exception):
            logger.debug("Prober %s raised: %s", name, result)
            continue
        if result is not None:
            done_order.append(result)

    if done_order:
        # Return the highest-priority hit (first in _ALL_PROBERS order)
        priority = list(_ALL_PROBERS.keys())
        done_order.sort(key=lambda r: priority.index(r["ats"]) if r["ats"] in priority else 99)
        return done_order[0]

    return {
        "ats": None,
        "slug": None,
        "jobs_found": 0,
        "careers_url": "",
        "probe_time_ms": 0,
        "status": "not_found",
    }
