"""
discover_workday.py — Auto-discover Workday tenant, data-centre, and site names.

Strategy
--------
For each company in companies.txt:

  1. Generate slug candidates (via normalizer.generate_slugs).
  2. For each slug × each known data-centre (wd1, wd3, wd5, wd12, wd13)
     × each common site name candidate:
       POST https://{slug}.{wdN}.myworkdayjobs.com/wday/cxs/{slug}/{site}/jobs
         · DNS failure (ConnectError) → this slug+server doesn't exist → next server
         · HTTP 404 / 422 → tenant exists but site name is wrong → try next site
         · HTTP 200 + total > 0 → confirmed hit → write to workday_companies.txt

Why POST instead of GET?
  The Workday tenant root page (GET /) returns 406 Not Acceptable regardless of
  headers — Cloudflare blocks it.  The CXS jobs endpoint (POST) is what the
  browser itself calls and is reliable.  Wrong site names return 404/422, correct
  ones return 200 with job data.

Why keep a site-name candidate list?
  Workday site names are configured per-company and can't be guessed from the
  company name.  However, a ~30-name list covers the vast majority of real-world
  deployments (External_Career_Site, external_experienced, Careers, etc.).

Usage
-----
    python discover_workday.py                          # default paths
    python discover_workday.py --input my_cos.txt
    python discover_workday.py --workers 3 --delay 0.2
    python discover_workday.py --dry-run
    python discover_workday.py --verbose
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from pathlib import Path

import httpx
from tqdm import tqdm

from normalizer import generate_slugs

logger = logging.getLogger("discover_workday")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Known Workday data-centre identifiers, ordered by approximate real-world
# prevalence.  wd1/wd3/wd5 cover the vast majority of companies.
_WD_SERVERS = ["wd1", "wd3", "wd5", "wd12", "wd13"]

# Maximum slug candidates to try per company.  generate_slugs() can return
# up to ~9 variants; capping at 5 avoids very long tails for unusual names.
_MAX_SLUGS = 5

# Common Workday site-name identifiers, ordered by approximate prevalence.
# These are the "{site}" segment in:
#   /wday/cxs/{tenant}/{site}/jobs
# Sourced from real-world Workday careers pages.
_SITE_CANDIDATES = [
    # Most common generic names
    "External_Career_Site",
    "External",
    "external",
    "Careers",
    "careers",
    "Jobs",
    "External_Careers",
    "ExternalCareers",
    "external_careers",
    "external_experienced",
    "External_Experienced",
    # Company-name–derived sites (using the slug itself — many companies
    # set the site name to their brand name)
    # → inserted dynamically per-slug in the probe loop below
    # Common patterns for large companies
    "Global_Careers",
    "Global",
    "global",
    "GlobalCareers",
    "Experienced",
    "experienced",
    "Professional",
    "professional",
    # Region / audience variants
    "External_Experienced_Careers",
    "ExternalExperienced",
    # Hyphenated site names (Oracle-style: oracle-careers)
    # These are NOT covered by the slug+suffix block (which uses no-separator
    # or underscore variants), so we list them explicitly.
    "oracle-careers",
    "careers-home",
    "global-careers",
    "external-careers",
]

_TIMEOUT = httpx.Timeout(10.0, connect=5.0)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

_JOBS_PAYLOAD = {"appliedFacets": {}, "limit": 20, "offset": 0, "searchText": ""}


# ---------------------------------------------------------------------------
# Core async probe
# ---------------------------------------------------------------------------

async def _probe_status(
    slug: str,
    wd_server: str,
    site: str,
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
) -> tuple[int, int | None]:
    """
    POST to one (slug, server, site) combination.

    Returns (http_status_code, job_count_or_None):
      (200, N)    — confirmed hit, N jobs
      (200, 0)    — valid endpoint but empty board
      (404, None) — correct data centre, wrong site name
      (422, None) — wrong data centre for this tenant
      (0,   None) — network error / timeout

    Raises httpx.ConnectError for DNS failures (NXDOMAIN) so the caller
    can treat them as a hard miss.
    """
    url = (
        f"https://{slug}.{wd_server}.myworkdayjobs.com"
        f"/wday/cxs/{slug}/{site}/jobs"
    )
    origin  = f"https://{slug}.{wd_server}.myworkdayjobs.com"
    referer = f"{origin}/{site}/"
    try:
        async with sem:
            resp = await client.post(
                url,
                json=_JOBS_PAYLOAD,
                headers={
                    **_HEADERS,
                    "Content-Type": "application/json",
                    "Origin":       origin,
                    "Referer":      referer,
                },
                timeout=_TIMEOUT,
            )
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, dict):
                postings = data.get("jobPostings", [])
                total    = data.get("total", 0)
                count = (
                    len(postings)
                    if isinstance(postings, list) and postings
                    else int(total)
                )
                logger.debug("%s.%s/%s → 200, %d jobs", slug, wd_server, site, count)
                return 200, count if count > 0 else None
            return 200, None
        logger.debug("%s.%s/%s → HTTP %s", slug, wd_server, site, resp.status_code)
        return resp.status_code, None
    except httpx.ConnectError:
        raise
    except Exception as exc:
        logger.debug("%s.%s/%s → error: %s", slug, wd_server, site, exc)
        return 0, None


async def discover_company(
    company: str,
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    inter_attempt_delay: float = 0.2,
) -> dict | None:
    """
    Two-phase discovery exploiting the 422-vs-404 signal:

    Phase 1 — Server sieve  (5 probes max per slug, one per server)
      POST /wday/cxs/{slug}/{first_site}/jobs on each (slug, server) pair.
      · (200, N)    → immediate hit — return result
      · (404, None) → correct data centre, wrong site → add to confirmed list
      · (422, None) → wrong data centre for this tenant → skip server entirely

    Phase 2 — Site sweep  (remaining site candidates, confirmed servers only)
      Try all remaining site candidates only on the server(s) confirmed in Phase 1.

    Worst case per slug:  5 (phase 1)  +  28 (phase 2, one server)  =  33 probes
    Old approach worst case:  5 × 28  =  140 probes.
    """
    slugs = generate_slugs(company)[:_MAX_SLUGS]
    t0 = time.monotonic()

    for slug in slugs:
        slug_lower = slug.lower()
        slug_title = slug_lower.capitalize()

        # Build ordered site candidate list: slug-derived first, generic after
        extra_sites = list(dict.fromkeys([
            slug,
            slug_lower,
            slug_title,
            f"{slug_lower}careers",         # proofpointcareers ✔️
            f"{slug_lower}_careers",
            f"{slug_title}Careers",
            f"{slug_title}_Careers",
            f"{slug_lower}jobs",
            f"{slug_lower}_jobs",
            f"{slug_title}CareerSite",
            f"{slug_lower}_career_site",
        ]))
        site_order = extra_sites + [s for s in _SITE_CANDIDATES if s not in extra_sites]
        first_site = site_order[0]

        # ── Phase 1: server sieve — single probe per server ───────────────
        confirmed_servers: list[str] = []

        for wd_server in _WD_SERVERS:
            try:
                status, jobs = await _probe_status(slug, wd_server, first_site, client, sem)
            except httpx.ConnectError:
                logger.debug("DNS fail %s.%s — skipping", slug, wd_server)
                await asyncio.sleep(inter_attempt_delay)
                continue

            if jobs is not None:
                # Instant hit on the very first site candidate
                elapsed_ms = round((time.monotonic() - t0) * 1000)
                logger.info(
                    "✅ %r → %s.%s / %s  (%d jobs, %d ms)",
                    company, slug, wd_server, first_site, jobs, elapsed_ms,
                )
                return {
                    "company":    company,
                    "tenant":     slug,
                    "wd_server":  wd_server,
                    "site":       first_site,
                    "jobs_found": jobs,
                }

            if status == 404:
                # Right data centre — tenant lives here, site name just wrong
                confirmed_servers.append(wd_server)
                logger.debug("Right DC: %s.%s (404 on %s)", slug, wd_server, first_site)
            # 422 or error → wrong data centre or unreachable — skip

            await asyncio.sleep(inter_attempt_delay)

        if not confirmed_servers:
            logger.debug("No confirmed DCs for slug %r", slug)
            continue

        # ── Phase 2: sweep remaining sites on confirmed servers only ───────
        for wd_server in confirmed_servers:
            for site in site_order[1:]:  # first_site already probed above
                try:
                    status, jobs = await _probe_status(slug, wd_server, site, client, sem)
                except httpx.ConnectError:
                    break  # shouldn't happen on a confirmed server

                if jobs is not None:
                    elapsed_ms = round((time.monotonic() - t0) * 1000)
                    logger.info(
                        "✅ %r → %s.%s / %s  (%d jobs, %d ms)",
                        company, slug, wd_server, site, jobs, elapsed_ms,
                    )
                    return {
                        "company":    company,
                        "tenant":     slug,
                        "wd_server":  wd_server,
                        "site":       site,
                        "jobs_found": jobs,
                    }

                await asyncio.sleep(inter_attempt_delay)

    logger.debug("No Workday tenant found for %r", company)
    return None


# ---------------------------------------------------------------------------
# File I/O
# ---------------------------------------------------------------------------

def _load_existing_companies(path: Path) -> set[str]:
    """Return company names already recorded in the output file."""
    if not path.exists():
        return set()
    names: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            parts = line.split("|")
            if parts:
                names.add(parts[0].strip())
    return names


def _load_found_yaml(path: Path) -> set[str]:
    """
    Parse companies_found.yaml and return a case-folded set of all company
    names already confirmed on any ATS.

    The YAML structure is:
        lever:
          - slug: foo
            name: Foo Company
        greenhouse:
          - slug: bar
            name: Bar Inc
        ...

    We extract every `name` value across all ATS sections and normalise to
    lowercase for case-insensitive comparison against companies.txt.
    """
    if not path.exists():
        return set()
    try:
        import yaml  # PyYAML — already a transitive dep via ruamel or available standalone
    except ImportError:
        # Fallback: manual regex parse (no PyYAML required)
        import re
        text = path.read_text(encoding="utf-8")
        return {
            m.group(1).strip().casefold()
            for m in re.finditer(r"^\s+name:\s*(.+)$", text, re.MULTILINE)
        }
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        names: set[str] = set()
        for ats_entries in data.values():
            if isinstance(ats_entries, list):
                for entry in ats_entries:
                    if isinstance(entry, dict) and "name" in entry:
                        names.add(str(entry["name"]).casefold())
        return names
    except Exception as exc:
        logger.warning("Could not parse %s: %s — skipping YAML filter", path, exc)
        return set()


def _append_result(path: Path, result: dict, write_header: bool = False) -> None:
    """Append one confirmed result line to the output file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if write_header:
        path.write_text(
            "# Workday companies — auto-discovered by discover_workday.py\n"
            "# Format: CompanyName|tenant|wd_server|site\n"
            "#\n",
            encoding="utf-8",
        )
    line = (
        f"{result['company']}|{result['tenant']}"
        f"|{result['wd_server']}|{result['site']}\n"
    )
    with path.open("a", encoding="utf-8") as fh:
        fh.write(line)
    logger.debug("Appended: %s", line.strip())


# ---------------------------------------------------------------------------
# Main async runner
# ---------------------------------------------------------------------------

async def run(
    companies: list[str],
    output_path: Path,
    found_yaml_path: Path,
    workers: int,
    dry_run: bool,
    delay: float,
) -> tuple[list[dict], list[str]]:
    """Discovery pipeline. Returns (found, not_found) lists."""
    # Skip companies already in workday_companies.txt
    existing_workday = _load_existing_companies(output_path)
    # Skip companies already found on ANY ATS in companies_found.yaml
    existing_yaml    = _load_found_yaml(found_yaml_path)

    to_probe = [
        c for c in companies
        if c not in existing_workday and c.casefold() not in existing_yaml
    ]

    skipped_wd   = sum(1 for c in companies if c in existing_workday)
    skipped_yaml = sum(
        1 for c in companies
        if c not in existing_workday and c.casefold() in existing_yaml
    )

    if skipped_wd:
        print(f"  Skipping {skipped_wd} already in {output_path.name}")
    if skipped_yaml:
        print(f"  Skipping {skipped_yaml} already found in {found_yaml_path.name} (other ATS)")

    if not to_probe:
        print("  Nothing new to probe.")
        return [], []

    max_attempts = _MAX_SLUGS * len(_WD_SERVERS) * len(_SITE_CANDIDATES)

    if dry_run:
        print(
            f"\nDry run — would probe {len(to_probe)} companies for Workday:\n"
            f"  Slugs   : up to {_MAX_SLUGS} per company\n"
            f"  Servers : {', '.join(_WD_SERVERS)}\n"
            f"  Sites   : {len(_SITE_CANDIDATES)} candidates\n"
            f"  Worst case: up to {max_attempts} probes/company\n"
        )
        for company in to_probe:
            slugs = generate_slugs(company)[:_MAX_SLUGS]
            print(f"  {company!r:32s} → slugs: {slugs}")
        return [], []

    sem = asyncio.Semaphore(workers)
    found: list[dict] = []
    not_found: list[str] = []
    need_header = not output_path.exists()

    print(
        f"\nDiscovering Workday tenants for {len(to_probe)} companies\n"
        f"  Servers : {', '.join(_WD_SERVERS)}\n"
        f"  Sites   : {len(_SITE_CANDIDATES)} candidates\n"
        f"  Slugs   : up to {_MAX_SLUGS} per company\n"
        f"  Workers : {workers}   Delay: {delay}s/miss\n"
    )

    async with httpx.AsyncClient() as client:
        with tqdm(
            total=len(to_probe),
            unit="co",
            ncols=82,
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} companies | {elapsed}",
        ) as pbar:
            for company in to_probe:
                pbar.set_description(f"{company[:24]:<24}")
                result = await discover_company(
                    company, client, sem, inter_attempt_delay=delay
                )

                if result:
                    found.append(result)
                    _append_result(output_path, result, write_header=need_header)
                    need_header = False
                    pbar.write(
                        f"  ✅  {company!r:30s} → "
                        f"{result['tenant']}.{result['wd_server']} / "
                        f"{result['site']}  ({result['jobs_found']} jobs)"
                    )
                else:
                    not_found.append(company)

                pbar.update(1)

    return found, not_found


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="discover_workday",
        description=(
            "Auto-discover Workday tenant / data-centre / site for each company\n"
            "in companies.txt.  Results are appended to workday_companies.txt\n"
            "and can then be verified + exported by main.py."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--input", "-i",
        default="companies.txt",
        metavar="FILE",
        help="Input file — one company name per line (default: companies.txt).",
    )
    p.add_argument(
        "--output", "-o",
        default="workday_companies.txt",
        metavar="FILE",
        help=(
            "Output file to append confirmed entries to "
            "(default: workday_companies.txt). "
            "Companies already in this file are skipped."
        ),
    )
    p.add_argument(
        "--workers", "-w",
        type=int,
        default=2,
        metavar="N",
        help="Max concurrent HTTP requests (default: 2).",
    )
    p.add_argument(
        "--delay",
        type=float,
        default=0.2,
        metavar="SECS",
        help=(
            "Sleep between failed probe attempts, in seconds (default: 0.2). "
            "Increase if you see frequent 429 responses."
        ),
    )
    p.add_argument(
        "--found", "-f",
        default="companies_found.yaml",
        metavar="FILE",
        help=(
            "Path to companies_found.yaml from main.py (default: companies_found.yaml). "
            "Companies already found on any ATS in this file are skipped."
        ),
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be probed without making any requests.",
    )
    p.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable DEBUG logging.",
    )
    return p


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stderr,
    )
    if not args.verbose:
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("httpcore").setLevel(logging.WARNING)

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"Error: input file not found: {input_path}", file=sys.stderr)
        return 1

    raw = input_path.read_text(encoding="utf-8").splitlines()
    companies = [
        line.strip()
        for line in raw
        if line.strip() and not line.startswith("#")
    ]
    if not companies:
        print("Error: no companies found in input file.", file=sys.stderr)
        return 1

    output_path = Path(args.output)
    print(f"Input  : {input_path} ({len(companies)} companies)")
    print(f"Output : {output_path}")

    try:
        found, not_found = asyncio.run(
            run(
                companies=companies,
                output_path=output_path,
                found_yaml_path=Path(args.found),
                workers=args.workers,
                dry_run=args.dry_run,
                delay=args.delay,
            )
        )
    except KeyboardInterrupt:
        print("\n\nInterrupted — partial results saved to disk.", file=sys.stderr)
        return 130

    if args.dry_run:
        return 0

    print(f"\nDone.")
    print(f"  ✅  Found on Workday : {len(found)}")
    print(f"  ❌  Not found        : {len(not_found)}")
    if not_found and args.verbose:
        print("\nNot found:")
        for c in not_found:
            print(f"    {c}")
    if found:
        print(f"\nResults appended to: {output_path}")
        print("Next step: run  python main.py  to verify and export YAML.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
