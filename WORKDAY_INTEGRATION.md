# Workday ATS — Integration Guide for Job Pipeline

> Written for a downstream coding agent integrating Workday as a job source.
> All findings are empirically verified.

---

## 1. How Workday Differs from Greenhouse / Lever / Ashby

| Aspect | Greenhouse / Lever / Ashby | Workday |
|---|---|---|
| URL structure | `boards.greenhouse.io/{slug}` — shared domain, company-specific slug | `{tenant}.{wd_server}.myworkdayjobs.com` — each company has its own **subdomain** |
| Slug guessing | Derivable from company name with reasonable confidence | **Three independent unknowns**: `tenant`, `wd_server`, `site` — none reliably guessable |
| API auth | Public, no auth needed | Public (unauthenticated), but requires correct `Origin`/`Referer` headers |
| HTTP method | Typically `GET` | Always **`POST`** to a CXS endpoint |
| Pagination | `?page=N` or `?offset=N` in query string | JSON body field `"offset": N` |
| Data centre | N/A (single shared SaaS domain) | 5+ known clusters: `wd1`, `wd3`, `wd5`, `wd12`, `wd13` — company-specific |
| Site path | Not applicable | Company-configured identifier (e.g. `proofpointcareers`, `external_experienced`) |

**Bottom line**: Workday cannot be integrated with a generic slug-guessing approach.
You must first **discover** each company's `{tenant}`, `{wd_server}`, and `{site}` values
(done by `discover_workday.py`), then store them in `workday_companies.txt`.

---

## 2. The Three Required Parameters

```
https://{tenant}.{wd_server}.myworkdayjobs.com/wday/cxs/{tenant}/{site}/jobs
```

| Parameter | Description | Example |
|---|---|---|
| `tenant` | Subdomain slug — usually the company name lowercased | `proofpoint`, `adobe`, `workday` |
| `wd_server` | Workday data-centre identifier | `wd1`, `wd3`, `wd5`, `wd12`, `wd13` |
| `site` | Career portal site name — company-configured | `proofpointcareers`, `external_experienced`, `Workday` |

All three are stored in `workday_companies.txt` as a pipe-delimited line:

```
# Format: CompanyName|tenant|wd_server|site
Proofpoint|proofpoint|wd5|proofpointcareers
Adobe|adobe|wd5|external_experienced
Workday|workday|wd5|workday
```

---

## 3. Fetching Jobs — The Exact API Call

### Endpoint

```
POST https://{tenant}.{wd_server}.myworkdayjobs.com/wday/cxs/{tenant}/{site}/jobs
```

### Required Headers

```http
Content-Type: application/json
Accept: application/json
Accept-Language: en-US,en;q=0.9
Origin: https://{tenant}.{wd_server}.myworkdayjobs.com
Referer: https://{tenant}.{wd_server}.myworkdayjobs.com/{site}/
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36
```

> **Important**: `Origin` and `Referer` must be set and must point to the correct
> tenant domain. Some Workday deployments behind Cloudflare/Akamai reject requests
> without these. No CSRF token or session cookie is required for the public listing.

### Request Body

```json
{
  "appliedFacets": {},
  "limit": 20,
  "offset": 0,
  "searchText": ""
}
```

| Field | Type | Notes |
|---|---|---|
| `appliedFacets` | object | Filters (location, job type, etc). Leave `{}` for all jobs |
| `limit` | int | Jobs per page. Max observed: `20`. Workday may cap it |
| `offset` | int | Pagination offset. First page = `0`, second = `20`, etc. |
| `searchText` | string | Keyword filter. Leave `""` for all jobs |

### Pagination

```python
offset = 0
all_jobs = []
while True:
    payload = {"appliedFacets": {}, "limit": 20, "offset": offset, "searchText": ""}
    resp = client.post(url, json=payload, headers=headers)
    data = resp.json()
    postings = data.get("jobPostings", [])
    all_jobs.extend(postings)
    total = data.get("total", 0)
    offset += len(postings)
    if offset >= total or not postings:
        break
```

---

## 4. Response Structure

### Top-level

```json
{
  "total": 177,
  "jobPostings": [ ... ],
  "facets": { ... },
  "userAuthenticated": false
}
```

| Key | Type | Description |
|---|---|---|
| `total` | int | Total number of matching jobs across all pages |
| `jobPostings` | array | Jobs on this page (up to `limit` items) |
| `facets` | object | Filter categories (locations, job families, etc) |
| `userAuthenticated` | bool | Always `false` for public unauthenticated calls |

### Single Job Posting Object

```json
{
  "title": "Director, Global Systems Integrator (GSI)",
  "externalPath": "/job/Texas/Director--Global-Systems-Integrator--GSI-_R13383",
  "locationsText": "Texas",
  "postedOn": "Posted Yesterday",
  "bulletFields": ["R13383"]
}
```

| Field | Type | Description |
|---|---|---|
| `title` | string | Job title |
| `externalPath` | string | Path relative to the tenant root — construct full URL as below |
| `locationsText` | string | Human-readable location string |
| `postedOn` | string | Relative time string (e.g. `"Posted 3 days ago"`, `"Posted Today"`) |
| `bulletFields` | array | Usually contains the internal job requisition ID (e.g. `"R13383"`) |

### Constructing the Full Job URL

```python
job_url = f"https://{tenant}.{wd_server}.myworkdayjobs.com/{site}{job['externalPath']}"
# Example:
# https://proofpoint.wd5.myworkdayjobs.com/proofpointcareers/job/Texas/Director-..._R13383
```

---

## 5. HTTP Status Code Reference

When probing (used by `discover_workday.py`, also useful for health checks):

| Status | Meaning |
|---|---|
| `200 OK` | Correct tenant + server + site. Response contains job data |
| `404 Not Found` | **Correct server for this tenant**, but site name is wrong |
| `422 Unprocessable Entity` | **Wrong server** — this tenant is not hosted on this data centre |
| `ConnectError` (DNS) | Extremely rare — all `*.wd{N}.myworkdayjobs.com` subdomains resolve |

> **Key insight**: 404 and 422 carry different semantics. If you're health-checking
> a known entry and get 404, the tenant moved or the site was renamed. If you get
> 422, the stored `wd_server` is wrong (rare but possible after Workday migrations).

---

## 6. Rate Limiting / Anti-Bot Notes

- Workday career sites are behind **Cloudflare** (public-facing) and historically
  **Akamai** (some tenants). Both track per-IP request rates across all
  `*.myworkdayjobs.com` subdomains.
- For **discovery** (brute-forcing server × site combos): keep concurrency ≤ 3,
  add ~200ms delay between misses.
- For **job fetching** (known endpoint, paginated): safe to do 5–10 req/s per tenant;
  much more relaxed than discovery.
- No CSRF token or session cookie is needed for the public jobs listing API.
- The `X-CALYPSO-CSRF-TOKEN` header seen in browser network tabs is session-specific
  and only required for authenticated actions (applying, saving jobs). Skip it.

---

## 7. Reading `workday_companies.txt`

```python
from pathlib import Path

def load_workday_companies(path="workday_companies.txt"):
    entries = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split("|")
        if len(parts) == 4:
            company, tenant, wd_server, site = parts
            entries.append({
                "company":   company.strip(),
                "tenant":    tenant.strip(),
                "wd_server": wd_server.strip(),
                "site":      site.strip(),
                "jobs_url":  (
                    f"https://{tenant.strip()}.{wd_server.strip()}.myworkdayjobs.com"
                    f"/wday/cxs/{tenant.strip()}/{site.strip()}/jobs"
                ),
            })
    return entries
```

---

## 8. Minimal Working Example

```python
import httpx

def fetch_workday_jobs(tenant: str, wd_server: str, site: str) -> list[dict]:
    """Fetch all job postings for a Workday company."""
    base = f"https://{tenant}.{wd_server}.myworkdayjobs.com"
    url  = f"{base}/wday/cxs/{tenant}/{site}/jobs"
    headers = {
        "User-Agent":      "Mozilla/5.0 (compatible; JobPipeline/1.0)",
        "Accept":          "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Content-Type":    "application/json",
        "Origin":          base,
        "Referer":         f"{base}/{site}/",
    }

    jobs, offset = [], 0
    with httpx.Client(timeout=15.0) as client:
        while True:
            resp = client.post(url, json={
                "appliedFacets": {}, "limit": 20,
                "offset": offset, "searchText": "",
            }, headers=headers)
            resp.raise_for_status()
            data = resp.json()
            postings = data.get("jobPostings", [])
            jobs.extend(postings)
            total  = data.get("total", 0)
            offset += len(postings)
            if offset >= total or not postings:
                break

    # Enrich with full URL
    for job in jobs:
        job["url"] = f"{base}/{site}{job['externalPath']}"
        job["company_tenant"] = tenant

    return jobs


# Usage
jobs = fetch_workday_jobs("proofpoint", "wd5", "proofpointcareers")
for job in jobs[:3]:
    print(job["title"], "|", job["locationsText"], "|", job["url"])
```

---

## 9. Discovery Workflow (How `workday_companies.txt` Gets Populated)

The file is populated by `discover_workday.py`, which runs a two-phase brute-force:

```
Phase 1 — Server sieve (1 probe per server per slug):
  POST /wday/cxs/{slug}/{slug}/jobs on each of [wd1, wd3, wd5, wd12, wd13]
    422 → wrong data centre for this tenant → skip all remaining sites on this server
    404 → correct data centre, wrong site   → add to "confirmed servers" list
    200 → immediate hit!

Phase 2 — Site sweep (only on confirmed servers):
  Try ~30 common site name candidates until 200 + total > 0.

Worst case:  5 servers (phase 1)  +  30 sites (phase 2, 1 server)  =  35 probes/slug
Typical hit: 3–8 probes total.
```

Run it as:
```bash
python discover_workday.py                  # reads companies.txt, writes workday_companies.txt
python discover_workday.py --dry-run        # show what would be probed
python discover_workday.py --workers 3      # increase concurrency (careful with rate limits)
```

---

## 10. Known Real-World Examples

| Company | tenant | wd_server | site |
|---|---|---|---|
| Proofpoint | `proofpoint` | `wd5` | `proofpointcareers` |
| Adobe | `adobe` | `wd5` | `external_experienced` |
| Workday (itself) | `workday` | `wd5` | `workday` |
| Samsung | `sec` | `wd3` | `Samsung_Careers` |
| Oracle | `oracle` | `wd5` | `oracle-careers` |
