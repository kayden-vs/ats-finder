"""
normalizer.py — Slug-guessing from company names.

Generates multiple slug candidates from a company name so that
ATS probers can try all plausible variants before giving up.
"""

import re
import unicodedata


def _strip_accents(text: str) -> str:
    """Normalize unicode characters to ASCII equivalents."""
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def _clean(name: str) -> str:
    """Remove punctuation and extra whitespace, strip accents."""
    name = _strip_accents(name.strip())
    # Remove characters that are not alphanumeric or whitespace
    name = re.sub(r"[^\w\s]", "", name)
    # Collapse whitespace
    name = re.sub(r"\s+", " ", name).strip()
    return name


def generate_slugs(company_name: str) -> list[str]:
    """
    Generate an ordered, deduplicated list of slug candidates for a company.

    Example:
        "Razorpay India" -> [
            "razorpay-india",   # hyphenated lowercase
            "razorpayindia",    # no-separator lowercase
            "razorpay",         # first word lowercase
            "RazorpayIndia",    # PascalCase
            "Razorpay",         # original first word casing
        ]

    The list is intended to be tried in order — stop probing once a hit is found.
    """
    name = _clean(company_name)
    words = name.split()

    if not words:
        return []

    lower = name.lower()
    lower_words = lower.split()

    # Core variants
    hyphenated = "-".join(lower_words)                            # razorpay-india
    no_space = "".join(lower_words)                               # razorpayindia
    first_word_lower = lower_words[0]                             # razorpay
    pascal = "".join(w.capitalize() for w in words)              # RazorpayIndia
    first_word_original = words[0]                                # Razorpay (original casing)

    candidates = [
        hyphenated,
        no_space,
        first_word_lower,
        pascal,
        first_word_original,
    ]

    # Additional variants for common suffixes that ATS slugs often omit
    _suffixes_to_strip = [
        "india", "technologies", "technology", "tech", "solutions",
        "services", "labs", "inc", "ltd", "limited", "pvt", "private",
        "payments", "financial", "fintech", "platform", "platforms",
        "ai", "hq", "group", "global", "digital",
    ]

    stripped_words = [
        w for w in lower_words
        if w not in _suffixes_to_strip
    ]

    if stripped_words and stripped_words != lower_words:
        candidates.append("-".join(stripped_words))        # razorpay (without "payments")
        candidates.append("".join(stripped_words))         # razorpay (no sep)
        candidates.append(stripped_words[0])               # first meaningful word

    # SmartRecruiters sometimes uses title-case full name without spaces
    title_no_space = "".join(w.title() for w in words)         # RazorpayIndia
    candidates.append(title_no_space)

    # Deduplicate while preserving insertion order
    seen = set()
    unique = []
    for c in candidates:
        if c and c not in seen:
            seen.add(c)
            unique.append(c)

    return unique


def careers_url(ats: str, slug: str) -> str:
    """Return the canonical careers page URL for a given ATS + slug."""
    _urls = {
        "greenhouse":       f"https://job-boards.greenhouse.io/{slug}",
        "lever":            f"https://jobs.lever.co/{slug}",
        "ashby":            f"https://jobs.ashbyhq.com/{slug}",
        "workable":         f"https://apply.workable.com/{slug}/",
        "smartrecruiters":  f"https://careers.smartrecruiters.com/{slug}",
        "rippling":         f"https://ats.rippling.com/{slug}/jobs",
        "bamboohr":         f"https://{slug}.bamboohr.com/careers",
        "recruitee":        f"https://{slug}.recruitee.com/",
        "personio":         f"https://{slug}.jobs.personio.de/",
        "teamtailor":       f"https://{slug}.teamtailor.com/",
        "freshteam":        f"https://{slug}.freshteam.com/jobs",
    }
    return _urls.get(ats, "")
