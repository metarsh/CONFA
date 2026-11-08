from __future__ import annotations

import hashlib
import re
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse


_TRACKING_PARAMS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "utm_name",
    "gclid",
    "fbclid",
    "mc_cid",
    "mc_eid",
}

_TRUNCATION_RE = re.compile(r"\[\+\d+ chars\]")


def canonicalize_url(url: str) -> str:
    if not url:
        return ""
    parsed = urlparse(url.strip())
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc.lower()

    filtered_query = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=False):
        if key.lower() not in _TRACKING_PARAMS:
            filtered_query.append((key, value))

    normalized = parsed._replace(
        scheme=scheme,
        netloc=netloc,
        query=urlencode(filtered_query, doseq=True),
        fragment="",
    )

    cleaned = urlunparse(normalized)
    if cleaned.endswith("/"):
        cleaned = cleaned[:-1]
    return cleaned


def make_article_id(source: str, canonical_url: str, title: str, published_date: str) -> str:
    seed = f"{source}|{canonical_url}|{title.strip().lower()}|{published_date}"
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()


def contains_truncation_artifact(text: str) -> bool:
    return bool(_TRUNCATION_RE.search(text or ""))


def is_probable_article_url(source: str, url: str) -> bool:
    lowered = (url or "").lower()
    if not lowered:
        return False

    parsed = urlparse(lowered)
    path = (parsed.path or "").strip("/")
    if not path:
        return False

    segments = [seg for seg in path.split("/") if seg]

    blocked_substrings = {
        "BBC": ["/sounds/", "/programmes/", "/iplayer/", "/sport/"],
        "CNN": ["/videos/", "/audio/"],
        "Fox News": ["/video/"],
        "NYT": ["/video/", "/podcasts/"],
        "WSJ": ["/video/"],
    }

    for blocked in blocked_substrings.get(source, []):
        if blocked in lowered:
            return False

    if source == "BBC" and "/news" not in lowered:
        return False

    if source == "CNN" and not _has_year_month_path(segments):
        return False

    return True


def _has_year_month_path(segments: list[str]) -> bool:
    for idx in range(len(segments) - 1):
        year = segments[idx]
        month = segments[idx + 1]
        if re.fullmatch(r"20\d{2}", year) and re.fullmatch(r"0[1-9]|1[0-2]", month):
            return True
    return False
