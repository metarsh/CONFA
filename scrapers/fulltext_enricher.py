"""Full-text fetch + extraction"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Iterable
from urllib.parse import urlparse

import requests
from newspaper import Article as NewspaperArticle
import trafilatura
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential
from models import Article, ArticleStub, RejectedArticle
from utils.config import Config, SOURCES
from utils.url_utils import (
    canonicalize_url,
    contains_truncation_artifact,
    is_probable_article_url,
    make_article_id,
)


logger = logging.getLogger(__name__)


_PAYWALL_PATTERNS = [
    re.compile(r"subscribe", re.IGNORECASE),
    re.compile(r"sign in to continue", re.IGNORECASE),
    re.compile(r"this article is for subscribers", re.IGNORECASE),
    re.compile(r"premium content", re.IGNORECASE),
]


class FullTextEnricher:
    """Turn discovered stubs into analysis-ready full-text articles."""

    def __init__(self, config: Config):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/121.0 Safari/537.36"
            }
        )

    def enrich(self, stubs: Iterable[ArticleStub]) -> tuple[list[Article], list[RejectedArticle], dict]:
        accepted: list[Article] = []
        rejected: list[RejectedArticle] = []

        deduped_stubs = self._dedupe_stubs(stubs)

        for stub in deduped_stubs:
            article, reject = self._process_stub(stub)
            if article:
                accepted.append(article)
            if reject:
                rejected.append(reject)

        accepted = self._dedupe_articles(accepted)
        stats = self._build_stats(deduped_stubs, accepted, rejected)
        return accepted, rejected, stats

    def process_stub(self, stub: ArticleStub) -> tuple[Article | None, RejectedArticle | None]:
        """Process a single stub"""
        return self._process_stub(stub)

    def _process_stub(self, stub: ArticleStub) -> tuple[Article | None, RejectedArticle | None]:
        canonical_url = canonicalize_url(stub.url)
        source_cfg = SOURCES.get(stub.source, {})

        if not canonical_url or not is_probable_article_url(stub.source, canonical_url):
            return None, self._reject(stub, "non_article_url")

        if not self._url_matches_source_domain(canonical_url, source_cfg.get("domains", [])):
            return None, self._reject(stub, "off_domain_url")

        try:
            html = self._fetch_html(canonical_url)
        except Exception as exc:
            logger.warning("Fetch failed for %s: %s", canonical_url, exc)
            return None, self._reject(stub, "fetch_failed")

        if self._is_paywalled_or_blocked(html):
            return None, self._reject(stub, "paywalled_or_blocked")

        text, content_source = self._extract_text(canonical_url, html)
        if not text:
            return None, self._reject(stub, "extraction_failed")

        word_count = len(text.split())
        if contains_truncation_artifact(text):
            return None, self._reject(stub, "truncation_artifact", text[:300])

        if word_count < self.config.MIN_ARTICLE_WORDS:
            return None, self._reject(stub, "below_min_words", text[:300])

        quality_score = self._quality_score(text, word_count)
        article = Article(
            article_id=make_article_id(stub.source, canonical_url, stub.title, stub.published_date),
            source=stub.source,
            title=stub.title,
            url=stub.url,
            canonical_url=canonical_url,
            content=text,
            published_date=stub.published_date,
            discovery_source=stub.discovery_source,
            content_source=content_source,
            raw_payload_ref=stub.raw_payload_ref,
            is_full_text=True,
            quality_score=quality_score,
            description=stub.description,
            author=stub.author,
            section=stub.section,
            article_type=stub.article_type,
            tags=stub.tags,
            word_count=word_count,
            api_source="fulltext_enricher",
            leaning=source_cfg.get("leaning"),
            language="en",
            collected_at=datetime.now().isoformat(),
        )
        return article, None

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        retry=retry_if_exception_type((requests.RequestException, TimeoutError)),
        reraise=True,
    )
    def _fetch_html(self, url: str) -> str:
        response = self.session.get(url, timeout=self.config.REQUEST_TIMEOUT)
        response.raise_for_status()
        if not response.text:
            raise TimeoutError("Empty response body")
        return response.text

    def _extract_text(self, url: str, html: str) -> tuple[str, str]:
        text = trafilatura.extract(
            html,
            url=url,
            include_comments=False,
            include_tables=False,
            no_fallback=False,
            favor_precision=True,
        )
        if text:
            return text.strip(), "trafilatura"

        try:
            article = NewspaperArticle(url=url)
            article.set_html(html)
            article.parse()
            fallback = (article.text or "").strip()
            if fallback:
                return fallback, "newspaper3k"
        except Exception as exc:
            logger.warning("newspaper3k extraction failed for %s: %s", url, exc)

        return "", "none"

    @staticmethod
    def _is_paywalled_or_blocked(html: str) -> bool:
        sample = html[:12000]
        return any(pattern.search(sample) for pattern in _PAYWALL_PATTERNS)

    @staticmethod
    def _url_matches_source_domain(url: str, allowed_domains: list[str]) -> bool:
        if not allowed_domains:
            return True
        try:
            host = (urlparse(url).netloc or "").lower()
        except Exception as exc:
            logger.warning("Failed to parse URL host '%s': %s", url, exc)
            return False
        if not host:
            return False
        for domain in allowed_domains:
            d = (domain or "").strip().lower()
            if not d:
                continue
            if host == d or host.endswith(f".{d}"):
                return True
        return False

    @staticmethod
    def _quality_score(text: str, word_count: int) -> float:
        length_component = min(1.0, word_count / 1200)
        paragraph_component = min(1.0, text.count("\n") / 20)
        return round((0.7 * length_component) + (0.3 * paragraph_component), 4)

    @staticmethod
    def _dedupe_stubs(stubs: Iterable[ArticleStub]) -> list[ArticleStub]:
        selected: dict[str, ArticleStub] = {}
        for stub in stubs:
            key = canonicalize_url(stub.url)
            if not key:
                continue
            current = selected.get(key)
            if not current:
                selected[key] = stub
                continue

            priority = {
                "nyt_archive": 5,
                "wayback_cdx": 3,
                "rss": 1,
                "sitemap": 0,
            }
            if priority.get(stub.discovery_source, 0) > priority.get(current.discovery_source, 0):
                selected[key] = stub
        return list(selected.values())

    @staticmethod
    def _dedupe_articles(articles: list[Article]) -> list[Article]:
        selected: dict[str, Article] = {}
        fallback_index: set[str] = set()

        for article in articles:
            key = article.canonical_url
            fallback = f"{article.source}|{article.title.strip().lower()}|{article.published_date[:10]}"
            if key in selected or fallback in fallback_index:
                continue
            selected[key] = article
            fallback_index.add(fallback)

        return list(selected.values())

    def _reject(self, stub: ArticleStub, reason: str, preview: str = "") -> RejectedArticle:
        logger.info("Rejected article [%s] %s: %s", stub.source, reason, stub.url)
        return RejectedArticle(
            source=stub.source,
            url=stub.url,
            title=stub.title,
            published_date=stub.published_date,
            discovery_source=stub.discovery_source,
            raw_payload_ref=stub.raw_payload_ref,
            drop_reason=reason,
            content_preview=(preview or "")[:300],
        )

    @staticmethod
    def _build_stats(stubs: list[ArticleStub], accepted: list[Article], rejected: list[RejectedArticle]) -> dict:
        rejected_reason_counts: dict[str, int] = {}
        for row in rejected:
            rejected_reason_counts[row.drop_reason] = rejected_reason_counts.get(row.drop_reason, 0) + 1

        per_source: dict[str, dict[str, int]] = {}
        for source in sorted({stub.source for stub in stubs}):
            per_source[source] = {
                "discovered": len([s for s in stubs if s.source == source]),
                "accepted": len([a for a in accepted if a.source == source]),
                "rejected": len([r for r in rejected if r.source == source]),
            }

        return {
            "discovered": len(stubs),
            "accepted": len(accepted),
            "rejected": len(rejected),
            "acceptance_rate": round(len(accepted) / len(stubs), 4) if stubs else 0.0,
            "rejected_reasons": rejected_reason_counts,
            "per_source": per_source,
        }
