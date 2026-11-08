"""URL discovery from APIs, RSS feeds, and sitemaps."""

from __future__ import annotations

import gzip
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Callable, Iterable
from urllib.parse import urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

from models import ArticleStub
from utils.config import SOURCES, Config
from utils.progress import progress
from utils.storage import DataStorage
from utils.url_utils import canonicalize_url, is_probable_article_url


logger = logging.getLogger(__name__)

_SITEMAP_HINT_PATTERNS = [
    re.compile(r"(?<!\d)(20\d{2})(\d{2})(\d{2})(?!\d)"),
    re.compile(r"(?<!\d)(20\d{2})[-_/](\d{2})[-_/](\d{2})(?!\d)"),
    re.compile(r"(?<!\d)(20\d{2})[-_/](\d{2})(?!\d)"),
]
_SITEMAP_REQUIRE_DATE_SIGNAL_THRESHOLD = 500


class URLDiscovery:
    """Collect article URLs and metadata before full-text extraction."""

    def __init__(self, config: Config, storage: DataStorage):
        self.config = config
        self.storage = storage
        self._wayback_globally_disabled = False
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/121.0 Safari/537.36"
            }
        )

    def discover(
        self,
        sources: Iterable[str],
        from_date: datetime,
        to_date: datetime,
        existing_stubs: list[ArticleStub] | None = None,
        progress_callback: Callable[[dict], None] | None = None,
    ) -> tuple[list[ArticleStub], dict]:
        all_stubs: list[ArticleStub] = []
        for stub in existing_stubs or []:
            if self._stub_passes_quality(stub, from_date, to_date):
                all_stubs.append(stub)
        stats: dict[str, dict[str, object]] = {}
        source_list = list(sources)
        total_sources = len(source_list)

        with progress(total=total_sources, desc="Discovery channels", unit="channel") as source_bar:
            for source_idx, source in enumerate(source_list, start=1):
                source_started = time.monotonic()
                logger.info("Discovery started for %s (%s/%s)", source, source_idx, total_sources)

                def _on_provider_progress(provider_name: str, source_partial: list[ArticleStub]) -> None:
                    deduped_partial = self._dedupe_stubs([*all_stubs, *source_partial])
                    self.storage.save_stubs(deduped_partial)
                    if progress_callback:
                        progress_callback(
                            {
                                "stage": "discovery",
                                "source": source,
                                "source_index": source_idx,
                                "total_sources": total_sources,
                                "provider": provider_name,
                                "source_discovered": len(source_partial),
                                "deduped_cumulative": len(deduped_partial),
                            }
                        )

                source_stubs, source_meta = self.discover_for_source(
                    source,
                    from_date,
                    to_date,
                    progress_callback=_on_provider_progress,
                )
                all_stubs.extend(source_stubs)
                deduped_so_far = self._dedupe_stubs(all_stubs)
                source_elapsed = round(time.monotonic() - source_started, 2)
                stats[source] = {
                    "discovered": len(source_stubs),
                    "deduped_cumulative": len(deduped_so_far),
                    "elapsed_seconds": source_elapsed,
                    "providers": source_meta.get("providers", {}),
                }
                # Checkpoint after each source so interrupted runs can resume.
                self.storage.save_stubs(deduped_so_far)
                logger.info(
                    "Discovery completed for %s: %s stubs (deduped cumulative %s) in %.2fs",
                    source,
                    len(source_stubs),
                    len(deduped_so_far),
                    source_elapsed,
                )
                source_bar.set_postfix_str(f"{source}: {len(source_stubs)} stubs")
                source_bar.update(1)
                if progress_callback:
                    progress_callback(
                        {
                            "stage": "discovery",
                            "source": source,
                            "source_index": source_idx,
                            "total_sources": total_sources,
                            "provider": "source_complete",
                            "source_discovered": len(source_stubs),
                            "deduped_cumulative": len(deduped_so_far),
                            "source_elapsed_seconds": source_elapsed,
                        }
                    )

        deduped = self._dedupe_stubs(all_stubs)
        stats["_total"] = {
            "discovered": len(all_stubs),
            "deduped": len(deduped),
            "duplicates_removed": len(all_stubs) - len(deduped),
        }

        self.storage.save_stubs(deduped)
        return deduped, stats

    def discover_for_source(
        self,
        source: str,
        from_date: datetime,
        to_date: datetime,
        progress_callback: Callable[[str, list[ArticleStub]], None] | None = None,
    ) -> tuple[list[ArticleStub], dict]:
        cfg = SOURCES[source]
        stubs: list[ArticleStub] = []
        provider_stats: dict[str, dict[str, object]] = {}

        def emit_progress(provider_name: str) -> None:
            if progress_callback:
                progress_callback(provider_name, list(stubs))

        def provider_done(
            provider_name: str,
            before_count: int,
            started_at: float,
            status: str,
            error: str = "",
        ) -> None:
            added = len(stubs) - before_count
            elapsed = time.monotonic() - started_at
            provider_stats[provider_name] = {
                "status": status,
                "added": added,
                "elapsed_seconds": round(elapsed, 2),
                "error": error,
            }
            logger.info(
                "[%s] %s discovery complete: status=%s +%s stubs (source total %s) in %.2fs",
                source,
                provider_name,
                status,
                added,
                len(stubs),
                elapsed,
            )
            if error:
                if status == "failed":
                    logger.error("[%s] %s discovery error: %s", source, provider_name, error)
                else:
                    logger.warning("[%s] %s discovery issues: %s", source, provider_name, error)
            emit_progress(provider_name)

        if self.config.USE_SITEMAPS:
            before = len(stubs)
            started = time.monotonic()
            logger.info("[%s] Sitemap discovery start", source)
            try:
                stubs.extend(self._discover_source_sitemaps(source, cfg, from_date, to_date))
                provider_done("sitemaps", before, started, "ok")
            except Exception as exc:
                logger.error("Sitemap discovery failed for %s: %s", source, exc)
                provider_done("sitemaps", before, started, "failed", f"{type(exc).__name__}: {exc}")
        else:
            provider_stats["sitemaps"] = {"status": "disabled", "added": 0, "elapsed_seconds": 0.0, "error": ""}

        if self.config.USE_RSS:
            before = len(stubs)
            started = time.monotonic()
            logger.info("[%s] RSS discovery start", source)
            failed_feeds: list[str] = []
            for feed_url in cfg.get("rss_feeds", []):
                try:
                    stubs.extend(self._discover_from_rss(source, feed_url, from_date, to_date))
                except Exception as exc:
                    failed_feeds.append(f"{feed_url} -> {type(exc).__name__}: {exc}")
                    logger.error("RSS discovery failed for %s (%s): %s", source, feed_url, exc)
            status = "failed" if failed_feeds and len(failed_feeds) == len(cfg.get("rss_feeds", [])) else "ok"
            provider_done("rss", before, started, status, "; ".join(failed_feeds[:3]))
        else:
            provider_stats["rss"] = {"status": "disabled", "added": 0, "elapsed_seconds": 0.0, "error": ""}

        if source == "NYT" and self.config.USE_NYT_ARCHIVE and self.config.NY_API_KEY:
            before = len(stubs)
            started = time.monotonic()
            logger.info("[%s] NYT archive discovery start", source)
            try:
                stubs.extend(self._discover_from_nyt_archive(from_date, to_date))
                provider_done("nyt_archive", before, started, "ok")
            except Exception as exc:
                logger.error("NYT archive discovery failed: %s", exc)
                provider_done("nyt_archive", before, started, "failed", f"{type(exc).__name__}: {exc}")
        elif source == "NYT":
            provider_stats["nyt_archive"] = {
                "status": "disabled",
                "added": 0,
                "elapsed_seconds": 0.0,
                "error": "",
            }

        if self.config.USE_WAYBACK and not self._wayback_globally_disabled and not stubs:
            before = len(stubs)
            started = time.monotonic()
            logger.info("[%s] Wayback discovery start (last resort)", source)
            try:
                stubs.extend(self._discover_from_wayback(source, cfg.get("domains", []), from_date, to_date))
                provider_done("wayback", before, started, "ok")
            except requests.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else None
                if status in {502, 503, 504}:
                    self._wayback_globally_disabled = True
                    logger.error(
                        "Wayback CDX returned HTTP %s. Disabling Wayback for the rest of this run.",
                        status,
                    )
                provider_done("wayback", before, started, "failed", f"HTTPError({status}): {exc}")
            except Exception as exc:
                logger.error("Wayback CDX discovery failed for %s: %s", source, exc)
                provider_done("wayback", before, started, "failed", f"{type(exc).__name__}: {exc}")
        elif self.config.USE_WAYBACK and stubs:
            provider_stats["wayback"] = {
                "status": "skipped",
                "added": 0,
                "elapsed_seconds": 0.0,
                "error": "skipped because primary providers already produced stubs",
            }
        else:
            provider_stats["wayback"] = {"status": "disabled", "added": 0, "elapsed_seconds": 0.0, "error": ""}

        pre_filter_count = len(stubs)

        filtered = [
            stub
            for stub in stubs
            if self._stub_passes_quality(
                stub,
                from_date,
                to_date,
                allowed_domains=cfg.get("domains", []),
            )
        ]
        removed = pre_filter_count - len(filtered)
        logger.info(
            "[%s] Source quality filter: kept %s/%s stubs (removed %s)",
            source,
            len(filtered),
            pre_filter_count,
            removed,
        )
        return filtered, {"providers": provider_stats}

    def _discover_source_sitemaps(
        self,
        source: str,
        cfg: dict,
        from_date: datetime,
        to_date: datetime,
    ) -> list[ArticleStub]:
        candidates = self._configured_sitemap_urls(cfg)
        if not candidates:
            return []

        if self._is_short_window(from_date, to_date):
            short_candidates = [url for url in candidates if self._is_short_window_sitemap(url)]
            if short_candidates:
                logger.info(
                    "[%s] Short-window mode active (%s days): limiting sitemap candidates %s -> %s",
                    source,
                    (to_date - from_date).days + 1,
                    len(candidates),
                    len(short_candidates),
                )
                candidates = short_candidates

        all_stubs: list[ArticleStub] = []
        errors: list[str] = []

        logger.info("[%s] Sitemap candidates: %s", source, len(candidates))
        for idx, sitemap_url in enumerate(candidates, start=1):
            logger.info("[%s] Sitemap candidate %s/%s: %s", source, idx, len(candidates), sitemap_url)
            started = time.monotonic()
            try:
                discovered = self._discover_from_sitemap(source, sitemap_url, from_date, to_date)
                all_stubs.extend(discovered)
                logger.info(
                    "[%s] Sitemap candidate %s/%s done: +%s stubs in %.2fs",
                    source,
                    idx,
                    len(candidates),
                    len(discovered),
                    time.monotonic() - started,
                )
            except requests.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else "unknown"
                errors.append(f"{sitemap_url} -> HTTP {status}")
                logger.error(
                    "[%s] Sitemap candidate %s/%s failed (HTTP %s) in %.2fs: %s",
                    source,
                    idx,
                    len(candidates),
                    status,
                    time.monotonic() - started,
                    sitemap_url,
                )
            except Exception as exc:
                errors.append(f"{sitemap_url} -> {exc}")
                logger.error(
                    "[%s] Sitemap candidate %s/%s failed in %.2fs: %s (%s)",
                    source,
                    idx,
                    len(candidates),
                    time.monotonic() - started,
                    sitemap_url,
                    exc,
                )

        if errors and all_stubs:
            logger.info(
                "Sitemap discovery for %s recovered after %s failed candidate URLs",
                source,
                len(errors),
            )
        if errors and not all_stubs:
            raise RuntimeError(
                f"All configured sitemap endpoints failed for {source}. "
                f"First errors: {'; '.join(errors[:3])}"
            )

        return all_stubs

    @staticmethod
    def _is_short_window(from_date: datetime, to_date: datetime) -> bool:
        return (to_date - from_date).days <= 14

    @staticmethod
    def _is_short_window_sitemap(url: str) -> bool:
        lowered = (url or "").lower()
        if not lowered:
            return False

        heavy_tokens = (
            "archive",
            "type=articles",
            "/article.xml",
            "/section.xml",
            "interactive",
        )
        if any(token in lowered for token in heavy_tokens):
            return False

        return "news" in lowered

    @staticmethod
    def _configured_sitemap_urls(cfg: dict) -> list[str]:
        candidates: list[str] = []
        seen: set[str] = set()

        for raw in cfg.get("sitemaps", []):
            sitemap_url = (raw or "").strip()
            if not sitemap_url or sitemap_url in seen:
                continue

            parsed = urlparse(sitemap_url)
            if parsed.scheme not in {"http", "https"} or not parsed.netloc:
                logger.error("Ignoring invalid sitemap URL in source config: %s", sitemap_url)
                continue

            seen.add(sitemap_url)
            candidates.append(sitemap_url)

        return candidates

    def _discover_from_wayback(
        self,
        source: str,
        domains: list[str],
        from_date: datetime,
        to_date: datetime,
    ) -> list[ArticleStub]:
        stubs: list[ArticleStub] = []
        if not domains:
            return stubs

        cursor = from_date
        while cursor <= to_date:
            chunk_end = min(cursor + timedelta(days=14), to_date)
            from_token = cursor.strftime("%Y%m%d")
            to_token = chunk_end.strftime("%Y%m%d")

            for domain in domains:
                page_count = self._wayback_num_pages(domain, from_token, to_token)
                max_pages = min(page_count, 200)

                if page_count > max_pages:
                    logger.info(
                        "Wayback page count truncated for %s %s (%s -> %s pages)",
                        source,
                        domain,
                        page_count,
                        max_pages,
                    )

                for page in range(max_pages):
                    params = self._wayback_base_params(domain, from_token, to_token)
                    params["page"] = str(page)
                    response = self.session.get(
                        "https://web.archive.org/cdx/search/cdx",
                        params=params,
                        timeout=self.config.REQUEST_TIMEOUT,
                    )
                    response.raise_for_status()

                    payload = response.json()
                    rows = self._wayback_rows(payload)
                    if not rows:
                        continue

                    raw_ref = self.storage.save_raw_response(
                        payload,
                        source,
                        f"wayback_{domain.replace('.', '_')}_{from_token}_{to_token}_p{page}",
                    )

                    for url, published in rows:
                        if not self._in_range(published, from_date, to_date):
                            continue

                        parsed = urlparse(url)
                        title = parsed.path.rstrip("/").split("/")[-1].replace("-", " ").strip() or "untitled"

                        stubs.append(
                            ArticleStub(
                                source=source,
                                url=url,
                                title=title,
                                published_date=published.isoformat(),
                                discovery_source="wayback_cdx",
                                raw_payload_ref=raw_ref,
                            )
                        )

            cursor = chunk_end + timedelta(days=1)

        return stubs

    def _wayback_base_params(self, domain: str, from_token: str, to_token: str) -> dict:
        return {
            "url": f"{domain}/*",
            "matchType": "domain",
            "from": from_token,
            "to": to_token,
            "output": "json",
            "fl": "original,timestamp",
            "filter": ["statuscode:200", "mimetype:text/html"],
            "collapse": "urlkey",
            "pageSize": "1",
        }

    def _wayback_num_pages(self, domain: str, from_token: str, to_token: str) -> int:
        params = self._wayback_base_params(domain, from_token, to_token)
        params["showNumPages"] = "true"

        response = self.session.get(
            "https://web.archive.org/cdx/search/cdx",
            params=params,
            timeout=self.config.REQUEST_TIMEOUT,
        )
        response.raise_for_status()

        text = response.text.strip()
        if not text:
            return 0

        try:
            return max(0, int(text))
        except ValueError:
            try:
                payload = response.json()
            except ValueError:
                return 0

            if isinstance(payload, dict):
                for key in ("pages", "num_pages", "page_count"):
                    if key in payload:
                        try:
                            return max(0, int(payload[key]))
                        except (TypeError, ValueError):
                            return 0
            return 0

    def _wayback_rows(self, payload: object) -> list[tuple[str, datetime]]:
        if not isinstance(payload, list) or len(payload) <= 1:
            return []

        header = payload[0]
        if not isinstance(header, list):
            return []

        try:
            original_idx = header.index("original")
            timestamp_idx = header.index("timestamp")
        except ValueError:
            return []

        rows: list[tuple[str, datetime]] = []
        for row in payload[1:]:
            if not isinstance(row, list):
                continue
            if len(row) <= max(original_idx, timestamp_idx):
                continue

            url = row[original_idx]
            timestamp = row[timestamp_idx]
            published = self._parse_dt(timestamp)
            if not url or not published:
                continue
            rows.append((url, published))

        return rows

    def _discover_from_nyt_archive(self, from_date: datetime, to_date: datetime) -> list[ArticleStub]:
        stubs: list[ArticleStub] = []
        cursor = datetime(from_date.year, from_date.month, 1)

        while cursor <= to_date:
            url = f"https://api.nytimes.com/svc/archive/v1/{cursor.year}/{cursor.month}.json"
            params = {"api-key": self.config.NY_API_KEY}
            response = self.session.get(url, params=params, timeout=self.config.REQUEST_TIMEOUT)
            response.raise_for_status()
            payload = response.json()
            raw_ref = self.storage.save_raw_response(payload, "NYT", f"nyt_archive_{cursor:%Y_%m}")

            docs = payload.get("response", {}).get("docs", [])
            for doc in docs:
                article_url = doc.get("web_url", "")
                title = (doc.get("headline") or {}).get("main", "")
                published = self._parse_dt(doc.get("pub_date", ""))
                if not article_url or not title or not published:
                    continue
                if not self._in_range(published, from_date, to_date):
                    continue

                byline = doc.get("byline") or {}
                person = byline.get("person", [])
                author = ", ".join(
                    [
                        " ".join([p.get("firstname", ""), p.get("lastname", "")]).strip()
                        for p in person
                        if p.get("firstname") or p.get("lastname")
                    ]
                )

                stubs.append(
                    ArticleStub(
                        source="NYT",
                        url=article_url,
                        title=title,
                        published_date=published.isoformat(),
                        discovery_source="nyt_archive",
                        raw_payload_ref=raw_ref,
                        description=doc.get("abstract") or doc.get("snippet", ""),
                        author=author,
                        section=doc.get("section_name") or doc.get("news_desk", ""),
                        article_type=doc.get("type_of_material") or "news",
                        tags=", ".join([kw.get("value", "") for kw in doc.get("keywords", []) if kw.get("value")]),
                    )
                )

            if cursor.month == 12:
                cursor = datetime(cursor.year + 1, 1, 1)
            else:
                cursor = datetime(cursor.year, cursor.month + 1, 1)

        return stubs

    def _discover_from_rss(
        self,
        source: str,
        feed_url: str,
        from_date: datetime,
        to_date: datetime,
    ) -> list[ArticleStub]:
        response = self.session.get(feed_url, timeout=self.config.REQUEST_TIMEOUT)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "xml")
        raw_ref = self.storage.save_raw_response(
            {"feed_url": feed_url, "content": response.text[:100000]},
            source,
            f"rss_{self._safe_name(feed_url)}",
        )

        stubs: list[ArticleStub] = []
        for item in soup.find_all("item"):
            title_node = item.find("title")
            link_node = item.find("link")
            date_node = item.find("pubDate") or item.find("published") or item.find("updated")

            title = title_node.text.strip() if title_node and title_node.text else ""
            url = link_node.text.strip() if link_node and link_node.text else ""
            published = self._parse_dt(date_node.text.strip() if date_node and date_node.text else "")

            if not title or not url:
                continue
            if not published:
                continue
            if not self._in_range(published, from_date, to_date):
                continue

            description_node = item.find("description")
            stubs.append(
                ArticleStub(
                    source=source,
                    url=url,
                    title=title,
                    published_date=published.isoformat() if published else "",
                    discovery_source="rss",
                    raw_payload_ref=raw_ref,
                    description=description_node.text.strip() if description_node and description_node.text else "",
                )
            )

        return stubs

    def _discover_from_sitemap(
        self,
        source: str,
        sitemap_url: str,
        from_date: datetime,
        to_date: datetime,
        visited: set[str] | None = None,
        depth: int = 0,
    ) -> list[ArticleStub]:
        visited = visited or set()
        if sitemap_url in visited:
            return []
        visited.add(sitemap_url)

        sitemap_xml = self._fetch_xml(sitemap_url)
        soup = BeautifulSoup(sitemap_xml, "xml")

        stubs: list[ArticleStub] = []
        if soup.find("sitemapindex"):
            nested_nodes = soup.find_all("sitemap")
            nested: list[str] = []
            skipped_by_date = 0
            skipped_missing_date_signal = 0
            for node in nested_nodes:
                loc_node = node.find("loc")
                if not loc_node or not loc_node.text:
                    continue
                nested_url = loc_node.text.strip()
                lastmod_node = node.find("lastmod")
                lastmod = self._parse_dt(lastmod_node.text.strip() if lastmod_node and lastmod_node.text else "")
                hinted_date = self._guess_date_from_sitemap_url(nested_url)
                if (
                    depth == 0
                    and len(nested_nodes) >= _SITEMAP_REQUIRE_DATE_SIGNAL_THRESHOLD
                    and lastmod is None
                    and hinted_date is None
                ):
                    skipped_missing_date_signal += 1
                    continue
                if not self._sitemap_index_entry_in_range(nested_url, lastmod, from_date, to_date):
                    skipped_by_date += 1
                    continue
                nested.append(nested_url)

            logger.info(
                "[%s] sitemapindex depth=%s url=%s nested=%s eligible=%s skipped_by_date=%s skipped_missing_date_signal=%s",
                source,
                depth,
                sitemap_url,
                len(nested_nodes),
                len(nested),
                skipped_by_date,
                skipped_missing_date_signal,
            )

            nested_errors = 0
            first_nested_error = ""
            for idx, nested_url in enumerate(nested, start=1):
                if idx == 1 or idx % 25 == 0 or idx == len(nested):
                    logger.info(
                        "[%s] sitemapindex depth=%s progress: %s/%s",
                        source,
                        depth,
                        idx,
                        len(nested),
                    )
                fetch_candidates = self._sitemap_fetch_candidates(source, nested_url)
                nested_ok = False
                last_error = ""

                for fetch_url in fetch_candidates:
                    try:
                        stubs.extend(
                            self._discover_from_sitemap(
                                source,
                                fetch_url,
                                from_date,
                                to_date,
                                visited=visited,
                                depth=depth + 1,
                            )
                        )
                        nested_ok = True
                        break
                    except requests.HTTPError as exc:
                        status = exc.response.status_code if exc.response is not None else "unknown"
                        last_error = f"HTTP {status}"
                    except (requests.Timeout, requests.ConnectionError) as exc:
                        last_error = f"{type(exc).__name__}: {exc}"
                    except Exception as exc:
                        last_error = f"{type(exc).__name__}: {exc}"

                if not nested_ok:
                    nested_errors += 1
                    if not first_nested_error:
                        first_nested_error = last_error or f"failed to fetch nested sitemap: {nested_url}"
                    logger.error(
                        "[%s] sitemap nested failure at depth=%s (%s): %s",
                        source,
                        depth + 1,
                        nested_url,
                        last_error or "unknown error",
                    )

            if nested_errors:
                logger.warning(
                    "[%s] sitemapindex depth=%s url=%s completed with nested_errors=%s",
                    source,
                    depth,
                    sitemap_url,
                    nested_errors,
                )

            if nested and not stubs and nested_errors >= len(nested):
                detail = first_nested_error or "all nested sitemap URLs failed"
                raise RuntimeError(
                    f"All eligible nested sitemap URLs failed for {sitemap_url}. "
                    f"first_error={detail}"
                )
            return stubs

        raw_ref = self.storage.save_raw_response(
            {"sitemap_url": sitemap_url, "content": sitemap_xml[:100000]},
            source,
            f"sitemap_{self._safe_name(sitemap_url)}",
        )

        for node in soup.find_all("url"):
            loc_node = node.find("loc")
            if not loc_node or not loc_node.text:
                continue

            url = loc_node.text.strip()
            published = self._extract_sitemap_published_dt(node, url)
            if not published:
                continue
            if not self._in_range(published, from_date, to_date):
                continue

            parsed = urlparse(url)
            if not parsed.scheme or not parsed.netloc:
                continue

            title = self._extract_sitemap_title(node, parsed.path)

            stubs.append(
                ArticleStub(
                    source=source,
                    url=url,
                    title=title,
                    published_date=published.isoformat() if published else "",
                    discovery_source="sitemap",
                    raw_payload_ref=raw_ref,
                )
            )

        return stubs

    def _extract_sitemap_published_dt(self, node: BeautifulSoup, entry_url: str) -> datetime | None:
        direct_tags = ["lastmod", "publication_date", "pubdate", "updated", "published", "date"]
        for tag in direct_tags:
            found = node.find(tag)
            if found and found.text:
                parsed = self._parse_dt(found.text.strip())
                if parsed:
                    return parsed

        for child in node.find_all():
            name = (getattr(child, "name", "") or "").lower().split(":")[-1]
            if name not in {"lastmod", "publication_date", "pubdate", "updated", "published", "date"}:
                continue
            value = (child.text or "").strip()
            if not value:
                continue
            parsed = self._parse_dt(value)
            if parsed:
                return parsed

        return self._guess_date_from_sitemap_url(entry_url)

    @staticmethod
    def _extract_sitemap_title(node: BeautifulSoup, path: str) -> str:
        direct_tags = ["title", "news:title"]
        for tag in direct_tags:
            found = node.find(tag)
            if found and found.text:
                text = found.text.strip()
                if text:
                    return text
        return path.rstrip("/").split("/")[-1].replace("-", " ").strip() or "untitled"

    def _fetch_xml(self, url: str) -> str:
        connect_timeout = min(8, max(1, int(self.config.REQUEST_TIMEOUT)))
        read_timeout = max(connect_timeout, int(self.config.REQUEST_TIMEOUT))
        response = self.session.get(url, timeout=(connect_timeout, read_timeout))
        response.raise_for_status()
        data = response.content

        if data[:2] == b"\x1f\x8b":
            data = gzip.decompress(data)

        return data.decode("utf-8", errors="ignore")

    def _sitemap_fetch_candidates(self, source: str, url: str) -> list[str]:
        candidates: list[str] = []
        for candidate in [self._normalize_sitemap_url_for_fetch(source, url), url]:
            if candidate not in candidates:
                candidates.append(candidate)
        return candidates

    @staticmethod
    def _normalize_sitemap_url_for_fetch(source: str, url: str) -> str:
        if source != "CNN":
            return url
        try:
            parsed = urlparse(url)
        except Exception:
            return url
        host = (parsed.netloc or "").lower()
        if host != "cms.cnn.com":
            return url
        return urlunparse(parsed._replace(netloc="www.cnn.com"))

    @staticmethod
    def _host_from_url(url: str) -> str:
        try:
            return (urlparse(url).netloc or "").lower()
        except Exception:
            return ""

    def _sitemap_index_entry_in_range(
        self,
        sitemap_url: str,
        lastmod: datetime | None,
        from_date: datetime,
        to_date: datetime,
    ) -> bool:
        start = self._to_utc_naive(from_date)
        end = self._to_utc_naive(to_date)

        if lastmod is not None:
            lower = start - timedelta(days=35)
            upper = end + timedelta(days=35)
            value = self._to_utc_naive(lastmod)
            return lower <= value <= upper

        hinted_date = self._guess_date_from_sitemap_url(sitemap_url)
        if hinted_date is not None:
            lower = start - timedelta(days=7)
            upper = end + timedelta(days=7)
            value = self._to_utc_naive(hinted_date)
            return lower <= value <= upper

        return True

    @staticmethod
    def _guess_date_from_sitemap_url(sitemap_url: str) -> datetime | None:
        text = sitemap_url.lower()
        for pattern in _SITEMAP_HINT_PATTERNS:
            match = pattern.search(text)
            if not match:
                continue

            groups = match.groups()
            try:
                if len(groups) == 3:
                    year = int(groups[0])
                    month = int(groups[1])
                    day = int(groups[2])
                    return datetime(year, month, day)
                if len(groups) == 2:
                    year = int(groups[0])
                    month = int(groups[1])
                    return datetime(year, month, 15)
            except ValueError:
                continue
        return None

    def _stub_passes_quality(
        self,
        stub: ArticleStub,
        from_date: datetime,
        to_date: datetime,
        allowed_domains: list[str] | None = None,
    ) -> bool:
        if not stub.url or not stub.title:
            return False
        if not is_probable_article_url(stub.source, stub.url):
            return False
        if allowed_domains and not self._url_matches_source_domain(stub.url, allowed_domains):
            return False

        published = self._parse_dt(stub.published_date or "")
        if not published:
            return False
        if not self._in_range(published, from_date, to_date):
            return False

        stub.published_date = published.isoformat()
        return True

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
            d = domain.lower().strip()
            if not d:
                continue
            if host == d or host.endswith(f".{d}"):
                return True
        return False

    def _dedupe_stubs(self, stubs: list[ArticleStub]) -> list[ArticleStub]:
        seen: dict[str, ArticleStub] = {}
        for stub in stubs:
            canonical = canonicalize_url(stub.url)
            if not canonical:
                continue
            existing = seen.get(canonical)
            if not existing:
                seen[canonical] = stub
            elif self._is_stub_preferred(stub, existing):
                seen[canonical] = stub
        return list(seen.values())

    @staticmethod
    def _is_stub_preferred(candidate: ArticleStub, current: ArticleStub) -> bool:
        preferred_order = {
            "nyt_archive": 5,
            "wayback_cdx": 3,
            "rss": 1,
            "sitemap": 0,
        }
        return preferred_order.get(candidate.discovery_source, 0) > preferred_order.get(current.discovery_source, 0)

    @staticmethod
    def _safe_name(value: str) -> str:
        return (
            value.replace("https://", "")
            .replace("http://", "")
            .replace("/", "_")
            .replace("?", "_")
            .replace("&", "_")
            .replace("=", "_")
        )

    @staticmethod
    def _in_range(value: datetime, from_date: datetime, to_date: datetime) -> bool:
        v = URLDiscovery._to_utc_naive(value)
        start = URLDiscovery._to_utc_naive(from_date)
        end = URLDiscovery._to_utc_naive(to_date)
        return start <= v <= end

    @staticmethod
    def _to_utc_naive(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value
        return value.astimezone(timezone.utc).replace(tzinfo=None)

    @staticmethod
    def _parse_dt(value: str) -> datetime | None:
        if not value:
            return None

        raw = value.strip()
        if not raw:
            return None

        for fmt in ("%Y%m%d%H%M%S", "%Y-%m-%d", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(raw, fmt)
            except ValueError:
                continue

        try:
            parsed_iso = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            parsed_iso = None
        if parsed_iso is not None:
            return parsed_iso

        try:
            return parsedate_to_datetime(raw)
        except Exception:
            return None
