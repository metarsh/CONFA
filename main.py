#!/usr/bin/env python3
"""News dataset pipeline: discover URLs, enrich full text, validate, resume, and export."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from typing import Iterable, List

from models import Article, ArticleStub, RejectedArticle, Video
from scrapers.fulltext_enricher import FullTextEnricher
from scrapers.url_discovery import URLDiscovery
from scrapers.youtube_scraper import YouTubeScraper
from utils.config import Config, ConfigError, SOURCES
from utils.progress import progress
from utils.storage import DataStorage
from utils.url_utils import canonicalize_url, contains_truncation_artifact


LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)

RETRYABLE_REJECTION_REASONS = {"fetch_failed", "extraction_failed"}


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the news pipeline",
    )
    return parser.parse_args(argv)


class NewsDatasetPipeline:
    def __init__(self, config: Config, sources: Iterable[str], storage: DataStorage):
        self.config = config
        self.sources = list(sources)
        self.start_date, self.end_date = self.config.get_date_range()
        self.storage = storage

        self.articles: List[Article] = []
        self.videos: List[Video] = []
        self.rejected_video_rows: List[dict] = []
        self.rejected_articles: List[RejectedArticle] = []

        self.discovery_stats: dict = {}
        self.enrichment_stats: dict = {}

        self._processed_article_urls: set[str] = set()
        self._processed_video_ids: set[str] = set()

    def run(self) -> int:
        logger.info("Running dataset pipeline for sources: %s", ", ".join(self.sources))
        logger.info("Date range: %s to %s", self.start_date.date(), self.end_date.date())
        logger.info("Run directory: %s", self.storage.run_dir)

        self._load_existing_progress()
        self._save_state("running", stage="init")

        try:
            stubs = self._discover_with_resume()
            logger.info("Discovery checkpoint has %s unique stubs", len(stubs))
            zero_discovery_sources: list[str] = []
            for source in self.sources:
                discovered = int(self.discovery_stats.get(source, {}).get("discovered", 0))
                if discovered == 0:
                    logger.warning("Discovery returned 0 stubs for %s", source)
                    provider_summary = self.discovery_stats.get(source, {}).get("providers", {})
                    if provider_summary:
                        logger.error("Provider summary for %s: %s", source, provider_summary)
                    zero_discovery_sources.append(source)

            if not stubs:
                logger.error(
                    "Discovery produced 0 stubs across all sources. "
                    "Stopping early so the run does not continue with empty data."
                )
                self._save_state("failed", stage="discovery_empty", stage_state={"discovery": "failed"})
                return 1

            if zero_discovery_sources and self.config.strict:
                logger.error(
                    "Strict mode: discovery returned 0 stubs for required sources: %s",
                    ", ".join(zero_discovery_sources),
                )
                self._save_state(
                    "failed",
                    stage="discovery_missing_sources",
                    stage_state={"discovery": "failed"},
                    progress={"missing_sources": zero_discovery_sources},
                )
                return 1

            self._enrich_with_resume(stubs)
            logger.info(
                "Accepted %s full-text articles; rejected %s",
                len(self.articles),
                len(self.rejected_articles),
            )

            if not self.config.skip_youtube:
                self._collect_videos()

            hard_failures, warnings = self._validate_outputs()
            quality_report = self._build_quality_report(hard_failures, warnings)

            article_paths = self.storage.save_articles(self.articles, "articles")
            rejected_paths = self.storage.save_rejected(self.rejected_articles, "rejected_articles")
            video_paths = self.storage.save_videos(self.videos, "videos")
            rejected_video_paths = self.storage.save_dict_rows(self.rejected_video_rows, "rejected_videos", "final")
            quality_path = self.storage.save_quality_report(quality_report)

            manifest = {
                "run_id": self.storage.run_id,
                "signature": self._run_signature(),
                "sources": self.sources,
                "start_date": self.start_date.isoformat(),
                "end_date": self.end_date.isoformat(),
                "strict_mode": self.config.strict,
                "output_formats": self.config.OUTPUT_FORMATS,
                "files": {
                    "articles": article_paths,
                    "rejected_articles": rejected_paths,
                    "videos": video_paths,
                    "rejected_videos": rejected_video_paths,
                    "quality_report": quality_path,
                },
                "discovery_stats": self.discovery_stats,
                "enrichment_stats": self.enrichment_stats,
                "hard_failures": hard_failures,
                "warnings": warnings,
                "rejected_videos_without_transcripts": len(self.rejected_video_rows),
            }
            self.storage.save_run_manifest(manifest)
            self._save_state("completed", stage="done")

            logger.info("Final articles exported: %s", article_paths)
            logger.info("Final videos exported: %s", video_paths)
            logger.info("Quality report: %s", quality_path)

            if hard_failures and self.config.strict:
                logger.error("Hard validation failures in strict mode: %s", hard_failures)
                return 1

            if hard_failures:
                logger.warning("Hard validation failures (non-strict mode): %s", hard_failures)

            return 0

        except KeyboardInterrupt:
            logger.warning("Interrupted by user; saving progress checkpoint")
            self.storage.save_articles(self.articles, "articles")
            self.storage.save_rejected(self.rejected_articles, "rejected_articles")
            self.storage.save_videos(self.videos, "videos")
            self.storage.save_dict_rows(self.rejected_video_rows, "rejected_videos", "final")
            self._save_state("interrupted", stage="checkpoint_saved")
            return 130

    def _discover_with_resume(self) -> list[ArticleStub]:
        existing_stub_rows = self.storage.load_table_dicts("article_stubs", "discovery")
        existing_stubs = [self._stub_from_row(row) for row in existing_stub_rows]
        discovery = URLDiscovery(self.config, self.storage)
        self._save_state("running", stage="discovery_running", stage_state={"discovery": "running"})

        def _on_discovery_progress(event: dict) -> None:
            source = event.get("source", "unknown")
            provider = event.get("provider", "unknown")
            source_token = str(source).replace(" ", "_").lower()
            provider_token = str(provider).replace(" ", "_").lower()
            self._save_state(
                "running",
                stage=f"discovery_{source_token}_{provider_token}",
                stage_state={"discovery": "running"},
                progress={
                    "source": source,
                    "source_index": event.get("source_index"),
                    "total_sources": event.get("total_sources"),
                    "provider": provider,
                    "source_discovered": event.get("source_discovered", 0),
                    "deduped_cumulative": event.get("deduped_cumulative", 0),
                    "source_elapsed_seconds": event.get("source_elapsed_seconds"),
                },
            )

        stubs, self.discovery_stats = discovery.discover(
            self.sources,
            self.start_date,
            self.end_date,
            existing_stubs=existing_stubs,
            progress_callback=_on_discovery_progress,
        )
        self.discovery_stats["resumed_with_existing_stubs"] = bool(existing_stubs)
        self._save_state("running", stage="discovery_complete", stage_state={"discovery": "complete"})
        return stubs

    def _enrich_with_resume(self, stubs: list[ArticleStub]) -> None:
        enricher = FullTextEnricher(self.config)
        self._prune_rejected_for_current_stubs(stubs)

        pending = [
            stub
            for stub in stubs
            if canonicalize_url(stub.url) not in self._processed_article_urls
        ]

        logger.info("Pending stubs for enrichment: %s", len(pending))

        processed_in_stage = 0
        accepted_in_stage = 0
        rejected_in_stage = 0

        pending_by_source: dict[str, list[ArticleStub]] = {source: [] for source in self.sources}
        for stub in pending:
            pending_by_source.setdefault(stub.source, []).append(stub)

        for source in self.sources:
            source_pending = pending_by_source.get(source, [])
            if not source_pending:
                continue

            with progress(total=len(source_pending), desc=f"Enrich {source}", unit="article") as source_bar:
                for stub in source_pending:
                    article, rejected = enricher.process_stub(stub)
                    if article:
                        self.articles.append(article)
                        self._processed_article_urls.add(article.canonical_url)
                        accepted_in_stage += 1
                    if rejected:
                        self.rejected_articles.append(rejected)
                        self._processed_article_urls.add(canonicalize_url(rejected.url))
                        rejected_in_stage += 1

                    processed_in_stage += 1
                    source_bar.update(1)

                    if processed_in_stage % 25 == 0:
                        self.storage.save_articles(self.articles, "articles")
                        self.storage.save_rejected(self.rejected_articles, "rejected_articles")
                        self._save_state(
                            "running",
                            stage="enrichment_checkpoint",
                            stage_state={"enrichment": "running"},
                            progress={
                                "source": source,
                                "processed_stubs": processed_in_stage,
                                "accepted": accepted_in_stage,
                                "rejected": rejected_in_stage,
                                "pending_remaining": len(pending) - processed_in_stage,
                            },
                        )

        self.enrichment_stats = {
            "pending": len(pending),
            "processed": processed_in_stage,
            "accepted": accepted_in_stage,
            "rejected": rejected_in_stage,
            "accepted_total": len(self.articles),
            "rejected_total": len(self.rejected_articles),
        }
        rejected_reason_counts: dict[str, int] = {}
        for row in self.rejected_articles:
            rejected_reason_counts[row.drop_reason] = rejected_reason_counts.get(row.drop_reason, 0) + 1
        if rejected_reason_counts:
            ordered = sorted(rejected_reason_counts.items(), key=lambda item: item[1], reverse=True)
            self.enrichment_stats["rejected_reason_counts"] = dict(ordered)
            logger.info("Rejected article reason counts: %s", dict(ordered))

        self.storage.save_articles(self.articles, "articles")
        self.storage.save_rejected(self.rejected_articles, "rejected_articles")
        self._save_state("running", stage="enrichment_complete", stage_state={"enrichment": "complete"})

    def _collect_videos(self):
        logger.info("Collecting YouTube transcripts...")
        youtube = YouTubeScraper(self.config.get_api_key("google"))

        rejected_video_ids = {
            row.get("video_id", "") for row in self.rejected_video_rows if row.get("video_id")
        }

        for source in self.sources:
            channel_id = SOURCES[source].get("youtube_channel")
            if not channel_id:
                continue

            try:
                exclude_ids = self._processed_video_ids | rejected_video_ids
                accepted, rejected = youtube.scrape_channel_transcripts(
                    channel_id=channel_id,
                    channel_name=source,
                    from_date=self.start_date,
                    to_date=self.end_date,
                    exclude_video_ids=exclude_ids,
                    max_videos=self.config.YOUTUBE_MAX_VIDEOS,
                )

                for row in accepted:
                    self.videos.append(
                        Video(
                            video_id=row.get("video_id", ""),
                            channel=source,
                            title=row.get("title", ""),
                            url=row.get("url", ""),
                            published_date=row.get("published_date", ""),
                            transcript=row.get("transcript", ""),
                            description=row.get("description", ""),
                            duration=row.get("duration", ""),
                            duration_seconds=int(row.get("duration_seconds", 0) or 0),
                            view_count=int(row.get("view_count", 0) or 0),
                            like_count=int(row.get("like_count", 0) or 0),
                            comment_count=int(row.get("comment_count", 0) or 0),
                            thumbnail=row.get("thumbnail", ""),
                            transcript_length=int(row.get("transcript_length", 0) or 0),
                            has_transcript=bool(row.get("has_transcript", False)),
                            channel_id=channel_id,
                            collected_at=row.get("collected_at", datetime.now().isoformat()),
                        )
                    )
                    if row.get("video_id"):
                        self._processed_video_ids.add(row["video_id"])

                for row in rejected:
                    if row.get("video_id") and row["video_id"] not in rejected_video_ids:
                        self.rejected_video_rows.append(row)
                        rejected_video_ids.add(row["video_id"])

                self.storage.save_videos(self.videos, "videos")
                self.storage.save_dict_rows(self.rejected_video_rows, "rejected_videos", "final")
                self._save_state(
                    "running",
                    stage=f"youtube_{source}_complete",
                    stage_state={"youtube": "running"},
                    progress={"videos_with_transcript": len(self.videos)},
                )

            except Exception as exc:
                logger.warning("YouTube scraping failed for %s: %s", source, exc)

        self._save_state("running", stage="youtube_complete", stage_state={"youtube": "complete"})
        logger.info(
            "Collected %s videos with transcripts (%s videos rejected without transcripts)",
            len(self.videos),
            len(self.rejected_video_rows),
        )

    def _load_existing_progress(self) -> None:
        article_rows = self.storage.load_table_dicts("articles", "final")
        rejected_rows = self.storage.load_table_dicts("rejected_articles", "final")
        video_rows = self.storage.load_table_dicts("videos", "final")
        rejected_video_rows = self.storage.load_table_dicts("rejected_videos", "final")

        self.articles = [self._article_from_row(row) for row in article_rows]
        self.rejected_articles = [self._rejected_from_row(row) for row in rejected_rows]
        self.videos = [self._video_from_row(row) for row in video_rows]
        self.rejected_video_rows = rejected_video_rows

        self._rebuild_processed_article_urls()
        self._processed_video_ids = {video.video_id for video in self.videos if video.video_id}

        if self.articles or self.videos or self.rejected_articles:
            logger.info(
                "Loaded previous progress: %s articles, %s rejected articles, %s videos",
                len(self.articles),
                len(self.rejected_articles),
                len(self.videos),
            )

    def _rebuild_processed_article_urls(self) -> None:
        self._processed_article_urls = {
            canonicalize_url(article.canonical_url or article.url)
            for article in self.articles
            if article.url
        }
        self._processed_article_urls |= {
            canonicalize_url(row.url)
            for row in self.rejected_articles
            if row.url and row.drop_reason not in RETRYABLE_REJECTION_REASONS
        }

    def _prune_rejected_for_current_stubs(self, stubs: list[ArticleStub]) -> None:
        current_urls = {canonicalize_url(stub.url) for stub in stubs if stub.url}
        before = len(self.rejected_articles)
        if before == 0:
            return

        self.rejected_articles = [
            row for row in self.rejected_articles if canonicalize_url(row.url) in current_urls
        ]
        removed = before - len(self.rejected_articles)
        if removed > 0:
            logger.info("Pruned %s stale rejected rows from previous discovery state", removed)
            self.storage.save_rejected(self.rejected_articles, "rejected_articles")
            self._rebuild_processed_article_urls()

    def _run_signature(self) -> dict:
        return {
            "sources": sorted(self.sources),
            "start_date": self.start_date.date().isoformat(),
            "end_date": self.end_date.date().isoformat(),
            "skip_youtube": self.config.skip_youtube,
            "min_article_words": self.config.MIN_ARTICLE_WORDS,
            "youtube_max_videos": self.config.YOUTUBE_MAX_VIDEOS,
            "strict_mode": self.config.strict,
            "output_formats": sorted(self.config.OUTPUT_FORMATS),
            "use_wayback": self.config.USE_WAYBACK,
            "use_sitemaps": self.config.USE_SITEMAPS,
            "use_rss": self.config.USE_RSS,
            "use_nyt_archive": self.config.USE_NYT_ARCHIVE,
        }

    def _save_state(
        self,
        status: str,
        stage: str,
        stage_state: dict[str, str] | None = None,
        progress: dict | None = None,
    ) -> None:
        prev = self.storage.load_run_state()
        stages = prev.get("stages", {})
        if stage_state:
            stages.update(stage_state)

        state = {
            "run_id": self.storage.run_id,
            "status": status,
            "stage": stage,
            "signature": self._run_signature(),
            "stages": stages,
            "counts": {
                "articles": len(self.articles),
                "rejected_articles": len(self.rejected_articles),
                "videos": len(self.videos),
                "rejected_videos": len(self.rejected_video_rows),
            },
            "progress": progress or {},
            "updated_at": datetime.now().isoformat(),
        }
        self.storage.save_run_state(state)

    def _validate_outputs(self) -> tuple[list[str], list[str]]:
        hard_failures: list[str] = []
        warnings: list[str] = []

        required_columns = {
            "article_id",
            "source",
            "title",
            "url",
            "canonical_url",
            "content",
            "published_date",
            "discovery_source",
            "content_source",
            "is_full_text",
            "quality_score",
            "language",
        }

        if self.articles:
            article_cols = set(self.articles[0].to_dict().keys())
            missing_cols = sorted(required_columns - article_cols)
            if missing_cols:
                hard_failures.append(f"missing_required_columns: {missing_cols}")

        bad_truncation = [a.url for a in self.articles if contains_truncation_artifact(a.content)]
        if bad_truncation:
            hard_failures.append(f"accepted_articles_with_truncation_artifact: {len(bad_truncation)}")

        bad_bbc = [
            a.url
            for a in self.articles
            if a.source == "BBC" and ("/sounds/" in a.url.lower() or "/programmes/" in a.url.lower())
        ]
        if bad_bbc:
            hard_failures.append(f"accepted_bbc_non_article_urls: {len(bad_bbc)}")

        for source in self.sources:
            count = len([article for article in self.articles if article.source == source])
            if count < 20:
                warnings.append(f"low_article_volume_{source}: {count}")

        valid_dates = 0
        for article in self.articles:
            try:
                datetime.fromisoformat(article.published_date.replace("Z", "+00:00"))
                valid_dates += 1
            except ValueError:
                continue

        if self.articles:
            ratio = valid_dates / len(self.articles)
            if ratio < 0.95:
                warnings.append(f"low_parseable_published_date_ratio: {ratio:.2%}")

        return hard_failures, warnings

    def _build_quality_report(self, hard_failures: list[str], warnings: list[str]) -> dict:
        by_source = {}
        for source in self.sources:
            by_source[source] = {
                "accepted_articles": len([a for a in self.articles if a.source == source]),
                "rejected_articles": len([r for r in self.rejected_articles if r.source == source]),
                "videos_with_transcript": len([v for v in self.videos if v.channel == source]),
            }

        return {
            "run_id": self.storage.run_id,
            "date_range": {
                "start": self.start_date.isoformat(),
                "end": self.end_date.isoformat(),
            },
            "sources": self.sources,
            "summary": {
                "discovered_stubs": self.discovery_stats.get("_total", {}).get("deduped", 0),
                "accepted_articles": len(self.articles),
                "rejected_articles": len(self.rejected_articles),
                "accepted_videos": len(self.videos),
                "rejected_videos_without_transcript": len(self.rejected_video_rows),
            },
            "discovery": self.discovery_stats,
            "enrichment": self.enrichment_stats,
            "validation": {
                "hard_failures": hard_failures,
                "warnings": warnings,
            },
            "by_source": by_source,
        }

    @staticmethod
    def _to_bool(value: object) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return False
        return str(value).strip().lower() in {"1", "true", "yes", "y"}

    @staticmethod
    def _to_int(value: object) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _to_float(value: object) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    def _stub_from_row(self, row: dict) -> ArticleStub:
        return ArticleStub(
            source=row.get("source", ""),
            url=row.get("url", ""),
            title=row.get("title", ""),
            published_date=row.get("published_date", ""),
            discovery_source=row.get("discovery_source", ""),
            raw_payload_ref=row.get("raw_payload_ref", ""),
            description=row.get("description") or None,
            author=row.get("author") or None,
            section=row.get("section") or None,
            article_type=row.get("article_type") or "news",
            tags=row.get("tags") or None,
            collected_at=row.get("collected_at") or datetime.now().isoformat(),
        )

    def _article_from_row(self, row: dict) -> Article:
        return Article(
            article_id=row.get("article_id", ""),
            source=row.get("source", ""),
            title=row.get("title", ""),
            url=row.get("url", ""),
            canonical_url=row.get("canonical_url") or canonicalize_url(row.get("url", "")),
            content=row.get("content", ""),
            published_date=row.get("published_date", ""),
            discovery_source=row.get("discovery_source", ""),
            content_source=row.get("content_source", ""),
            raw_payload_ref=row.get("raw_payload_ref", ""),
            is_full_text=self._to_bool(row.get("is_full_text", False)),
            quality_score=self._to_float(row.get("quality_score", 0.0)),
            description=row.get("description") or None,
            author=row.get("author") or None,
            updated_date=row.get("updated_date") or None,
            section=row.get("section") or None,
            article_type=row.get("article_type") or "news",
            tags=row.get("tags") or None,
            word_count=self._to_int(row.get("word_count", 0)),
            api_source=row.get("api_source") or "unknown",
            leaning=row.get("leaning") or None,
            language=row.get("language") or "en",
            drop_reason=row.get("drop_reason") or None,
            collected_at=row.get("collected_at") or datetime.now().isoformat(),
        )

    def _rejected_from_row(self, row: dict) -> RejectedArticle:
        return RejectedArticle(
            source=row.get("source", ""),
            url=row.get("url", ""),
            title=row.get("title", ""),
            published_date=row.get("published_date", ""),
            discovery_source=row.get("discovery_source", ""),
            raw_payload_ref=row.get("raw_payload_ref", ""),
            drop_reason=row.get("drop_reason", ""),
            content_preview=row.get("content_preview", ""),
            collected_at=row.get("collected_at") or datetime.now().isoformat(),
        )

    def _video_from_row(self, row: dict) -> Video:
        return Video(
            video_id=row.get("video_id", ""),
            channel=row.get("channel", ""),
            title=row.get("title", ""),
            url=row.get("url", ""),
            published_date=row.get("published_date", ""),
            transcript=row.get("transcript", ""),
            description=row.get("description") or None,
            duration=row.get("duration") or None,
            duration_seconds=self._to_int(row.get("duration_seconds", 0)),
            view_count=self._to_int(row.get("view_count", 0)),
            like_count=self._to_int(row.get("like_count", 0)),
            comment_count=self._to_int(row.get("comment_count", 0)),
            thumbnail=row.get("thumbnail") or None,
            transcript_length=self._to_int(row.get("transcript_length", 0)),
            has_transcript=self._to_bool(row.get("has_transcript", False)),
            channel_id=row.get("channel_id") or None,
            collected_at=row.get("collected_at") or datetime.now().isoformat(),
        )


def _signature_matches(candidate: dict, expected: dict) -> bool:
    if not candidate:
        return False

    def _norm_bool(value: object) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return False
        return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}

    return (
        candidate.get("sources") == expected.get("sources")
        and candidate.get("start_date") == expected.get("start_date")
        and candidate.get("end_date") == expected.get("end_date")
        and _norm_bool(candidate.get("skip_youtube")) == _norm_bool(expected.get("skip_youtube"))
        and int(candidate.get("min_article_words", 0)) == int(expected.get("min_article_words", 0))
        and int(candidate.get("youtube_max_videos", 0)) == int(expected.get("youtube_max_videos", 0))
        and _norm_bool(candidate.get("strict_mode")) == _norm_bool(expected.get("strict_mode"))
        and sorted(candidate.get("output_formats", [])) == sorted(expected.get("output_formats", []))
        and _norm_bool(candidate.get("use_wayback")) == _norm_bool(expected.get("use_wayback"))
        and _norm_bool(candidate.get("use_sitemaps")) == _norm_bool(expected.get("use_sitemaps"))
        and _norm_bool(candidate.get("use_rss")) == _norm_bool(expected.get("use_rss"))
        and _norm_bool(candidate.get("use_nyt_archive")) == _norm_bool(expected.get("use_nyt_archive"))
    )


def _pick_resume_run_id(expected_signature: dict) -> str | None:
    for run_id in DataStorage.list_run_ids("data"):
        storage = DataStorage(data_root="data", run_id=run_id, output_formats=["csv"])
        state = storage.load_run_state()
        manifest = storage.load_run_manifest()

        signature = state.get("signature") or manifest.get("signature")
        if _signature_matches(signature, expected_signature):
            return run_id

    return None


def main(argv: list[str]) -> int:
    parse_args(argv)

    try:
        config = Config()
        selected_sources = config.get_selected_sources()
        config.validate(selected_sources)

        start_date, end_date = config.get_date_range()
        expected_signature = {
            "sources": sorted(selected_sources),
            "start_date": start_date.date().isoformat(),
            "end_date": end_date.date().isoformat(),
            "skip_youtube": config.skip_youtube,
            "min_article_words": config.MIN_ARTICLE_WORDS,
            "youtube_max_videos": config.YOUTUBE_MAX_VIDEOS,
            "strict_mode": config.strict,
            "output_formats": sorted(config.OUTPUT_FORMATS),
            "use_wayback": config.USE_WAYBACK,
            "use_sitemaps": config.USE_SITEMAPS,
            "use_rss": config.USE_RSS,
            "use_nyt_archive": config.USE_NYT_ARCHIVE,
        }

        run_id = _pick_resume_run_id(expected_signature)
        if run_id:
            logger.info("Using existing run_id for resume/append: %s", run_id)

        storage = DataStorage(run_id=run_id, output_formats=config.OUTPUT_FORMATS)
        pipeline = NewsDatasetPipeline(config, selected_sources, storage=storage)
        return pipeline.run()

    except ConfigError as exc:
        logger.error("Configuration error: %s", exc)
        return 2
    except KeyboardInterrupt:
        logger.error("Interrupted by user")
        return 130
    except Exception as exc:
        logger.exception("Unhandled pipeline error: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
