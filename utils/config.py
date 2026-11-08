import os
import logging
from datetime import datetime, timedelta
from typing import Optional, Iterable

from dotenv import load_dotenv

from models import SourceConfig


load_dotenv()
logger = logging.getLogger(__name__)


class ConfigError(Exception):
    """Raised when configuration is invalid."""


class Config:

    DEFAULT_START_DATE = "2025-08-13"
    DEFAULT_END_DATE = "2026-02-13"

    def __init__(self):
        self.NY_API_KEY: Optional[str] = os.getenv("NY_API_KEY")
        self.GOOGLE_CLOUD_KEY: Optional[str] = os.getenv("GOOGLE_CLOUD_KEY")

        self.REQUEST_TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT", "20"))

        self.MONTHS_OF_DATA: int = int(os.getenv("MONTHS_OF_DATA", "6"))
        self.MIN_ARTICLE_WORDS: int = int(os.getenv("MIN_ARTICLE_WORDS", "250"))
        self.OUTPUT_FORMATS = self._parse_output_formats(os.getenv("OUTPUT_FORMATS", "parquet,csv"))
        self.YOUTUBE_MAX_VIDEOS: int = int(os.getenv("YOUTUBE_MAX_VIDEOS", "10000"))

        self.USE_SITEMAPS: bool = self._env_bool("USE_SITEMAPS", True)
        self.USE_RSS: bool = self._env_bool("USE_RSS", True)
        self.USE_WAYBACK: bool = self._env_bool("USE_WAYBACK", False)
        self.USE_NYT_ARCHIVE: bool = self._env_bool("USE_NYT_ARCHIVE", True)

        self.skip_youtube: bool = self._env_bool("SKIP_YOUTUBE", False)
        self.strict: bool = self._env_bool("STRICT_MODE", True)

        self._explicit_start = os.getenv("START_DATE")
        self._explicit_end = os.getenv("END_DATE")
        self._source_selection = os.getenv("SOURCES", "").strip()

    @staticmethod
    def _env_bool(key: str, default: bool) -> bool:
        raw = os.getenv(key)
        if raw is None:
            return default
        return raw.strip().lower() in {"1", "true", "yes", "y", "on"}

    @staticmethod
    def _parse_output_formats(raw: str) -> list[str]:
        formats = [entry.strip().lower() for entry in raw.split(",") if entry.strip()]
        normalized = []
        for fmt in formats:
            if fmt in {"csv", "parquet"} and fmt not in normalized:
                normalized.append(fmt)
        return normalized or ["csv"]

    @staticmethod
    def parse_date(date_str: str) -> datetime:
        return datetime.strptime(date_str, "%Y-%m-%d")

    def get_date_range(self) -> tuple[datetime, datetime]:
        """Calculate date range from explicit dates or month window."""
        if self._explicit_start and self._explicit_end:
            start = self.parse_date(self._explicit_start)
            end = self.parse_date(self._explicit_end)
        elif self._explicit_start and not self._explicit_end:
            start = self.parse_date(self._explicit_start)
            end = start + timedelta(days=self.MONTHS_OF_DATA * 30)
        elif self._explicit_end and not self._explicit_start:
            end = self.parse_date(self._explicit_end)
            start = end - timedelta(days=self.MONTHS_OF_DATA * 30)
        elif self.MONTHS_OF_DATA == 6:
            start = self.parse_date(self.DEFAULT_START_DATE)
            end = self.parse_date(self.DEFAULT_END_DATE)
        else:
            end = datetime.now()
            start = end - timedelta(days=self.MONTHS_OF_DATA * 30)

        if start > end:
            raise ConfigError("START_DATE cannot be after END_DATE")

        return start, end

    def get_selected_sources(self) -> list[str]:
        """Get selected sources from SOURCES env or default to all."""
        all_sources = SourceConfig.get_all_sources()
        if not self._source_selection:
            return all_sources

        normalized_map = {name.lower(): name for name in all_sources}
        selected: list[str] = []

        for token in self._source_selection.split(","):
            key = token.strip().lower()
            if not key:
                continue
            if key not in normalized_map:
                raise ConfigError(
                    f"Unknown source '{token}'. Valid: {', '.join(all_sources)}"
                )
            selected.append(normalized_map[key])

        if not selected:
            raise ConfigError("SOURCES was set but no valid source names were provided")

        return selected

    def validate(self, selected_sources: Optional[Iterable[str]] = None) -> bool:
        sources = set(selected_sources or SourceConfig.get_all_sources())

        if not self.skip_youtube and not self.GOOGLE_CLOUD_KEY:
            raise ConfigError(
                "Missing required API key: GOOGLE_CLOUD_KEY. "
                "Add it to .env or set SKIP_YOUTUBE=true."
            )

        if not self.USE_WAYBACK:
            logger.info("USE_WAYBACK=false; skipping Wayback discovery by default")

        if self.USE_NYT_ARCHIVE:
            if "NYT" in sources and not self.NY_API_KEY:
                raise ConfigError(
                    "NY_API_KEY is required when NYT is selected and USE_NYT_ARCHIVE=true."
                )
        elif "NYT" in sources:
            logger.warning("USE_NYT_ARCHIVE=false; NYT coverage may be significantly lower")

        has_non_nyt_provider = self.USE_SITEMAPS or self.USE_RSS or self.USE_WAYBACK
        if not has_non_nyt_provider:
            only_nyt_provider = self.USE_NYT_ARCHIVE and "NYT" in sources
            if not only_nyt_provider:
                raise ConfigError(
                    "No discovery providers are enabled. Enable at least one of "
                    "USE_SITEMAPS/USE_RSS/USE_WAYBACK (and optionally USE_NYT_ARCHIVE for NYT)."
                )

        return True

    def get_api_key(self, name: str) -> str:
        key_map = {
            "nyt": self.NY_API_KEY,
            "google": self.GOOGLE_CLOUD_KEY,
        }
        key = key_map.get(name.lower())
        if not key:
            raise ConfigError(f"API key '{name}' not found")
        return key


SOURCES = SourceConfig.SOURCES
