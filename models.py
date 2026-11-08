"""data models for discovery, enrichment, and analysis exports."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List


@dataclass
class ArticleStub:
    """article record discovered from APIs/RSS/sitemaps."""

    source: str
    url: str
    title: str
    published_date: str
    discovery_source: str
    raw_payload_ref: str
    description: Optional[str] = None
    author: Optional[str] = None
    section: Optional[str] = None
    article_type: str = "news"
    tags: Optional[str] = None
    collected_at: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> dict:
        return {
            "source": self.source,
            "url": self.url,
            "title": self.title,
            "published_date": self.published_date,
            "discovery_source": self.discovery_source,
            "raw_payload_ref": self.raw_payload_ref,
            "description": self.description or "",
            "author": self.author or "",
            "section": self.section or "",
            "article_type": self.article_type,
            "tags": self.tags or "",
            "collected_at": self.collected_at,
        }


@dataclass
class Article:
    """Analysis-ready full text article"""

    article_id: str
    source: str
    title: str
    url: str
    canonical_url: str
    content: str
    published_date: str
    discovery_source: str
    content_source: str
    raw_payload_ref: str
    is_full_text: bool
    quality_score: float
    description: Optional[str] = None
    author: Optional[str] = None
    updated_date: Optional[str] = None
    section: Optional[str] = None
    article_type: str = "news"
    tags: Optional[str] = None
    word_count: int = 0
    api_source: str = "unknown"
    leaning: Optional[str] = None
    language: str = "en"
    drop_reason: Optional[str] = None
    collected_at: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> dict:
        return {
            "article_id": self.article_id,
            "source": self.source,
            "title": self.title,
            "url": self.url,
            "canonical_url": self.canonical_url,
            "content": self.content,
            "description": self.description or "",
            "author": self.author or "",
            "published_date": self.published_date,
            "updated_date": self.updated_date or "",
            "section": self.section or "",
            "article_type": self.article_type,
            "tags": self.tags or "",
            "word_count": int(self.word_count),
            "api_source": self.api_source,
            "leaning": self.leaning or "",
            "language": self.language,
            "discovery_source": self.discovery_source,
            "content_source": self.content_source,
            "is_full_text": bool(self.is_full_text),
            "quality_score": float(self.quality_score),
            "drop_reason": self.drop_reason or "",
            "raw_payload_ref": self.raw_payload_ref,
            "collected_at": self.collected_at,
        }


@dataclass
class RejectedArticle:
    """Article stub that failed quality/access checks."""

    source: str
    url: str
    title: str
    published_date: str
    discovery_source: str
    raw_payload_ref: str
    drop_reason: str
    content_preview: str = ""
    collected_at: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> dict:
        return {
            "source": self.source,
            "url": self.url,
            "title": self.title,
            "published_date": self.published_date,
            "discovery_source": self.discovery_source,
            "raw_payload_ref": self.raw_payload_ref,
            "drop_reason": self.drop_reason,
            "content_preview": self.content_preview,
            "collected_at": self.collected_at,
        }


@dataclass
class Video:
    """Standardized video data structure."""

    video_id: str
    channel: str
    title: str
    url: str
    published_date: str
    transcript: str
    description: Optional[str] = None
    duration: Optional[str] = None
    duration_seconds: int = 0
    view_count: int = 0
    like_count: int = 0
    comment_count: int = 0
    thumbnail: Optional[str] = None
    transcript_length: int = 0
    has_transcript: bool = False
    channel_id: Optional[str] = None
    collected_at: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> dict:
        return {
            "video_id": self.video_id,
            "channel": self.channel,
            "channel_id": self.channel_id or "",
            "title": self.title,
            "url": self.url,
            "description": self.description or "",
            "published_date": self.published_date,
            "duration": self.duration or "",
            "duration_seconds": int(self.duration_seconds),
            "view_count": int(self.view_count),
            "like_count": int(self.like_count),
            "comment_count": int(self.comment_count),
            "thumbnail": self.thumbnail or "",
            "transcript": self.transcript,
            "transcript_length": int(self.transcript_length),
            "has_transcript": bool(self.has_transcript),
            "collected_at": self.collected_at,
        }


class SourceConfig:
    """Configuration for each news source."""

    SOURCES = {
        "CNN": {
            "name": "CNN",
            "leaning": "center-left",
            "domains": ["cnn.com", "edition.cnn.com"],
            "youtube_channel": "UCupvZG-5ko_eiXAupbDfxWw",
            "rss_feeds": [
                "http://rss.cnn.com/rss/cnn_allpolitics.rss",
                "http://rss.cnn.com/rss/cnn_topstories.rss",
                "http://rss.cnn.com/rss/cnn_world.rss",
            ],
            "sitemaps": [
                "https://www.cnn.com/sitemaps/cnn/news.xml",
                "https://www.cnn.com/sitemap/news.xml",
                "https://edition.cnn.com/sitemaps/news.xml",
                "https://www.cnn.com/sitemap/article.xml",
            ],
        },
        "Fox News": {
            "name": "Fox News",
            "leaning": "right",
            "domains": ["foxnews.com", "foxbusiness.com"],
            "youtube_channel": "UCXIJgqnII2ZOINSWNOGFThA",
            "rss_feeds": [
                "https://moxie.foxnews.com/google-publisher/latest.xml",
                "https://moxie.foxnews.com/google-publisher/politics.xml",
            ],
            "sitemaps": [
                "https://www.foxnews.com/sitemap.xml?type=news",
                "https://www.foxnews.com/sitemap.xml?type=articles",
            ],
        },
        "BBC": {
            "name": "BBC",
            "leaning": "center",
            "domains": ["bbc.com", "bbc.co.uk"],
            "youtube_channel": "UC16niRr50-MSBwiOjYb3Mg",
            "rss_feeds": [
                "https://feeds.bbci.co.uk/news/rss.xml",
                "https://feeds.bbci.co.uk/news/world/rss.xml",
                "https://feeds.bbci.co.uk/news/politics/rss.xml",
            ],
            "sitemaps": [
                "https://www.bbc.com/sitemaps/https-index-com-news.xml",
                "https://www.bbc.com/sitemaps/https-index-com-archive.xml",
                "https://www.bbc.co.uk/sitemaps/https-index-uk-news.xml",
                "https://www.bbc.co.uk/sitemaps/https-index-uk-archive.xml",
            ],
        },
        "NYT": {
            "name": "NYT",
            "leaning": "center-left",
            "domains": ["nytimes.com"],
            "youtube_channel": "UCUPfM4A4h0iKb5i3JStZMWA",
            "rss_feeds": [
                "https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml",
                "https://rss.nytimes.com/services/xml/rss/nyt/Politics.xml",
                "https://rss.nytimes.com/services/xml/rss/nyt/World.xml",
            ],
            "sitemaps": [
                "https://www.nytimes.com/sitemaps/new/news.xml.gz",
                "https://www.nytimes.com/sitemaps/new/sitemap.xml.gz",
            ],
        },
        "WSJ": {
            "name": "WSJ",
            "leaning": "center-right",
            "domains": ["wsj.com"],
            "youtube_channel": "UCK7tptUDHh-RYdSUH4CkFw",
            "rss_feeds": [
                "https://feeds.a.dj.com/rss/RSSWorldNews.xml",
                "https://feeds.a.dj.com/rss/WSJcomUSBusiness.xml",
            ],
            "sitemaps": [
                "https://www.wsj.com/live_news_sitemap.xml",
            ],
        },
    }

    @classmethod
    def get_source(cls, name: str) -> dict:
        return cls.SOURCES.get(name, {})

    @classmethod
    def get_all_sources(cls) -> List[str]:
        return list(cls.SOURCES.keys())

    @classmethod
    def get_sources_by_leaning(cls, leaning: str) -> List[str]:
        return [name for name, config in cls.SOURCES.items() if config["leaning"] == leaning]
