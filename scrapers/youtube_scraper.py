"""YouTube scraping for channel videos and transcripts."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Dict, List

from googleapiclient.discovery import build
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import NoTranscriptFound, TranscriptsDisabled

from utils.progress import progress


logger = logging.getLogger(__name__)


class YouTubeScraper:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.youtube = build("youtube", "v3", developerKey=api_key)

    def get_channel_videos(
        self,
        channel_id: str,
        channel_name: str,
        from_date: datetime,
        to_date: datetime,
        max_videos: int = 10000,
        exclude_video_ids: set[str] | None = None,
    ) -> List[Dict]:
        videos: List[Dict] = []
        excluded = exclude_video_ids or set()

        channel_response = self.youtube.channels().list(part="contentDetails", id=channel_id).execute()

        if not channel_response.get("items"):
            logger.warning("Could not find channel: %s (%s)", channel_name, channel_id)
            return videos

        uploads_playlist_id = channel_response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

        next_page_token = None
        stop_due_to_age = False

        while True:
            playlist_response = (
                self.youtube.playlistItems()
                .list(
                    part="snippet",
                    playlistId=uploads_playlist_id,
                    maxResults=50,
                    pageToken=next_page_token,
                )
                .execute()
            )

            for item in playlist_response.get("items", []):
                snippet = item.get("snippet", {})
                published_at = snippet.get("publishedAt", "")
                video_date = self._parse_yt_datetime(published_at)
                if not video_date:
                    continue

                if video_date < from_date:
                    stop_due_to_age = True
                    break

                if from_date <= video_date <= to_date:
                    video_id = snippet.get("resourceId", {}).get("videoId", "")
                    if not video_id:
                        continue
                    if video_id in excluded:
                        continue

                    videos.append(
                        {
                            "video_id": video_id,
                            "channel": channel_name,
                            "channel_id": channel_id,
                            "title": snippet.get("title", ""),
                            "description": snippet.get("description", ""),
                            "published_date": published_at,
                            "thumbnail": snippet.get("thumbnails", {}).get("high", {}).get("url", ""),
                            "url": f"https://youtube.com/watch?v={video_id}",
                        }
                    )

                    if len(videos) >= max_videos:
                        logger.warning(
                            "Reached max_videos cap (%s) for %s. Increase YOUTUBE_MAX_VIDEOS for fuller coverage.",
                            max_videos,
                            channel_name,
                        )
                        stop_due_to_age = True
                        break

            if stop_due_to_age:
                break

            next_page_token = playlist_response.get("nextPageToken")
            if not next_page_token:
                break

        return videos

    def get_video_details(self, video_ids: List[str]) -> Dict[str, Dict]:
        details: Dict[str, Dict] = {}

        for idx in range(0, len(video_ids), 50):
            batch = video_ids[idx : idx + 50]
            response = (
                self.youtube.videos().list(part="contentDetails,statistics", id=",".join(batch)).execute()
            )

            for item in response.get("items", []):
                video_id = item.get("id", "")
                content_details = item.get("contentDetails", {})
                statistics = item.get("statistics", {})
                iso_duration = content_details.get("duration", "")

                details[video_id] = {
                    "duration": iso_duration,
                    "duration_seconds": self._duration_to_seconds(iso_duration),
                    "view_count": self._to_int(statistics.get("viewCount", 0)),
                    "like_count": self._to_int(statistics.get("likeCount", 0)),
                    "comment_count": self._to_int(statistics.get("commentCount", 0)),
                }

        return details

    def get_transcript(self, video_id: str) -> str:
        try:
            transcript_list = YouTubeTranscriptApi.get_transcript(video_id)
            return " ".join([entry.get("text", "") for entry in transcript_list]).strip()
        except (TranscriptsDisabled, NoTranscriptFound):
            return ""
        except Exception as exc:
            logger.debug("Transcript fetch failed for %s: %s", video_id, exc)
            return ""

    def scrape_channel_transcripts(
        self,
        channel_id: str,
        channel_name: str,
        from_date: datetime,
        to_date: datetime,
        exclude_video_ids: set[str] | None = None,
        max_videos: int = 10000,
    ) -> tuple[List[Dict], List[Dict]]:
        """Return (videos_with_transcripts, rejected_without_transcripts)."""
        videos = self.get_channel_videos(
            channel_id,
            channel_name,
            from_date,
            to_date,
            max_videos=max_videos,
            exclude_video_ids=exclude_video_ids,
        )
        if not videos:
            return [], []

        details = self.get_video_details([video["video_id"] for video in videos])

        with_transcripts: List[Dict] = []
        rejected: List[Dict] = []

        with progress(total=len(videos), desc=f"YouTube {channel_name}", unit="video") as video_bar:
            for idx, video in enumerate(videos, start=1):
                video_id = video["video_id"]
                if video_id in details:
                    video.update(details[video_id])

                transcript = self.get_transcript(video_id)
                video["transcript"] = transcript
                video["transcript_length"] = len(transcript)
                video["has_transcript"] = bool(transcript)
                video["collected_at"] = datetime.now(timezone.utc).isoformat()

                if video["has_transcript"]:
                    with_transcripts.append(video)
                else:
                    rejected.append(video)

                video_bar.update(1)
                if idx % 25 == 0:
                    logger.info("Processed %s/%s videos for %s", idx, len(videos), channel_name)

        return with_transcripts, rejected

    @staticmethod
    def _to_int(value) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _duration_to_seconds(iso_duration: str) -> int:
        # Supports PT#H#M#S pattern.
        if not iso_duration:
            return 0
        match = re.fullmatch(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", iso_duration)
        if not match:
            return 0
        hours = int(match.group(1) or 0)
        minutes = int(match.group(2) or 0)
        seconds = int(match.group(3) or 0)
        return (hours * 3600) + (minutes * 60) + seconds

    @staticmethod
    def _parse_yt_datetime(value: str) -> datetime | None:
        if not value:
            return None
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return dt.astimezone(timezone.utc).replace(tzinfo=None)
        except ValueError:
            return None
