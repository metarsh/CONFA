from __future__ import annotations

import json
import logging
import importlib.util
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Optional

import pandas as pd

from models import Article, ArticleStub, RejectedArticle, Video


logger = logging.getLogger(__name__)


class DataStorage:

    def __init__(
        self,
        data_root: str = "data",
        run_id: Optional[str] = None,
        output_formats: Optional[Iterable[str]] = None,
    ):
        self.data_root = Path(data_root)
        self.data_root.mkdir(exist_ok=True)

        self.runs_dir = self.data_root / "runs"
        self.runs_dir.mkdir(exist_ok=True)

        self.run_id = run_id or datetime.now().strftime("%Y%m%d_%H%M%S")
        self.run_dir = self.runs_dir / self.run_id
        self.run_dir.mkdir(exist_ok=True)

        self.discovery_dir = self.run_dir / "discovery"
        self.final_dir = self.run_dir / "final"
        self.raw_dir = self.run_dir / "raw"

        for path in [self.discovery_dir, self.final_dir, self.raw_dir]:
            path.mkdir(exist_ok=True)

        formats = [fmt.lower() for fmt in (output_formats or ["csv"]) if fmt]
        self.output_formats = [fmt for fmt in formats if fmt in {"csv", "parquet"}] or ["csv"]
        if "parquet" in self.output_formats and not self._parquet_available():
            raise RuntimeError(
                "Parquet output requested but pyarrow/fastparquet is not installed. "
                "Install pyarrow or remove parquet from OUTPUT_FORMATS."
            )

    @staticmethod
    def _parquet_available() -> bool:
        return bool(importlib.util.find_spec("pyarrow") or importlib.util.find_spec("fastparquet"))

    @staticmethod
    def latest_run_id(data_root: str = "data") -> Optional[str]:
        runs_dir = Path(data_root) / "runs"
        if not runs_dir.exists():
            return None

        dirs = [entry for entry in runs_dir.iterdir() if entry.is_dir()]
        if not dirs:
            return None

        dirs.sort(key=lambda item: item.stat().st_mtime, reverse=True)
        return dirs[0].name

    @staticmethod
    def list_run_ids(data_root: str = "data") -> list[str]:
        runs_dir = Path(data_root) / "runs"
        if not runs_dir.exists():
            return []

        dirs = [entry for entry in runs_dir.iterdir() if entry.is_dir()]
        dirs.sort(key=lambda item: item.stat().st_mtime, reverse=True)
        return [entry.name for entry in dirs]

    def _write_dataframe(self, df: pd.DataFrame, base_path: Path) -> dict[str, str]:
        out: dict[str, str] = {}

        if "csv" in self.output_formats:
            csv_path = base_path.with_suffix(".csv")
            df.to_csv(csv_path, index=False)
            out["csv"] = str(csv_path)

        if "parquet" in self.output_formats:
            parquet_path = base_path.with_suffix(".parquet")
            df.to_parquet(parquet_path, index=False)
            out["parquet"] = str(parquet_path)

        return out

    @staticmethod
    def _read_dataframe(base_path: Path) -> pd.DataFrame:
        parquet_path = base_path.with_suffix(".parquet")
        csv_path = base_path.with_suffix(".csv")

        if parquet_path.exists():
            return pd.read_parquet(parquet_path)

        if csv_path.exists():
            return pd.read_csv(csv_path)

        return pd.DataFrame()

    def load_table_dicts(self, name: str, area: str = "final") -> list[dict[str, Any]]:
        area_map = {
            "final": self.final_dir,
            "discovery": self.discovery_dir,
        }
        target_dir = area_map.get(area)
        if target_dir is None:
            raise ValueError(f"Invalid area '{area}'")

        df = self._read_dataframe(target_dir / name)
        if df.empty:
            return []
        return df.fillna("").to_dict(orient="records")

    def has_table(self, name: str, area: str = "final") -> bool:
        area_map = {
            "final": self.final_dir,
            "discovery": self.discovery_dir,
        }
        target_dir = area_map.get(area)
        if target_dir is None:
            return False

        base_path = target_dir / name
        return base_path.with_suffix(".csv").exists() or base_path.with_suffix(".parquet").exists()

    def save_raw_response(self, data: Any, source: str, endpoint: str) -> str:
        source_dir = self.raw_dir / source.lower().replace(" ", "_")
        source_dir.mkdir(exist_ok=True)

        ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        file_path = source_dir / f"{endpoint}_{ts}.json"

        with open(file_path, "w", encoding="utf-8") as handle:
            json.dump(data, handle, indent=2, ensure_ascii=False, default=str)

        return str(file_path)

    def save_dict_rows(self, rows: list[dict[str, Any]], name: str, area: str = "final") -> dict[str, str]:
        area_map = {
            "final": self.final_dir,
            "discovery": self.discovery_dir,
        }
        target_dir = area_map.get(area)
        if target_dir is None:
            raise ValueError(f"Invalid area '{area}'")

        df = pd.DataFrame(rows)
        return self._write_dataframe(df, target_dir / name)

    def save_stubs(self, stubs: list[ArticleStub], name: str = "article_stubs") -> dict[str, str]:
        if not stubs:
            df = pd.DataFrame(columns=list(ArticleStub.__dataclass_fields__.keys()))
        else:
            df = pd.DataFrame([stub.to_dict() for stub in stubs])
        return self._write_dataframe(df, self.discovery_dir / name)

    def save_articles(self, articles: list[Article], name: str = "articles") -> dict[str, str]:
        if not articles:
            df = pd.DataFrame(columns=list(Article.__dataclass_fields__.keys()))
        else:
            df = pd.DataFrame([article.to_dict() for article in articles])
        return self._write_dataframe(df, self.final_dir / name)

    def save_rejected(self, rejected: list[RejectedArticle], name: str = "rejected_articles") -> dict[str, str]:
        if not rejected:
            df = pd.DataFrame(columns=list(RejectedArticle.__dataclass_fields__.keys()))
        else:
            df = pd.DataFrame([row.to_dict() for row in rejected])
        return self._write_dataframe(df, self.final_dir / name)

    def save_videos(self, videos: list[Video], name: str = "videos") -> dict[str, str]:
        if not videos:
            df = pd.DataFrame(columns=list(Video.__dataclass_fields__.keys()))
        else:
            df = pd.DataFrame([video.to_dict() for video in videos])
        return self._write_dataframe(df, self.final_dir / name)

    def save_quality_report(self, report: dict[str, Any], name: str = "quality_report") -> str:
        path = self.final_dir / f"{name}.json"
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2, ensure_ascii=False, default=str)
        return str(path)

    def save_run_manifest(self, manifest: dict[str, Any], name: str = "run_manifest") -> str:
        path = self.run_dir / f"{name}.json"
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(manifest, handle, indent=2, ensure_ascii=False, default=str)
        return str(path)

    def save_run_state(self, state: dict[str, Any], name: str = "run_state") -> str:
        path = self.run_dir / f"{name}.json"
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(state, handle, indent=2, ensure_ascii=False, default=str)
        return str(path)

    def load_run_manifest(self, name: str = "run_manifest") -> dict[str, Any]:
        path = self.run_dir / f"{name}.json"
        if not path.exists():
            return {}
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)

    def load_run_state(self, name: str = "run_state") -> dict[str, Any]:
        path = self.run_dir / f"{name}.json"
        if not path.exists():
            return {}
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)

    def get_stats(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "run_dir": str(self.run_dir),
            "discovery_dir": str(self.discovery_dir),
            "final_dir": str(self.final_dir),
            "raw_dir": str(self.raw_dir),
            "output_formats": self.output_formats,
        }
