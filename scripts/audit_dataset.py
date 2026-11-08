#!/usr/bin/env python3
"""Audit exported dataset quality"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import pandas as pd


TRUNCATION_RE = re.compile(r"\[\+\d+ chars\]")
REQUIRED_COLUMNS = {
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit news dataset output")
    parser.add_argument("--run-dir", type=str, help="Run directory (data/runs/<id>)")
    parser.add_argument("--articles", type=str, help="Path to article CSV/Parquet")
    parser.add_argument("--report-out", type=str, help="Write report JSON to this path")
    parser.add_argument("--strict", action="store_true", help="Exit non-zero on hard failures")
    return parser.parse_args()


def resolve_articles_path(args: argparse.Namespace) -> Path:
    if args.articles:
        return Path(args.articles)

    if not args.run_dir:
        raise ValueError("Provide --run-dir or --articles")

    run_dir = Path(args.run_dir)
    parquet_path = run_dir / "final" / "articles.parquet"
    csv_path = run_dir / "final" / "articles.csv"

    if parquet_path.exists():
        return parquet_path
    if csv_path.exists():
        return csv_path

    raise FileNotFoundError(f"No article file found under {run_dir}/final")


def load_frame(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


def audit(df: pd.DataFrame) -> dict:
    hard_failures: list[str] = []
    warnings: list[str] = []

    missing_cols = sorted(REQUIRED_COLUMNS - set(df.columns))
    if missing_cols:
        hard_failures.append(f"missing_required_columns: {missing_cols}")

    if "content" in df.columns:
        trunc_count = int(df["content"].fillna("").str.contains(TRUNCATION_RE).sum())
        if trunc_count > 0:
            hard_failures.append(f"accepted_articles_with_truncation_artifact: {trunc_count}")
    else:
        trunc_count = 0

    if {"source", "url"}.issubset(df.columns):
        bbc_bad = int(
            (
                (df["source"] == "BBC")
                & (
                    df["url"].fillna("").str.lower().str.contains("/sounds/")
                    | df["url"].fillna("").str.lower().str.contains("/programmes/")
                )
            ).sum()
        )
        if bbc_bad > 0:
            hard_failures.append(f"accepted_bbc_non_article_urls: {bbc_bad}")
    else:
        bbc_bad = 0

    if "url" in df.columns:
        duplicate_urls = int(df["url"].duplicated().sum())
    else:
        duplicate_urls = 0

    if "source" in df.columns:
        source_counts = df["source"].value_counts().to_dict()
        for source, count in source_counts.items():
            if count < 20:
                warnings.append(f"low_article_volume_{source}: {int(count)}")
    else:
        source_counts = {}

    if "published_date" in df.columns:
        parsed = pd.to_datetime(df["published_date"], errors="coerce", utc=True)
        parse_ratio = float(parsed.notna().mean()) if len(parsed) else 0.0
        if parse_ratio < 0.95:
            warnings.append(f"low_parseable_published_date_ratio: {parse_ratio:.2%}")
        date_min = str(parsed.min()) if parsed.notna().any() else ""
        date_max = str(parsed.max()) if parsed.notna().any() else ""
    else:
        parse_ratio = 0.0
        date_min = ""
        date_max = ""

    full_text_ratio = 0.0
    if "is_full_text" in df.columns and len(df) > 0:
        full_text_ratio = float(df["is_full_text"].fillna(False).mean())

    return {
        "rows": int(len(df)),
        "source_counts": {k: int(v) for k, v in source_counts.items()},
        "date_min": date_min,
        "date_max": date_max,
        "full_text_ratio": full_text_ratio,
        "truncation_artifact_count": trunc_count,
        "bbc_non_article_count": bbc_bad,
        "duplicate_url_count": duplicate_urls,
        "parseable_published_date_ratio": parse_ratio,
        "hard_failures": hard_failures,
        "warnings": warnings,
    }


def main() -> int:
    args = parse_args()
    articles_path = resolve_articles_path(args)
    df = load_frame(articles_path)
    report = audit(df)

    report_out = Path(args.report_out) if args.report_out else articles_path.parent / "audit_report.json"
    with open(report_out, "w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, ensure_ascii=False)

    print(json.dumps(report, indent=2, ensure_ascii=False))

    if args.strict and report["hard_failures"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
