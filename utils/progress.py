"""Progress bar helper."""

from __future__ import annotations

from typing import Iterable, TypeVar

from tqdm import tqdm as _tqdm
T = TypeVar("T")


def progress(
    iterable: Iterable[T] | None = None,
    *args,
    **kwargs,
):
    return _tqdm(iterable, *args, **kwargs)
