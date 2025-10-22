"""Utilities to download source documents with retry and hashing."""
from __future__ import annotations

import hashlib
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.error import URLError
from urllib.parse import urlparse
from urllib.request import urlopen

from camels.ingestion.catalog import SourceDefinition


class DownloadError(RuntimeError):
    """Raised when a source cannot be downloaded after retries."""


@dataclass(slots=True)
class DownloadResult:
    """Metadata for a downloaded source artifact."""

    source: SourceDefinition
    path: Path
    sha256: str
    size_bytes: int
    content_type: str | None
    elapsed: float


def _copy_stream(read_handle, target: Path) -> int:
    target.parent.mkdir(parents=True, exist_ok=True)
    bytes_written = 0
    with target.open("wb") as dest:
        while True:
            chunk = read_handle.read(1024 * 64)
            if not chunk:
                break
            dest.write(chunk)
            bytes_written += len(chunk)
    return bytes_written


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _resolve_filename(source: SourceDefinition, destination: Path) -> Path:
    suffix = {
        "csv": ".csv",
        "xlsx": ".xlsx",
        "xls": ".xls",
        "pdf": ".pdf",
    }.get(source.format, "")
    timestamp = int(time.time() * 1000)
    return destination / f"{source.slug}_{timestamp}{suffix}"


def _download_http(url: str, destination: Path, timeout: int) -> tuple[Path, Optional[str]]:
    with urlopen(url, timeout=timeout) as response:  # nosec - controlled URLs from catalog
        content_type = response.headers.get("Content-Type") if response.headers else None
        path = destination
        _copy_stream(response, path)
    return path, content_type


def _download_local(url: str, destination: Path) -> tuple[Path, Optional[str]]:
    source_path = Path(url)
    if not source_path.exists():
        raise DownloadError(f"Local file {source_path} does not exist")
    shutil.copy2(source_path, destination)
    return destination, None


def download_source(
    source: SourceDefinition,
    directory: Path,
    *,
    retries: int = 3,
    backoff: float = 1.0,
    timeout: int = 60,
) -> DownloadResult:
    """Download *source* into *directory* and return the resulting metadata."""

    parsed = urlparse(source.url)
    filename = _resolve_filename(source, directory)
    directory.mkdir(parents=True, exist_ok=True)
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        start = time.perf_counter()
        try:
            if parsed.scheme in {"http", "https"}:
                path, content_type = _download_http(source.url, filename, timeout)
            elif parsed.scheme == "file" or parsed.scheme == "":
                local_path = parsed.path if parsed.scheme == "file" else source.url
                path, content_type = _download_local(local_path, filename)
            else:
                raise DownloadError(f"Unsupported URL scheme '{parsed.scheme}' for {source.url}")
            elapsed = time.perf_counter() - start
            checksum = _hash_file(path)
            size_bytes = path.stat().st_size
            return DownloadResult(
                source=source,
                path=path,
                sha256=checksum,
                size_bytes=size_bytes,
                content_type=content_type,
                elapsed=elapsed,
            )
        except (URLError, OSError, DownloadError) as exc:
            last_error = exc
            if attempt == retries:
                break
            time.sleep(backoff * attempt)
    raise DownloadError(
        f"Failed to download {source.url} after {retries} attempts: {last_error}"
    )
