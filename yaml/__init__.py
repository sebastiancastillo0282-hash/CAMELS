"""Minimal YAML parser used when PyYAML is unavailable."""
from __future__ import annotations

from typing import Any, List, Tuple


class YAMLError(Exception):
    """Fallback error type for the stub implementation."""


def _strip_comments(text: str) -> List[str]:
    lines: List[str] = []
    for raw in text.splitlines():
        if "#" in raw:
            raw = raw.split("#", 1)[0]
        cleaned = raw.rstrip("\n")
        if cleaned.strip():
            lines.append(cleaned)
    return lines


def _parse_scalar(value: str) -> Any:
    value = value.strip()
    if not value:
        return ""
    if value.startswith("\"") and value.endswith("\""):
        return value[1:-1]
    if value.startswith("'") and value.endswith("'"):
        return value[1:-1]
    lowered = value.lower()
    if lowered in {"true", "yes"}:
        return True
    if lowered in {"false", "no"}:
        return False
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return value


def _parse_block(lines: List[str], index: int, indent: int) -> Tuple[int, Any]:
    result: Any = None
    while index < len(lines):
        line = lines[index]
        current_indent = len(line) - len(line.lstrip(" "))
        if current_indent < indent:
            break
        content = line.strip()
        if content.startswith("- "):
            if result is None:
                result = []
            elif not isinstance(result, list):
                break
            item_content = content[2:].strip()
            index += 1
            if not item_content:
                index, value = _parse_block(lines, index, current_indent + 2)
                result.append(value)
                continue
            if item_content.endswith(":"):
                key = item_content[:-1].strip()
                index, value = _parse_block(lines, index, current_indent + 2)
                result.append({key: value})
                continue
            if ":" in item_content:
                key, _, remainder = item_content.partition(":")
                item_dict = {key.strip(): _parse_scalar(remainder)}
                index, extra = _parse_block(lines, index, current_indent + 2)
                if isinstance(extra, dict):
                    item_dict.update(extra)
                result.append(item_dict)
                continue
            result.append(_parse_scalar(item_content))
            continue
        if result is None:
            result = {}
        elif isinstance(result, list):
            break
        key, _, remainder = content.partition(":")
        key = key.strip()
        remainder = remainder.strip()
        index += 1
        if remainder:
            result[key] = _parse_scalar(remainder)
            continue
        index, value = _parse_block(lines, index, current_indent + 2)
        result[key] = value
    if result is None:
        result = {}
    return index, result


def safe_load(stream: Any) -> Any:
    """Parse *stream* into Python objects using a limited YAML subset."""

    if stream is None:
        return {}
    if hasattr(stream, "read"):
        text = stream.read()
    else:
        text = stream
    if isinstance(text, bytes):
        text = text.decode("utf-8")
    if not isinstance(text, str):
        return {}
    lines = _strip_comments(text)
    _, result = _parse_block(lines, 0, 0)
    return result


__all__ = ["safe_load", "YAMLError"]
