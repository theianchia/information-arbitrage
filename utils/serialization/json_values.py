"""Normalize values for JSON tool responses (MCP, HTTP, etc.)."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any


def json_safe(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, date):
        return obj.isoformat()
    if isinstance(obj, float):
        if obj != obj:  # NaN
            return None
        return round(obj, 6)
    return obj
