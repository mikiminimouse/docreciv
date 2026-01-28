"""
UI Utilities for WebUI
"""

import logging
from typing import Any, Dict, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


def format_datetime(dt: Optional[datetime]) -> str:
    """Format datetime for display."""
    if dt is None:
        return "Never"
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def format_bytes(bytes_count: int) -> str:
    """Format bytes to human readable format."""
    if bytes_count < 1024:
        return f"{bytes_count} B"
    elif bytes_count < 1024 * 1024:
        return f"{bytes_count / 1024:.2f} KB"
    elif bytes_count < 1024 * 1024 * 1024:
        return f"{bytes_count / (1024 * 1024):.2f} MB"
    else:
        return f"{bytes_count / (1024 * 1024 * 1024):.2f} GB"


def format_duration(seconds: float) -> str:
    """Format duration in seconds to human readable format."""
    if seconds < 60:
        return f"{seconds:.2f} seconds"
    elif seconds < 3600:
        return f"{seconds / 60:.2f} minutes"
    else:
        return f"{seconds / 3600:.2f} hours"


def truncate_text(text: str, max_length: int = 100) -> str:
    """Truncate text to max_length and add ellipsis if needed."""
    if len(text) <= max_length:
        return text
    return text[:max_length - 3] + "..."


def format_config_value(value: Any) -> str:
    """Format configuration value for display."""
    if isinstance(value, (dict, list)):
        return str(value)
    elif isinstance(value, bool):
        return "✅ Yes" if value else "❌ No"
    elif value is None:
        return "Not set"
    else:
        return str(value)


def create_status_indicator(status: str) -> str:
    """Create a status indicator with appropriate emoji."""
    status_indicators = {
        "success": "✅",
        "completed": "✅",
        "running": "🔄",
        "pending": "⏳",
        "failed": "❌",
        "error": "❌",
        "warning": "⚠️",
        "cancelled": "⏹️"
    }
    return f"{status_indicators.get(status.lower(), '❓')} {status.title()}"


def format_percentage(value: float, total: float) -> str:
    """Format percentage with appropriate precision."""
    if total == 0:
        return "0%"
    percentage = (value / total) * 100
    return f"{percentage:.1f}%"


def safe_get(dictionary: Dict, key: str, default: Any = "") -> Any:
    """Safely get value from dictionary."""
    try:
        return dictionary.get(key, default)
    except Exception:
        return default


def format_list(items: List[Any], max_items: int = 10) -> str:
    """Format list for display with truncation."""
    if not items:
        return "None"
    
    if len(items) <= max_items:
        return ", ".join(str(item) for item in items)
    
    return ", ".join(str(item) for item in items[:max_items]) + f" and {len(items) - max_items} more..."





