"""Enhanced status card components with visual indicators."""
import gradio as gr
from typing import Literal, Optional


def create_status_card(
    label: str,
    status: Literal["ok", "warning", "error", "unknown"] = "unknown",
    value: str = "",
    details: str = "",
    elem_classes: Optional[list] = None
) -> gr.HTML:
    """
    Create a status card with animated visual indicator.

    Args:
        label: Card title
        status: Status type for coloring (ok, warning, error, unknown)
        value: Main status value
        details: Additional details text
        elem_classes: Additional CSS classes

    Returns:
        Gradio HTML component with status card
    """
    status_icons = {
        "ok": "🟢",
        "warning": "🟡",
        "error": "🔴",
        "unknown": "⚪"
    }

    status_icon = status_icons.get(status, "⚪")
    classes = ["status-card", f"status-{status}"]
    if elem_classes:
        classes.extend(elem_classes)

    card_html = f"""
    <div class="{' '.join(classes)}">
        <div class="metric-label">{status_icon} {label}</div>
        <div class="metric-value">{value}</div>
        {f'<div class="metric-details">{details}</div>' if details else ''}
    </div>
    """

    return gr.HTML(card_html, elem_classes=classes)


def create_metric_card(
    label: str,
    value: str | int | float,
    unit: str = "",
    trend: Literal["up", "down", "neutral"] = "neutral",
    elem_classes: Optional[list] = None
) -> gr.HTML:
    """
    Create a metric card with value and optional trend indicator.

    Args:
        label: Metric label
        value: Metric value
        unit: Unit of measurement (e.g., "MB/s", "%", "files")
        trend: Trend direction (up, down, neutral)
        elem_classes: Additional CSS classes

    Returns:
        Gradio HTML component with metric card
    """
    trend_icons = {
        "up": "↗",
        "down": "↘",
        "neutral": "→"
    }

    trend_colors = {
        "up": "text-emerald",
        "down": "text-coral",
        "neutral": "text-muted"
    }

    trend_icon = trend_icons.get(trend, "")
    trend_color = trend_colors.get(trend, "")

    classes = ["metric-card", "animate-in"]
    if elem_classes:
        classes.extend(elem_classes)

    # Format value based on type
    if isinstance(value, float):
        formatted_value = f"{value:.2f}"
    else:
        formatted_value = str(value)

    card_html = f"""
    <div class="{' '.join(classes)}">
        <div class="metric-label">{label}</div>
        <div class="metric-value">
            {formatted_value}
            {f'<span class="metric-unit">{unit}</span>' if unit else ''}
            {f'<span class="metric-trend {trend_color}">{trend_icon}</span>' if trend != "neutral" else ''}
        </div>
    </div>
    """

    return gr.HTML(card_html, elem_classes=classes)


def create_info_banner(
    message: str,
    type: Literal["info", "warning", "error", "success"] = "info"
) -> gr.HTML:
    """
    Create an information banner with icon.

    Args:
        message: Banner message
        type: Banner type (info, warning, error, success)

    Returns:
        Gradio HTML component with banner
    """
    icons = {
        "info": "ℹ️",
        "warning": "⚠️",
        "error": "❌",
        "success": "✅"
    }

    colors = {
        "info": "text-cyan",
        "warning": "text-amber",
        "error": "text-coral",
        "success": "text-emerald"
    }

    icon = icons.get(type, "ℹ️")
    color = colors.get(type, "text-cyan")

    banner_html = f"""
    <div class="status-indicator status-{type}">
        <span>{icon}</span>
        <span class="{color}">{message}</span>
    </div>
    """

    return gr.HTML(banner_html, elem_classes=["info-banner"])


def create_section_header(
    title: str,
    subtitle: str = "",
    icon: str = ""
) -> gr.HTML:
    """
    Create a section header with optional icon and subtitle.

    Args:
        title: Section title
        subtitle: Optional subtitle text
        icon: Optional emoji icon

    Returns:
        Gradio HTML component with section header
    """
    header_html = f"""
    <div class="section-header">
        <h2>{icon + ' ' if icon else ''}{title}</h2>
        {f'<p class="text-muted">{subtitle}</p>' if subtitle else ''}
    </div>
    """

    return gr.HTML(header_html, elem_classes=["section-header"])


def format_bytes(bytes: int) -> str:
    """
    Format bytes to human-readable format.

    Args:
        bytes: Number of bytes

    Returns:
        Formatted string (e.g., "1.5 GB")
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes < 1024.0:
            return f"{bytes:.2f} {unit}"
        bytes /= 1024.0
    return f"{bytes:.2f} PB"


def format_duration(seconds: float) -> str:
    """
    Format duration in seconds to human-readable format.

    Args:
        seconds: Duration in seconds

    Returns:
        Formatted string (e.g., "2h 15m 30s")
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        minutes = (seconds % 3600) / 60
        return f"{int(hours)}h {int(minutes)}m"
