"""
Sync Handlers for WebUI
"""

import logging
import traceback
from typing import Tuple, Optional
from datetime import datetime

from docreciv.webui.services.ui_service import get_ui_service
from docreciv.webui.utils.metrics_visualization import (
    create_sync_progress_chart_figure,
    figure_to_pil_image
)

logger = logging.getLogger(__name__)


def sync_protocols_handler() -> Tuple[str, str, str, any]:
    """
    Handler for syncing protocols with detailed metrics.
    
    Returns:
        Tuple (status_text, metrics_text, errors_text, chart_image)
    """
    try:
        ui_service = get_ui_service()
        sync_service = ui_service.get_sync_service()
        
        if sync_service is None:
            return "❌ Sync service not available", "", "", None
        
        # Perform sync
        result = sync_service.sync_daily_updates()
        
        # Format detailed metrics
        metrics_text = f"""📊 Detailed Sync Metrics:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⏱️  Execution Time: {result.duration_seconds:.2f} seconds
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📈 Main Indicators:
  • Protocols Scanned: {result.total_processed}
  • Documents Inserted: {result.total_inserted}
  • Documents Skipped: {result.total_skipped}
  • Errors: {result.total_errors}
  
📊 Statistics:
  • Success Rate: {(result.total_inserted / result.total_processed * 100) if result.total_processed > 0 else 0:.1f}%
  • Processing Speed: {result.total_processed / result.duration_seconds if result.duration_seconds > 0 else 0:.2f} docs/sec
"""
        
        # Add statistics from result.statistics if available
        if result.statistics:
            stats = result.statistics
            if "total_size" in stats:
                total_size_mb = stats["total_size"] / (1024 * 1024)
                metrics_text += f"  • Total Size: {total_size_mb:.2f} MB\n"
            if "avg_file_size" in stats:
                avg_size_kb = stats["avg_file_size"] / 1024
                metrics_text += f"  • Average File Size: {avg_size_kb:.2f} KB\n"
        
        # Format error information
        errors_text = ""
        if result.errors and len(result.errors) > 0:
            errors_text = f"\n⚠️  Errors ({len(result.errors)}):\n"
            for i, error in enumerate(result.errors[:10], 1):
                errors_text += f"  {i}. {error}\n"
            if len(result.errors) > 10:
                errors_text += f"  ... and {len(result.errors) - 10} more errors\n"
        
        # Create progress chart
        chart_data = {
            "scanned": result.total_processed,
            "inserted": result.total_inserted,
            "skipped": result.total_skipped,
            "errors": result.total_errors
        }
        
        # Create chart (returns Figure)
        chart_fig = create_sync_progress_chart_figure(chart_data)
        
        # Convert Figure to PIL Image
        chart_image = figure_to_pil_image(chart_fig) if chart_fig else None
        
        # Overall status
        if result.total_errors == 0:
            status_text = f"✅ Sync completed successfully"
        else:
            status_text = f"⚠️ Sync completed with {result.total_errors} errors"
        
        return status_text, metrics_text, errors_text, chart_image
    except Exception as e:
        error_details = traceback.format_exc()
        logger.error(f"Error in sync_protocols_handler: {error_details}")
        return f"❌ Error: {e}", "", f"Error Details:\n{error_details}", None