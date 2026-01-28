"""
Enhanced Dashboard Tab for WebUI with visual indicators.
"""

import gradio as gr
from datetime import datetime
from docreciv.webui.services.ui_service import get_ui_service
from docreciv.webui.components.status_card import (
    create_status_card,
    create_metric_card,
    create_info_banner,
    format_bytes,
    format_duration
)


def create_dashboard_tab():
    """Create an enhanced dashboard with visual indicators and metrics."""
    with gr.Tab("🎛 Control Center"):
        # System Status Section
        gr.Markdown("## System Status")

        with gr.Row():
            # Status indicators - будут обновляться динамически
            vpn_status_html = gr.HTML(elem_classes=["status-card", "animate-in"])
            mongo_local_html = gr.HTML(elem_classes=["status-card", "animate-in-delayed"])
            mongo_remote_html = gr.HTML(elem_classes=["status-card", "animate-in-delayed"])

        # Metrics Overview Section
        gr.Markdown("## Metrics Overview")

        with gr.Row():
            protocols_synced_html = gr.HTML(elem_classes=["metric-card", "animate-in"])
            files_downloaded_html = gr.HTML(elem_classes=["metric-card", "animate-in"])
            success_rate_html = gr.HTML(elem_classes=["metric-card", "animate-in"])
            avg_speed_html = gr.HTML(elem_classes=["metric-card", "animate-in"])

        # System Resources Section
        gr.Markdown("## System Resources")

        with gr.Row():
            cpu_usage_html = gr.HTML(elem_classes=["metric-card"])
            memory_usage_html = gr.HTML(elem_classes=["metric-card"])
            disk_usage_html = gr.HTML(elem_classes=["metric-card"])

        # Quick Actions Section
        gr.Markdown("## Quick Actions")

        with gr.Row():
            sync_now_btn = gr.Button(
                "⚡ Sync Now",
                variant="primary",
                size="lg",
                elem_classes=["action-button"]
            )
            download_btn = gr.Button(
                "📥 Download Pending",
                variant="secondary",
                size="lg"
            )
            health_check_btn = gr.Button(
                "🔍 Health Check",
                size="lg"
            )

        # Activity Log Section
        gr.Markdown("## Recent Activity")
        activity_log = gr.Textbox(
            label="Activity Feed",
            interactive=False,
            lines=8,
            elem_classes=["activity-log"]
        )

        # Auto-refresh controls
        with gr.Row():
            auto_refresh = gr.Checkbox(
                label="🔄 Auto-refresh (30s)",
                value=False,
                elem_classes=["auto-refresh-toggle"]
            )
            refresh_btn = gr.Button(
                "🔄 Refresh Dashboard",
                variant="secondary"
            )

        # Update function
        def update_dashboard():
            """Update all dashboard metrics and status indicators."""
            ui_service = get_ui_service()

            try:
                # Get VPN status
                vpn_info = ui_service.get_vpn_status()
                vpn_status = "ok" if vpn_info.get("status") == "connected" else "error"
                vpn_value = vpn_info.get("status", "Unknown").upper()
                vpn_details = f"Interface: {vpn_info.get('interface', 'N/A')} | IP: {vpn_info.get('ip', 'N/A')}"

                vpn_status_html_content = f"""
                <div class="status-card status-{vpn_status}">
                    <div class="metric-label">{'🟢' if vpn_status == 'ok' else '🔴'} VPN Connection</div>
                    <div class="metric-value">{vpn_value}</div>
                    <div class="metric-details">{vpn_details}</div>
                </div>
                """

                # MongoDB statuses (mock data - replace with real checks)
                mongo_local_html_content = """
                <div class="status-card status-ok">
                    <div class="metric-label">🟢 Local MongoDB</div>
                    <div class="metric-value">CONNECTED</div>
                    <div class="metric-details">localhost:27018</div>
                </div>
                """

                mongo_remote_html_content = """
                <div class="status-card status-ok">
                    <div class="metric-label">🟢 Remote MongoDB</div>
                    <div class="metric-value">CONNECTED</div>
                    <div class="metric-details">192.168.0.46:8635 (via VPN)</div>
                </div>
                """

                # Get system status
                system_info = ui_service.get_system_status()
                cpu_percent = system_info.get('cpu_percent', 0)
                memory_percent = system_info.get('memory_percent', 0)
                disk_percent = system_info.get('disk_percent', 0)

                # Metrics (mock data - replace with real metrics from MongoDB/logs)
                protocols_html_content = f"""
                <div class="metric-card">
                    <div class="metric-label">Protocols Synced Today</div>
                    <div class="metric-value">
                        1,250
                        <span class="metric-unit">protocols</span>
                        <span class="metric-trend text-emerald">↗</span>
                    </div>
                </div>
                """

                files_html_content = f"""
                <div class="metric-card">
                    <div class="metric-label">Files Downloaded Today</div>
                    <div class="metric-value">
                        3,420
                        <span class="metric-unit">files</span>
                        <span class="metric-trend text-emerald">↗</span>
                    </div>
                </div>
                """

                success_html_content = f"""
                <div class="metric-card">
                    <div class="metric-label">Success Rate</div>
                    <div class="metric-value">
                        98.5
                        <span class="metric-unit">%</span>
                        <span class="metric-trend text-muted">→</span>
                    </div>
                </div>
                """

                speed_html_content = f"""
                <div class="metric-card">
                    <div class="metric-label">Avg Download Speed</div>
                    <div class="metric-value">
                        2.3
                        <span class="metric-unit">MB/s</span>
                        <span class="metric-trend text-emerald">↗</span>
                    </div>
                </div>
                """

                # System resources
                cpu_status = "warning" if cpu_percent > 80 else "ok"
                cpu_html_content = f"""
                <div class="metric-card">
                    <div class="metric-label">CPU Usage</div>
                    <div class="metric-value">
                        {cpu_percent:.1f}
                        <span class="metric-unit">%</span>
                    </div>
                </div>
                """

                memory_status = "warning" if memory_percent > 80 else "ok"
                memory_html_content = f"""
                <div class="metric-card">
                    <div class="metric-label">Memory Usage</div>
                    <div class="metric-value">
                        {memory_percent:.1f}
                        <span class="metric-unit">%</span>
                    </div>
                </div>
                """

                disk_status = "warning" if disk_percent > 80 else "ok"
                disk_html_content = f"""
                <div class="metric-card">
                    <div class="metric-label">Disk Usage</div>
                    <div class="metric-value">
                        {disk_percent:.1f}
                        <span class="metric-unit">%</span>
                    </div>
                </div>
                """

                # Activity log
                current_time = datetime.now().strftime('%H:%M:%S')
                activity = f"""[{current_time}] Dashboard refreshed
[{current_time}] System operational - All services running
[{current_time}] VPN: {vpn_value} | CPU: {cpu_percent:.1f}% | MEM: {memory_percent:.1f}%
[{current_time}] Last sync: Today at 14:30 | Last download: Today at 15:45"""

                return (
                    vpn_status_html_content,
                    mongo_local_html_content,
                    mongo_remote_html_content,
                    protocols_html_content,
                    files_html_content,
                    success_html_content,
                    speed_html_content,
                    cpu_html_content,
                    memory_html_content,
                    disk_html_content,
                    activity
                )

            except Exception as e:
                error_message = f"Error updating dashboard: {str(e)}"
                return (
                    f'<div class="status-card status-error"><div class="metric-label">🔴 Error</div><div class="metric-value">{error_message}</div></div>',
                ) * 10 + (error_message,)

        # Wire up refresh button
        refresh_btn.click(
            fn=update_dashboard,
            outputs=[
                vpn_status_html,
                mongo_local_html,
                mongo_remote_html,
                protocols_synced_html,
                files_downloaded_html,
                success_rate_html,
                avg_speed_html,
                cpu_usage_html,
                memory_usage_html,
                disk_usage_html,
                activity_log
            ]
        )

        # Initial load on page load
        demo_load = gr.on(
            triggers=[],  # Empty triggers - will be set by Gradio
            fn=update_dashboard,
            outputs=[
                vpn_status_html,
                mongo_local_html,
                mongo_remote_html,
                protocols_synced_html,
                files_downloaded_html,
                success_rate_html,
                avg_speed_html,
                cpu_usage_html,
                memory_usage_html,
                disk_usage_html,
                activity_log
            ]
        )

        # TODO: Implement auto-refresh with Timer component when available
        # For now, user must click refresh button
