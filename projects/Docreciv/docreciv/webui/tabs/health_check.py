"""
Enhanced Health Check Tab for WebUI with Technical Precision design.
"""

import gradio as gr
from docreciv.webui.services.ui_service import get_ui_service
import psutil


def health_result_to_dict(health_result):
    """Convert HealthCheckResult object to dictionary."""
    if hasattr(health_result, 'status'):
        return {
            'status': health_result.status,
            'message': health_result.message,
            'details': health_result.details
        }
    return health_result


def create_health_status_card(title, status, message, details_text, emoji):
    """Create a health status card with Technical Precision styling."""
    # Map status to color class
    status_classes = {
        'healthy': 'status-ok',
        'degraded': 'status-warning',
        'unhealthy': 'status-error',
        'unknown': 'status-unknown'
    }

    status_emojis = {
        'healthy': '🟢',
        'degraded': '🟡',
        'unhealthy': '🔴',
        'unknown': '⚪'
    }

    status_class = status_classes.get(status, 'status-unknown')
    status_emoji = status_emojis.get(status, '⚪')
    status_text = status.upper() if status != 'healthy' else 'HEALTHY!'

    html = f"""
    <div class="status-card {status_class}">
        <div class="metric-label">{status_emoji} {emoji} {title}</div>
        <div class="metric-value">{status_text}</div>
        <div class="metric-details">{message}</div>
        {f'<div class="metric-details" style="margin-top: 8px; font-size: 0.75rem; opacity: 0.7;">{details_text}</div>' if details_text else ''}
    </div>
    """
    return html


def create_health_check_tab():
    """Create an enhanced health check tab with visual indicators."""
    with gr.Tab("🏥 Health Check"):
        gr.Markdown("## System Health Monitoring")
        gr.Markdown("*Comprehensive diagnostics and connectivity checks*")

        # Status Cards Row 1: Network & Connectivity
        gr.Markdown("### 🌐 Network & Connectivity")
        with gr.Row():
            vpn_status_html = gr.HTML(elem_classes=["status-card", "animate-in"])
            zakupki_status_html = gr.HTML(elem_classes=["status-card", "animate-in-delayed"])

        with gr.Row():
            check_vpn_btn = gr.Button("🔍 Check VPN", size="sm")
            check_zakupki_btn = gr.Button("🔍 Check Zakupki Access", size="sm")

        # Status Cards Row 2: Database Connectivity
        gr.Markdown("### 🗄️ Database Connectivity")
        with gr.Row():
            remote_mongo_html = gr.HTML(elem_classes=["status-card", "animate-in"])
            local_mongo_html = gr.HTML(elem_classes=["status-card", "animate-in-delayed"])

        with gr.Row():
            check_remote_mongo_btn = gr.Button("🔍 Check Remote MongoDB", size="sm")
            check_local_mongo_btn = gr.Button("🔍 Check Local MongoDB", size="sm")

        # Status Cards Row 3: System Resources
        gr.Markdown("### 💻 System Resources")
        with gr.Row():
            cpu_status_html = gr.HTML(elem_classes=["metric-card", "animate-in"])
            memory_status_html = gr.HTML(elem_classes=["metric-card", "animate-in-delayed"])
            disk_status_html = gr.HTML(elem_classes=["metric-card", "animate-in-delayed"])

        with gr.Row():
            check_system_btn = gr.Button("🔍 Check System Resources", size="sm")

        # Status Cards Row 4: Configuration
        gr.Markdown("### ⚙️ Configuration")
        with gr.Row():
            env_status_html = gr.HTML(elem_classes=["status-card", "animate-in"])
            ssl_status_html = gr.HTML(elem_classes=["status-card", "animate-in-delayed"])

        with gr.Row():
            check_env_btn = gr.Button("🔍 Check Environment", size="sm")
            check_ssl_btn = gr.Button("🔍 Check SSL Certificate", size="sm")

        # Overall Health Check Section
        gr.Markdown("### 🎯 Overall System Health")
        with gr.Row():
            run_all_checks_btn = gr.Button(
                "⚡ Run All Health Checks",
                variant="primary",
                size="lg",
                elem_classes=["action-button"]
            )

        overall_status_html = gr.HTML(elem_classes=["status-card"])

        # Detailed Results Section
        with gr.Accordion("📋 Detailed Results", open=False):
            detailed_results = gr.Textbox(
                label="Complete Health Check Log",
                interactive=False,
                lines=15,
                elem_classes=["activity-log"]
            )

        # ============ CHECK FUNCTIONS ============

        def check_vpn():
            """Check VPN connectivity only."""
            ui_service = get_ui_service()
            try:
                results = ui_service.run_health_check()
                vpn_result = health_result_to_dict(results.get('vpn', {}))

                status = vpn_result.get('status', 'unknown')
                message = vpn_result.get('message', 'No information available')
                details = vpn_result.get('details', {})

                details_text = ""
                if isinstance(details, dict):
                    for key, value in details.items():
                        if key != 'suggestion':
                            details_text += f"{key}: {value} | "
                    details_text = details_text.rstrip(" | ")

                html = create_health_status_card(
                    "VPN Connection",
                    status,
                    message,
                    details_text,
                    "🔒"
                )
                return html
            except Exception as e:
                return create_health_status_card(
                    "VPN Connection",
                    "unhealthy",
                    f"Error: {str(e)}",
                    "",
                    "🔒"
                )

        def check_zakupki():
            """Check Zakupki access (same as VPN but separate UI)."""
            # Zakupki is checked as part of VPN connectivity
            return check_vpn()

        def check_remote_mongo():
            """Check Remote MongoDB connectivity only."""
            ui_service = get_ui_service()
            try:
                results = ui_service.run_health_check()
                mongo_result = health_result_to_dict(results.get('remote_mongodb', {}))

                status = mongo_result.get('status', 'unknown')
                message = mongo_result.get('message', 'No information available')
                details = mongo_result.get('details', {})

                details_text = ""
                if isinstance(details, dict):
                    server = details.get('server', 'N/A')
                    database = details.get('database', 'N/A')
                    conn_time = details.get('connection_time_ms', 'N/A')
                    details_text = f"Server: {server} | DB: {database} | Time: {conn_time}ms"

                html = create_health_status_card(
                    "Remote MongoDB",
                    status,
                    message,
                    details_text,
                    "🌐"
                )
                return html
            except Exception as e:
                return create_health_status_card(
                    "Remote MongoDB",
                    "unhealthy",
                    f"Error: {str(e)}",
                    "",
                    "🌐"
                )

        def check_local_mongo():
            """Check Local MongoDB connectivity only."""
            ui_service = get_ui_service()
            try:
                results = ui_service.run_health_check()
                mongo_result = health_result_to_dict(results.get('local_mongodb', {}))

                status = mongo_result.get('status', 'unknown')
                message = mongo_result.get('message', 'No information available')
                details = mongo_result.get('details', {})

                details_text = ""
                if isinstance(details, dict):
                    server = details.get('server', 'N/A')
                    database = details.get('database', 'N/A')
                    conn_time = details.get('connection_time_ms', 'N/A')
                    details_text = f"Server: {server} | DB: {database} | Time: {conn_time}ms"

                html = create_health_status_card(
                    "Local MongoDB",
                    status,
                    message,
                    details_text,
                    "💾"
                )
                return html
            except Exception as e:
                return create_health_status_card(
                    "Local MongoDB",
                    "unhealthy",
                    f"Error: {str(e)}",
                    "",
                    "💾"
                )

        def check_system_resources():
            """Check system resources (CPU, Memory, Disk)."""
            try:
                cpu_percent = psutil.cpu_percent(interval=1)
                memory = psutil.virtual_memory()
                disk = psutil.disk_usage('/')

                # CPU Card
                cpu_status = "healthy" if cpu_percent < 80 else "degraded" if cpu_percent < 95 else "unhealthy"
                cpu_html = f"""
                <div class="metric-card">
                    <div class="metric-label">⚙️ CPU Usage</div>
                    <div class="metric-value">
                        {cpu_percent:.1f}
                        <span class="metric-unit">%</span>
                        <span class="metric-trend {'text-emerald' if cpu_percent < 50 else 'text-amber' if cpu_percent < 80 else 'text-coral'}">
                            {'✓' if cpu_percent < 80 else '⚠' if cpu_percent < 95 else '✗'}
                        </span>
                    </div>
                    <div class="metric-details">Status: {cpu_status.upper()}</div>
                </div>
                """

                # Memory Card
                memory_percent = memory.percent
                memory_status = "healthy" if memory_percent < 80 else "degraded" if memory_percent < 95 else "unhealthy"
                memory_html = f"""
                <div class="metric-card">
                    <div class="metric-label">🧠 Memory Usage</div>
                    <div class="metric-value">
                        {memory_percent:.1f}
                        <span class="metric-unit">%</span>
                        <span class="metric-trend {'text-emerald' if memory_percent < 50 else 'text-amber' if memory_percent < 80 else 'text-coral'}">
                            {'✓' if memory_percent < 80 else '⚠' if memory_percent < 95 else '✗'}
                        </span>
                    </div>
                    <div class="metric-details">
                        {memory.used / (1024**3):.1f}GB / {memory.total / (1024**3):.1f}GB
                    </div>
                </div>
                """

                # Disk Card
                disk_percent = (disk.used / disk.total) * 100
                disk_status = "healthy" if disk_percent < 80 else "degraded" if disk_percent < 95 else "unhealthy"
                disk_html = f"""
                <div class="metric-card">
                    <div class="metric-label">💾 Disk Usage</div>
                    <div class="metric-value">
                        {disk_percent:.1f}
                        <span class="metric-unit">%</span>
                        <span class="metric-trend {'text-emerald' if disk_percent < 50 else 'text-amber' if disk_percent < 80 else 'text-coral'}">
                            {'✓' if disk_percent < 80 else '⚠' if disk_percent < 95 else '✗'}
                        </span>
                    </div>
                    <div class="metric-details">
                        {disk.used / (1024**3):.1f}GB / {disk.total / (1024**3):.1f}GB
                    </div>
                </div>
                """

                return cpu_html, memory_html, disk_html
            except Exception as e:
                error_html = f"""
                <div class="metric-card">
                    <div class="metric-label">❌ Error</div>
                    <div class="metric-value">Failed</div>
                    <div class="metric-details">{str(e)}</div>
                </div>
                """
                return error_html, error_html, error_html

        def check_environment():
            """Check environment variables."""
            ui_service = get_ui_service()
            try:
                results = ui_service.run_health_check()
                env_result = health_result_to_dict(results.get('environment', {}))

                status = env_result.get('status', 'unknown')
                message = env_result.get('message', 'No information available')
                details = env_result.get('details', {})

                details_text = ""
                if isinstance(details, dict):
                    for key, value in details.items():
                        if key not in ['missing_remote_vars', 'missing_local_vars']:
                            details_text += f"{key}: {value} | "
                    details_text = details_text.rstrip(" | ")

                html = create_health_status_card(
                    "Environment Variables",
                    status,
                    message,
                    details_text,
                    "🔧"
                )
                return html
            except Exception as e:
                return create_health_status_card(
                    "Environment Variables",
                    "unhealthy",
                    f"Error: {str(e)}",
                    "",
                    "🔧"
                )

        def check_ssl():
            """Check SSL certificate."""
            ui_service = get_ui_service()
            try:
                results = ui_service.run_health_check()
                ssl_result = health_result_to_dict(results.get('ssl_certificate', {}))

                if not ssl_result:
                    return create_health_status_card(
                        "SSL Certificate",
                        "healthy",
                        "No SSL certificate configured (not required for local setup)",
                        "",
                        "🔐"
                    )

                status = ssl_result.get('status', 'unknown')
                message = ssl_result.get('message', 'No information available')
                details = ssl_result.get('details', {})

                details_text = ""
                if isinstance(details, dict):
                    cert_path = details.get('cert_path', 'N/A')
                    file_size = details.get('file_size', 'N/A')
                    details_text = f"Path: {cert_path} | Size: {file_size} bytes"

                html = create_health_status_card(
                    "SSL Certificate",
                    status,
                    message,
                    details_text,
                    "🔐"
                )
                return html
            except Exception as e:
                return create_health_status_card(
                    "SSL Certificate",
                    "unhealthy",
                    f"Error: {str(e)}",
                    "",
                    "🔐"
                )

        def run_all_health_checks():
            """Run all health checks and return results."""
            ui_service = get_ui_service()

            try:
                results = ui_service.run_health_check()

                # Convert all results to dicts
                converted_results = {}
                for key, value in results.items():
                    converted_results[key] = health_result_to_dict(value)

                # VPN Status
                vpn_result = converted_results.get('vpn', {})
                vpn_html = create_health_status_card(
                    "VPN Connection",
                    vpn_result.get('status', 'unknown'),
                    vpn_result.get('message', 'N/A'),
                    "",
                    "🔒"
                )

                # Zakupki (same as VPN for display)
                zakupki_html = vpn_html

                # Remote MongoDB
                remote_mongo_result = converted_results.get('remote_mongodb', {})
                remote_mongo_html = create_health_status_card(
                    "Remote MongoDB",
                    remote_mongo_result.get('status', 'unknown'),
                    remote_mongo_result.get('message', 'N/A'),
                    "",
                    "🌐"
                )

                # Local MongoDB
                local_mongo_result = converted_results.get('local_mongodb', {})
                local_mongo_html = create_health_status_card(
                    "Local MongoDB",
                    local_mongo_result.get('status', 'unknown'),
                    local_mongo_result.get('message', 'N/A'),
                    "",
                    "💾"
                )

                # System Resources
                cpu_html, memory_html, disk_html = check_system_resources()

                # Environment
                env_result = converted_results.get('environment', {})
                env_html = create_health_status_card(
                    "Environment Variables",
                    env_result.get('status', 'unknown'),
                    env_result.get('message', 'N/A'),
                    "",
                    "🔧"
                )

                # SSL
                ssl_result = converted_results.get('ssl_certificate', {})
                if ssl_result:
                    ssl_html = create_health_status_card(
                        "SSL Certificate",
                        ssl_result.get('status', 'unknown'),
                        ssl_result.get('message', 'N/A'),
                        "",
                        "🔐"
                    )
                else:
                    ssl_html = create_health_status_card(
                        "SSL Certificate",
                        "healthy",
                        "No SSL certificate configured",
                        "",
                        "🔐"
                    )

                # Overall Status
                healthy_count = sum(1 for r in converted_results.values() if r.get('status') == 'healthy')
                total_count = len(converted_results)

                if healthy_count == total_count:
                    overall_status = "healthy"
                    overall_message = f"🎉 All {total_count} checks passed! System is fully operational."
                elif healthy_count > total_count / 2:
                    overall_status = "degraded"
                    overall_message = f"⚠️ {healthy_count}/{total_count} checks passed. Some issues detected."
                else:
                    overall_status = "unhealthy"
                    overall_message = f"❌ Only {healthy_count}/{total_count} checks passed. Critical issues detected."

                overall_html = create_health_status_card(
                    "Overall System Health",
                    overall_status,
                    overall_message,
                    f"Passed: {healthy_count} | Total: {total_count}",
                    "🎯"
                )

                # Detailed results log
                detailed_log = "="*60 + "\n"
                detailed_log += "🏥 COMPREHENSIVE HEALTH CHECK REPORT\n"
                detailed_log += "="*60 + "\n\n"

                for check_name, result in converted_results.items():
                    status = result.get('status', 'unknown')
                    status_icon = "✅" if status == "healthy" else "⚠️" if status == "degraded" else "❌"

                    detailed_log += f"{status_icon} {check_name.upper()}\n"
                    detailed_log += f"   Status: {status.upper()}\n"
                    detailed_log += f"   Message: {result.get('message', 'N/A')}\n"

                    if result.get('details'):
                        detailed_log += "   Details:\n"
                        for key, value in result.get('details', {}).items():
                            detailed_log += f"     {key}: {value}\n"
                    detailed_log += "\n"

                detailed_log += "="*60 + "\n"
                detailed_log += f"OVERALL: {overall_status.upper()}\n"
                detailed_log += f"{overall_message}\n"
                detailed_log += "="*60

                return (
                    vpn_html,
                    zakupki_html,
                    remote_mongo_html,
                    local_mongo_html,
                    cpu_html,
                    memory_html,
                    disk_html,
                    env_html,
                    ssl_html,
                    overall_html,
                    detailed_log
                )

            except Exception as e:
                error_html = create_health_status_card(
                    "System Error",
                    "unhealthy",
                    f"Error running health checks: {str(e)}",
                    "",
                    "❌"
                )
                return (error_html,) * 10 + (f"Error: {str(e)}",)

        # Wire up button click handlers
        check_vpn_btn.click(
            fn=check_vpn,
            outputs=[vpn_status_html]
        )

        check_zakupki_btn.click(
            fn=check_zakupki,
            outputs=[zakupki_status_html]
        )

        check_remote_mongo_btn.click(
            fn=check_remote_mongo,
            outputs=[remote_mongo_html]
        )

        check_local_mongo_btn.click(
            fn=check_local_mongo,
            outputs=[local_mongo_html]
        )

        check_system_btn.click(
            fn=check_system_resources,
            outputs=[cpu_status_html, memory_status_html, disk_status_html]
        )

        check_env_btn.click(
            fn=check_environment,
            outputs=[env_status_html]
        )

        check_ssl_btn.click(
            fn=check_ssl,
            outputs=[ssl_status_html]
        )

        run_all_checks_btn.click(
            fn=run_all_health_checks,
            outputs=[
                vpn_status_html,
                zakupki_status_html,
                remote_mongo_html,
                local_mongo_html,
                cpu_status_html,
                memory_status_html,
                disk_status_html,
                env_status_html,
                ssl_status_html,
                overall_status_html,
                detailed_results
            ]
        )
