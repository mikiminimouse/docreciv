"""
Enhanced Configuration Tab for WebUI with Technical Precision design.
"""

import gradio as gr
import os
from pathlib import Path
from docreciv.webui.services.ui_service import get_ui_service


def create_configuration_tab():
    """Create an enhanced configuration tab with editable forms."""
    with gr.Tab("⚙️ Configuration"):
        gr.Markdown("## System Configuration")
        gr.Markdown("*Edit and manage system settings*")

        # Status banner
        config_status = gr.HTML(elem_classes=["status-card"])

        # ============ MongoDB Configuration ============
        gr.Markdown("### 🗄️ MongoDB Configuration")

        with gr.Row():
            with gr.Column():
                gr.Markdown("**Local MongoDB**")
                local_mongo_server = gr.Textbox(
                    label="MONGO_METADATA_SERVER",
                    placeholder="localhost:27018",
                    info="Local MongoDB server address"
                )
                local_mongo_user = gr.Textbox(
                    label="MONGO_METADATA_USER",
                    placeholder="admin"
                )
                local_mongo_password = gr.Textbox(
                    label="MONGO_METADATA_PASSWORD",
                    type="password",
                    placeholder="password"
                )
                local_mongo_db = gr.Textbox(
                    label="MONGO_METADATA_DB",
                    placeholder="docling_metadata"
                )

            with gr.Column():
                gr.Markdown("**Remote MongoDB (VPN Required)**")
                remote_mongo_server = gr.Textbox(
                    label="MONGO_SERVER",
                    placeholder="192.168.0.46:8635",
                    info="Remote MongoDB server (requires VPN)"
                )
                remote_mongo_user = gr.Textbox(
                    label="MONGO_USER",
                    placeholder="readProtocols223"
                )
                remote_mongo_password = gr.Textbox(
                    label="MONGO_PASSWORD",
                    type="password",
                    placeholder="***"
                )
                remote_mongo_ssl_cert = gr.Textbox(
                    label="MONGO_SSL_CERT",
                    placeholder="/path/to/cert.crt",
                    info="Path to SSL certificate"
                )
                remote_mongo_use_vpn = gr.Checkbox(
                    label="REMOTE_MONGO_USE_VPN",
                    value=True,
                    info="Require VPN for remote MongoDB connection"
                )

        # ============ VPN Configuration ============
        gr.Markdown("### 🔒 VPN Configuration")

        with gr.Row():
            with gr.Column():
                vpn_enabled = gr.Checkbox(
                    label="VPN_ENABLED",
                    value=True,
                    info="Enable VPN functionality"
                )
                vpn_required = gr.Checkbox(
                    label="VPN_REQUIRED",
                    value=True,
                    info="Require VPN for system operation"
                )
                vpn_enabled_remote_mongo = gr.Checkbox(
                    label="VPN_ENABLED_REMOTE_MONGO",
                    value=True,
                    info="Enable VPN for Remote MongoDB"
                )
                vpn_enabled_zakupki = gr.Checkbox(
                    label="VPN_ENABLED_ZAKUPKI",
                    value=True,
                    info="Enable VPN for Zakupki.gov.ru access"
                )

            with gr.Column():
                vpn_config_file = gr.Textbox(
                    label="VPN_CONFIG_FILE",
                    placeholder="/path/to/config.ovpn",
                    info="Path to OpenVPN configuration file"
                )
                zakupki_url = gr.Textbox(
                    label="ZAKUPKI_URL",
                    value="https://zakupki.gov.ru",
                    info="Zakupki.gov.ru URL"
                )

        # ============ Processing Configuration ============
        gr.Markdown("### ⚙️ Processing Configuration")

        with gr.Row():
            with gr.Column():
                input_dir = gr.Textbox(
                    label="INPUT_DIR",
                    placeholder="/path/to/input",
                    info="Input directory for downloaded files"
                )
                output_dir = gr.Textbox(
                    label="OUTPUT_DIR",
                    placeholder="/path/to/output",
                    info="Output directory for processed files"
                )

            with gr.Column():
                max_urls_per_protocol = gr.Number(
                    label="MAX_URLS_PER_PROTOCOL",
                    value=15,
                    precision=0,
                    info="Maximum URLs to process per protocol"
                )
                download_http_timeout = gr.Number(
                    label="DOWNLOAD_HTTP_TIMEOUT",
                    value=120,
                    precision=0,
                    info="HTTP timeout in seconds"
                )
                download_concurrency = gr.Number(
                    label="DOWNLOAD_CONCURRENCY",
                    value=20,
                    precision=0,
                    info="Number of concurrent downloads"
                )
                protocols_concurrency = gr.Number(
                    label="PROTOCOLS_CONCURRENCY",
                    value=20,
                    precision=0,
                    info="Number of concurrent protocol processing"
                )

        # ============ Scheduler Configuration ============
        gr.Markdown("### ⏰ Scheduler Configuration")

        with gr.Row():
            scheduler_enabled = gr.Checkbox(
                label="SCHEDULER_ENABLED",
                value=False,
                info="Enable automatic scheduling"
            )
            schedule_cron = gr.Textbox(
                label="SCHEDULE_CRON",
                value="*/15 * * * *",
                info="Cron expression for download schedule (every 15 min)"
            )
            sync_schedule_cron = gr.Textbox(
                label="SYNC_SCHEDULE_CRON",
                value="0 2 * * *",
                info="Cron expression for sync schedule (2 AM daily)"
            )

        # ============ Action Buttons ============
        gr.Markdown("### 💾 Actions")

        with gr.Row():
            load_config_btn = gr.Button(
                "🔄 Load Current Configuration",
                variant="secondary",
                size="lg"
            )
            save_config_btn = gr.Button(
                "💾 Save Configuration",
                variant="primary",
                size="lg",
                elem_classes=["action-button"]
            )
            validate_config_btn = gr.Button(
                "✓ Validate Configuration",
                size="lg"
            )

        with gr.Row():
            backup_config_btn = gr.Button(
                "📦 Backup Configuration",
                size="sm"
            )
            restore_config_btn = gr.Button(
                "📤 Restore from Backup",
                size="sm"
            )

        # ============ FUNCTIONS ============

        def load_current_config():
            """Load current configuration from .env file."""
            try:
                env_file = Path("/root/winners_preprocessor/final_preprocessing/receiver/.env")

                if not env_file.exists():
                    return create_status_banner(
                        "error",
                        "Configuration file not found",
                        f".env file does not exist at {env_file}"
                    ) + ({},)  # Return empty dict for all fields

                # Read .env file
                env_vars = {}
                with open(env_file, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#'):
                            if '=' in line:
                                key, value = line.split('=', 1)
                                env_vars[key.strip()] = value.strip()

                # Helper to get boolean
                def get_bool(key, default=False):
                    value = env_vars.get(key, str(default)).lower()
                    return value in ('true', '1', 'yes')

                # Return all values
                return (
                    create_status_banner("ok", "Configuration Loaded", "Successfully loaded from .env file"),
                    # Local MongoDB
                    env_vars.get('MONGO_METADATA_SERVER', 'localhost:27018'),
                    env_vars.get('MONGO_METADATA_USER', 'admin'),
                    env_vars.get('MONGO_METADATA_PASSWORD', ''),
                    env_vars.get('MONGO_METADATA_DB', 'docling_metadata'),
                    # Remote MongoDB
                    env_vars.get('MONGO_SERVER', '192.168.0.46:8635'),
                    env_vars.get('MONGO_USER', 'readProtocols223'),
                    env_vars.get('MONGO_PASSWORD', ''),
                    env_vars.get('MONGO_SSL_CERT', ''),
                    get_bool('REMOTE_MONGO_USE_VPN', True),
                    # VPN
                    get_bool('VPN_ENABLED', True),
                    get_bool('VPN_REQUIRED', True),
                    get_bool('VPN_ENABLED_REMOTE_MONGO', True),
                    get_bool('VPN_ENABLED_ZAKUPKI', True),
                    env_vars.get('VPN_CONFIG_FILE', ''),
                    env_vars.get('ZAKUPKI_URL', 'https://zakupki.gov.ru'),
                    # Processing
                    env_vars.get('INPUT_DIR', '/root/winners_preprocessor/final_preprocessing/Data'),
                    env_vars.get('OUTPUT_DIR', '/root/winners_preprocessor/final_preprocessing/Data'),
                    int(env_vars.get('MAX_URLS_PER_PROTOCOL', '15')),
                    int(env_vars.get('DOWNLOAD_HTTP_TIMEOUT', '120')),
                    int(env_vars.get('DOWNLOAD_CONCURRENCY', '20')),
                    int(env_vars.get('PROTOCOLS_CONCURRENCY', '20')),
                    # Scheduler
                    get_bool('SCHEDULER_ENABLED', False),
                    env_vars.get('SCHEDULE_CRON', '*/15 * * * *'),
                    env_vars.get('SYNC_SCHEDULE_CRON', '0 2 * * *'),
                )

            except Exception as e:
                return (
                    create_status_banner("error", "Load Failed", str(e)),
                ) + ("",) * 24  # Return empty strings for all fields

        def save_configuration(
            # Local MongoDB
            loc_srv, loc_usr, loc_pwd, loc_db,
            # Remote MongoDB
            rem_srv, rem_usr, rem_pwd, rem_ssl, rem_vpn,
            # VPN
            vpn_en, vpn_req, vpn_mongo, vpn_zak, vpn_cfg, zak_url,
            # Processing
            in_dir, out_dir, max_urls, http_to, dl_conc, prot_conc,
            # Scheduler
            sch_en, sch_cron, sync_cron
        ):
            """Save configuration to .env file."""
            try:
                env_file = Path("/root/winners_preprocessor/final_preprocessing/receiver/.env")

                # Build .env content
                content = "# MongoDB Configuration\n"
                content += f"MONGO_METADATA_SERVER={loc_srv}\n"
                content += f"LOCAL_MONGO_SERVER={loc_srv}\n"
                content += f"MONGO_METADATA_USER={loc_usr}\n"
                content += f"MONGO_METADATA_PASSWORD={loc_pwd}\n"
                content += f"MONGO_METADATA_DB={loc_db}\n"
                content += "\n"

                content += "# Remote MongoDB (for sync) - Requires VPN\n"
                content += f"MONGO_SERVER={rem_srv}\n"
                content += f"MONGO_USER={rem_usr}\n"
                content += f"MONGO_PASSWORD={rem_pwd}\n"
                content += f"MONGO_SSL_CERT={rem_ssl}\n"
                content += f"REMOTE_MONGO_USE_VPN={'true' if rem_vpn else 'false'}\n"
                content += "\n"

                content += "# Processing Configuration\n"
                content += f"INPUT_DIR={in_dir}\n"
                content += f"OUTPUT_DIR={out_dir}\n"
                content += f"MAX_URLS_PER_PROTOCOL={int(max_urls)}\n"
                content += f"DOWNLOAD_HTTP_TIMEOUT={int(http_to)}\n"
                content += f"DOWNLOAD_CONCURRENCY={int(dl_conc)}\n"
                content += f"PROTOCOLS_CONCURRENCY={int(prot_conc)}\n"
                content += "\n"

                content += "# Scheduler Configuration\n"
                content += f"SCHEDULER_ENABLED={'true' if sch_en else 'false'}\n"
                content += f"SCHEDULE_CRON=\"{sch_cron}\"\n"
                content += f"SYNC_SCHEDULE_CRON=\"{sync_cron}\"\n"
                content += "\n"

                content += "# VPN Configuration\n"
                content += f"VPN_ENABLED={'true' if vpn_en else 'false'}\n"
                content += f"VPN_ENABLED_REMOTE_MONGO={'true' if vpn_mongo else 'false'}\n"
                content += f"VPN_ENABLED_ZAKUPKI={'true' if vpn_zak else 'false'}\n"
                content += f"VPN_REQUIRED={'true' if vpn_req else 'false'}\n"
                content += f"VPN_CONFIG_FILE={vpn_cfg}\n"
                content += f"ZAKUPKI_URL={zak_url}\n"

                # Write to file
                with open(env_file, 'w') as f:
                    f.write(content)

                return create_status_banner(
                    "ok",
                    "✅ Configuration Saved Successfully!",
                    f"Saved to {env_file}. Restart services for changes to take effect."
                )

            except Exception as e:
                return create_status_banner(
                    "error",
                    "❌ Save Failed",
                    f"Error: {str(e)}"
                )

        def validate_configuration(
            # Local MongoDB
            loc_srv, loc_usr, loc_pwd, loc_db,
            # Remote MongoDB
            rem_srv, rem_usr, rem_pwd, rem_ssl, rem_vpn,
            # VPN
            vpn_en, vpn_req, vpn_mongo, vpn_zak, vpn_cfg, zak_url,
            # Processing
            in_dir, out_dir, max_urls, http_to, dl_conc, prot_conc,
            # Scheduler
            sch_en, sch_cron, sync_cron
        ):
            """Validate configuration values."""
            errors = []

            # Validate MongoDB
            if not loc_srv:
                errors.append("❌ Local MongoDB server is required")
            if not loc_usr or not loc_pwd:
                errors.append("❌ Local MongoDB credentials are required")

            if not rem_srv:
                errors.append("⚠️ Remote MongoDB server not set")
            if rem_srv and (not rem_usr or not rem_pwd):
                errors.append("❌ Remote MongoDB credentials required when server is set")
            if rem_ssl and not Path(rem_ssl).exists():
                errors.append(f"❌ SSL certificate not found: {rem_ssl}")

            # Validate VPN
            if vpn_en and vpn_cfg and not Path(vpn_cfg).exists():
                errors.append(f"❌ VPN config file not found: {vpn_cfg}")

            # Validate Processing
            if not in_dir:
                errors.append("❌ Input directory is required")
            if not out_dir:
                errors.append("❌ Output directory is required")
            if max_urls < 1:
                errors.append("❌ MAX_URLS_PER_PROTOCOL must be >= 1")
            if http_to < 1:
                errors.append("❌ HTTP timeout must be >= 1")
            if dl_conc < 1:
                errors.append("❌ Download concurrency must be >= 1")
            if prot_conc < 1:
                errors.append("❌ Protocol concurrency must be >= 1")

            # Validate Scheduler
            if sch_en:
                if not sch_cron or not sync_cron:
                    errors.append("❌ Cron expressions required when scheduler is enabled")

            if errors:
                return create_status_banner(
                    "error",
                    f"Validation Failed ({len(errors)} issues)",
                    "\n".join(errors)
                )
            else:
                return create_status_banner(
                    "ok",
                    "✅ Validation Passed!",
                    "All configuration values are valid. You can safely save."
                )

        def backup_configuration():
            """Backup current .env file."""
            try:
                import shutil
                from datetime import datetime

                env_file = Path("/root/winners_preprocessor/final_preprocessing/receiver/.env")
                if not env_file.exists():
                    return create_status_banner("error", "Backup Failed", ".env file not found")

                backup_dir = env_file.parent / "backups"
                backup_dir.mkdir(exist_ok=True)

                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_file = backup_dir / f".env.backup_{timestamp}"

                shutil.copy2(env_file, backup_file)

                return create_status_banner(
                    "ok",
                    "✅ Backup Created",
                    f"Backed up to: {backup_file}"
                )
            except Exception as e:
                return create_status_banner("error", "Backup Failed", str(e))

        def restore_configuration():
            """Restore from latest backup."""
            try:
                env_file = Path("/root/winners_preprocessor/final_preprocessing/receiver/.env")
                backup_dir = env_file.parent / "backups"

                if not backup_dir.exists():
                    return create_status_banner("error", "Restore Failed", "No backups directory found")

                # Find latest backup
                backups = sorted(backup_dir.glob(".env.backup_*"), reverse=True)
                if not backups:
                    return create_status_banner("error", "Restore Failed", "No backups found")

                latest_backup = backups[0]

                # Create safety backup of current
                import shutil
                from datetime import datetime
                safety_backup = env_file.parent / f".env.before_restore_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                if env_file.exists():
                    shutil.copy2(env_file, safety_backup)

                # Restore
                shutil.copy2(latest_backup, env_file)

                return create_status_banner(
                    "ok",
                    "✅ Configuration Restored",
                    f"Restored from: {latest_backup.name}. Current config backed up to: {safety_backup.name}"
                )
            except Exception as e:
                return create_status_banner("error", "Restore Failed", str(e))

        def create_status_banner(status, title, message):
            """Create a status banner."""
            status_classes = {
                'ok': 'status-ok',
                'warning': 'status-warning',
                'error': 'status-error'
            }
            status_emojis = {
                'ok': '🟢',
                'warning': '🟡',
                'error': '🔴'
            }

            status_class = status_classes.get(status, 'status-ok')
            status_emoji = status_emojis.get(status, '🟢')

            # Format message with line breaks
            formatted_message = message.replace('\n', '<br>')

            return f"""
            <div class="status-card {status_class}" style="margin-bottom: 1rem;">
                <div class="metric-label">{status_emoji} {title}</div>
                <div class="metric-details" style="white-space: pre-wrap; font-size: 0.85rem; margin-top: 8px;">{formatted_message}</div>
            </div>
            """

        # Wire up buttons
        all_inputs = [
            local_mongo_server, local_mongo_user, local_mongo_password, local_mongo_db,
            remote_mongo_server, remote_mongo_user, remote_mongo_password, remote_mongo_ssl_cert, remote_mongo_use_vpn,
            vpn_enabled, vpn_required, vpn_enabled_remote_mongo, vpn_enabled_zakupki, vpn_config_file, zakupki_url,
            input_dir, output_dir, max_urls_per_protocol, download_http_timeout, download_concurrency, protocols_concurrency,
            scheduler_enabled, schedule_cron, sync_schedule_cron
        ]

        load_config_btn.click(
            fn=load_current_config,
            outputs=[config_status] + all_inputs
        )

        save_config_btn.click(
            fn=save_configuration,
            inputs=all_inputs,
            outputs=[config_status]
        )

        validate_config_btn.click(
            fn=validate_configuration,
            inputs=all_inputs,
            outputs=[config_status]
        )

        backup_config_btn.click(
            fn=backup_configuration,
            outputs=[config_status]
        )

        restore_config_btn.click(
            fn=restore_configuration,
            outputs=[config_status]
        )
