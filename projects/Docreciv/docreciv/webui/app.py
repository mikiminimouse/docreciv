"""
Main Gradio WebUI Application
Assembles UI from modular components
"""

import os
import sys
import gradio as gr
import logging
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load configuration
from docreciv.core.config import load_env_file, get_config
env_file = project_root / ".env"
if env_file.exists():
    load_env_file(env_file)
    logger.info(f"✅ Loaded configuration from {env_file}")
else:
    logger.warning(f"⚠️  .env file not found: {env_file}")

config = get_config()

# Import UI components
from docreciv.webui.tabs.dashboard import create_dashboard_tab
from docreciv.webui.tabs.sync_control import create_sync_control_tab
from docreciv.webui.tabs.sync_manager import create_sync_manager_tab
from docreciv.webui.tabs.download_control import create_download_control_tab
from docreciv.webui.tabs.health_check import create_health_check_tab
from docreciv.webui.tabs.configuration import create_configuration_tab

# Import services
from docreciv.webui.services.ui_service import get_ui_service

# Custom CSS path
CUSTOM_CSS_PATH = Path(__file__).parent / "static" / "custom_theme.css"

def initialize_services():
    """Initialize all required services."""
    try:
        ui_service = get_ui_service()
        logger.info("✅ UI services initialized")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to initialize UI services: {e}")
        return False

def load_custom_css():
    """Load custom CSS theme."""
    if CUSTOM_CSS_PATH.exists():
        try:
            with open(CUSTOM_CSS_PATH, 'r', encoding='utf-8') as f:
                css_content = f.read()
                logger.info(f"✅ Loaded custom CSS theme from {CUSTOM_CSS_PATH}")
                return css_content
        except Exception as e:
            logger.error(f"❌ Failed to load custom CSS: {e}")
            return ""
    else:
        logger.warning(f"⚠️  Custom CSS file not found: {CUSTOM_CSS_PATH}")
        return ""

def main():
    """Main application entry point."""
    # Initialize services
    if not initialize_services():
        logger.error("❌ Failed to initialize services")
        return

    # Load custom CSS
    custom_css = load_custom_css()

    # Create Gradio interface with custom theme
    with gr.Blocks(css=custom_css) as demo:
        gr.Markdown("# 🎛 RECEIVER CONTROL PANEL")
        gr.Markdown("*Technical Operations Interface v2.0*")

        # Create tabs
        create_dashboard_tab()
        create_sync_control_tab()
        create_sync_manager_tab()
        create_download_control_tab()
        create_health_check_tab()
        create_configuration_tab()

    # Launch the app
    demo.launch(
        server_name="0.0.0.0",
        server_port=7860,
        share=False,
        show_error=True,
        quiet=False
    )

if __name__ == "__main__":
    main()