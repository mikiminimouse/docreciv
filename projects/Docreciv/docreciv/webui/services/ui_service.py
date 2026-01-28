"""
UI Service for WebUI
Centralized service for managing UI-related operations
"""

import logging
from typing import Optional, Dict, Any
from datetime import datetime

from docreciv.core.config import get_config
from docreciv.sync_db.enhanced_service import EnhancedSyncService
from docreciv.sync_db.manager import SyncManagerService
from docreciv.downloader.enhanced_service import EnhancedProtocolDownloader
from docreciv.vpn_utils import get_vpn_status, check_vpn_connectivity
from docreciv.sync_db.health_checks import run_comprehensive_health_check

logger = logging.getLogger(__name__)


class UIService:
    """Centralized service for UI operations."""
    
    def __init__(self):
        """Initialize the UI service."""
        self.config = get_config()
        self.sync_service: Optional[EnhancedSyncService] = None
        self.sync_manager_service: Optional[SyncManagerService] = None
        self.downloader_service: Optional[EnhancedProtocolDownloader] = None
        
        self._initialize_services()
    
    def _initialize_services(self):
        """Initialize all required services."""
        try:
            # Initialize sync service
            self.sync_service = EnhancedSyncService()
            logger.info("✅ EnhancedSyncService initialized")
        except Exception as e:
            logger.error(f"❌ Failed to initialize EnhancedSyncService: {e}")
            self.sync_service = None
        
        try:
            # Initialize sync manager service
            self.sync_manager_service = SyncManagerService()
            logger.info("✅ SyncManagerService initialized")
        except Exception as e:
            logger.error(f"❌ Failed to initialize SyncManagerService: {e}")
            self.sync_manager_service = None
        
        try:
            # Initialize downloader service
            self.downloader_service = EnhancedProtocolDownloader()
            logger.info("✅ EnhancedProtocolDownloader initialized")
        except Exception as e:
            logger.error(f"❌ Failed to initialize EnhancedProtocolDownloader: {e}")
            self.downloader_service = None
    
    def get_sync_service(self) -> Optional[EnhancedSyncService]:
        """Get the sync service."""
        return self.sync_service
    
    def get_sync_manager_service(self) -> Optional[SyncManagerService]:
        """Get the sync manager service."""
        return self.sync_manager_service
    
    def get_downloader_service(self) -> Optional[EnhancedProtocolDownloader]:
        """Get the downloader service."""
        return self.downloader_service
    
    def get_config(self) -> Any:
        """Get the current configuration."""
        return self.config
    
    def get_vpn_status(self) -> Dict[str, Any]:
        """Get VPN status information."""
        try:
            return get_vpn_status()
        except Exception as e:
            logger.error(f"Error getting VPN status: {e}")
            return {
                "status": "error",
                "interface": "unknown",
                "ip": "unknown",
                "details": str(e)
            }
    
    def get_system_status(self) -> Dict[str, Any]:
        """Get system status information."""
        try:
            import psutil
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            return {
                "cpu_percent": cpu_percent,
                "memory_percent": memory.percent,
                "disk_percent": (disk.used / disk.total) * 100,
                "memory_used": memory.used,
                "memory_total": memory.total,
                "disk_used": disk.used,
                "disk_total": disk.total
            }
        except Exception as e:
            logger.error(f"Error getting system status: {e}")
            return {
                "cpu_percent": 0,
                "memory_percent": 0,
                "disk_percent": 0
            }
    
    def get_last_sync_info(self) -> Optional[datetime]:
        """Get information about the last sync."""
        try:
            if self.sync_service:
                # This would need to be implemented in the sync service
                # For now, return None
                return None
            return None
        except Exception as e:
            logger.error(f"Error getting last sync info: {e}")
            return None
    
    def get_last_download_info(self) -> Optional[datetime]:
        """Get information about the last download."""
        try:
            if self.downloader_service:
                return self.downloader_service.get_last_download_timestamp()
            return None
        except Exception as e:
            logger.error(f"Error getting last download info: {e}")
            return None
    
    def run_health_check(self) -> Dict[str, Any]:
        """Run comprehensive health check."""
        try:
            return run_comprehensive_health_check()
        except Exception as e:
            logger.error(f"Error running health check: {e}")
            return {
                "error": {
                    "status": "error",
                    "details": str(e)
                }
            }


# Global instance
_ui_service_instance: Optional[UIService] = None


def get_ui_service() -> UIService:
    """Get the global UI service instance."""
    global _ui_service_instance
    if _ui_service_instance is None:
        _ui_service_instance = UIService()
    return _ui_service_instance