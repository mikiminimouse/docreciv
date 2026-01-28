"""
Тесты для проверки полного пайплайна препроцессинга.
"""

import sys
import os
from pathlib import Path
import unittest
from unittest.mock import patch, MagicMock
import tempfile
import shutil

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

class TestCompletePipeline(unittest.TestCase):
    """Тесты для проверки полного пайплайна препроцессинга."""

    def setUp(self):
        """Настройка перед каждым тестом."""
        # Создаем временную директорию для тестов
        self.test_dir = tempfile.mkdtemp()
        
        # Мокаем переменные окружения
        self.original_env = dict(os.environ)
        os.environ["INPUT_DIR"] = self.test_dir
        os.environ["OUTPUT_DIR"] = self.test_dir
        os.environ["MONGO_METADATA_SERVER"] = "localhost:27017"
        os.environ["MONGO_METADATA_USER"] = "test_user"
        os.environ["MONGO_METADATA_PASSWORD"] = "test_password"
        os.environ["MONGO_SERVER"] = "localhost:27017"
        os.environ["MONGO_USER"] = "test_user"
        os.environ["MONGO_PASSWORD"] = "test_password"

    def tearDown(self):
        """Очистка после каждого теста."""
        # Восстанавливаем переменные окружения
        os.environ.clear()
        os.environ.update(self.original_env)
        
        # Удаляем временную директорию
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_sync_service_initialization(self):
        """Тест инициализации сервиса синхронизации."""
        try:
            from docreciv.sync_db.enhanced_service import EnhancedSyncService
            service = EnhancedSyncService()
            self.assertIsNotNone(service)
            service.close()
        except Exception as e:
            self.fail(f"Failed to initialize EnhancedSyncService: {e}")

    def test_downloader_service_initialization(self):
        """Тест инициализации сервиса загрузки."""
        try:
            from docreciv.downloader.enhanced_service import EnhancedProtocolDownloader
            downloader = EnhancedProtocolDownloader()
            self.assertIsNotNone(downloader)
        except Exception as e:
            self.fail(f"Failed to initialize EnhancedProtocolDownloader: {e}")

    def test_analytics_service_initialization(self):
        """Тест инициализации сервиса аналитики."""
        try:
            from docreciv.sync_db.analytics import SyncAnalytics
            analytics = SyncAnalytics()
            self.assertIsNotNone(analytics)
            analytics.close()
        except Exception as e:
            self.fail(f"Failed to initialize SyncAnalytics: {e}")

    def test_health_checks_initialization(self):
        """Тест инициализации проверок здоровья."""
        try:
            from docreciv.sync_db.health_checks import run_comprehensive_health_check
            self.assertTrue(callable(run_comprehensive_health_check))
        except Exception as e:
            self.fail(f"Failed to import health checks: {e}")

    def test_vpn_utils_initialization(self):
        """Тест инициализации утилит VPN."""
        try:
            from docreciv.vpn_utils import (
                is_vpn_required,
                check_vpn_connectivity,
                check_zakupki_access
            )
            self.assertTrue(callable(is_vpn_required))
            self.assertTrue(callable(check_vpn_connectivity))
            self.assertTrue(callable(check_zakupki_access))
        except Exception as e:
            self.fail(f"Failed to import vpn utils: {e}")

    @patch('receiver.sync_db.connector.MongoClient')
    def test_mongo_connector_initialization(self, mock_mongo_client):
        """Тест инициализации коннектора MongoDB."""
        try:
            from docreciv.sync_db.connector import MongoConnector
            connector = MongoConnector()
            self.assertIsNotNone(connector)
        except Exception as e:
            self.fail(f"Failed to initialize MongoConnector: {e}")

    def test_config_loading(self):
        """Тест загрузки конфигурации."""
        try:
            from docreciv.core.config import get_config
            config = get_config()
            self.assertIsNotNone(config)
        except Exception as e:
            self.fail(f"Failed to load config: {e}")

    @patch('receiver.downloader.utils.requests.Session')
    def test_downloader_utils_initialization(self, mock_session):
        """Тест инициализации утилит загрузчика."""
        try:
            from docreciv.downloader.utils import (
                get_session,
                check_zakupki_health,
                get_metadata_client
            )
            self.assertTrue(callable(get_session))
            self.assertTrue(callable(check_zakupki_health))
            self.assertTrue(callable(get_metadata_client))
        except Exception as e:
            self.fail(f"Failed to import downloader utils: {e}")

    def test_data_structures_creation(self):
        """Тест создания структур данных."""
        try:
            from docreciv.sync_db.enhanced_service import EnhancedSyncResult
            from docreciv.downloader.enhanced_service import DownloadResult
            from docreciv.sync_db.analytics import SyncStatistics
            
            # Тест EnhancedSyncResult
            sync_result = EnhancedSyncResult(
                success=True,
                message="Test",
                scanned=10,
                inserted=5,
                errors_count=0
            )
            self.assertTrue(sync_result.success)
            
            # Тест DownloadResult
            download_result = DownloadResult(
                status="success",
                message="Test",
                processed=10,
                downloaded=5,
                failed=0
            )
            self.assertEqual(download_result.status, "success")
            
            # Тест SyncStatistics
            from datetime import datetime
            stats = SyncStatistics(
                session_id="test",
                start_time=datetime.now(),
                end_time=datetime.now(),
                duration_seconds=1.0,
                scanned_documents=10,
                inserted_documents=5,
                skipped_duplicates=2,
                processing_errors=1
            )
            self.assertEqual(stats.session_id, "test")
        except Exception as e:
            self.fail(f"Failed to create data structures: {e}")

if __name__ == "__main__":
    unittest.main()
