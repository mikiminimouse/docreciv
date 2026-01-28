"""
Тесты для компонентов WebUI.
"""

import sys
import os
from pathlib import Path
import unittest
from unittest.mock import patch, MagicMock

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

class TestWebUIComponents(unittest.TestCase):
    """Тесты для компонентов WebUI."""

    def setUp(self):
        """Настройка перед каждым тестом."""
        pass

    def tearDown(self):
        """Очистка после каждого теста."""
        pass

    def test_charts_module_import(self):
        """Тест импорта модуля charts."""
        try:
            from docreciv.webui.charts import (
                create_sync_trend_chart, 
                create_performance_chart,
                create_error_distribution_chart,
                create_download_stats_chart
            )
            self.assertTrue(True, "Charts module imported successfully")
        except ImportError as e:
            self.fail(f"Failed to import charts module: {e}")

    def test_controls_module_import(self):
        """Тест импорта модуля controls."""
        try:
            from docreciv.webui.controls import (
                create_sync_controls,
                create_download_controls,
                create_configuration_controls,
                create_vpn_controls
            )
            self.assertTrue(True, "Controls module imported successfully")
        except ImportError as e:
            self.fail(f"Failed to import controls module: {e}")

    def test_placeholder_chart_creation(self):
        """Тест создания placeholder графика."""
        from docreciv.webui.charts import create_placeholder_chart
        
        # Тест создания placeholder
        result = create_placeholder_chart("Test message")
        self.assertIsInstance(result, str)
        self.assertTrue(result.startswith("data:image/png;base64,"))

    @patch('receiver.webui.charts.plt')
    def test_sync_trend_chart_creation(self, mock_plt):
        """Тест создания графика трендов синхронизации."""
        from docreciv.webui.charts import create_sync_trend_chart
        
        # Тест с пустыми данными
        result = create_sync_trend_chart({})
        self.assertIsInstance(result, str)
        self.assertTrue(result.startswith("data:image/png;base64,"))
        
        # Тест с данными
        test_data = {
            "daily_averages": {
                "2023-01-01": {
                    "avg_scanned": 100,
                    "avg_inserted": 90,
                    "avg_errors": 5
                }
            }
        }
        result = create_sync_trend_chart(test_data)
        self.assertIsInstance(result, str)
        self.assertTrue(result.startswith("data:image/png;base64,"))

    def test_control_elements_creation(self):
        """Тест создания элементов управления."""
        # Поскольку Gradio элементы требуют контекст, мы просто тестируем импорт функций
        try:
            from docreciv.webui.controls import (
                create_sync_controls,
                create_download_controls,
                create_configuration_controls,
                create_vpn_controls
            )
            # Проверяем, что функции существуют
            self.assertTrue(callable(create_sync_controls))
            self.assertTrue(callable(create_download_controls))
            self.assertTrue(callable(create_configuration_controls))
            self.assertTrue(callable(create_vpn_controls))
        except Exception as e:
            self.fail(f"Failed to import control functions: {e}")

if __name__ == "__main__":
    unittest.main()
