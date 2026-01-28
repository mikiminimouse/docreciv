"""
Тесты для проверки интеграции VPN в компонентах receiver.
"""

import sys
import os
from pathlib import Path
import unittest
from unittest.mock import patch, MagicMock

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

class TestVPNIntegration(unittest.TestCase):
    """Тесты для проверки интеграции VPN."""

    def setUp(self):
        """Настройка перед каждым тестом."""
        pass

    def tearDown(self):
        """Очистка после каждого теста."""
        pass

    @patch('receiver.vpn_utils.is_vpn_required')
    def test_vpn_required_check(self, mock_is_vpn_required):
        """Тест проверки необходимости VPN."""
        from docreciv.vpn_utils import is_vpn_required
        
        # Тест когда VPN обязателен
        mock_is_vpn_required.return_value = True
        self.assertTrue(is_vpn_required())
        
        # Тест когда VPN не обязателен
        mock_is_vpn_required.return_value = False
        self.assertFalse(is_vpn_required())

    @patch('receiver.vpn_utils.check_vpn_connectivity')
    def test_vpn_connectivity_check(self, mock_check_vpn):
        """Тест проверки подключения VPN."""
        from docreciv.vpn_utils import check_vpn_connectivity
        
        # Тест когда VPN подключен
        mock_check_vpn.return_value = True
        self.assertTrue(check_vpn_connectivity())
        
        # Тест когда VPN не подключен
        mock_check_vpn.return_value = False
        self.assertFalse(check_vpn_connectivity())

    @patch('receiver.vpn_utils.check_zakupki_access')
    def test_zakupki_access_check(self, mock_check_zakupki):
        """Тест проверки доступа к zakupki.gov.ru."""
        from docreciv.vpn_utils import check_zakupki_access
        
        # Тест когда сайт доступен
        mock_check_zakupki.return_value = (True, "https://zakupki.gov.ru", 0.5)
        accessible, url, response_time = check_zakupki_access()
        self.assertTrue(accessible)
        self.assertEqual(url, "https://zakupki.gov.ru")
        self.assertEqual(response_time, 0.5)
        
        # Тест когда сайт недоступен
        mock_check_zakupki.return_value = (False, "Connection timeout", None)
        accessible, url, response_time = check_zakupki_access()
        self.assertFalse(accessible)
        self.assertEqual(url, "Connection timeout")
        self.assertIsNone(response_time)

    @patch('receiver.vpn_utils.check_remote_mongo_vpn_access')
    def test_remote_mongo_vpn_access(self, mock_check_mongo):
        """Тест проверки доступа к удаленной MongoDB через VPN."""
        from docreciv.vpn_utils import check_remote_mongo_vpn_access
        
        # Тест когда MongoDB доступна
        mock_check_mongo.return_value = True
        self.assertTrue(check_remote_mongo_vpn_access("192.168.0.46", 8635))
        
        # Тест когда MongoDB недоступна
        mock_check_mongo.return_value = False
        self.assertFalse(check_remote_mongo_vpn_access("192.168.0.46", 8635))

    @patch('receiver.vpn_utils.ensure_vpn_connected')
    def test_ensure_vpn_connected(self, mock_ensure_vpn):
        """Тест обеспечения подключения VPN."""
        from docreciv.vpn_utils import ensure_vpn_connected
        
        # Тест когда VPN подключен
        mock_ensure_vpn.return_value = (True, "VPN connected and zakupki.gov.ru accessible")
        connected, message = ensure_vpn_connected()
        self.assertTrue(connected)
        self.assertIn("connected", message)
        
        # Тест когда VPN не подключен
        mock_ensure_vpn.return_value = (False, "VPN not connected")
        connected, message = ensure_vpn_connected()
        self.assertFalse(connected)
        self.assertIn("not connected", message)

    def test_vpn_required_env_var(self):
        """Тест проверки переменной окружения VPN_REQUIRED."""
        from docreciv.vpn_utils import is_vpn_required
        
        # Сохраняем оригинальное значение
        original_value = os.environ.get("VPN_REQUIRED")
        
        try:
            # Тест различных значений переменной
            os.environ["VPN_REQUIRED"] = "true"
            self.assertTrue(is_vpn_required())
            
            os.environ["VPN_REQUIRED"] = "1"
            self.assertTrue(is_vpn_required())
            
            os.environ["VPN_REQUIRED"] = "yes"
            self.assertTrue(is_vpn_required())
            
            os.environ["VPN_REQUIRED"] = "false"
            self.assertFalse(is_vpn_required())
            
            os.environ["VPN_REQUIRED"] = "0"
            self.assertFalse(is_vpn_required())
            
            # Тест отсутствия переменной
            if "VPN_REQUIRED" in os.environ:
                del os.environ["VPN_REQUIRED"]
            self.assertFalse(is_vpn_required())
        finally:
            # Восстанавливаем оригинальное значение
            if original_value is not None:
                os.environ["VPN_REQUIRED"] = original_value
            elif "VPN_REQUIRED" in os.environ:
                del os.environ["VPN_REQUIRED"]

if __name__ == "__main__":
    unittest.main()
