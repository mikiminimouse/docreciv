"""
Утилиты для проверки и работы с VPN подключением.

Содержит функции для проверки доступности zakupki.gov.ru и удаленной MongoDB
через VPN, а также для управления VPN подключением.
"""

import os
import subprocess
import socket
import time
import logging
from typing import Tuple, Optional, List, Dict, Any
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path

logger = logging.getLogger(__name__)


def is_vpn_required() -> bool:
    """
    Проверяет, требуется ли VPN для работы системы.
    
    Returns:
        bool: True если VPN обязателен, False если нет
    """
    return os.environ.get("VPN_REQUIRED", "false").lower() in ("true", "1", "yes")


def is_openvpn_running() -> bool:
    """
    Проверяет, запущен ли процесс OpenVPN.
    
    Returns:
        bool: True если OpenVPN запущен, False если нет
    """
    try:
        # Проверяем через ps
        result = subprocess.run(
            ["pgrep", "-f", "openvpn"],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0 and result.stdout.strip():
            return True
        
        # Альтернативная проверка через systemctl (если OpenVPN запущен как сервис)
        result = subprocess.run(
            ["systemctl", "is-active", "--quiet", "openvpn"],
            capture_output=True,
            timeout=5
        )
        if result.returncode == 0:
            return True
        
        return False
    except Exception as e:
        logger.debug(f"Error checking OpenVPN process: {e}")
        return False


def is_vpn_interface_up(interface: str = "tun0") -> bool:
    """
    Проверяет, поднят ли VPN интерфейс.
    
    Args:
        interface: Имя VPN интерфейса (по умолчанию tun0)
        
    Returns:
        bool: True если интерфейс поднят, False если нет
    """
    try:
        # Проверяем через ip link
        result = subprocess.run(
            ["ip", "link", "show", interface],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            # Проверяем, что интерфейс в состоянии UP
            return "state UP" in result.stdout or "UP" in result.stdout
        
        # Альтернативная проверка через ifconfig (если ip недоступен)
        result = subprocess.run(
            ["ifconfig", interface],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            return True
        
        return False
    except Exception as e:
        logger.debug(f"Error checking VPN interface {interface}: {e}")
        return False


def check_vpn_routes(hosts: List[str]) -> Dict[str, bool]:
    """
    Проверяет наличие маршрутов к указанным хостам через VPN.
    
    Args:
        hosts: Список хостов для проверки
        
    Returns:
        Dict[str, bool]: Словарь с результатами проверки для каждого хоста
    """
    results = {}
    
    for host in hosts:
        try:
            # Разрешаем имя хоста в IP
            try:
                ip = socket.gethostbyname(host)
            except socket.gaierror:
                results[host] = False
                continue
            
            # Проверяем маршрут к IP через ip route get
            result = subprocess.run(
                ["ip", "route", "get", ip],
                capture_output=True,
                text=True,
                timeout=5
            )
            
            if result.returncode == 0:
                # Проверяем, что маршрут идет через tun интерфейс
                output = result.stdout
                # Маршрут должен содержать dev tun или dev tap
                has_tun_route = "dev tun" in output or "dev tap" in output
                results[host] = has_tun_route
            else:
                results[host] = False
                
        except Exception as e:
            logger.debug(f"Error checking route for {host}: {e}")
            results[host] = False
    
    return results


def check_vpn_connectivity(host: str = "8.8.8.8", port: int = 53, timeout: int = 3) -> bool:
    """
    Проверяет наличие активного VPN подключения.
    ВНИМАНИЕ: Эта функция проверяет только интернет-соединение, не VPN туннель.
    Используйте get_vpn_status() для полной проверки VPN.
    
    Args:
        host: Хост для проверки подключения (по умолчанию Google DNS)
        port: Порт для проверки подключения
        timeout: Таймаут в секундах
        
    Returns:
        bool: True если соединение активно, False если нет
    """
    try:
        socket.setdefaulttimeout(timeout)
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((host, port))
        return True
    except Exception:
        return False


def get_vpn_status() -> Dict[str, Any]:
    """
    Получает полный статус VPN подключения.
    
    Returns:
        Dict с информацией о статусе VPN:
        - openvpn_running: bool - запущен ли процесс OpenVPN
        - interface_up: bool - поднят ли VPN интерфейс
        - routes_ok: Dict[str, bool] - статус маршрутов к целевым хостам
        - zakupki_accessible: bool - доступен ли zakupki.gov.ru
        - mongo_accessible: bool - доступен ли MongoDB сервер
        - overall_status: str - общий статус ("healthy", "degraded", "unhealthy")
        - details: Dict - детальная информация
    """
    status = {
        "openvpn_running": False,
        "interface_up": False,
        "routes_ok": {},
        "zakupki_accessible": False,
        "mongo_accessible": False,
        "overall_status": "unhealthy",
        "details": {}
    }
    
    if not is_vpn_required():
        status["overall_status"] = "healthy"
        status["details"]["message"] = "VPN not required"
        return status
    
    # Проверяем OpenVPN процесс
    status["openvpn_running"] = is_openvpn_running()
    status["details"]["openvpn_running"] = status["openvpn_running"]
    
    # Проверяем VPN интерфейс
    status["interface_up"] = is_vpn_interface_up("tun0")
    if not status["interface_up"]:
        status["interface_up"] = is_vpn_interface_up("tap0")  # Проверяем альтернативный интерфейс
    status["details"]["interface_up"] = status["interface_up"]
    
    # Проверяем маршруты к целевым хостам
    target_hosts = ["zakupki.gov.ru", "www.zakupki.gov.ru"]
    
    # Добавляем MongoDB сервер из конфигурации
    mongo_server = os.environ.get("MONGO_SERVER", "")
    if mongo_server:
        mongo_host = mongo_server.split(":")[0]
        target_hosts.append(mongo_host)
    
    status["routes_ok"] = check_vpn_routes(target_hosts)
    status["details"]["routes"] = status["routes_ok"]
    
    # Проверяем доступность zakupki.gov.ru
    accessible, message, _ = check_zakupki_access()
    status["zakupki_accessible"] = accessible
    status["details"]["zakupki_message"] = message
    
    # Проверяем доступность MongoDB
    if mongo_server:
        mongo_host = mongo_server.split(":")[0]
        mongo_port = int(mongo_server.split(":")[1]) if ":" in mongo_server else 27017
        status["mongo_accessible"] = check_remote_mongo_vpn_access(mongo_host, mongo_port)
        status["details"]["mongo_accessible"] = status["mongo_accessible"]
    
    # Определяем общий статус
    # Если VPN не требуется, статус всегда healthy
    if not is_vpn_required():
        status["overall_status"] = "healthy"
    elif status["openvpn_running"] and status["interface_up"] and status["zakupki_accessible"]:
        if all(status["routes_ok"].values()):
            status["overall_status"] = "healthy"
        else:
            status["overall_status"] = "degraded"
    elif status["openvpn_running"] or status["interface_up"]:
        status["overall_status"] = "degraded"
    else:
        status["overall_status"] = "unhealthy"
    
    return status


def check_zakupki_access() -> Tuple[bool, Optional[str], Optional[float]]:
    """
    Проверяет доступность zakupki.gov.ru через VPN.
    
    Returns:
        Tuple[bool, Optional[str], Optional[float]]: 
            (доступен, URL, время отклика в секундах)
    """
    # Проверяем флаг VPN_ENABLED для zakupki
    vpn_enabled = os.environ.get("VPN_ENABLED_ZAKUPKI", os.environ.get("VPN_ENABLED", "false")).lower() in ("true", "1", "yes")
    vpn_required = is_vpn_required()
    
    # Если VPN не требуется, возвращаем успех
    if not vpn_required:
        return True, "VPN not required", 0.0
    
    # Если VPN требуется, но не включен - предупреждаем
    if vpn_required and not vpn_enabled:
        logger.warning("VPN_REQUIRED=true but VPN_ENABLED_ZAKUPKI=false. Проверка может не пройти.")
    
    # Проверяем маршрут к zakupki.gov.ru перед запросом
    routes = check_vpn_routes(["zakupki.gov.ru", "www.zakupki.gov.ru"])
    if not any(routes.values()):
        logger.warning("Route to zakupki.gov.ru not found through VPN interface. Request may fail.")
    
    zakupki_url = os.environ.get("ZAKUPKI_URL", "https://zakupki.gov.ru")
    
    try:
        # Создаем сессию с повторными попытками
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Устанавливаем заголовки браузера
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        })
        
        start_time = time.time()
        response = session.get(zakupki_url, timeout=15, allow_redirects=True)
        end_time = time.time()
        
        response_time = end_time - start_time
        
        if response.status_code in [200, 301, 302]:
            return True, response.url, response_time
        else:
            return False, f"HTTP {response.status_code}", response_time
            
    except Exception as e:
        logger.error(f"Error checking zakupki access: {e}")
        return False, str(e), None


def check_remote_mongo_vpn_access(mongo_host: str, mongo_port: int = 27017) -> bool:
    """
    Проверяет доступность удаленного MongoDB сервера через VPN.
    
    Args:
        mongo_host: Хост MongoDB сервера
        mongo_port: Порт MongoDB сервера
        
    Returns:
        bool: True если сервер доступен, False если нет
    """
    if not is_vpn_required():
        return True
    
    # Проверяем маршрут к MongoDB серверу перед подключением
    routes = check_vpn_routes([mongo_host])
    if not routes.get(mongo_host, False):
        logger.warning(f"Route to MongoDB server {mongo_host} not found through VPN interface. Connection may fail.")
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((mongo_host, mongo_port))
        sock.close()
        return result == 0
    except Exception as e:
        logger.error(f"Error checking MongoDB VPN access: {e}")
        return False


def ensure_vpn_connected() -> Tuple[bool, str]:
    """
    Проверяет и при необходимости устанавливает VPN соединение.
    
    Returns:
        Tuple[bool, str]: (подключен, сообщение)
    """
    if not is_vpn_required():
        return True, "VPN not required"
    
    # Получаем полный статус VPN
    vpn_status = get_vpn_status()
    
    # Формируем детальное сообщение
    issues = []
    
    if not vpn_status["openvpn_running"]:
        issues.append("OpenVPN process not running")
    
    if not vpn_status["interface_up"]:
        issues.append("VPN interface (tun0/tap0) not up")
    
    # Проверяем маршруты
    failed_routes = [host for host, ok in vpn_status["routes_ok"].items() if not ok]
    if failed_routes:
        issues.append(f"Routes missing for: {', '.join(failed_routes)}")
    
    if not vpn_status["zakupki_accessible"]:
        issues.append("zakupki.gov.ru not accessible")
    
    if vpn_status["overall_status"] == "healthy":
        return True, "VPN fully operational"
    elif vpn_status["overall_status"] == "degraded":
        return False, f"VPN partially working: {'; '.join(issues)}"
    else:
        return False, f"VPN not connected: {'; '.join(issues) if issues else 'Unknown issue'}"


def setup_vpn_route() -> Tuple[bool, str]:
    """
    Настраивает маршрут через VPN для доступа к zakupki.gov.ru и MongoDB.
    
    Returns:
        Tuple[bool, str]: (успешно, сообщение)
    """
    try:
        # Проверяем наличие скрипта настройки маршрутов
        route_script = os.environ.get(
            "VPN_ROUTE_SCRIPT",
            str(Path(__file__).parent / "route-up-zakupki.sh")
        )
        if os.path.exists(route_script):
            result = subprocess.run(
                ["sudo", "bash", route_script],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                time.sleep(2)  # Даем время на установку маршрутов
                return True, "VPN routes configured successfully"
            else:
                return False, f"Failed to configure VPN routes: {result.stderr}"
        else:
            return False, f"Route setup script not found: {route_script}"
            
    except Exception as e:
        return False, f"Error setting up VPN routes: {e}"
